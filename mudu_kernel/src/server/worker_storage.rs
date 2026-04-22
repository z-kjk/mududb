use std::collections::{BTreeMap, Bound};
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::sync::{Arc, Mutex, OnceLock, Weak};

use futures::executor::block_on;
use mudu::common::id::OID;
use mudu::common::result::RS;
use mudu::error::ec::EC;
use mudu::m_error;
use scc::HashMap as SccHashMap;

use crate::contract::data_row::DataRow;
use crate::contract::meta_mgr::MetaMgr;
use crate::contract::partition_rule_binding::TablePartitionBinding;
use crate::contract::schema_table::SchemaTable;
use crate::contract::table_desc::TableDesc;
use crate::contract::timestamp::Timestamp;
use crate::contract::version_tuple::VersionTuple;
use crate::index::index_key::key_tuple::KeyTuple;
use crate::server::partition_router::DEFAULT_UNPARTITIONED_TABLE_PARTITION_ID;
use crate::server::worker_snapshot::{KvItem, WorkerSnapshot};
use crate::server::worker_tx_manager::WorkerTxManager;
use crate::storage::relation::relation::Relation;
use crate::wal::xl_batch::XLBatch;
use crate::wal::xl_data_op::{XLDelete, XLInsert};
use crate::wal::xl_entry::TxOp;
use crate::x_engine::tx_mgr::{PhysicalRelationId, TxMgr};

type WorkerStorageRegistry = std::collections::HashMap<String, Vec<Weak<WorkerStorage>>>;

fn storage_registry() -> &'static Mutex<WorkerStorageRegistry> {
    static REGISTRY: OnceLock<Mutex<WorkerStorageRegistry>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(std::collections::HashMap::new()))
}

#[derive(Clone, Debug)]
pub(crate) struct PreparedWorkerCommit {
    xid: u64,
    relation_rows: BTreeMap<PhysicalRelationId, BTreeMap<Vec<u8>, Option<Vec<u8>>>>,
    kv_rows: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    batch: XLBatch,
}

pub struct WorkerStorage {
    mgr: Arc<dyn MetaMgr>,
    default_partition_id: OID,
    relation_path: String,
    relation_store: SccHashMap<PhysicalRelationId, Relation>,
    kv_store: SccHashMap<Vec<u8>, DataRow>,
}

impl WorkerStorage {
    fn relation_id(&self, table_id: OID, partition_id: OID) -> PhysicalRelationId {
        PhysicalRelationId {
            table_id,
            partition_id,
        }
    }

    pub fn new(mgr: Arc<dyn MetaMgr>, partition_id: OID, relation_path: String) -> Self {
        Self {
            mgr,
            default_partition_id: partition_id,
            relation_path,
            relation_store: SccHashMap::new(),
            kv_store: SccHashMap::new(),
        }
    }

    fn physical_partition_id(&self, partition_id: Option<OID>) -> OID {
        partition_id.unwrap_or(self.default_partition_id)
    }

    pub fn register_global(self: &Arc<Self>) {
        let mut guard = storage_registry().lock().unwrap();
        guard
            .entry(self.relation_path.clone())
            .or_default()
            .push(Arc::downgrade(self));
    }

    pub fn bootstrap_existing_tables_sync(&self) -> RS<()> {
        for schema in block_on(self.mgr.list_schemas())? {
            self.bootstrap_table_local(&schema)?;
        }
        Ok(())
    }

    pub async fn create_table_async(&self, schema: &SchemaTable) -> RS<()> {
        self.mgr.create_table(schema).await?;
        self.broadcast_create_table(schema)
    }

    pub async fn drop_table_async(&self, oid: OID) -> RS<()> {
        self.mgr.drop_table(oid).await?;
        self.broadcast_drop_table(oid)
    }

    #[allow(dead_code)]
    pub async fn contains_key(&self, oid: OID, key: &KeyTuple, txm: &dyn TxMgr) -> RS<bool> {
        self.contains_key_on_partition(oid, None, key, txm).await
    }

    pub async fn contains_key_on_partition(
        &self,
        oid: OID,
        partition_id: Option<OID>,
        key: &KeyTuple,
        txm: &dyn TxMgr,
    ) -> RS<bool> {
        let relation_id = self.relation_id(oid, self.physical_partition_id(partition_id));
        if let Some(staged) = txm.get_relation(relation_id, key.as_slice()) {
            return Ok(staged.is_some());
        }
        self.read_visible_relation_exists(oid, partition_id, key, &txm.snapshot())
            .await
    }

    #[allow(dead_code)]
    pub async fn get(&self, oid: OID, key: &[u8], txm: &dyn TxMgr) -> RS<Option<Vec<u8>>> {
        self.get_on_partition(oid, None, key, txm).await
    }

    pub async fn get_on_partition(
        &self,
        oid: OID,
        partition_id: Option<OID>,
        key: &[u8],
        txm: &dyn TxMgr,
    ) -> RS<Option<Vec<u8>>> {
        let relation_id = self.relation_id(oid, self.physical_partition_id(partition_id));
        if let Some(staged) = txm.get_relation(relation_id, key) {
            return Ok(staged);
        }
        let key = KeyTuple::from(key.to_vec());
        self.read_visible_relation_value(oid, partition_id, &key, &txm.snapshot())
            .await
    }

    #[allow(dead_code)]
    pub async fn put(&self, oid: OID, key: Vec<u8>, value: Vec<u8>, txm: &dyn TxMgr) -> RS<()> {
        self.put_on_partition(oid, None, key, value, txm).await
    }

    pub async fn put_on_partition(
        &self,
        oid: OID,
        partition_id: Option<OID>,
        key: Vec<u8>,
        value: Vec<u8>,
        txm: &dyn TxMgr,
    ) -> RS<()> {
        let key_tuple = KeyTuple::from(key.clone());
        let relation_id = self.relation_id(oid, self.physical_partition_id(partition_id));

        self.ensure_no_relation_write_conflict(oid, partition_id, &key_tuple, &txm.snapshot())
            .await?;
        txm.put_relation(relation_id, key, value);
        Ok(())
    }

    #[allow(dead_code)]
    pub async fn remove(&self, oid: OID, key: &[u8], txm: &dyn TxMgr) -> RS<Option<Vec<u8>>> {
        self.remove_on_partition(oid, None, key, txm).await
    }

    pub async fn remove_on_partition(
        &self,
        oid: OID,
        partition_id: Option<OID>,
        key: &[u8],
        txm: &dyn TxMgr,
    ) -> RS<Option<Vec<u8>>> {
        let key_tuple = KeyTuple::from(key.to_vec());
        let relation_id = self.relation_id(oid, self.physical_partition_id(partition_id));
        self.ensure_no_relation_write_conflict(oid, partition_id, &key_tuple, &txm.snapshot())
            .await?;
        let current = match txm.get_relation(relation_id, key) {
            Some(staged) => staged,
            None => {
                self.read_visible_relation_value(oid, partition_id, &key_tuple, &txm.snapshot())
                    .await?
            }
        };
        if current.is_some() {
            txm.delete_relation(relation_id, key.to_vec());
        }
        Ok(current)
    }

    pub async fn range(
        &self,
        oid: OID,
        bounds: (Bound<&[u8]>, Bound<&[u8]>),
        txm: &dyn TxMgr,
    ) -> RS<Vec<(Vec<u8>, Vec<u8>)>> {
        self.range_on_partition(oid, None, bounds, txm).await
    }

    pub async fn range_on_partition(
        &self,
        oid: OID,
        partition_id: Option<OID>,
        bounds: (Bound<&[u8]>, Bound<&[u8]>),
        txm: &dyn TxMgr,
    ) -> RS<Vec<(Vec<u8>, Vec<u8>)>> {
        let base_items = self
            .range_visible_relation(oid, partition_id, bounds, &txm.snapshot())
            .await?;
        let (start_key, end_key) = bounds_to_scan(&bounds);
        let relation_id = self.relation_id(oid, self.physical_partition_id(partition_id));
        let staged_items = txm.staged_relation_items_in_range(relation_id, &start_key, &end_key);

        let mut merged = BTreeMap::new();
        for (key, value) in base_items {
            merged.insert(key, Some(value));
        }
        for (key, value) in staged_items {
            merged.insert(key, value);
        }

        Ok(merged
            .into_iter()
            .filter_map(|(key, value)| value.map(|value| (key, value)))
            .collect())
    }

    pub async fn kv_get(
        &self,
        key: &[u8],
        snapshot: Option<&WorkerSnapshot>,
    ) -> RS<Option<Vec<u8>>> {
        let row = self.kv_store.get_sync(key).map(|entry| entry.get().clone());
        let version = match snapshot {
            Some(snapshot) => match row {
                Some(row) => {
                    let snapshot = snapshot.to_snapshot();
                    row.read(&snapshot).await?
                }
                None => None,
            },
            None => match row {
                Some(row) => row.read_latest().await?,
                None => None,
            },
        };
        Ok(version
            .filter(|version| !version.is_deleted())
            .map(|version| version.tuple().clone()))
    }

    pub async fn kv_range(
        &self,
        start_key: &[u8],
        end_key: &[u8],
        snapshot: Option<&WorkerSnapshot>,
    ) -> RS<Vec<KvItem>> {
        let mut rows = Vec::new();
        self.kv_store.iter_sync(|key, row| {
            let in_range = if end_key.is_empty() {
                key.as_slice() >= start_key
            } else {
                key.as_slice() >= start_key && key.as_slice() < end_key
            };
            if in_range {
                rows.push((key.clone(), row.clone()));
            }
            true
        });

        let mut items = Vec::new();
        for (key, row) in rows {
            let visible = match snapshot {
                Some(snapshot) => {
                    let snapshot = snapshot.to_snapshot();
                    row.read(&snapshot).await?
                }
                None => row.read_latest().await?,
            };
            if let Some(visible) = visible.filter(|version| !version.is_deleted()) {
                items.push(KvItem {
                    key,
                    value: visible.tuple().clone(),
                });
            }
        }
        items.sort_by(|left, right| left.key.cmp(&right.key));
        Ok(items)
    }

    #[allow(dead_code)]
    pub(crate) async fn commit_tx(&self, txm: &mut WorkerTxManager) -> RS<()> {
        let prepared = self.prepare_commit_async(txm).await?;
        self.apply_relation_rows_async(&prepared).await?;
        self.apply_kv_rows_async(&prepared).await?;
        Ok(())
    }

    pub(crate) async fn prepare_commit_async(&self, txm: &dyn TxMgr) -> RS<PreparedWorkerCommit> {
        self.prepare_commit_parts_async(
            &txm.snapshot(),
            txm.xid(),
            txm.staged_relation_ops(),
            txm.staged_put_items().into_iter().collect(),
            txm.xl_batch(),
        )
        .await
    }

    pub(crate) fn prepare_commit(&self, txm: &dyn TxMgr) -> RS<PreparedWorkerCommit> {
        self.prepare_commit_parts(
            &txm.snapshot(),
            txm.xid(),
            txm.staged_relation_ops(),
            txm.staged_put_items().into_iter().collect(),
            txm.xl_batch(),
        )
    }

    pub(crate) fn prepare_worker_kv_commit(
        &self,
        snapshot: &WorkerSnapshot,
        xid: u64,
        items: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
        batch: XLBatch,
    ) -> RS<PreparedWorkerCommit> {
        self.prepare_commit_parts(snapshot, xid, BTreeMap::new(), items, batch)
    }

    pub(crate) fn prepare_worker_kv_autocommit(
        &self,
        xid: u64,
        key: Vec<u8>,
        value: Option<Vec<u8>>,
        batch: XLBatch,
    ) -> PreparedWorkerCommit {
        PreparedWorkerCommit {
            xid,
            relation_rows: BTreeMap::new(),
            kv_rows: BTreeMap::from([(key, value)]),
            batch,
        }
    }

    pub(crate) fn apply_prepared_commit(&self, prepared: PreparedWorkerCommit) -> RS<()> {
        self.apply_relation_rows(&prepared)?;
        self.apply_kv_rows(&prepared)?;
        Ok(())
    }

    pub(crate) async fn apply_prepared_commit_async(
        &self,
        prepared: PreparedWorkerCommit,
    ) -> RS<()> {
        self.apply_relation_rows_async(&prepared).await?;
        self.apply_kv_rows_async(&prepared).await?;
        Ok(())
    }

    pub(crate) fn replay_batch(&self, batch: XLBatch) -> RS<()> {
        for entry in batch.entries {
            for op in entry.ops {
                match op {
                    TxOp::Insert(insert) if insert.table_id == 0 && insert.partition_id == 0 => {
                        self.worker_put_local(insert.key, insert.value, entry.xid)?;
                    }
                    TxOp::Delete(delete) if delete.table_id == 0 && delete.partition_id == 0 => {
                        self.worker_delete_local(delete.key, entry.xid)?;
                    }
                    TxOp::Insert(insert) => {
                        self.apply_relation_replay_insert(insert, entry.xid)?;
                    }
                    TxOp::Delete(delete) => {
                        self.apply_relation_replay_delete(delete, entry.xid)?;
                    }
                    _ => {}
                }
            }
        }
        Ok(())
    }

    pub(crate) fn worker_put_local(&self, key: Vec<u8>, value: Vec<u8>, xid: u64) -> RS<()> {
        write_version_to_kv_store(&self.kv_store, key, Some(value), xid)
    }

    pub(crate) fn worker_delete_local(&self, key: Vec<u8>, xid: u64) -> RS<()> {
        write_version_to_kv_store(&self.kv_store, key, None, xid)
    }

    fn prepare_commit_parts(
        &self,
        snapshot: &WorkerSnapshot,
        xid: u64,
        relation_rows: BTreeMap<PhysicalRelationId, BTreeMap<Vec<u8>, Option<Vec<u8>>>>,
        kv_rows: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
        batch: XLBatch,
    ) -> RS<PreparedWorkerCommit> {
        self.ensure_no_relation_conflicts(snapshot, xid, &relation_rows)?;
        self.ensure_no_kv_conflicts(snapshot, xid, &kv_rows)?;

        Ok(PreparedWorkerCommit {
            xid,
            relation_rows,
            kv_rows,
            batch,
        })
    }

    async fn prepare_commit_parts_async(
        &self,
        snapshot: &WorkerSnapshot,
        xid: u64,
        relation_rows: BTreeMap<PhysicalRelationId, BTreeMap<Vec<u8>, Option<Vec<u8>>>>,
        kv_rows: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
        batch: XLBatch,
    ) -> RS<PreparedWorkerCommit> {
        self.ensure_no_relation_conflicts_async(snapshot, xid, &relation_rows)
            .await?;
        self.ensure_no_kv_conflicts(snapshot, xid, &kv_rows)?;

        Ok(PreparedWorkerCommit {
            xid,
            relation_rows,
            kv_rows,
            batch,
        })
    }

    fn ensure_no_relation_conflicts(
        &self,
        snapshot: &WorkerSnapshot,
        xid: u64,
        relation_rows: &BTreeMap<PhysicalRelationId, BTreeMap<Vec<u8>, Option<Vec<u8>>>>,
    ) -> RS<()> {
        for (relation_id, rows) in relation_rows {
            let relation = self.relation_store.get_sync(relation_id).ok_or_else(|| {
                m_error!(
                    EC::NoSuchElement,
                    format!(
                        "no such table {} partition {}",
                        relation_id.table_id, relation_id.partition_id
                    )
                )
            })?;
            for key in rows.keys() {
                let key_tuple = KeyTuple::from(key.clone());
                if relation
                    .get()
                    .has_write_conflict_sync(&key_tuple, snapshot)?
                {
                    return Err(m_error!(
                        EC::TxErr,
                        format!(
                            "write-write conflict on table {} partition {} key {:?} for transaction {}",
                            relation_id.table_id, relation_id.partition_id, key, xid
                        )
                    ));
                }
            }
        }
        Ok(())
    }

    async fn ensure_no_relation_conflicts_async(
        &self,
        snapshot: &WorkerSnapshot,
        xid: u64,
        relation_rows: &BTreeMap<PhysicalRelationId, BTreeMap<Vec<u8>, Option<Vec<u8>>>>,
    ) -> RS<()> {
        for (relation_id, rows) in relation_rows {
            let relation = self.relation_store.get_sync(relation_id).ok_or_else(|| {
                m_error!(
                    EC::NoSuchElement,
                    format!(
                        "no such table {} partition {}",
                        relation_id.table_id, relation_id.partition_id
                    )
                )
            })?;
            for key in rows.keys() {
                let key_tuple = KeyTuple::from(key.clone());
                if relation
                    .get()
                    .has_write_conflict(&key_tuple, snapshot)
                    .await?
                {
                    return Err(m_error!(
                        EC::TxErr,
                        format!(
                            "write-write conflict on table {} partition {} key {:?} for transaction {}",
                            relation_id.table_id, relation_id.partition_id, key, xid
                        )
                    ));
                }
            }
        }
        Ok(())
    }

    fn ensure_no_kv_conflicts(
        &self,
        snapshot: &WorkerSnapshot,
        xid: u64,
        kv_rows: &BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    ) -> RS<()> {
        for key in kv_rows.keys() {
            let conflict = self
                .kv_store
                .get_sync(key)
                .and_then(|entry| latest_version(entry.get()))
                .map(|latest| !snapshot.is_visible(latest.timestamp().c_min()))
                .unwrap_or(false);
            if conflict {
                return Err(m_error!(
                    EC::TxErr,
                    format!(
                        "write-write conflict on key {:?} for transaction {}",
                        String::from_utf8_lossy(key),
                        xid
                    )
                ));
            }
        }
        Ok(())
    }

    fn apply_relation_rows(&self, prepared: &PreparedWorkerCommit) -> RS<()> {
        for (relation_id, rows) in &prepared.relation_rows {
            let relation = self.relation_store.get_sync(relation_id).ok_or_else(|| {
                m_error!(
                    EC::NoSuchElement,
                    format!(
                        "no such table {} partition {}",
                        relation_id.table_id, relation_id.partition_id
                    )
                )
            })?;
            for (key, value) in rows {
                relation
                    .get()
                    .write_row_sync(key.clone(), value.clone(), prepared.xid)?;
            }
        }
        Ok(())
    }

    async fn apply_relation_rows_async(&self, prepared: &PreparedWorkerCommit) -> RS<()> {
        for (relation_id, rows) in &prepared.relation_rows {
            let relation = self.relation_store.get_sync(relation_id).ok_or_else(|| {
                m_error!(
                    EC::NoSuchElement,
                    format!(
                        "no such table {} partition {}",
                        relation_id.table_id, relation_id.partition_id
                    )
                )
            })?;
            for (key, value) in rows {
                relation
                    .get()
                    .write_row(key.clone(), value.clone(), prepared.xid)
                    .await?;
            }
        }
        Ok(())
    }

    fn apply_kv_rows(&self, prepared: &PreparedWorkerCommit) -> RS<()> {
        for (key, value) in &prepared.kv_rows {
            write_version_to_kv_store(&self.kv_store, key.clone(), value.clone(), prepared.xid)?;
        }
        Ok(())
    }

    async fn apply_kv_rows_async(&self, prepared: &PreparedWorkerCommit) -> RS<()> {
        for (key, value) in &prepared.kv_rows {
            write_version_to_kv_store_async(
                &self.kv_store,
                key.clone(),
                value.clone(),
                prepared.xid,
            )
            .await?;
        }
        Ok(())
    }

    fn apply_relation_replay_insert(&self, insert: XLInsert, xid: u64) -> RS<()> {
        let relation = self
            .relation_store
            .get_sync(&self.relation_id(insert.table_id, insert.partition_id))
            .ok_or_else(|| {
                m_error!(
                    EC::NoSuchElement,
                    format!(
                        "no such table {} partition {}",
                        insert.table_id, insert.partition_id
                    )
                )
            })?;
        relation
            .get()
            .write_value_sync(insert.key, insert.value, xid)
    }

    fn apply_relation_replay_delete(&self, delete: XLDelete, xid: u64) -> RS<()> {
        let relation = self
            .relation_store
            .get_sync(&self.relation_id(delete.table_id, delete.partition_id))
            .ok_or_else(|| {
                m_error!(
                    EC::NoSuchElement,
                    format!(
                        "no such table {} partition {}",
                        delete.table_id, delete.partition_id
                    )
                )
            })?;
        relation.get().write_delete_sync(delete.key, xid)
    }

    #[allow(dead_code)]
    async fn read_visible_relation_exists(
        &self,
        oid: OID,
        partition_id: Option<OID>,
        key: &KeyTuple,
        snapshot: &WorkerSnapshot,
    ) -> RS<bool> {
        let relation = self
            .relation_store
            .get_sync(&self.relation_id(oid, self.physical_partition_id(partition_id)))
            .ok_or_else(|| m_error!(EC::NoSuchElement, format!("no such table {}", oid)))?;
        relation.get().has_visible_version(key, snapshot).await
    }

    async fn read_visible_relation_value(
        &self,
        oid: OID,
        partition_id: Option<OID>,
        key: &KeyTuple,
        snapshot: &WorkerSnapshot,
    ) -> RS<Option<Vec<u8>>> {
        self.ensure_relation_index(oid, partition_id).await?;
        let relation = self
            .relation_store
            .get_sync(&self.relation_id(oid, self.physical_partition_id(partition_id)))
            .ok_or_else(|| m_error!(EC::NoSuchElement, format!("no such table {}", oid)))?;
        relation.get().visible_value(key, snapshot).await
    }

    async fn range_visible_relation(
        &self,
        oid: OID,
        partition_id: Option<OID>,
        bounds: (Bound<&[u8]>, Bound<&[u8]>),
        snapshot: &WorkerSnapshot,
    ) -> RS<Vec<(Vec<u8>, Vec<u8>)>> {
        self.ensure_relation_index(oid, partition_id).await?;
        let relation = self
            .relation_store
            .get_sync(&self.relation_id(oid, self.physical_partition_id(partition_id)))
            .ok_or_else(|| m_error!(EC::NoSuchElement, format!("no such table {}", oid)))?;
        relation.get().visible_range(bounds, snapshot).await
    }

    async fn ensure_no_relation_write_conflict(
        &self,
        oid: OID,
        partition_id: Option<OID>,
        key: &KeyTuple,
        snapshot: &WorkerSnapshot,
    ) -> RS<()> {
        self.ensure_relation_index(oid, partition_id).await?;
        let relation = self
            .relation_store
            .get_sync(&self.relation_id(oid, self.physical_partition_id(partition_id)))
            .ok_or_else(|| m_error!(EC::NoSuchElement, format!("no such table {}", oid)))?;
        if relation.get().has_write_conflict(key, snapshot).await? {
            return Err(m_error!(
                EC::TxErr,
                format!(
                    "write-write conflict on table {} key {:?} for transaction {}",
                    oid,
                    key.as_slice(),
                    snapshot.xid()
                )
            ));
        }
        Ok(())
    }

    fn create_unpartitioned_relation_index(&self, oid: OID, table_desc: &TableDesc) -> RS<()> {
        self.create_relation_index_for_partition(
            oid,
            DEFAULT_UNPARTITIONED_TABLE_PARTITION_ID,
            table_desc,
        )
    }

    fn create_relation_index_for_partition(
        &self,
        oid: OID,
        partition_id: OID,
        table_desc: &TableDesc,
    ) -> RS<()> {
        let _ = self.relation_store.insert_sync(
            self.relation_id(oid, partition_id),
            Relation::new(oid, partition_id, self.relation_path.clone(), table_desc)?,
        );
        Ok(())
    }

    async fn ensure_relation_index(&self, oid: OID, partition_id: Option<OID>) -> RS<()> {
        let partition_id = self.physical_partition_id(partition_id);
        if self
            .relation_store
            .contains_sync(&self.relation_id(oid, partition_id))
        {
            return Ok(());
        }
        let table_desc = self.mgr.get_table_by_id(oid).await?;
        self.create_relation_index_for_partition(oid, partition_id, table_desc.as_ref())
    }

    fn apply_create_table_local(&self, schema: &SchemaTable) -> RS<()> {
        let table_desc =
            crate::contract::table_info::TableInfo::new(schema.clone())?.table_desc()?;
        self.create_unpartitioned_relation_index(schema.id(), table_desc.as_ref())
    }

    fn bootstrap_table_local(&self, schema: &SchemaTable) -> RS<()> {
        let table_desc =
            crate::contract::table_info::TableInfo::new(schema.clone())?.table_desc()?;
        let binding = block_on(self.mgr.get_table_partition_binding(schema.id()))?;
        match binding {
            Some(binding) => {
                self.create_partitioned_relations(schema.id(), &binding, table_desc.as_ref())
            }
            None => self.create_unpartitioned_relation_index(schema.id(), table_desc.as_ref()),
        }
    }

    fn create_partitioned_relations(
        &self,
        oid: OID,
        binding: &TablePartitionBinding,
        table_desc: &TableDesc,
    ) -> RS<()> {
        let rule = block_on(self.mgr.get_partition_rule_by_id(binding.rule_id))?;
        for partition in &rule.partitions {
            self.create_relation_index_for_partition(oid, partition.partition_id, table_desc)?;
        }
        Ok(())
    }

    fn apply_drop_table_local(&self, oid: OID) {
        let _ = self
            .relation_store
            .remove_sync(&self.relation_id(oid, self.default_partition_id));
    }

    fn broadcast_create_table(&self, schema: &SchemaTable) -> RS<()> {
        let peers = self.peer_instances();
        if peers.is_empty() {
            return self.apply_create_table_local(schema);
        }
        for storage in peers {
            storage.apply_create_table_local(schema)?;
        }
        Ok(())
    }

    fn broadcast_drop_table(&self, oid: OID) -> RS<()> {
        let peers = self.peer_instances();
        if peers.is_empty() {
            self.apply_drop_table_local(oid);
            return Ok(());
        }
        for storage in peers {
            storage.apply_drop_table_local(oid);
        }
        Ok(())
    }

    fn peer_instances(&self) -> Vec<Arc<WorkerStorage>> {
        let mut guard = storage_registry().lock().unwrap();
        let peers = guard.entry(self.relation_path.clone()).or_default();
        let mut live = Vec::with_capacity(peers.len());
        peers.retain(|weak| match weak.upgrade() {
            Some(storage) => {
                live.push(storage);
                true
            }
            None => false,
        });
        live
    }
}

impl PreparedWorkerCommit {
    pub(crate) fn batch(&self) -> &XLBatch {
        &self.batch
    }
}

fn new_value_version(xid: u64, value: Vec<u8>) -> VersionTuple {
    VersionTuple::new(Timestamp::new(xid, u64::MAX), value)
}

fn write_version_to_kv_store(
    kv_store: &SccHashMap<Vec<u8>, DataRow>,
    key: Vec<u8>,
    value: Option<Vec<u8>>,
    xid: u64,
) -> RS<()> {
    let row = kv_store
        .get_sync(&key)
        .map(|entry| entry.get().clone())
        .unwrap_or_else(|| DataRow::new(0));
    let version = match value {
        Some(value) => new_value_version(xid, value),
        None => VersionTuple::new_delete(Timestamp::new(xid, u64::MAX)),
    };
    row.write_sync(version, None)?;
    let _ = kv_store.insert_sync(key, row);
    Ok(())
}

async fn write_version_to_kv_store_async(
    kv_store: &SccHashMap<Vec<u8>, DataRow>,
    key: Vec<u8>,
    value: Option<Vec<u8>>,
    xid: u64,
) -> RS<()> {
    let row = kv_store
        .get_sync(&key)
        .map(|entry| entry.get().clone())
        .unwrap_or_else(|| DataRow::new(0));
    let version = match value {
        Some(value) => new_value_version(xid, value),
        None => VersionTuple::new_delete(Timestamp::new(xid, u64::MAX)),
    };
    row.write(version, None).await?;
    let _ = kv_store.insert_sync(key, row);
    Ok(())
}

fn latest_version(row: &DataRow) -> Option<VersionTuple> {
    row.read_latest_sync().ok().flatten()
}

fn bounds_to_scan(bounds: &(Bound<&[u8]>, Bound<&[u8]>)) -> (Vec<u8>, Vec<u8>) {
    let start = match bounds.0 {
        Included(key) | Excluded(key) => key.to_vec(),
        Unbounded => Vec::new(),
    };
    let end = match bounds.1 {
        Included(key) | Excluded(key) => key.to_vec(),
        Unbounded => Vec::new(),
    };
    (start, end)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::ops::Bound;
    use std::sync::Mutex;

    use mudu::common::id::OID;
    use mudu_type::dat_type_id::DatTypeID;
    use mudu_type::dt_info::DTInfo;

    use crate::contract::meta_mgr::MetaMgr;
    use crate::contract::schema_column::SchemaColumn;
    use crate::contract::table_info::TableInfo;

    use super::*;

    struct TestMetaMgr {
        schemas: Mutex<HashMap<OID, SchemaTable>>,
        tables: Mutex<HashMap<OID, Arc<TableDesc>>>,
    }

    impl TestMetaMgr {
        fn new() -> Self {
            Self {
                schemas: Mutex::new(HashMap::new()),
                tables: Mutex::new(HashMap::new()),
            }
        }
    }

    #[async_trait::async_trait]
    impl MetaMgr for TestMetaMgr {
        async fn get_table_by_id(&self, oid: OID) -> RS<Arc<TableDesc>> {
            self.tables
                .lock()
                .unwrap()
                .get(&oid)
                .cloned()
                .ok_or_else(|| m_error!(EC::NoSuchElement, format!("no such table {}", oid)))
        }

        async fn get_table_by_name(&self, name: &String) -> RS<Option<Arc<TableDesc>>> {
            Ok(self
                .tables
                .lock()
                .unwrap()
                .values()
                .find(|table| table.name() == name)
                .cloned())
        }

        async fn create_table(&self, schema: &SchemaTable) -> RS<()> {
            let table = TableInfo::new(schema.clone())?.table_desc()?;
            self.schemas
                .lock()
                .unwrap()
                .insert(schema.id(), schema.clone());
            self.tables.lock().unwrap().insert(schema.id(), table);
            Ok(())
        }

        async fn drop_table(&self, table_id: OID) -> RS<()> {
            self.schemas.lock().unwrap().remove(&table_id);
            self.tables.lock().unwrap().remove(&table_id);
            Ok(())
        }

        async fn list_schemas(&self) -> RS<Vec<SchemaTable>> {
            Ok(self.schemas.lock().unwrap().values().cloned().collect())
        }
    }

    fn test_schema() -> SchemaTable {
        SchemaTable::new(
            "t".to_string(),
            vec![
                SchemaColumn::new(
                    "id".to_string(),
                    DatTypeID::I32,
                    DTInfo::from_text(DatTypeID::I32, String::new()),
                ),
                SchemaColumn::new(
                    "v".to_string(),
                    DatTypeID::I32,
                    DTInfo::from_text(DatTypeID::I32, String::new()),
                ),
            ],
            vec![0],
            vec![1],
        )
    }

    fn test_storage() -> (WorkerStorage, OID) {
        let mgr = Arc::new(TestMetaMgr::new());
        let storage = WorkerStorage::new(
            mgr,
            0,
            std::env::temp_dir()
                .join(format!(
                    "worker_storage_test_{}",
                    mudu::common::id::gen_oid()
                ))
                .to_string_lossy()
                .to_string(),
        );
        let schema = test_schema();
        let oid = schema.id();
        futures::executor::block_on(storage.create_table_async(&schema)).unwrap();
        (storage, oid)
    }

    fn test_shared_storage() -> (
        Arc<TestMetaMgr>,
        Arc<WorkerStorage>,
        Arc<WorkerStorage>,
        OID,
    ) {
        let mgr = Arc::new(TestMetaMgr::new());
        let root = std::env::temp_dir()
            .join(format!(
                "worker_storage_shared_test_{}",
                mudu::common::id::gen_oid()
            ))
            .to_string_lossy()
            .to_string();
        let storage1 = Arc::new(WorkerStorage::new(mgr.clone(), 1, root.clone()));
        storage1.register_global();
        storage1.bootstrap_existing_tables_sync().unwrap();
        let storage2 = Arc::new(WorkerStorage::new(mgr.clone(), 2, root));
        storage2.register_global();
        storage2.bootstrap_existing_tables_sync().unwrap();

        let schema = test_schema();
        let oid = schema.id();
        futures::executor::block_on(storage1.create_table_async(&schema)).unwrap();
        (mgr, storage1, storage2, oid)
    }

    fn begin_tx(xid: u64, running: Vec<u64>) -> WorkerTxManager {
        WorkerTxManager::new(WorkerSnapshot::new(xid, running))
    }

    fn i32_bytes(v: i32) -> Vec<u8> {
        v.to_be_bytes().to_vec()
    }

    #[test]
    fn worker_storage_broadcasts_create_and_drop_to_peer_workers() {
        let (mgr, _storage1, storage2, oid) = test_shared_storage();
        let mut tx = begin_tx(1, vec![]);
        block_on(storage2.put(oid, i32_bytes(7), i32_bytes(70), &mut tx)).unwrap();
        block_on(storage2.commit_tx(&mut tx)).unwrap();
        assert!(futures::executor::block_on(mgr.get_table_by_id(oid)).is_ok());

        futures::executor::block_on(storage2.drop_table_async(oid)).unwrap();
        assert!(futures::executor::block_on(mgr.get_table_by_id(oid)).is_err());

        let mut tx = begin_tx(2, vec![]);
        let err = block_on(storage2.put(oid, i32_bytes(8), i32_bytes(80), &mut tx)).unwrap_err();
        assert!(format!("{err}").contains("no such table"));
    }

    #[test]
    fn worker_storage_reads_own_writes() {
        let (storage, oid) = test_storage();
        let mut tx = begin_tx(10, vec![]);

        block_on(storage.put(oid, i32_bytes(1), i32_bytes(11), &mut tx)).unwrap();

        assert_eq!(
            block_on(storage.get(oid, &i32_bytes(1), &mut tx)).unwrap(),
            Some(i32_bytes(11))
        );
        assert!(
            block_on(storage.contains_key(oid, &KeyTuple::from(i32_bytes(1)), &mut tx)).unwrap()
        );
    }

    #[test]
    fn worker_storage_snapshot_hides_later_commit() {
        let (storage, oid) = test_storage();
        let mut tx1 = begin_tx(1, vec![]);
        block_on(storage.put(oid, i32_bytes(1), i32_bytes(10), &mut tx1)).unwrap();
        block_on(storage.commit_tx(&mut tx1)).unwrap();

        let mut old_tx = begin_tx(2, vec![]);
        let mut new_tx = begin_tx(3, vec![2]);
        block_on(storage.put(oid, i32_bytes(1), i32_bytes(20), &mut new_tx)).unwrap();
        block_on(storage.commit_tx(&mut new_tx)).unwrap();

        assert_eq!(
            block_on(storage.get(oid, &i32_bytes(1), &mut old_tx)).unwrap(),
            Some(i32_bytes(10))
        );
    }

    #[test]
    fn worker_storage_range_is_stable_with_snapshot() {
        let (storage, oid) = test_storage();
        let mut seed = begin_tx(1, vec![]);
        block_on(storage.put(oid, i32_bytes(1), i32_bytes(10), &mut seed)).unwrap();
        block_on(storage.commit_tx(&mut seed)).unwrap();

        let mut old_tx = begin_tx(2, vec![]);
        let mut new_tx = begin_tx(3, vec![2]);
        block_on(storage.put(oid, i32_bytes(2), i32_bytes(20), &mut new_tx)).unwrap();
        block_on(storage.commit_tx(&mut new_tx)).unwrap();

        let rows = block_on(storage.range(
            oid,
            (
                Bound::Included(i32_bytes(1).as_slice()),
                Bound::Included(i32_bytes(9).as_slice()),
            ),
            &mut old_tx,
        ))
        .unwrap();
        assert_eq!(rows, vec![(i32_bytes(1), i32_bytes(10))]);
    }

    #[test]
    fn worker_storage_first_committer_wins() {
        let (storage, oid) = test_storage();
        let mut seed = begin_tx(1, vec![]);
        block_on(storage.put(oid, i32_bytes(1), i32_bytes(10), &mut seed)).unwrap();
        block_on(storage.commit_tx(&mut seed)).unwrap();

        let mut tx1 = begin_tx(2, vec![]);
        let mut tx2 = begin_tx(3, vec![2]);
        block_on(storage.put(oid, i32_bytes(1), i32_bytes(11), &mut tx1)).unwrap();
        block_on(storage.put(oid, i32_bytes(1), i32_bytes(12), &mut tx2)).unwrap();
        block_on(storage.commit_tx(&mut tx1)).unwrap();
        let err = block_on(storage.commit_tx(&mut tx2)).unwrap_err();

        assert!(err.to_string().contains("write-write conflict"));
    }

    #[test]
    fn worker_storage_delete_respects_snapshot() {
        let (storage, oid) = test_storage();
        let mut seed = begin_tx(1, vec![]);
        block_on(storage.put(oid, i32_bytes(1), i32_bytes(10), &mut seed)).unwrap();
        block_on(storage.commit_tx(&mut seed)).unwrap();

        let mut old_tx = begin_tx(2, vec![]);
        let mut delete_tx = begin_tx(3, vec![2]);
        assert_eq!(
            block_on(storage.remove(oid, &i32_bytes(1), &mut delete_tx)).unwrap(),
            Some(i32_bytes(10))
        );
        block_on(storage.commit_tx(&mut delete_tx)).unwrap();

        assert_eq!(
            block_on(storage.get(oid, &i32_bytes(1), &mut old_tx)).unwrap(),
            Some(i32_bytes(10))
        );
        let mut fresh_tx = begin_tx(4, vec![]);
        assert_eq!(
            block_on(storage.get(oid, &i32_bytes(1), &mut fresh_tx)).unwrap(),
            None
        );
    }

    #[test]
    fn worker_storage_kv_snapshot_hides_later_commit() {
        let (storage, _oid) = test_storage();
        storage
            .worker_put_local(b"a".to_vec(), b"0".to_vec(), 1)
            .unwrap();

        let snapshot = WorkerSnapshot::new(2, vec![]);
        let prepared = storage.prepare_worker_kv_autocommit(
            3,
            b"a".to_vec(),
            Some(b"1".to_vec()),
            XLBatch::new(vec![]),
        );
        storage.apply_prepared_commit(prepared).unwrap();

        assert_eq!(
            block_on(storage.kv_get(b"a", Some(&snapshot))).unwrap(),
            Some(b"0".to_vec())
        );
        assert_eq!(
            block_on(storage.kv_get(b"a", None)).unwrap(),
            Some(b"1".to_vec())
        );
    }

    #[test]
    fn worker_storage_kv_range_is_stable_with_snapshot() {
        let (storage, _oid) = test_storage();
        storage
            .worker_put_local(b"a".to_vec(), b"1".to_vec(), 1)
            .unwrap();
        let snapshot = WorkerSnapshot::new(2, vec![]);
        storage
            .worker_put_local(b"b".to_vec(), b"2".to_vec(), 3)
            .unwrap();

        let rows = block_on(storage.kv_range(b"a", b"z", Some(&snapshot))).unwrap();
        assert_eq!(
            rows,
            vec![KvItem {
                key: b"a".to_vec(),
                value: b"1".to_vec()
            }]
        );
    }

    #[test]
    fn worker_storage_kv_allows_concurrent_commits_on_different_keys() {
        let (storage, _oid) = test_storage();
        let snapshot1 = WorkerSnapshot::new(1, vec![]);
        let snapshot2 = WorkerSnapshot::new(2, vec![1]);

        let prepared1 = storage
            .prepare_worker_kv_commit(
                &snapshot1,
                snapshot1.xid(),
                BTreeMap::from([(b"a".to_vec(), Some(b"1".to_vec()))]),
                XLBatch::new(vec![]),
            )
            .unwrap();
        let prepared2 = storage
            .prepare_worker_kv_commit(
                &snapshot2,
                snapshot2.xid(),
                BTreeMap::from([(b"b".to_vec(), Some(b"2".to_vec()))]),
                XLBatch::new(vec![]),
            )
            .unwrap();

        storage.apply_prepared_commit(prepared1).unwrap();
        storage.apply_prepared_commit(prepared2).unwrap();

        assert_eq!(
            block_on(storage.kv_get(b"a", None)).unwrap(),
            Some(b"1".to_vec())
        );
        assert_eq!(
            block_on(storage.kv_get(b"b", None)).unwrap(),
            Some(b"2".to_vec())
        );
    }

    #[test]
    fn worker_storage_replay_batch_restores_kv_and_relation_rows() {
        let (storage, oid) = test_storage();
        let batch = XLBatch::new(vec![crate::wal::xl_entry::XLEntry {
            xid: 9,
            ops: vec![
                TxOp::Begin,
                TxOp::Insert(XLInsert {
                    table_id: 0,
                    partition_id: 0,
                    tuple_id: 0,
                    key: b"k".to_vec(),
                    value: b"v".to_vec(),
                }),
                TxOp::Insert(XLInsert {
                    table_id: oid,
                    partition_id: 0,
                    tuple_id: 0,
                    key: i32_bytes(7),
                    value: i32_bytes(70),
                }),
                TxOp::Commit,
            ],
        }]);

        storage.replay_batch(batch).unwrap();

        assert_eq!(
            block_on(storage.kv_get(b"k", None)).unwrap(),
            Some(b"v".to_vec())
        );
        let mut tx = begin_tx(10, vec![]);
        assert_eq!(
            block_on(storage.get(oid, &i32_bytes(7), &mut tx)).unwrap(),
            Some(i32_bytes(70))
        );
    }

    #[test]
    fn worker_storage_replay_batch_applies_kv_delete() {
        let (storage, _oid) = test_storage();
        storage
            .worker_put_local(b"k".to_vec(), b"v".to_vec(), 1)
            .unwrap();

        let batch = XLBatch::new(vec![crate::wal::xl_entry::XLEntry {
            xid: 2,
            ops: vec![
                TxOp::Begin,
                TxOp::Delete(XLDelete {
                    table_id: 0,
                    partition_id: 0,
                    tuple_id: 0,
                    key: b"k".to_vec(),
                }),
                TxOp::Commit,
            ],
        }]);

        storage.replay_batch(batch).unwrap();

        assert_eq!(block_on(storage.kv_get(b"k", None)).unwrap(), None);
    }

    #[test]
    fn worker_storage_bootstrap_uses_partition_zero_for_unpartitioned_tables() {
        let mgr = Arc::new(TestMetaMgr::new());
        let schema = test_schema();
        let oid = schema.id();
        futures::executor::block_on(mgr.create_table(&schema)).unwrap();

        let storage = WorkerStorage::new(
            mgr,
            123,
            std::env::temp_dir()
                .join(format!(
                    "worker_storage_bootstrap_test_{}",
                    mudu::common::id::gen_oid()
                ))
                .to_string_lossy()
                .to_string(),
        );
        storage.bootstrap_existing_tables_sync().unwrap();

        let batch = XLBatch::new(vec![crate::wal::xl_entry::XLEntry {
            xid: 11,
            ops: vec![
                TxOp::Begin,
                TxOp::Insert(XLInsert {
                    table_id: oid,
                    partition_id: 0,
                    tuple_id: 0,
                    key: i32_bytes(5),
                    value: i32_bytes(50),
                }),
                TxOp::Commit,
            ],
        }]);

        storage.replay_batch(batch).unwrap();

        let mut tx = begin_tx(12, vec![]);
        assert_eq!(
            block_on(storage.get_on_partition(oid, Some(0), &i32_bytes(5), &mut tx)).unwrap(),
            Some(i32_bytes(50))
        );
    }
}
