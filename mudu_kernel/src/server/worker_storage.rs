use std::collections::{BTreeMap, Bound};
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::sync::Arc;

use mudu::common::id::OID;
use mudu::common::result::RS;
use mudu::error::ec::EC;
use mudu::m_error;
use scc::HashMap as SccHashMap;

use crate::contract::data_row::DataRow;
use crate::contract::meta_mgr::MetaMgr;
use crate::contract::schema_table::SchemaTable;
use crate::contract::table_desc::TableDesc;
use crate::contract::timestamp::Timestamp;
use crate::contract::version_tuple::VersionTuple;
use crate::index::index_key::key_tuple::KeyTuple;
use crate::server::worker_snapshot::{KvItem, WorkerSnapshot};
use crate::server::worker_tx_manager::WorkerTxManager;
use crate::storage::relation::relation::Relation;
use crate::wal::xl_batch::XLBatch;
use crate::wal::xl_data_op::{XLDelete, XLInsert};
use crate::wal::xl_entry::TxOp;
#[derive(Clone, Debug)]
pub(crate) struct PreparedWorkerCommit {
    xid: u64,
    relation_rows: BTreeMap<OID, BTreeMap<Vec<u8>, Option<Vec<u8>>>>,
    kv_rows: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    batch: XLBatch,
}

pub struct WorkerStorage {
    mgr: Arc<dyn MetaMgr>,
    partition_id: OID,
    relation_path: String,
    relation_store: SccHashMap<OID, Relation>,
    kv_store: SccHashMap<Vec<u8>, DataRow>,
}

impl WorkerStorage {
    pub fn new(mgr: Arc<dyn MetaMgr>, partition_id: OID, relation_path: String) -> Self {
        Self {
            mgr,
            partition_id,
            relation_path,
            relation_store: SccHashMap::new(),
            kv_store: SccHashMap::new(),
        }
    }

    pub async fn create_table_async(&self, schema: &SchemaTable) -> RS<()> {
        let oid = schema.id();
        self.mgr.create_table(schema).await?;
        let table_desc = self.mgr.get_table_by_id(oid).await?;
        self.create_relation_index(oid, table_desc.as_ref())?;
        Ok(())
    }

    pub async fn drop_table_async(&self, oid: OID) -> RS<()> {
        self.mgr.drop_table(oid).await?;
        let _ = self.relation_store.remove_sync(&oid);
        Ok(())
    }

    #[allow(dead_code)]
    pub fn contains_key(&self, oid: OID, key: &KeyTuple, txm: &mut WorkerTxManager) -> RS<bool> {
        if let Some(staged) = txm.get_relation(oid, key.as_slice()) {
            return Ok(staged.is_some());
        }
        self.read_visible_relation_exists(oid, key, txm.snapshot())
    }

    pub fn get(&self, oid: OID, key: &[u8], txm: &mut WorkerTxManager) -> RS<Option<Vec<u8>>> {
        if let Some(staged) = txm.get_relation(oid, key) {
            return Ok(staged);
        }

        let key = KeyTuple::from(key.to_vec());
        self.read_visible_relation_value(oid, &key, txm.snapshot())
    }

    pub fn insert(
        &self,
        oid: OID,
        key: Vec<u8>,
        value: Vec<u8>,
        txm: &mut WorkerTxManager,
    ) -> RS<()> {
        let key_tuple = KeyTuple::from(key.clone());
        self.ensure_no_relation_write_conflict(oid, &key_tuple, txm.snapshot())?;
        txm.put_relation(oid, key, value);
        Ok(())
    }

    pub fn remove(&self, oid: OID, key: &[u8], txm: &mut WorkerTxManager) -> RS<Option<Vec<u8>>> {
        let key_tuple = KeyTuple::from(key.to_vec());
        self.ensure_no_relation_write_conflict(oid, &key_tuple, txm.snapshot())?;

        let current = match txm.get_relation(oid, key) {
            Some(staged) => staged,
            None => self.read_visible_relation_value(oid, &key_tuple, txm.snapshot())?,
        };

        if current.is_some() {
            txm.delete_relation(oid, key.to_vec());
        }
        Ok(current)
    }

    pub fn range(
        &self,
        oid: OID,
        bounds: (Bound<&[u8]>, Bound<&[u8]>),
        txm: &mut WorkerTxManager,
    ) -> RS<Vec<(Vec<u8>, Vec<u8>)>> {
        let base_items = self.range_visible_relation(oid, bounds, txm.snapshot())?;
        let (start_key, end_key) = bounds_to_scan(&bounds);
        let staged_items = txm.staged_relation_items_in_range(oid, &start_key, &end_key);

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

    pub fn worker_get(&self, key: &[u8], snapshot: Option<&WorkerSnapshot>) -> RS<Option<Vec<u8>>> {
        let row = self.kv_store.get_sync(key);
        let version = match snapshot {
            Some(snapshot) => row.and_then(|row| {
                let snapshot = snapshot.to_snapshot();
                read_visible_version(row.get(), &snapshot)
            }),
            None => row.and_then(|row| latest_version(row.get())),
        };
        Ok(version
            .filter(|version| !version.is_deleted())
            .map(|version| version.tuple().clone()))
    }

    pub fn worker_range(
        &self,
        start_key: &[u8],
        end_key: &[u8],
        snapshot: Option<&WorkerSnapshot>,
    ) -> RS<Vec<KvItem>> {
        let mut items = Vec::new();
        self.kv_store.iter_sync(|key, row| {
            let in_range = if end_key.is_empty() {
                key.as_slice() >= start_key
            } else {
                key.as_slice() >= start_key && key.as_slice() < end_key
            };
            if !in_range {
                return true;
            }

            let visible = match snapshot {
                Some(snapshot) => {
                    let snapshot = snapshot.to_snapshot();
                    read_visible_version(row, &snapshot)
                }
                None => latest_version(row),
            };
            if let Some(visible) = visible.filter(|version| !version.is_deleted()) {
                items.push(KvItem {
                    key: key.clone(),
                    value: visible.tuple().clone(),
                });
            }
            true
        });
        items.sort_by(|left, right| left.key.cmp(&right.key));
        Ok(items)
    }

    #[allow(dead_code)]
    pub(crate) fn commit_tx(&self, txm: &mut WorkerTxManager) -> RS<()> {
        let prepared = self.prepare_commit(txm)?;
        self.apply_prepared_commit(prepared)
    }

    pub(crate) fn prepare_commit(&self, txm: &WorkerTxManager) -> RS<PreparedWorkerCommit> {
        self.prepare_commit_parts(
            txm.snapshot(),
            txm.xid(),
            txm.staged_relation_ops().clone(),
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

    pub(crate) fn replay_batch(&self, batch: XLBatch) -> RS<()> {
        for entry in batch.entries {
            for op in entry.ops {
                match op {
                    TxOp::Insert(insert) if insert.table_id == 0 => {
                        self.worker_put_local(insert.key, insert.value, entry.xid)?;
                    }
                    TxOp::Delete(delete) if delete.table_id == 0 => {
                        self.worker_delete_local(delete.key, entry.xid)?;
                    }
                    TxOp::Insert(insert) => {
                        self.apply_relation_replay_insert(insert, entry.xid)?;
                    }
                    TxOp::Delete(delete) if delete.table_id != 0 => {
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
        relation_rows: BTreeMap<OID, BTreeMap<Vec<u8>, Option<Vec<u8>>>>,
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

    fn ensure_no_relation_conflicts(
        &self,
        snapshot: &WorkerSnapshot,
        xid: u64,
        relation_rows: &BTreeMap<OID, BTreeMap<Vec<u8>, Option<Vec<u8>>>>,
    ) -> RS<()> {
        for (oid, rows) in relation_rows {
            let relation = self
                .relation_store
                .get_sync(oid)
                .ok_or_else(|| m_error!(EC::NoSuchElement, format!("no such table {}", oid)))?;
            for key in rows.keys() {
                let key_tuple = KeyTuple::from(key.clone());
                if relation.get().has_write_conflict(&key_tuple, snapshot)? {
                    return Err(m_error!(
                        EC::TxErr,
                        format!(
                            "write-write conflict on table {} key {:?} for transaction {}",
                            oid, key, xid
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
        for (oid, rows) in &prepared.relation_rows {
            let relation = self
                .relation_store
                .get_sync(oid)
                .ok_or_else(|| m_error!(EC::NoSuchElement, format!("no such table {}", oid)))?;
            for (key, value) in rows {
                relation
                    .get()
                    .write_row(key.clone(), value.clone(), prepared.xid)?;
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

    fn apply_relation_replay_insert(&self, insert: XLInsert, xid: u64) -> RS<()> {
        let relation = self
            .relation_store
            .get_sync(&insert.table_id)
            .ok_or_else(|| {
                m_error!(
                    EC::NoSuchElement,
                    format!("no such table {}", insert.table_id)
                )
            })?;
        relation.get().write_value(insert.key, insert.value, xid)
    }

    fn apply_relation_replay_delete(&self, delete: XLDelete, xid: u64) -> RS<()> {
        let relation = self
            .relation_store
            .get_sync(&delete.table_id)
            .ok_or_else(|| {
                m_error!(
                    EC::NoSuchElement,
                    format!("no such table {}", delete.table_id)
                )
            })?;
        relation.get().write_delete(delete.key, xid)
    }

    fn read_visible_relation_exists(
        &self,
        oid: OID,
        key: &KeyTuple,
        snapshot: &WorkerSnapshot,
    ) -> RS<bool> {
        let relation = self
            .relation_store
            .get_sync(&oid)
            .ok_or_else(|| m_error!(EC::NoSuchElement, format!("no such table {}", oid)))?;
        relation.get().has_visible_version(key, snapshot)
    }

    fn read_visible_relation_value(
        &self,
        oid: OID,
        key: &KeyTuple,
        snapshot: &WorkerSnapshot,
    ) -> RS<Option<Vec<u8>>> {
        let relation = self
            .relation_store
            .get_sync(&oid)
            .ok_or_else(|| m_error!(EC::NoSuchElement, format!("no such table {}", oid)))?;
        relation.get().visible_value(key, snapshot)
    }

    fn range_visible_relation(
        &self,
        oid: OID,
        bounds: (Bound<&[u8]>, Bound<&[u8]>),
        snapshot: &WorkerSnapshot,
    ) -> RS<Vec<(Vec<u8>, Vec<u8>)>> {
        let relation = self
            .relation_store
            .get_sync(&oid)
            .ok_or_else(|| m_error!(EC::NoSuchElement, format!("no such table {}", oid)))?;
        relation.get().visible_range(bounds, snapshot)
    }

    fn ensure_no_relation_write_conflict(
        &self,
        oid: OID,
        key: &KeyTuple,
        snapshot: &WorkerSnapshot,
    ) -> RS<()> {
        let relation = self
            .relation_store
            .get_sync(&oid)
            .ok_or_else(|| m_error!(EC::NoSuchElement, format!("no such table {}", oid)))?;
        if relation.get().has_write_conflict(key, snapshot)? {
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

    fn create_relation_index(&self, oid: OID, table_desc: &TableDesc) -> RS<()> {
        let _ = self.relation_store.insert_sync(
            oid,
            Relation::new(
                oid,
                self.partition_id,
                self.relation_path.clone(),
                table_desc,
            ),
        );
        Ok(())
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

fn latest_version(row: &DataRow) -> Option<VersionTuple> {
    row.read_latest_sync().ok().flatten()
}

fn read_visible_version(
    row: &DataRow,
    snapshot: &crate::contract::snapshot::Snapshot,
) -> Option<VersionTuple> {
    row.read_sync(snapshot).ok().flatten()
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
        tables: Mutex<HashMap<OID, Arc<TableDesc>>>,
    }

    impl TestMetaMgr {
        fn new() -> Self {
            Self {
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
            self.tables.lock().unwrap().insert(schema.id(), table);
            Ok(())
        }

        async fn drop_table(&self, table_id: OID) -> RS<()> {
            self.tables.lock().unwrap().remove(&table_id);
            Ok(())
        }
    }

    fn test_schema() -> SchemaTable {
        SchemaTable::new(
            "t".to_string(),
            vec![SchemaColumn::new(
                "id".to_string(),
                DatTypeID::I32,
                DTInfo::from_text(DatTypeID::I32, String::new()),
            )],
            vec![SchemaColumn::new(
                "v".to_string(),
                DatTypeID::I32,
                DTInfo::from_text(DatTypeID::I32, String::new()),
            )],
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

    fn begin_tx(xid: u64, running: Vec<u64>) -> WorkerTxManager {
        WorkerTxManager::new(WorkerSnapshot::new(xid, running))
    }

    fn i32_bytes(v: i32) -> Vec<u8> {
        v.to_be_bytes().to_vec()
    }

    #[test]
    fn worker_storage_reads_own_writes() {
        let (storage, oid) = test_storage();
        let mut tx = begin_tx(10, vec![]);

        storage
            .insert(oid, i32_bytes(1), i32_bytes(11), &mut tx)
            .unwrap();

        assert_eq!(
            storage.get(oid, &i32_bytes(1), &mut tx).unwrap(),
            Some(i32_bytes(11))
        );
        assert!(storage
            .contains_key(oid, &KeyTuple::from(i32_bytes(1)), &mut tx)
            .unwrap());
    }

    #[test]
    fn worker_storage_snapshot_hides_later_commit() {
        let (storage, oid) = test_storage();
        let mut tx1 = begin_tx(1, vec![]);
        storage
            .insert(oid, i32_bytes(1), i32_bytes(10), &mut tx1)
            .unwrap();
        storage.commit_tx(&mut tx1).unwrap();

        let mut old_tx = begin_tx(2, vec![]);
        let mut new_tx = begin_tx(3, vec![2]);
        storage
            .insert(oid, i32_bytes(1), i32_bytes(20), &mut new_tx)
            .unwrap();
        storage.commit_tx(&mut new_tx).unwrap();

        assert_eq!(
            storage.get(oid, &i32_bytes(1), &mut old_tx).unwrap(),
            Some(i32_bytes(10))
        );
    }

    #[test]
    fn worker_storage_range_is_stable_with_snapshot() {
        let (storage, oid) = test_storage();
        let mut seed = begin_tx(1, vec![]);
        storage
            .insert(oid, i32_bytes(1), i32_bytes(10), &mut seed)
            .unwrap();
        storage.commit_tx(&mut seed).unwrap();

        let mut old_tx = begin_tx(2, vec![]);
        let mut new_tx = begin_tx(3, vec![2]);
        storage
            .insert(oid, i32_bytes(2), i32_bytes(20), &mut new_tx)
            .unwrap();
        storage.commit_tx(&mut new_tx).unwrap();

        let rows = storage
            .range(
                oid,
                (
                    Bound::Included(i32_bytes(1).as_slice()),
                    Bound::Included(i32_bytes(9).as_slice()),
                ),
                &mut old_tx,
            )
            .unwrap();
        assert_eq!(rows, vec![(i32_bytes(1), i32_bytes(10))]);
    }

    #[test]
    fn worker_storage_first_committer_wins() {
        let (storage, oid) = test_storage();
        let mut seed = begin_tx(1, vec![]);
        storage
            .insert(oid, i32_bytes(1), i32_bytes(10), &mut seed)
            .unwrap();
        storage.commit_tx(&mut seed).unwrap();

        let mut tx1 = begin_tx(2, vec![]);
        let mut tx2 = begin_tx(3, vec![2]);
        storage
            .insert(oid, i32_bytes(1), i32_bytes(11), &mut tx1)
            .unwrap();
        storage
            .insert(oid, i32_bytes(1), i32_bytes(12), &mut tx2)
            .unwrap();
        storage.commit_tx(&mut tx1).unwrap();
        let err = storage.commit_tx(&mut tx2).unwrap_err();

        assert!(err.to_string().contains("write-write conflict"));
    }

    #[test]
    fn worker_storage_delete_respects_snapshot() {
        let (storage, oid) = test_storage();
        let mut seed = begin_tx(1, vec![]);
        storage
            .insert(oid, i32_bytes(1), i32_bytes(10), &mut seed)
            .unwrap();
        storage.commit_tx(&mut seed).unwrap();

        let mut old_tx = begin_tx(2, vec![]);
        let mut delete_tx = begin_tx(3, vec![2]);
        assert_eq!(
            storage.remove(oid, &i32_bytes(1), &mut delete_tx).unwrap(),
            Some(i32_bytes(10))
        );
        storage.commit_tx(&mut delete_tx).unwrap();

        assert_eq!(
            storage.get(oid, &i32_bytes(1), &mut old_tx).unwrap(),
            Some(i32_bytes(10))
        );
        let mut fresh_tx = begin_tx(4, vec![]);
        assert_eq!(
            storage.get(oid, &i32_bytes(1), &mut fresh_tx).unwrap(),
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
            XLBatch { entries: vec![] },
        );
        storage.apply_prepared_commit(prepared).unwrap();

        assert_eq!(
            storage.worker_get(b"a", Some(&snapshot)).unwrap(),
            Some(b"0".to_vec())
        );
        assert_eq!(storage.worker_get(b"a", None).unwrap(), Some(b"1".to_vec()));
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

        let rows = storage.worker_range(b"a", b"z", Some(&snapshot)).unwrap();
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
                XLBatch { entries: vec![] },
            )
            .unwrap();
        let prepared2 = storage
            .prepare_worker_kv_commit(
                &snapshot2,
                snapshot2.xid(),
                BTreeMap::from([(b"b".to_vec(), Some(b"2".to_vec()))]),
                XLBatch { entries: vec![] },
            )
            .unwrap();

        storage.apply_prepared_commit(prepared1).unwrap();
        storage.apply_prepared_commit(prepared2).unwrap();

        assert_eq!(storage.worker_get(b"a", None).unwrap(), Some(b"1".to_vec()));
        assert_eq!(storage.worker_get(b"b", None).unwrap(), Some(b"2".to_vec()));
    }

    #[test]
    fn worker_storage_replay_batch_restores_kv_and_relation_rows() {
        let (storage, oid) = test_storage();
        let batch = XLBatch {
            entries: vec![crate::wal::xl_entry::XLEntry {
                xid: 9,
                ops: vec![
                    TxOp::Begin,
                    TxOp::Insert(XLInsert {
                        table_id: 0,
                        tuple_id: 0,
                        key: b"k".to_vec(),
                        value: b"v".to_vec(),
                    }),
                    TxOp::Insert(XLInsert {
                        table_id: oid,
                        tuple_id: 0,
                        key: i32_bytes(7),
                        value: i32_bytes(70),
                    }),
                    TxOp::Commit,
                ],
            }],
        };

        storage.replay_batch(batch).unwrap();

        assert_eq!(storage.worker_get(b"k", None).unwrap(), Some(b"v".to_vec()));
        let mut tx = begin_tx(10, vec![]);
        assert_eq!(
            storage.get(oid, &i32_bytes(7), &mut tx).unwrap(),
            Some(i32_bytes(70))
        );
    }

    #[test]
    fn worker_storage_replay_batch_applies_kv_delete() {
        let (storage, _oid) = test_storage();
        storage
            .worker_put_local(b"k".to_vec(), b"v".to_vec(), 1)
            .unwrap();

        let batch = XLBatch {
            entries: vec![crate::wal::xl_entry::XLEntry {
                xid: 2,
                ops: vec![
                    TxOp::Begin,
                    TxOp::Delete(XLDelete {
                        table_id: 0,
                        tuple_id: 0,
                        key: b"k".to_vec(),
                    }),
                    TxOp::Commit,
                ],
            }],
        };

        storage.replay_batch(batch).unwrap();

        assert_eq!(storage.worker_get(b"k", None).unwrap(), None);
    }
}
