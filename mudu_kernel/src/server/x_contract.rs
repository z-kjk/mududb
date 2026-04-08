use async_trait::async_trait;
use mudu::common::buf::Buf;
use mudu::common::id::{AttrIndex, OID};
use mudu::common::result::RS;
use mudu::common::xid::XID;
use mudu::error::ec::EC;
use mudu::m_error;
use mudu_contract::tuple::build_tuple::build_tuple;
use mudu_contract::tuple::tuple_binary::TupleBinary as TupleRaw;
use mudu_contract::tuple::update_tuple::update_tuple;
use std::collections::HashMap;
use std::ops::Bound;
use std::sync::{Arc, Mutex};

use crate::contract::meta_mgr::MetaMgr;
use crate::contract::schema_table::SchemaTable;
use crate::contract::table_desc::TableDesc;
use crate::server::worker_snapshot::{KvItem, WorkerSnapshot, WorkerSnapshotMgr};
use crate::server::worker_storage::{PreparedWorkerCommit, WorkerStorage};
use crate::server::worker_tx_manager::WorkerTxManager;
use crate::server::x_lock_mgr::XLockMgr;
use crate::wal::worker_log::ChunkedWorkerLogBackend;
use crate::wal::xl_batch::{new_xl_batch_writer, XLBatch};
use crate::x_engine::api::{
    AlterTable, Filter, OptDelete, OptInsert, OptRead, OptUpdate, Predicate, RSCursor, RangeData,
    TupleRow, VecDatum, VecSelTerm, XContract,
};
type DatBin = Buf;

pub struct IoUringXContract {
    inner: Mutex<IoUringXContractInner>,
    // commit_gate: AsyncMutex<()>,
}

struct IoUringXContractInner {
    meta_mgr: Arc<dyn MetaMgr>,
    storage: Arc<WorkerStorage>,
    log: Option<ChunkedWorkerLogBackend>,
    snapshot_mgr: WorkerSnapshotMgr,
    tx_ctx: HashMap<XID, WorkerTxManager>,
    tx_lock: XLockMgr,
}

struct VecCursor {
    inner: Mutex<VecCursorInner>,
}

struct VecCursorInner {
    rows: Vec<TupleRow>,
    index: usize,
}

impl IoUringXContract {
    pub fn new(meta_mgr: Arc<dyn MetaMgr>) -> Self {
        Self::with_log_and_data_dir(meta_mgr, None, 0, default_worker_storage_data_dir())
    }

    pub fn with_log(meta_mgr: Arc<dyn MetaMgr>, log: Option<ChunkedWorkerLogBackend>) -> Self {
        Self::with_log_and_data_dir(meta_mgr, log, 0, default_worker_storage_data_dir())
    }

    pub fn with_log_and_data_dir(
        meta_mgr: Arc<dyn MetaMgr>,
        log: Option<ChunkedWorkerLogBackend>,
        partition_id: OID,
        data_dir: String,
    ) -> Self {
        Self {
            inner: Mutex::new(IoUringXContractInner {
                meta_mgr: meta_mgr.clone(),
                storage: Arc::new(WorkerStorage::new(meta_mgr, partition_id, data_dir)),
                log,
                snapshot_mgr: WorkerSnapshotMgr::default(),
                tx_ctx: HashMap::new(),
                tx_lock: XLockMgr::new(),
            }),
        }
    }

    pub fn with_worker_log(log: ChunkedWorkerLogBackend) -> Self {
        Self::with_worker_log_and_data_dir(log, 0, default_worker_storage_data_dir())
    }

    pub fn with_worker_log_and_data_dir(
        log: ChunkedWorkerLogBackend,
        partition_id: OID,
        data_dir: String,
    ) -> Self {
        let meta_mgr: Arc<dyn MetaMgr> = Arc::new(NoopMetaMgr);
        Self {
            inner: Mutex::new(IoUringXContractInner {
                meta_mgr: meta_mgr.clone(),
                storage: Arc::new(WorkerStorage::new(meta_mgr, partition_id, data_dir)),
                log: Some(log.clone()),
                snapshot_mgr: WorkerSnapshotMgr::default(),
                tx_ctx: HashMap::new(),
                tx_lock: XLockMgr::new(),
            }),
        }
    }

    fn lock_inner(&self) -> RS<std::sync::MutexGuard<'_, IoUringXContractInner>> {
        self.inner
            .lock()
            .map_err(|_| m_error!(EC::InternalErr, "io_uring xcontract lock poisoned"))
    }

    pub fn worker_log(&self) -> Option<ChunkedWorkerLogBackend> {
        self.lock_inner().ok().and_then(|guard| guard.log.clone())
    }

    pub fn worker_begin_tx(&self) -> RS<WorkerSnapshot> {
        let mut guard = self.lock_inner()?;
        Ok(guard.snapshot_mgr.begin_tx())
    }

    pub fn worker_rollback_tx(&self, xid: u64) -> RS<()> {
        self.lock_inner()?.snapshot_mgr.end_tx(xid)
    }

    pub fn worker_put(&self, key: Vec<u8>, value: Vec<u8>) -> RS<()> {
        let prepared = {
            let mut guard = self.lock_inner()?;
            let xid = guard.snapshot_mgr.alloc_committed_ts();
            (
                guard.storage.clone(),
                guard.log.clone(),
                guard.storage.prepare_worker_kv_autocommit(
                    xid,
                    key.clone(),
                    Some(value.clone()),
                    single_put_batch(xid, key, value),
                ),
            )
        };
        let (storage, log, prepared) = prepared;
        if let Some(log) = log {
            new_xl_batch_writer(log).append_sync(prepared.batch())?;
        }
        storage.apply_prepared_commit(prepared)
    }

    pub async fn worker_put_async(&self, key: Vec<u8>, value: Vec<u8>) -> RS<()> {
        let (storage, log, prepared) = {
            let mut guard = self.lock_inner()?;
            let xid = guard.snapshot_mgr.alloc_committed_ts();
            (
                guard.storage.clone(),
                guard.log.clone(),
                guard.storage.prepare_worker_kv_autocommit(
                    xid,
                    key.clone(),
                    Some(value.clone()),
                    single_put_batch(xid, key, value),
                ),
            )
        };
        if let Some(log) = log {
            new_xl_batch_writer(log).append(prepared.batch()).await?;
        }
        storage.apply_prepared_commit(prepared)
    }

    pub fn worker_delete(&self, key: &[u8]) -> RS<()> {
        let key = key.to_vec();
        let prepared = {
            let mut guard = self.lock_inner()?;
            let xid = guard.snapshot_mgr.alloc_committed_ts();
            (
                guard.storage.clone(),
                guard.log.clone(),
                guard.storage.prepare_worker_kv_autocommit(
                    xid,
                    key.clone(),
                    None,
                    single_delete_batch(xid, key),
                ),
            )
        };
        let (storage, log, prepared) = prepared;
        if let Some(log) = log {
            new_xl_batch_writer(log).append_sync(prepared.batch())?;
        }
        storage.apply_prepared_commit(prepared)
    }

    pub async fn worker_delete_async(&self, key: &[u8]) -> RS<()> {
        let key = key.to_vec();
        let (storage, log, prepared) = {
            let mut guard = self.lock_inner()?;
            let xid = guard.snapshot_mgr.alloc_committed_ts();
            (
                guard.storage.clone(),
                guard.log.clone(),
                guard.storage.prepare_worker_kv_autocommit(
                    xid,
                    key.clone(),
                    None,
                    single_delete_batch(xid, key),
                ),
            )
        };
        if let Some(log) = log {
            new_xl_batch_writer(log).append(prepared.batch()).await?;
        }
        storage.apply_prepared_commit(prepared)
    }

    pub fn worker_get(&self, key: &[u8]) -> RS<Option<Vec<u8>>> {
        let storage = { self.lock_inner()?.storage.clone() };
        storage.worker_get(key, None)
    }

    pub fn worker_get_with_snapshot(
        &self,
        snapshot: &WorkerSnapshot,
        key: &[u8],
    ) -> RS<Option<Vec<u8>>> {
        let storage = { self.lock_inner()?.storage.clone() };
        storage.worker_get(key, Some(snapshot))
    }

    pub fn worker_range_scan(&self, start_key: &[u8], end_key: &[u8]) -> RS<Vec<KvItem>> {
        let storage = { self.lock_inner()?.storage.clone() };
        storage.worker_range(start_key, end_key, None)
    }

    pub fn worker_range_scan_with_snapshot(
        &self,
        snapshot: &WorkerSnapshot,
        start_key: &[u8],
        end_key: &[u8],
    ) -> RS<Vec<KvItem>> {
        let storage = { self.lock_inner()?.storage.clone() };
        storage.worker_range(start_key, end_key, Some(snapshot))
    }

    pub fn worker_commit_put_batch(
        &self,
        snapshot: &WorkerSnapshot,
        xid: u64,
        items: std::collections::BTreeMap<Vec<u8>, Option<Vec<u8>>>,
        batch: XLBatch,
    ) -> RS<()> {
        if items.is_empty() {
            return self.worker_rollback_tx(xid);
        }
        let (storage, log, prepared) = {
            let guard = self.lock_inner()?;
            let prepared = guard
                .storage
                .prepare_worker_kv_commit(snapshot, xid, items, batch)?;
            (guard.storage.clone(), guard.log.clone(), prepared)
        };
        if let Some(log) = log {
            new_xl_batch_writer(log.clone()).append_sync(prepared.batch())?;
            log.flush()?;
        }
        storage.apply_prepared_commit(prepared)?;
        self.worker_rollback_tx(xid)
    }

    pub async fn worker_commit_put_batch_async(
        &self,
        snapshot: &WorkerSnapshot,
        xid: u64,
        items: std::collections::BTreeMap<Vec<u8>, Option<Vec<u8>>>,
        batch: XLBatch,
    ) -> RS<()> {
        if items.is_empty() {
            return self.worker_rollback_tx(xid);
        }
        let (storage, log, prepared) = {
            let guard = self.lock_inner()?;
            let prepared = guard
                .storage
                .prepare_worker_kv_commit(snapshot, xid, items, batch)?;
            (guard.storage.clone(), guard.log.clone(), prepared)
        };
        if let Some(log) = log {
            new_xl_batch_writer(log.clone())
                .append(prepared.batch())
                .await?;
            log.flush_async().await?;
        }
        storage.apply_prepared_commit(prepared)?;
        self.worker_rollback_tx(xid)
    }

    pub fn replay_worker_log_batch(&self, batch: XLBatch) -> RS<()> {
        let max_xid = batch.entries.iter().map(|entry| entry.xid).max();
        let storage = {
            let mut guard = self.lock_inner()?;
            if let Some(max_xid) = max_xid {
                guard.snapshot_mgr.observe_committed_ts(max_xid);
            }
            guard.storage.clone()
        };
        storage.replay_batch(batch)
    }
}

fn default_worker_storage_data_dir() -> String {
    std::env::temp_dir()
        .join(format!(
            "mududb-worker-storage-{}",
            mudu::common::id::gen_oid()
        ))
        .to_string_lossy()
        .to_string()
}

struct NoopMetaMgr;

#[async_trait]
impl MetaMgr for NoopMetaMgr {
    async fn get_table_by_id(&self, oid: OID) -> RS<Arc<TableDesc>> {
        Err(m_error!(
            EC::NoSuchElement,
            format!("no such table {} in worker-local io_uring xcontract", oid)
        ))
    }

    async fn get_table_by_name(&self, _name: &String) -> RS<Option<Arc<TableDesc>>> {
        Ok(None)
    }

    async fn create_table(&self, _schema: &SchemaTable) -> RS<()> {
        Err(m_error!(
            EC::NotImplemented,
            "create table is not available in worker-local io_uring xcontract"
        ))
    }

    async fn drop_table(&self, _table_id: OID) -> RS<()> {
        Err(m_error!(
            EC::NotImplemented,
            "drop table is not available in worker-local io_uring xcontract"
        ))
    }
}

impl IoUringXContractInner {
    fn begin_tx(&mut self) -> XID {
        let snapshot = self.snapshot_mgr.begin_tx();
        let xid = snapshot.xid() as XID;
        self.tx_ctx.insert(xid, WorkerTxManager::new(snapshot));
        xid
    }

    #[allow(dead_code)]
    fn commit_tx(&mut self, xid: XID) -> RS<()> {
        let mut tx = self.take_tx(xid)?;
        let result = self.storage.commit_tx(&mut tx);
        self.end_tx(xid);
        result
    }

    fn commit_tx_prepare(
        &mut self,
        xid: XID,
    ) -> RS<(
        Option<PreparedWorkerCommit>,
        WorkerTxManager,
        Arc<WorkerStorage>,
        Option<ChunkedWorkerLogBackend>,
    )> {
        let mut tx = self.take_tx(xid)?;
        tx.build_write_ops();
        let can_commit = self.tx_lock.try_lock_some(xid, tx.write_ops());
        if can_commit {
            let prepared = self.storage.prepare_commit(&tx)?;
            Ok((Some(prepared), tx, self.storage.clone(), self.log.clone()))
        } else {
            Ok((None, tx, self.storage.clone(), self.log.clone()))
        }
    }

    fn finish_tx(&mut self, xid: XID) {
        self.end_tx(xid);
    }

    fn abort_tx(&mut self, xid: XID) -> RS<()> {
        let _ = self.take_tx(xid)?;
        self.end_tx(xid);
        Ok(())
    }

    fn insert(
        &mut self,
        desc: Arc<TableDesc>,
        xid: XID,
        table_id: OID,
        keys: &VecDatum,
        values: &VecDatum,
        _opt_insert: &OptInsert,
    ) -> RS<()> {
        let key = build_key_tuple(keys, &desc)?;
        let value = build_value_tuple(values, &desc)?;
        let mut tx = self.take_tx(xid)?;
        let result = self.storage.insert(table_id, key, value, &mut tx);
        self.tx_ctx.insert(xid, tx);
        result
    }

    fn read_key(
        &mut self,
        desc: Arc<TableDesc>,
        xid: XID,
        table_id: OID,
        pred_key: &VecDatum,
        select: &VecSelTerm,
        _opt_read: &OptRead,
    ) -> RS<Option<Vec<DatBin>>> {
        let key = build_key_tuple(pred_key, &desc)?;
        let mut tx = self.take_tx(xid)?;
        let opt_value = self.storage.get(table_id, &key, &mut tx)?;
        self.tx_ctx.insert(xid, tx);
        match opt_value {
            Some(value) => project_selected_fields(&desc, &key, &value, select).map(Some),
            None => Ok(None),
        }
    }

    fn read_range(
        &mut self,
        desc: Arc<TableDesc>,
        xid: XID,
        table_id: OID,
        pred_key: &RangeData,
        pred_non_key: &Predicate,
        select: &VecSelTerm,
        _opt_read: &OptRead,
    ) -> RS<Arc<dyn RSCursor>> {
        ensure_supported_predicate(pred_non_key)?;
        let start = build_bound_key(pred_key.start(), &desc)?;
        let end = build_bound_key(pred_key.end(), &desc)?;
        let mut tx = self.take_tx(xid)?;
        let rows = self.storage.range(table_id, (start, end), &mut tx)?;
        self.tx_ctx.insert(xid, tx);
        let projected = rows
            .into_iter()
            .map(|(key, value)| {
                project_selected_fields(&desc, &key, &value, select).map(TupleRow::new)
            })
            .collect::<RS<Vec<_>>>()?;
        Ok(Arc::new(VecCursor {
            inner: Mutex::new(VecCursorInner {
                rows: projected,
                index: 0,
            }),
        }))
    }

    fn delete(
        &mut self,
        desc: Arc<TableDesc>,
        xid: XID,
        table_id: OID,
        pred_key: &VecDatum,
        pred_non_key: &Predicate,
        _opt_delete: &OptDelete,
    ) -> RS<usize> {
        ensure_supported_predicate(pred_non_key)?;
        let key = build_key_tuple(pred_key, &desc)?;
        let mut tx = self.take_tx(xid)?;
        let deleted = self.storage.remove(table_id, &key, &mut tx)?;
        self.tx_ctx.insert(xid, tx);
        Ok(usize::from(deleted.is_some()))
    }

    fn update(
        &mut self,
        desc: Arc<TableDesc>,
        xid: XID,
        table_id: OID,
        pred_key: &VecDatum,
        pred_non_key: &Predicate,
        values: &VecDatum,
        _opt_update: &OptUpdate,
    ) -> RS<usize> {
        ensure_supported_predicate(pred_non_key)?;
        let key = build_key_tuple(pred_key, &desc)?;
        let mut tx = self.take_tx(xid)?;
        let current = self.storage.get(table_id, &key, &mut tx)?;
        let Some(current) = current else {
            self.tx_ctx.insert(xid, tx);
            return Ok(0);
        };
        let updated = apply_value_update(&current, values, &desc)?;
        let result = self.storage.insert(table_id, key, updated, &mut tx);
        self.tx_ctx.insert(xid, tx);
        result.map(|()| 1)
    }

    fn take_tx(&mut self, xid: XID) -> RS<WorkerTxManager> {
        self.tx_ctx
            .remove(&xid)
            .ok_or_else(|| m_error!(EC::NoSuchElement, format!("no such transaction {}", xid)))
    }

    fn end_tx(&mut self, xid: XID) {
        let _ = self.snapshot_mgr.end_tx(xid as u64);
    }
}

#[async_trait]
impl XContract for IoUringXContract {
    async fn create_table(&self, _xid: XID, schema: &SchemaTable) -> RS<()> {
        let storage = {
            let guard = self.lock_inner()?;
            guard.storage.clone()
        };
        storage.create_table_async(schema).await
    }

    async fn drop_table(&self, _xid: XID, oid: OID) -> RS<()> {
        let storage = {
            let guard = self.lock_inner()?;
            guard.storage.clone()
        };
        storage.drop_table_async(oid).await
    }

    async fn alter_table(&self, _xid: XID, _oid: OID, _alter_table: &AlterTable) -> RS<()> {
        Err(m_error!(
            EC::NotImplemented,
            "alter table is not implemented"
        ))
    }

    async fn begin_tx(&self) -> RS<XID> {
        Ok(self.lock_inner()?.begin_tx())
    }

    async fn commit_tx(&self, xid: XID) -> RS<()> {
        let prepared = {
            let mut guard = self.lock_inner()?;

            guard.commit_tx_prepare(xid)
        };
        let result = match prepared {
            Ok((opt_prepared, tx, storage, log)) => {
                if let Some(prepared) = opt_prepared {
                    if let Some(log) = log {
                        new_xl_batch_writer(log.clone())
                            .append(prepared.batch())
                            .await?;
                        log.flush_async().await?;
                    }
                    storage.apply_prepared_commit(prepared)?;
                    {
                        let guard = self.inner.lock().unwrap();
                        guard.tx_lock.release(xid, tx.write_ops());
                        Ok(())
                    }
                } else {
                    let guard = self.inner.lock().unwrap();
                    guard.tx_lock.release(xid, tx.write_ops());
                    Ok(())
                }
            }
            Err(err) => Err(err),
        };
        self.lock_inner()?.finish_tx(xid);
        result
    }

    async fn abort_tx(&self, xid: XID) -> RS<()> {
        self.lock_inner()?.abort_tx(xid)
    }

    async fn update(
        &self,
        xid: XID,
        table_id: OID,
        pred_key: &VecDatum,
        pred_non_key: &Predicate,
        values: &VecDatum,
        opt_update: &OptUpdate,
    ) -> RS<usize> {
        let meta_mgr = { self.lock_inner()?.meta_mgr.clone() };
        let desc = meta_mgr.get_table_by_id(table_id).await?;
        self.lock_inner()?.update(
            desc,
            xid,
            table_id,
            pred_key,
            pred_non_key,
            values,
            opt_update,
        )
    }

    async fn read_key(
        &self,
        xid: XID,
        table_id: OID,
        pred_key: &VecDatum,
        select: &VecSelTerm,
        opt_read: &OptRead,
    ) -> RS<Option<Vec<DatBin>>> {
        let meta_mgr = { self.lock_inner()?.meta_mgr.clone() };
        let desc = meta_mgr.get_table_by_id(table_id).await?;
        self.lock_inner()?
            .read_key(desc, xid, table_id, pred_key, select, opt_read)
    }

    async fn read_range(
        &self,
        xid: XID,
        table_id: OID,
        pred_key: &RangeData,
        pred_non_key: &Predicate,
        select: &VecSelTerm,
        opt_read: &OptRead,
    ) -> RS<Arc<dyn RSCursor>> {
        let meta_mgr = { self.lock_inner()?.meta_mgr.clone() };
        let desc = meta_mgr.get_table_by_id(table_id).await?;
        self.lock_inner()?.read_range(
            desc,
            xid,
            table_id,
            pred_key,
            pred_non_key,
            select,
            opt_read,
        )
    }

    async fn delete(
        &self,
        xid: XID,
        table_id: OID,
        pred_key: &VecDatum,
        pred_non_key: &Predicate,
        opt_delete: &OptDelete,
    ) -> RS<usize> {
        let meta_mgr = { self.lock_inner()?.meta_mgr.clone() };
        let desc = meta_mgr.get_table_by_id(table_id).await?;
        self.lock_inner()?
            .delete(desc, xid, table_id, pred_key, pred_non_key, opt_delete)
    }

    async fn insert(
        &self,
        xid: XID,
        table_id: OID,
        keys: &VecDatum,
        values: &VecDatum,
        opt_insert: &OptInsert,
    ) -> RS<()> {
        let meta_mgr = { self.lock_inner()?.meta_mgr.clone() };
        let desc = meta_mgr.get_table_by_id(table_id).await?;
        self.lock_inner()?
            .insert(desc, xid, table_id, keys, values, opt_insert)
    }
}

#[async_trait]
impl RSCursor for VecCursor {
    async fn next(&self) -> RS<Option<TupleRow>> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| m_error!(EC::InternalErr, "range cursor lock poisoned"))?;
        if inner.index >= inner.rows.len() {
            return Ok(None);
        }
        let row = inner.rows[inner.index].clone();
        inner.index += 1;
        Ok(Some(row))
    }
}

fn ensure_supported_predicate(predicate: &Predicate) -> RS<()> {
    match predicate {
        Predicate::CNF(items) | Predicate::DNF(items) if items.is_empty() => Ok(()),
        Predicate::CNF(items) | Predicate::DNF(items) => {
            let _ = items
                .iter()
                .flatten()
                .map(|(_oid, _filter): &(AttrIndex, Filter)| ())
                .count();
            Err(m_error!(
                EC::NotImplemented,
                "non-key predicates are not implemented in io_uring xcontract"
            ))
        }
    }
}

fn build_key_tuple(data: &VecDatum, desc: &TableDesc) -> RS<Vec<u8>> {
    build_tuple_for::<true>(data.data(), desc)
}

fn build_value_tuple(data: &VecDatum, desc: &TableDesc) -> RS<Vec<u8>> {
    build_tuple_for::<false>(data.data(), desc)
}

fn build_tuple_for<const IS_KEY: bool>(
    data: &Vec<(AttrIndex, DatBin)>,
    desc: &TableDesc,
) -> RS<Vec<u8>> {
    let mut vec_data = data.clone();
    let mut ok = true;
    vec_data.sort_by(|(id1, _), (id2, _)| {
        let (f1, f2) = (desc.get_attr(*id1), desc.get_attr(*id2));
        if f1.is_primary() != IS_KEY || f2.is_primary() != IS_KEY {
            ok = false;
        }
        f1.datum_index().cmp(&f2.datum_index())
    });
    if !ok {
        return Err(m_error!(EC::TupleErr));
    }
    let values: Vec<_> = vec_data.into_iter().map(|(_, v)| v).collect();
    let tuple_desc = if IS_KEY {
        desc.key_desc()
    } else {
        desc.value_desc()
    };
    if tuple_desc.field_count() != values.len() {
        return Err(m_error!(EC::TupleErr));
    }
    build_tuple(&values, tuple_desc)
}

fn build_bound_key(
    bound: &Bound<Vec<(AttrIndex, DatBin)>>,
    desc: &TableDesc,
) -> RS<Bound<&'static [u8]>> {
    match bound {
        Bound::Included(values) => {
            let tuple = build_key_tuple(&VecDatum::new(values.clone()), desc)?;
            Ok(Bound::Included(Box::leak(tuple.into_boxed_slice())))
        }
        Bound::Excluded(values) => {
            let tuple = build_key_tuple(&VecDatum::new(values.clone()), desc)?;
            Ok(Bound::Excluded(Box::leak(tuple.into_boxed_slice())))
        }
        Bound::Unbounded => Ok(Bound::Unbounded),
    }
}

fn project_selected_fields(
    desc: &TableDesc,
    key: &[u8],
    value: &[u8],
    select: &VecSelTerm,
) -> RS<Vec<DatBin>> {
    let mut tuple_ret = vec![];
    for i in select.vec() {
        let f = desc.get_attr(*i);
        let index = f.datum_index();
        let field_desc = if f.is_primary() {
            desc.key_desc().get_field_desc(index)
        } else {
            desc.value_desc().get_field_desc(index)
        };
        let src = if f.is_primary() { key } else { value };
        let slice = field_desc.get(src)?;
        tuple_ret.push(slice.to_vec());
    }
    Ok(tuple_ret)
}

fn apply_value_update(current: &TupleRaw, values: &VecDatum, desc: &TableDesc) -> RS<Vec<u8>> {
    let mut updated = current.clone();
    let mut data = values.data().clone();
    data.sort_by(|(id1, _), (id2, _)| id1.cmp(id2));
    for (id, dat) in data.iter() {
        let mut delta = vec![];
        update_tuple(*id as _, dat, desc.value_desc(), current, &mut delta)?;
        for item in delta {
            item.apply_to(&mut updated);
        }
    }
    Ok(updated)
}

fn single_put_batch(xid: u64, key: Vec<u8>, value: Vec<u8>) -> XLBatch {
    XLBatch {
        entries: vec![crate::wal::xl_entry::XLEntry {
            xid,
            ops: vec![
                crate::wal::xl_entry::TxOp::Begin,
                crate::wal::xl_entry::TxOp::Insert(crate::wal::xl_data_op::XLInsert {
                    table_id: 0,
                    tuple_id: 0,
                    key,
                    value,
                }),
                crate::wal::xl_entry::TxOp::Commit,
            ],
        }],
    }
}

fn single_delete_batch(xid: u64, key: Vec<u8>) -> XLBatch {
    XLBatch {
        entries: vec![crate::wal::xl_entry::XLEntry {
            xid,
            ops: vec![
                crate::wal::xl_entry::TxOp::Begin,
                crate::wal::xl_entry::TxOp::Delete(crate::wal::xl_data_op::XLDelete {
                    table_id: 0,
                    tuple_id: 0,
                    key,
                }),
                crate::wal::xl_entry::TxOp::Commit,
            ],
        }],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contract::schema_column::SchemaColumn;
    use crate::contract::table_info::TableInfo;
    use crate::wal::worker_log::{decode_frames, ChunkedWorkerLogBackend, WorkerLogLayout};
    use crate::wal::xl_data_op::XLInsert;
    use crate::wal::xl_entry::TxOp;
    use futures::executor::block_on;
    use mudu::common::id::gen_oid;
    use mudu_type::dat_type_id::DatTypeID;
    use mudu_type::dt_info::DTInfo;
    use std::collections::HashMap;
    use std::env::temp_dir;

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

    #[async_trait]
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

    fn datum(v: i32) -> Vec<u8> {
        v.to_be_bytes().to_vec()
    }

    fn key_row(v: i32) -> VecDatum {
        VecDatum::new(vec![(0, datum(v))])
    }

    fn value_row(v: i32) -> VecDatum {
        VecDatum::new(vec![(1, datum(v))])
    }

    #[test]
    fn relation_commit_log_round_trips() {
        let mgr = Arc::new(TestMetaMgr::new());
        let storage = WorkerStorage::new(
            mgr.clone(),
            0,
            std::env::temp_dir()
                .join(format!(
                    "xcontract_relation_log_{}",
                    mudu::common::id::gen_oid()
                ))
                .to_string_lossy()
                .to_string(),
        );
        let schema = test_schema();
        let table_id = schema.id();
        block_on(storage.create_table_async(&schema)).unwrap();
        let mut txm = WorkerTxManager::new(crate::server::worker_snapshot::WorkerSnapshot::new(
            9,
            vec![],
        ));
        storage
            .insert(table_id, b"k1".to_vec(), b"v1".to_vec(), &mut txm)
            .unwrap();
        storage.remove(table_id, b"k1", &mut txm).unwrap();
        let prepared = storage.prepare_commit(&txm).unwrap();

        assert_eq!(prepared.batch().entries.len(), 1);
        assert_eq!(prepared.batch().entries[0].xid, 9);
        assert!(matches!(prepared.batch().entries[0].ops[0], TxOp::Begin));
    }

    #[test]
    fn iouring_xcontract_commit_persists_relation_log() {
        let dir = temp_dir().join(format!("iouring_xcontract_log_{}", gen_oid()));
        let layout = WorkerLogLayout::new(dir, gen_oid(), 4096).unwrap();
        let log = ChunkedWorkerLogBackend::new(layout.clone()).unwrap();
        let meta_mgr = Arc::new(TestMetaMgr::new());
        let schema = test_schema();
        let table_id = schema.id();
        let contract = IoUringXContract::with_log(meta_mgr, Some(log));

        block_on(contract.create_table(0, &schema)).unwrap();
        let xid = block_on(contract.begin_tx()).unwrap();
        block_on(contract.insert(
            xid,
            table_id,
            &key_row(1),
            &value_row(10),
            &OptInsert::default(),
        ))
        .unwrap();
        block_on(contract.commit_tx(xid)).unwrap();

        let bytes = std::fs::read(layout.chunk_path(0)).unwrap();
        let frames = decode_frames(&bytes).unwrap();
        let decoded = crate::wal::xl_batch::decode_xl_batches(&frames).unwrap();
        assert_eq!(decoded.len(), 1);
        let insert = decoded[0].entries[0]
            .ops
            .iter()
            .find_map(|op| match op {
                TxOp::Insert(insert) => Some(insert),
                _ => None,
            })
            .unwrap();
        assert_eq!(insert.table_id, table_id);
        assert_eq!(
            insert.key,
            build_key_tuple(&key_row(1), &meta_table(&schema).unwrap()).unwrap()
        );
        assert_eq!(
            insert.value,
            build_value_tuple(&value_row(10), &meta_table(&schema).unwrap()).unwrap()
        );
    }

    #[test]
    fn iouring_xcontract_replay_restores_worker_kv_and_relation_rows() {
        let meta_mgr = Arc::new(TestMetaMgr::new());
        let schema = test_schema();
        let table_id = schema.id();
        let contract = IoUringXContract::with_log(meta_mgr, None);

        block_on(contract.create_table(0, &schema)).unwrap();
        let batch = XLBatch {
            entries: vec![crate::wal::xl_entry::XLEntry {
                xid: 11,
                ops: vec![
                    TxOp::Begin,
                    TxOp::Insert(XLInsert {
                        table_id: 0,
                        tuple_id: 0,
                        key: b"wk".to_vec(),
                        value: b"wv".to_vec(),
                    }),
                    TxOp::Insert(XLInsert {
                        table_id,
                        tuple_id: 0,
                        key: build_key_tuple(&key_row(3), &meta_table(&schema).unwrap()).unwrap(),
                        value: build_value_tuple(&value_row(30), &meta_table(&schema).unwrap())
                            .unwrap(),
                    }),
                    TxOp::Commit,
                ],
            }],
        };

        contract.replay_worker_log_batch(batch).unwrap();

        assert_eq!(contract.worker_get(b"wk").unwrap(), Some(b"wv".to_vec()));

        let xid = block_on(contract.begin_tx()).unwrap();
        let relation = block_on(contract.read_key(
            xid,
            table_id,
            &key_row(3),
            &VecSelTerm::new(vec![1]),
            &OptRead::default(),
        ))
        .unwrap();
        assert_eq!(relation, Some(vec![datum(30)]));
    }

    #[test]
    fn iouring_xcontract_replay_applies_worker_kv_delete() {
        let contract = IoUringXContract::with_worker_log(
            ChunkedWorkerLogBackend::new(
                WorkerLogLayout::new(
                    temp_dir().join(format!("iouring_xcontract_worker_log_{}", gen_oid())),
                    gen_oid(),
                    4096,
                )
                .unwrap(),
            )
            .unwrap(),
        );

        contract.worker_put(b"wk".to_vec(), b"wv".to_vec()).unwrap();
        let batch = XLBatch {
            entries: vec![crate::wal::xl_entry::XLEntry {
                xid: 7,
                ops: vec![
                    TxOp::Begin,
                    TxOp::Delete(crate::wal::xl_data_op::XLDelete {
                        table_id: 0,
                        tuple_id: 0,
                        key: b"wk".to_vec(),
                    }),
                    TxOp::Commit,
                ],
            }],
        };

        contract.replay_worker_log_batch(batch).unwrap();

        assert_eq!(contract.worker_get(b"wk").unwrap(), None);
    }

    fn meta_table(schema: &SchemaTable) -> RS<Arc<TableDesc>> {
        TableInfo::new(schema.clone())?.table_desc()
    }
}
