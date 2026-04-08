use crate::server::worker_snapshot::WorkerSnapshot;
use crate::wal::xl_batch::XLBatch;
use crate::wal::xl_data_op::{XLDelete, XLInsert};
use crate::wal::xl_entry::{TxOp, XLEntry};
use mudu::common::id::OID;
use std::collections::BTreeMap;

pub struct WorkerTxManager {
    snapshot: WorkerSnapshot,
    staged_puts: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    staged_relation_ops: BTreeMap<OID, BTreeMap<Vec<u8>, Option<Vec<u8>>>>,
    write_ops: Vec<(OID, Vec<u8>)>,
    log_buffer: Vec<TxOp>,
}

impl WorkerTxManager {
    pub fn new(snapshot: WorkerSnapshot) -> Self {
        Self {
            snapshot,
            staged_puts: BTreeMap::new(),
            staged_relation_ops: BTreeMap::new(),
            write_ops: vec![],
            log_buffer: Vec::new(),
        }
    }

    pub fn xid(&self) -> u64 {
        self.snapshot.xid()
    }

    pub fn snapshot(&self) -> &WorkerSnapshot {
        &self.snapshot
    }

    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.staged_puts.insert(key.clone(), Some(value.clone()));
        self.log_buffer.push(TxOp::Insert(XLInsert {
            table_id: 0,
            tuple_id: 0,
            key,
            value,
        }));
    }

    pub fn delete(&mut self, key: Vec<u8>) {
        self.staged_puts.insert(key.clone(), None);
        self.log_buffer.push(TxOp::Delete(XLDelete {
            table_id: 0,
            tuple_id: 0,
            key,
        }));
    }

    pub fn get(&self, key: &[u8]) -> Option<Option<Vec<u8>>> {
        self.staged_puts.get(key).cloned()
    }

    pub fn put_relation(&mut self, oid: OID, key: Vec<u8>, value: Vec<u8>) {
        self.staged_relation_ops
            .entry(oid)
            .or_default()
            .insert(key.clone(), Some(value.clone()));
        self.log_buffer.push(TxOp::Insert(XLInsert {
            table_id: oid,
            tuple_id: 0,
            key,
            value,
        }));
    }

    pub fn delete_relation(&mut self, oid: OID, key: Vec<u8>) {
        self.staged_relation_ops
            .entry(oid)
            .or_default()
            .insert(key.clone(), None);
        self.log_buffer.push(TxOp::Delete(XLDelete {
            table_id: oid,
            tuple_id: 0,
            key,
        }));
    }

    pub fn get_relation(&self, oid: OID, key: &[u8]) -> Option<Option<Vec<u8>>> {
        self.staged_relation_ops
            .get(&oid)
            .and_then(|rows| rows.get(key).map(|value| value.clone()))
    }

    pub fn staged_relation_items_in_range(
        &self,
        oid: OID,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
        self.staged_relation_ops
            .get(&oid)
            .map(|rows| {
                rows.iter()
                    .filter(|(key, _)| is_key_in_range(key, start_key, end_key))
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn staged_relation_ops(&self) -> &BTreeMap<OID, BTreeMap<Vec<u8>, Option<Vec<u8>>>> {
        &self.staged_relation_ops
    }

    #[allow(dead_code)]
    pub fn drain_relation_ops(&mut self) -> BTreeMap<OID, BTreeMap<Vec<u8>, Option<Vec<u8>>>> {
        std::mem::take(&mut self.staged_relation_ops)
    }

    pub fn staged_items_in_range(
        &self,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
        self.staged_puts
            .iter()
            .filter(|(key, _)| is_key_in_range(key, start_key, end_key))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect()
    }

    pub fn staged_put_items(&self) -> BTreeMap<Vec<u8>, Option<Vec<u8>>> {
        self.staged_puts.clone()
    }

    pub fn write_ops(&self) -> &Vec<(OID, Vec<u8>)> {
        &self.write_ops
    }

    pub fn build_write_ops(&mut self) {
        for (k, _) in self.staged_puts.iter() {
            self.write_ops.push((0, k.clone()));
        }

        for (oid, ops) in self.staged_relation_ops.iter() {
            for (k, _) in ops.iter() {
                self.write_ops.push((*oid, k.clone()));
            }
        }
        self.write_ops.sort();
    }

    pub fn xl_batch(&self) -> XLBatch {
        let xid = self.snapshot.xid();
        let mut ops = Vec::with_capacity(self.log_buffer.len() + 2);
        ops.push(TxOp::Begin);
        ops.extend(self.log_buffer.clone());
        ops.push(TxOp::Commit);
        XLBatch {
            entries: vec![XLEntry { xid, ops }],
        }
    }

    pub fn into_xl_batch(self) -> XLBatch {
        let xid = self.snapshot.xid();
        let mut ops = Vec::with_capacity(self.log_buffer.len() + 2);
        ops.push(TxOp::Begin);
        ops.extend(self.log_buffer);
        ops.push(TxOp::Commit);
        XLBatch {
            entries: vec![XLEntry { xid, ops }],
        }
    }
}

fn is_key_in_range(key: &[u8], start_key: &[u8], end_key: &[u8]) -> bool {
    key >= start_key && (end_key.is_empty() || key < end_key)
}
