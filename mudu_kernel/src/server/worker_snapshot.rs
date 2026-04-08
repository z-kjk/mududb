use crate::contract::snapshot::{RunningXList, Snapshot};
use mudu::common::result::RS;
use mudu::error::ec::EC;
use mudu::m_error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvItem {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerSnapshot {
    xid: u64,
    running: Vec<u64>,
}

#[derive(Default)]
pub struct WorkerSnapshotMgr {
    next_ts: u64,
    running: Vec<u64>,
}

impl WorkerSnapshot {
    pub fn new(xid: u64, running: Vec<u64>) -> Self {
        Self { xid, running }
    }

    pub fn xid(&self) -> u64 {
        self.xid
    }

    pub fn is_visible(&self, version_xid: u64) -> bool {
        is_visible_to_snapshot(version_xid, self)
    }

    pub fn to_snapshot(&self) -> Snapshot {
        Snapshot::from(RunningXList::new(self.xid, self.running.clone()))
    }
}

impl WorkerSnapshotMgr {
    pub fn begin_tx(&mut self) -> WorkerSnapshot {
        self.next_ts += 1;
        let xid = self.next_ts;
        let snapshot = WorkerSnapshot {
            xid,
            running: self.running.clone(),
        };
        insert_sorted_unique(&mut self.running, xid);
        snapshot
    }

    pub fn alloc_committed_ts(&mut self) -> u64 {
        self.next_ts += 1;
        self.next_ts
    }

    pub fn observe_committed_ts(&mut self, xid: u64) {
        if self.next_ts < xid {
            self.next_ts = xid;
        }
    }

    pub fn end_tx(&mut self, xid: u64) -> RS<()> {
        match self.running.binary_search(&xid) {
            Ok(index) => {
                self.running.remove(index);
                Ok(())
            }
            Err(_) => Err(m_error!(
                EC::NoSuchElement,
                format!("transaction {} is not active", xid)
            )),
        }
    }
}

fn is_visible_to_snapshot(version_xid: u64, snapshot: &WorkerSnapshot) -> bool {
    if version_xid > snapshot.xid {
        return false;
    }
    snapshot.running.binary_search(&version_xid).is_err()
}

fn insert_sorted_unique(values: &mut Vec<u64>, value: u64) {
    match values.binary_search(&value) {
        Ok(_) => {}
        Err(index) => values.insert(index, value),
    }
}
