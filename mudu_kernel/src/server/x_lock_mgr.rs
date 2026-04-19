use mudu::common::id::OID;
use crate::x_engine::tx_mgr::PhysicalRelationId;
use std::collections::HashMap;
use std::sync::Mutex;

pub struct XLockMgr {
    lock: Mutex<HashMap<PhysicalRelationId, HashMap<Vec<u8>, OID>>>,
}

impl XLockMgr {
    pub fn new() -> Self {
        Self {
            lock: Mutex::new(HashMap::new()),
        }
    }

    pub fn try_lock_some(&self, oid: OID, table_keys: &Vec<(PhysicalRelationId, Vec<u8>)>) -> bool {
        let mut lock = self.lock.lock().unwrap();
        for (relation_id, key) in table_keys.iter() {
            let map = lock.entry(*relation_id).or_default();
            if map.contains_key(key) {
                return false;
            } else {
                map.insert(key.clone(), oid);
            }
        }
        true
    }

    pub fn release(&self, oid: OID, table_keys: &Vec<(PhysicalRelationId, Vec<u8>)>) {
        let mut lock = self.lock.lock().unwrap();
        for (relation_id, key) in table_keys.iter() {
            let map = lock.entry(*relation_id).or_default();
            if let Some(tx) = map.get(key) {
                if *tx == oid {
                    map.remove(key);
                }
            }
        }
    }
}
