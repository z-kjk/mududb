use mudu::common::id::OID;
use std::cell::RefCell;
use std::collections::HashMap;

pub struct XLockMgr {
    lock: RefCell<HashMap<OID, HashMap<Vec<u8>, OID>>>,
}

impl XLockMgr {
    pub fn new() -> Self {
        Self {
            lock: Default::default(),
        }
    }

    pub fn try_lock_some(&self, oid: OID, table_keys: &Vec<(OID, Vec<u8>)>) -> bool {
        let mut lock = self.lock.borrow_mut();
        for (table_oid, key) in table_keys.iter() {
            let map = lock.entry(table_oid.clone()).or_default();
            if map.contains_key(key) {
                return false;
            } else {
                map.insert(key.clone(), oid);
            }
        }
        true
    }

    pub fn release(&self, oid: OID, table_keys: &Vec<(OID, Vec<u8>)>) {
        let mut lock = self.lock.borrow_mut();
        for (table_oid, key) in table_keys.iter() {
            let map = lock.entry(table_oid.clone()).or_default();
            if let Some(tx) = map.get(key) {
                if *tx == oid {
                    map.remove(key);
                }
            }
        }
    }
}
