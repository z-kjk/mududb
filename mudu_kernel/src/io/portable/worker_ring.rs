use std::sync::Arc;

use mudu::common::result::RS;
use mudu::error::ec::EC;
use mudu::m_error;

pub(crate) struct WorkerLocalRing;

pub(crate) enum WorkerRingOp {
    File(crate::io::file::FileIoRequest),
}

impl WorkerLocalRing {
    pub(crate) fn register(&self, _op: WorkerRingOp) -> RS<u64> {
        Err(m_error!(
            EC::NotImplemented,
            "worker ring is only available on linux"
        ))
    }
}

pub(crate) fn set_current_worker_ring(_ring: Arc<WorkerLocalRing>) {}

pub(crate) fn unset_current_worker_ring() {}

pub(crate) fn has_current_worker_ring() -> bool {
    false
}

pub(crate) fn current_ring() -> &'static WorkerLocalRing {
    panic!("worker ring is only available on linux")
}

pub(crate) fn with_current_ring<F, R>(_f: F) -> RS<R>
where
    F: FnOnce(&Arc<WorkerLocalRing>) -> RS<R>,
{
    Err(m_error!(
        EC::NotImplemented,
        "worker ring is only available on linux"
    ))
}
