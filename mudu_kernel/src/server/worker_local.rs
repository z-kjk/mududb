use crate::server::worker_snapshot::KvItem;
use async_trait::async_trait;
use mudu::common::id::OID;
use mudu::common::result::RS;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerExecute {
    BeginTx,
    CommitTx,
    RollbackTx,
}

#[async_trait]
pub trait WorkerLocal: Send + Sync {
    async fn open_async(&self) -> RS<OID>;

    async fn open_argv_async(&self, worker_id: OID) -> RS<OID> {
        if worker_id == 0 {
            self.open_async().await
        } else {
            Err(mudu::m_error!(
                mudu::error::ec::EC::NotImplemented,
                format!("worker-local open on worker {} is not supported", worker_id)
            ))
        }
    }

    async fn close_async(&self, session_id: OID) -> RS<()>;

    async fn execute_async(&self, session_id: OID, instruction: WorkerExecute) -> RS<()>;

    async fn put_async(&self, session_id: OID, key: Vec<u8>, value: Vec<u8>) -> RS<()>;

    async fn delete_async(&self, session_id: OID, key: &[u8]) -> RS<()>;

    async fn get_async(&self, session_id: OID, key: &[u8]) -> RS<Option<Vec<u8>>>;

    async fn range_async(
        &self,
        session_id: OID,
        start_key: &[u8],
        end_key: &[u8],
    ) -> RS<Vec<KvItem>>;
}

pub type WorkerLocalRef = Arc<dyn WorkerLocal + Send + Sync>;
