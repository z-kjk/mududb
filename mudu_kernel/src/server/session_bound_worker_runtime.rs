use crate::server::request_response_worker::{RequestResponseWorker, WorkerRuntimeRef};
use crate::server::routing::{SessionOpenConfig, SessionOpenTransferAction};
use crate::server::worker::IoUringWorker;
use crate::server::worker_local::{WorkerExecute, WorkerLocal, WorkerLocalRef};
use crate::server::worker_registry::WorkerRegistry;
use crate::server::worker_snapshot::KvItem;
use async_trait::async_trait;
use mudu::common::id::OID;
use mudu::common::result::RS;
use mudu::error::ec::EC;
use mudu::m_error;
use mudu_contract::protocol::{ProcedureInvokeRequest, ProcedureInvokeResponse};
use std::sync::Arc;

struct SessionBoundWorkerRuntime {
    worker: Arc<IoUringWorker>,
    current_session_id: OID,
}

pub(crate) fn new_session_bound_worker_runtime(
    worker: IoUringWorker,
    current_session_id: OID,
) -> WorkerRuntimeRef {
    Arc::new(SessionBoundWorkerRuntime {
        worker: Arc::new(worker),
        current_session_id,
    })
}

pub(crate) fn as_worker_local_ref(worker: WorkerRuntimeRef) -> WorkerLocalRef {
    worker
}

#[async_trait]
impl WorkerLocal for SessionBoundWorkerRuntime {
    async fn open_async(&self) -> RS<OID> {
        self.worker.open_session(self.current_session_id)
    }

    async fn open_argv_async(&self, worker_id: OID) -> RS<OID> {
        if worker_id == 0 || worker_id == self.worker.worker_id() {
            self.open_async().await
        } else {
            Err(m_error!(
                EC::NotImplemented,
                format!(
                    "worker-local open cannot move from worker {} to worker {}",
                    self.worker.worker_id(),
                    worker_id
                )
            ))
        }
    }

    async fn close_async(&self, session_id: OID) -> RS<()> {
        self.worker.close_session_by_id(session_id)
    }

    async fn execute_async(&self, session_id: OID, instruction: WorkerExecute) -> RS<()> {
        self.worker.execute_tx_async(session_id, instruction).await
    }

    async fn put_async(&self, session_id: OID, key: Vec<u8>, value: Vec<u8>) -> RS<()> {
        self.worker
            .put_in_session_async(session_id, key, value)
            .await
    }

    async fn delete_async(&self, session_id: OID, key: &[u8]) -> RS<()> {
        self.worker.delete_in_session_async(session_id, key).await
    }

    async fn get_async(&self, session_id: OID, key: &[u8]) -> RS<Option<Vec<u8>>> {
        self.worker.get_in_session(session_id, key)
    }

    async fn range_async(
        &self,
        session_id: OID,
        start_key: &[u8],
        end_key: &[u8],
    ) -> RS<Vec<KvItem>> {
        self.worker.range_in_session(session_id, start_key, end_key)
    }
}

#[async_trait]
impl RequestResponseWorker for SessionBoundWorkerRuntime {
    fn worker_index(&self) -> usize {
        self.worker.worker_index()
    }

    fn worker_id(&self) -> OID {
        self.worker.worker_id()
    }

    fn registry(&self) -> Arc<WorkerRegistry> {
        self.worker.registry().clone()
    }

    fn open_session_with_config(&self, conn_id: u64, config: SessionOpenConfig) -> RS<OID> {
        self.worker.open_session_with_config(conn_id, config)
    }

    fn prepare_connection_transfer(
        &self,
        conn_id: u64,
        action: Option<SessionOpenTransferAction>,
    ) -> RS<Vec<OID>> {
        self.worker.prepare_connection_transfer(conn_id, action)
    }

    fn close_session_for_connection(&self, conn_id: u64, session_id: OID) -> RS<bool> {
        self.worker.close_session(conn_id, session_id)
    }

    async fn handle_procedure_request(
        &self,
        conn_id: u64,
        request: &ProcedureInvokeRequest,
    ) -> RS<ProcedureInvokeResponse> {
        self.worker.handle_procedure_request(conn_id, request).await
    }
}
