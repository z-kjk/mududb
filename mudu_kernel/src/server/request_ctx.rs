use mudu::common::id::OID;
use mudu::common::result::RS;
use mudu_contract::protocol::{
    encode_get_response, encode_procedure_invoke_response, encode_put_response,
    encode_range_scan_response, encode_session_close_response, encode_session_create_response,
    GetResponse, KeyValue, ProcedureInvokeResponse, PutResponse, RangeScanResponse,
    SessionCloseResponse, SessionCreateResponse,
};
use std::sync::Arc;

use crate::server::async_func_task::HandleResult;
use crate::server::request_response_worker::WorkerRuntimeRef;
use crate::server::routing::parse_session_open_config;
use crate::server::routing::{SessionOpenConfig, SessionOpenTransferAction};
use crate::server::worker_registry::WorkerRegistry;

#[derive(Clone)]
pub(in crate::server) struct RequestCtx {
    worker: WorkerRuntimeRef,
    conn_id: u64,
    request_id: u64,
}

impl RequestCtx {
    pub(in crate::server) fn new(worker: WorkerRuntimeRef, conn_id: u64, request_id: u64) -> Self {
        Self {
            worker,
            conn_id,
            request_id,
        }
    }

    #[allow(dead_code)]
    pub(in crate::server) fn conn_id(&self) -> u64 {
        self.conn_id
    }

    #[allow(dead_code)]
    pub(in crate::server) fn request_id(&self) -> u64 {
        self.request_id
    }

    pub(in crate::server) fn worker_index(&self) -> usize {
        self.worker.worker_index()
    }

    pub(in crate::server) fn worker_id(&self) -> OID {
        self.worker.worker_id()
    }

    pub(in crate::server) fn registry(&self) -> Arc<WorkerRegistry> {
        self.worker.registry()
    }

    pub(in crate::server) fn parse_session_open_config(
        &self,
        config_json: Option<&str>,
    ) -> RS<SessionOpenConfig> {
        parse_session_open_config(
            config_json,
            self.worker_index(),
            self.worker_id(),
            self.registry().as_ref(),
        )
    }

    pub(in crate::server) async fn get(&self, session_id: OID, key: &[u8]) -> RS<HandleResult> {
        let value = self.worker.get_async(session_id, key).await?;
        Ok(HandleResult::Response(encode_get_response(
            self.request_id,
            &GetResponse::new(value),
        )?))
    }

    pub(in crate::server) async fn put(
        &self,
        session_id: OID,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> RS<HandleResult> {
        self.worker.put_async(session_id, key, value).await?;
        Ok(HandleResult::Response(encode_put_response(
            self.request_id,
            &PutResponse::new(true),
        )?))
    }

    pub(in crate::server) async fn invoke_procedure(
        &self,
        request: mudu_contract::protocol::ProcedureInvokeRequest,
    ) -> RS<HandleResult> {
        let response = self
            .worker
            .handle_procedure_request(self.conn_id, &request)
            .await?;
        Ok(HandleResult::Response(encode_procedure_invoke_response(
            self.request_id,
            &ProcedureInvokeResponse::new(response.into_result()),
        )?))
    }

    pub(in crate::server) async fn range_scan(
        &self,
        session_id: OID,
        start_key: &[u8],
        end_key: &[u8],
    ) -> RS<HandleResult> {
        let items = self
            .worker
            .range_async(session_id, start_key, end_key)
            .await?;
        Ok(HandleResult::Response(encode_range_scan_response(
            self.request_id,
            &RangeScanResponse::new(
                items
                    .into_iter()
                    .map(|item| KeyValue::new(item.key, item.value))
                    .collect(),
            ),
        )?))
    }

    pub(in crate::server) async fn session_create(
        &self,
        config: SessionOpenConfig,
    ) -> RS<HandleResult> {
        if config.target_worker_index() == self.worker.worker_index() {
            Ok(HandleResult::Response(encode_session_create_response(
                self.request_id,
                &SessionCreateResponse::new(
                    self.worker.open_session_with_config(self.conn_id, config)?,
                ),
            )?))
        } else {
            let action = SessionOpenTransferAction::new(self.request_id, config);
            let session_ids = self
                .worker
                .prepare_connection_transfer(self.conn_id, Some(action))?;
            Ok(HandleResult::Transfer(
                crate::server::async_func_task::SessionTransferDispatch::new(
                    config.target_worker_index(),
                    session_ids,
                    action,
                ),
            ))
        }
    }

    pub(in crate::server) async fn session_close(&self, session_id: OID) -> RS<HandleResult> {
        Ok(HandleResult::Response(encode_session_close_response(
            self.request_id,
            &SessionCloseResponse::new(
                self.worker
                    .close_session_for_connection(self.conn_id, session_id)?,
            ),
        )?))
    }
}
