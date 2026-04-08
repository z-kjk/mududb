use async_trait::async_trait;
use mudu::common::result::RS;
use mudu_contract::protocol::{decode_procedure_invoke_request, Frame, MessageType};

use crate::server::async_func_task::HandleResult;
use crate::server::message_dispatcher::MessageHandler;
use crate::server::request_ctx::RequestCtx;

pub(in crate::server) struct ProcedureInvokeHandler;

#[async_trait]
impl MessageHandler for ProcedureInvokeHandler {
    fn message_type(&self) -> MessageType {
        MessageType::ProcedureInvoke
    }

    async fn handle(&self, ctx: &RequestCtx, frame: &Frame) -> RS<HandleResult> {
        let request = decode_procedure_invoke_request(frame)?;
        ctx.invoke_procedure(request).await
    }
}
