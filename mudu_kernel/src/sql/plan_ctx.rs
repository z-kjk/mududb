use crate::contract::meta_mgr::MetaMgr;
use crate::x_engine::api::XContract;
use mudu::common::xid::XID;
use std::sync::Arc;

#[derive(Clone)]
pub struct PlanCtx {
    pub xid: XID,
    pub meta_mgr: Arc<dyn MetaMgr>,
    pub x_contract: Arc<dyn XContract>,
}
