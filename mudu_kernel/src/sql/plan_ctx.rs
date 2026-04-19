use crate::contract::meta_mgr::MetaMgr;
use crate::x_engine::api::XContract;
use crate::x_engine::tx_mgr::TxMgr;
use std::sync::Arc;

#[derive(Clone)]
pub struct PlanCtx {
    pub tx_mgr: Arc<dyn TxMgr>,
    pub meta_mgr: Arc<dyn MetaMgr>,
    pub x_contract: Arc<dyn XContract>,
}
