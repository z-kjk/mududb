use crate::contract::cmd_exec::CmdExec;
use crate::contract::meta_mgr::MetaMgr;
use crate::x_engine::api::{OptInsert, XContract};
use crate::x_engine::x_param::PInsertKeyValue;
use async_trait::async_trait;
use mudu::common::result::RS;
use mudu::error::ec::EC as ER;
use mudu::m_error;
use mudu_utils::sync::a_mutex::AMutex;
use mudu_utils::task_trace;
use std::sync::Arc;

pub struct InsertKeyValue {
    inner: AMutex<_InsertKeyValue>,
}

struct _InsertKeyValue {
    param: PInsertKeyValue,
    x_contract: Arc<dyn XContract>,
    meta_mgr: Arc<dyn MetaMgr>,
    affected_rows: u64,
}

impl InsertKeyValue {
    pub fn new(
        param: PInsertKeyValue,
        x_contract: Arc<dyn XContract>,
        meta_mgr: Arc<dyn MetaMgr>,
    ) -> Self {
        Self {
            inner: AMutex::new(_InsertKeyValue::new(param, x_contract, meta_mgr)),
        }
    }
}

impl _InsertKeyValue {
    fn new(
        param: PInsertKeyValue,
        x_contract: Arc<dyn XContract>,
        meta_mgr: Arc<dyn MetaMgr>,
    ) -> Self {
        Self {
            param,
            x_contract,
            meta_mgr,
            affected_rows: 0,
        }
    }
}

#[async_trait]
impl CmdExec for InsertKeyValue {
    async fn prepare(&self) -> RS<()> {
        let inner = self.inner.lock().await;
        inner.prepare().await
    }

    async fn run(&self) -> RS<()> {
        let mut inner = self.inner.lock().await;
        inner.insert().await
    }

    async fn affected_rows(&self) -> RS<u64> {
        let inner = self.inner.lock().await;
        Ok(inner.affected_rows())
    }
}

impl _InsertKeyValue {
    async fn prepare(&self) -> RS<()> {
        let _ = self.meta_mgr.get_table_by_id(self.param.table_id).await?;
        if self.param.key.data().is_empty() {
            return Err(m_error!(ER::NoSuchElement, "key is empty"));
        }
        Ok(())
    }

    async fn insert(&mut self) -> RS<()> {
        task_trace!();
        self.x_contract
            .insert(
                self.param.xid,
                self.param.table_id,
                &self.param.key,
                &self.param.value,
                &OptInsert::default(),
            )
            .await?;
        self.affected_rows = 1;
        Ok(())
    }

    fn affected_rows(&self) -> u64 {
        self.affected_rows
    }
}
