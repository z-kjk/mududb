use crate::contract::cmd_exec::CmdExec;
use crate::contract::meta_mgr::MetaMgr;
use crate::x_engine::api::XContract;
use crate::x_engine::x_param::PCreateTable;
use async_trait::async_trait;
use mudu::common::result::RS;
use mudu::error::ec::EC as ER;
use mudu::m_error;
use mudu_utils::sync::a_mutex::AMutex;
use mudu_utils::task_trace;
use std::sync::Arc;

pub struct CreateTable {
    inner: AMutex<_InnerCreateTable>,
}

struct _InnerCreateTable {
    param: PCreateTable,
    x_contract: Arc<dyn XContract>,
    meta_mgr: Arc<dyn MetaMgr>,
}

impl CreateTable {
    pub fn new(
        param: PCreateTable,
        x_contract: Arc<dyn XContract>,
        meta_mgr: Arc<dyn MetaMgr>,
    ) -> Self {
        Self {
            inner: AMutex::new(_InnerCreateTable::new(param, x_contract, meta_mgr)),
        }
    }
}

#[async_trait]
impl CmdExec for CreateTable {
    async fn prepare(&self) -> RS<()> {
        task_trace!();
        let inner = self.inner.lock().await;
        inner.prepare().await
    }

    async fn run(&self) -> RS<()> {
        task_trace!();
        let mut inner = self.inner.lock().await;
        inner.run().await
    }

    async fn affected_rows(&self) -> RS<u64> {
        task_trace!();
        Ok(0)
    }
}

impl _InnerCreateTable {
    fn new(
        param: PCreateTable,
        x_contract: Arc<dyn XContract>,
        meta_mgr: Arc<dyn MetaMgr>,
    ) -> Self {
        Self {
            param,
            x_contract,
            meta_mgr,
        }
    }

    async fn prepare(&self) -> RS<()> {
        let table_name = self.param.schema.table_name().clone();
        if self
            .meta_mgr
            .get_table_by_name(&table_name)
            .await?
            .is_some()
        {
            return Err(m_error!(
                ER::ExistingSuchElement,
                format!("table {} already exists", table_name)
            ));
        }
        Ok(())
    }

    async fn run(&mut self) -> RS<()> {
        task_trace!();
        self.x_contract
            .create_table(self.param.xid, &self.param.schema)
            .await
    }
}
