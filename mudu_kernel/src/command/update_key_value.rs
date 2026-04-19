use crate::contract::cmd_exec::CmdExec;
use crate::contract::meta_mgr::MetaMgr;
use crate::x_engine::api::{OptUpdate, Predicate, XContract};
use crate::x_engine::x_param::PUpdateKeyValue;
use async_trait::async_trait;
use mudu::common::result::RS;
use mudu::error::ec::EC as ER;
use mudu::m_error;
use mudu_utils::sync::a_mutex::AMutex;
use std::sync::Arc;

pub struct UpdateKeyValue {
    inner: AMutex<_UpdateKeyValue>,
}

struct _UpdateKeyValue {
    param: PUpdateKeyValue,
    x_contract: Arc<dyn XContract>,
    meta_mgr: Arc<dyn MetaMgr>,
    affected_rows: u64,
}

impl UpdateKeyValue {
    pub fn new(
        param: PUpdateKeyValue,
        x_contract: Arc<dyn XContract>,
        meta_mgr: Arc<dyn MetaMgr>,
    ) -> Self {
        Self {
            inner: AMutex::new(_UpdateKeyValue::new(param, x_contract, meta_mgr)),
        }
    }
}

impl _UpdateKeyValue {
    fn new(
        param: PUpdateKeyValue,
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

    async fn prepare(&self) -> RS<()> {
        let _ = self.meta_mgr.get_table_by_id(self.param.table_id).await?;
        if self.param.key.data().is_empty() {
            return Err(m_error!(ER::NoSuchElement, "update key is empty"));
        }
        if self.param.value.data().is_empty() {
            return Err(m_error!(ER::NoSuchElement, "update value is empty"));
        }
        Ok(())
    }

    async fn run(&mut self) -> RS<()> {
        // The SQL binder only emits key-equality updates for now.
        let updated = self
            .x_contract
            .update(
                self.param.tx_mgr.clone(),
                self.param.table_id,
                &self.param.key,
                &Predicate::CNF(Vec::new()),
                &self.param.value,
                &OptUpdate {},
            )
            .await?;
        self.affected_rows = updated as u64;
        Ok(())
    }

    fn affected_rows(&self) -> u64 {
        self.affected_rows
    }
}

#[async_trait]
impl CmdExec for UpdateKeyValue {
    async fn prepare(&self) -> RS<()> {
        let inner = self.inner.lock().await;
        inner.prepare().await
    }

    async fn run(&self) -> RS<()> {
        let mut inner = self.inner.lock().await;
        inner.run().await
    }

    async fn affected_rows(&self) -> RS<u64> {
        let inner = self.inner.lock().await;
        Ok(inner.affected_rows())
    }
}
