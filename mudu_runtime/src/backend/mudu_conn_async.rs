use crate::backend::mudu_conn_core::MuduConnCore;
use crate::backend::mudu_prepared_stmt::MuduPreparedStmt;
use async_trait::async_trait;
use mudu::common::result::RS;
use mudu::common::xid::XID;
use mudu_contract::database::db_conn::DBConnAsync;
use mudu_contract::database::prepared_stmt::PreparedStmt;
use mudu_contract::database::result_set::ResultSetAsync;
use mudu_contract::database::sql_params::SQLParams;
use mudu_contract::database::sql_stmt::SQLStmt;
use mudu_kernel::contract::meta_mgr::MetaMgr;
use mudu_kernel::x_engine::api::XContract;
use std::sync::Arc;

pub struct MuduConnAsync {
    core: Arc<MuduConnCore>,
}

impl MuduConnAsync {
    pub fn new(meta_mgr: Arc<dyn MetaMgr>, x_contract: Arc<dyn XContract>) -> Self {
        Self {
            core: Arc::new(MuduConnCore::new(meta_mgr, x_contract)),
        }
    }
}

#[async_trait]
impl DBConnAsync for MuduConnAsync {
    async fn prepare(&self, stmt: Box<dyn SQLStmt>) -> RS<Arc<dyn PreparedStmt>> {
        let parsed = self.core.parse_one(stmt.as_ref())?;
        let desc = self.core.describe_stmt(parsed.clone()).await?;
        Ok(Arc::new(MuduPreparedStmt::new(
            self.core.clone(),
            parsed,
            desc,
        )))
    }

    async fn exec_silent(&self, sql_text: String) -> RS<()> {
        let stmts = self.core.parse_many(&sql_text)?;
        for stmt in stmts {
            match stmt {
                sql_parser::ast::stmt_type::StmtType::Select(_) => {
                    let _ = self.core.query(stmt, Box::new(())).await?;
                }
                sql_parser::ast::stmt_type::StmtType::Command(_) => {
                    let _ = self.core.execute(stmt, Box::new(())).await?;
                }
            }
        }
        Ok(())
    }

    async fn begin_tx(&self) -> RS<XID> {
        self.core.begin_tx().await
    }

    async fn rollback_tx(&self) -> RS<()> {
        self.core.rollback_tx().await
    }

    async fn commit_tx(&self) -> RS<()> {
        self.core.commit_tx().await
    }

    async fn query(
        &self,
        sql: Box<dyn SQLStmt>,
        param: Box<dyn SQLParams>,
    ) -> RS<Arc<dyn ResultSetAsync>> {
        let parsed = self.core.parse_one(sql.as_ref())?;
        self.core.query(parsed, param).await
    }

    async fn execute(&self, sql: Box<dyn SQLStmt>, param: Box<dyn SQLParams>) -> RS<u64> {
        let parsed = self.core.parse_one(sql.as_ref())?;
        self.core.execute(parsed, param).await
    }

    async fn batch(&self, sql: Box<dyn SQLStmt>, param: Box<dyn SQLParams>) -> RS<u64> {
        if param.size() != 0 {
            return Err(mudu::m_error!(
                mudu::error::ec::EC::NotImplemented,
                "batch with parameters is not implemented"
            ));
        }
        let stmts = self.core.parse_many(sql.as_ref())?;
        let mut total = 0;
        for stmt in stmts {
            total += self.core.execute(stmt, Box::new(())).await?;
        }
        Ok(total)
    }
}
