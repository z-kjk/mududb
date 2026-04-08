use crate::backend::mudu_conn_core::MuduConnCore;
use async_trait::async_trait;
use mudu::common::result::RS;
use mudu_contract::database::prepared_stmt::PreparedStmt;
use mudu_contract::database::result_set::ResultSetAsync;
use mudu_contract::database::sql_params::SQLParams;
use mudu_contract::tuple::tuple_field_desc::TupleFieldDesc;
use sql_parser::ast::stmt_type::StmtType;
use std::sync::Arc;

pub struct MuduPreparedStmt {
    core: Arc<MuduConnCore>,
    stmt: StmtType,
    desc: Arc<TupleFieldDesc>,
}

impl MuduPreparedStmt {
    pub fn new(core: Arc<MuduConnCore>, stmt: StmtType, desc: Arc<TupleFieldDesc>) -> Self {
        Self { core, stmt, desc }
    }
}

#[async_trait]
impl PreparedStmt for MuduPreparedStmt {
    async fn query(&self, params: Box<dyn SQLParams>) -> RS<Arc<dyn ResultSetAsync>> {
        self.core.query(self.stmt.clone(), params).await
    }

    async fn execute(&self, params: Box<dyn SQLParams>) -> RS<u64> {
        self.core.execute(self.stmt.clone(), params).await
    }

    async fn desc(&self) -> RS<Arc<TupleFieldDesc>> {
        Ok(self.desc.clone())
    }

    async fn reset(&self) -> RS<()> {
        Ok(())
    }
}
