use mudu::common::result::RS;
use mudu::error::ec::EC;
use mudu::m_error;
use mudu_contract::database::sql_params::SQLParams;
use mudu_contract::database::sql_stmt::SQLStmt;
use mudu_contract::tuple::tuple_field_desc::TupleFieldDesc;
use mudu_contract::tuple::tuple_value::TupleValue;
use mudu_contract::tuple::typed_bin::TypedBin;
use mudu_type::datum::DatumDyn;
use sql_parser::ast::parser::SQLParser;
use sql_parser::ast::stmt_type::StmtType;
use std::sync::Arc;

use crate::contract::meta_mgr::MetaMgr;
use crate::contract::query_exec::QueryExec;
use crate::mudu_conn::mudu_result_set_async::MuduResultSetAsync;
use crate::sql::binder::Binder;
use crate::sql::bound_stmt::BoundStmt;
use crate::sql::describer::Describer;
use crate::sql::plan_ctx::PlanCtx;
use crate::sql::planner::Planner;
use crate::x_engine::api::XContract;
use crate::x_engine::tx_mgr::TxMgr;

pub struct MuduConnCore {
    meta_mgr: Arc<dyn MetaMgr>,
    parser: Arc<SQLParser>,
}

impl MuduConnCore {
    pub fn new(meta_mgr: Arc<dyn MetaMgr>) -> Self {
        Self {
            meta_mgr,
            parser: Arc::new(SQLParser::new()),
        }
    }

    pub fn parse_one(&self, sql: &dyn SQLStmt) -> RS<StmtType> {
        let stmt_list = self.parser.parse(&sql.to_sql_string())?;
        let mut stmts = stmt_list.into_stmts();
        if stmts.len() != 1 {
            return Err(m_error!(EC::ParseErr, "expected exactly one statement"));
        }
        Ok(stmts.remove(0))
    }

    pub fn parse_many(&self, sql: &dyn SQLStmt) -> RS<Vec<StmtType>> {
        Ok(self.parser.parse(&sql.to_sql_string())?.into_stmts())
    }

    pub async fn describe_stmt(&self, stmt: StmtType) -> RS<Arc<TupleFieldDesc>> {
        let desc = Describer::describe(self.meta_mgr.as_ref(), stmt).await?;
        Ok(Arc::new(desc))
    }

    pub async fn query(
        &self,
        stmt: StmtType,
        params: Box<dyn SQLParams>,
        tx_mgr: Arc<dyn TxMgr>,
        x_contract: Arc<dyn XContract>,
    ) -> RS<Arc<dyn mudu_contract::database::result_set::ResultSetAsync>> {
        let (rows, desc) = self.query_rows(stmt, params, tx_mgr, x_contract).await?;
        Ok(Arc::new(MuduResultSetAsync::from_rows(rows, desc)))
    }

    pub async fn query_rows(
        &self,
        stmt: StmtType,
        params: Box<dyn SQLParams>,
        tx_mgr: Arc<dyn TxMgr>,
        x_contract: Arc<dyn XContract>,
    ) -> RS<(Vec<TupleValue>, TupleFieldDesc)> {
        self.query_inner(stmt, params, tx_mgr, x_contract).await
    }

    pub async fn execute(
        &self,
        stmt: StmtType,
        params: Box<dyn SQLParams>,
        tx_mgr: Arc<dyn TxMgr>,
        x_contract: Arc<dyn XContract>,
    ) -> RS<u64> {
        self.execute_inner(stmt, params, tx_mgr, x_contract).await
    }

    async fn query_inner(
        &self,
        stmt: StmtType,
        params: Box<dyn SQLParams>,
        tx_mgr: Arc<dyn TxMgr>,
        x_contract: Arc<dyn XContract>,
    ) -> RS<(Vec<TupleValue>, TupleFieldDesc)> {
        let bound = Binder::new(self.meta_mgr.clone())
            .bind(stmt, params.as_ref())
            .await?;
        let BoundStmt::Query(bound_query) = bound else {
            return Err(m_error!(EC::TypeErr, "statement is not a query"));
        };
        let planner = Planner::new(PlanCtx {
            tx_mgr,
            meta_mgr: self.meta_mgr.clone(),
            x_contract,
        });
        let exec = planner.plan_query(bound_query).await?;
        query_exec_to_rows(exec).await
    }

    async fn execute_inner(
        &self,
        stmt: StmtType,
        params: Box<dyn SQLParams>,
        tx_mgr: Arc<dyn TxMgr>,
        x_contract: Arc<dyn XContract>,
    ) -> RS<u64> {
        let bound = Binder::new(self.meta_mgr.clone())
            .bind(stmt, params.as_ref())
            .await?;
        let BoundStmt::Command(bound_command) = bound else {
            return Err(m_error!(EC::TypeErr, "statement is not a command"));
        };
        let planner = Planner::new(PlanCtx {
            tx_mgr,
            meta_mgr: self.meta_mgr.clone(),
            x_contract,
        });
        let cmd = planner.plan_command(bound_command).await?;
        cmd.prepare().await?;
        cmd.run().await?;
        cmd.affected_rows().await
    }
}

pub async fn query_exec_to_rows(exec: Arc<dyn QueryExec>) -> RS<(Vec<TupleValue>, TupleFieldDesc)> {
    exec.open().await?;
    let desc = exec.tuple_desc()?;
    let mut rows = Vec::new();
    while let Some(row) = exec.next().await? {
        rows.push(tuple_field_to_value(row, &desc)?);
    }
    Ok((rows, desc))
}

fn tuple_field_to_value(
    row: mudu_contract::tuple::tuple_field::TupleField,
    desc: &TupleFieldDesc,
) -> RS<TupleValue> {
    let mut values = Vec::with_capacity(row.fields().len());
    for (index, field) in row.fields().iter().enumerate() {
        let datum_desc = &desc.fields()[index];
        let typed = TypedBin::new(datum_desc.dat_type_id(), field.clone());
        values.push(typed.to_value(datum_desc.dat_type())?);
    }
    Ok(TupleValue::from(values))
}
