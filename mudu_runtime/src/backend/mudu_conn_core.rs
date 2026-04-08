use crate::backend::mudu_result_set_async::MuduResultSetAsync;
use mudu::common::result::RS;
use mudu::common::xid::XID;
use mudu::error::ec::EC;
use mudu::m_error;
use mudu_contract::database::result_set::ResultSetAsync;
use mudu_contract::database::sql_params::SQLParams;
use mudu_contract::tuple::tuple_field_desc::TupleFieldDesc;
use mudu_contract::tuple::tuple_value::TupleValue;
use mudu_contract::tuple::typed_bin::TypedBin;
use mudu_kernel::contract::meta_mgr::MetaMgr;
use mudu_kernel::contract::query_exec::QueryExec;
use mudu_kernel::sql::binder::Binder;
use mudu_kernel::sql::bound_stmt::BoundStmt;
use mudu_kernel::sql::describer::Describer;
use mudu_kernel::sql::plan_ctx::PlanCtx;
use mudu_kernel::sql::planner::Planner;
use mudu_kernel::x_engine::api::XContract;
use mudu_type::datum::DatumDyn;
use sql_parser::ast::parser::SQLParser;
use sql_parser::ast::stmt_type::StmtType;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct MuduConnCore {
    meta_mgr: Arc<dyn MetaMgr>,
    x_contract: Arc<dyn XContract>,
    parser: Arc<SQLParser>,
    tx_state: Arc<Mutex<Option<XID>>>,
}

enum TxScope {
    Auto(XID),
    Existing,
}

impl MuduConnCore {
    pub fn new(meta_mgr: Arc<dyn MetaMgr>, x_contract: Arc<dyn XContract>) -> Self {
        Self {
            meta_mgr,
            x_contract,
            parser: Arc::new(SQLParser::new()),
            tx_state: Arc::new(Mutex::new(None)),
        }
    }

    pub fn parse_one(&self, sql: &dyn mudu_contract::database::sql_stmt::SQLStmt) -> RS<StmtType> {
        let stmt_list = self.parser.parse(&sql.to_sql_string())?;
        let mut stmts = stmt_list.into_stmts();
        if stmts.len() != 1 {
            return Err(m_error!(EC::ParseErr, "expected exactly one statement"));
        }
        Ok(stmts.remove(0))
    }

    pub fn parse_many(
        &self,
        sql: &dyn mudu_contract::database::sql_stmt::SQLStmt,
    ) -> RS<Vec<StmtType>> {
        Ok(self.parser.parse(&sql.to_sql_string())?.into_stmts())
    }

    pub async fn describe_stmt(&self, stmt: StmtType) -> RS<Arc<TupleFieldDesc>> {
        let desc = Describer::new(self.meta_mgr.clone()).describe(stmt).await?;
        Ok(Arc::new(desc))
    }

    pub async fn query(
        &self,
        stmt: StmtType,
        params: Box<dyn SQLParams>,
    ) -> RS<Arc<dyn ResultSetAsync>> {
        let (scope, xid) = self.enter_tx().await?;
        let result = self.query_inner(stmt, params, xid).await;
        match self.leave_tx(scope, result.is_ok()).await {
            Ok(()) => {}
            Err(e) => return Err(e),
        }
        result.map(|rs| Arc::new(rs) as Arc<dyn ResultSetAsync>)
    }

    pub async fn execute(&self, stmt: StmtType, params: Box<dyn SQLParams>) -> RS<u64> {
        let (scope, xid) = self.enter_tx().await?;
        let result = self.execute_inner(stmt, params, xid).await;
        self.leave_tx(scope, result.is_ok()).await?;
        result
    }

    async fn query_inner(
        &self,
        stmt: StmtType,
        params: Box<dyn SQLParams>,
        xid: XID,
    ) -> RS<MuduResultSetAsync> {
        let bound = Binder::new(self.meta_mgr.clone())
            .bind(stmt, params.as_ref())
            .await?;
        let BoundStmt::Query(bound_query) = bound else {
            return Err(m_error!(EC::TypeErr, "statement is not a query"));
        };
        let planner = Planner::new(PlanCtx {
            xid,
            meta_mgr: self.meta_mgr.clone(),
            x_contract: self.x_contract.clone(),
        });
        let exec = planner.plan_query(bound_query).await?;
        MuduResultSetAsync::from_query_exec(exec).await
    }

    async fn execute_inner(&self, stmt: StmtType, params: Box<dyn SQLParams>, xid: XID) -> RS<u64> {
        let bound = Binder::new(self.meta_mgr.clone())
            .bind(stmt, params.as_ref())
            .await?;
        let BoundStmt::Command(bound_command) = bound else {
            return Err(m_error!(EC::TypeErr, "statement is not a command"));
        };
        let planner = Planner::new(PlanCtx {
            xid,
            meta_mgr: self.meta_mgr.clone(),
            x_contract: self.x_contract.clone(),
        });
        let cmd = planner.plan_command(bound_command).await?;
        cmd.prepare().await?;
        cmd.run().await?;
        cmd.affected_rows().await
    }

    async fn enter_tx(&self) -> RS<(TxScope, XID)> {
        let guard = self.tx_state.lock().await;
        if let Some(xid) = *guard {
            return Ok((TxScope::Existing, xid));
        }
        drop(guard);
        let xid = self.x_contract.begin_tx().await?;
        Ok((TxScope::Auto(xid), xid))
    }

    async fn leave_tx(&self, scope: TxScope, success: bool) -> RS<()> {
        match scope {
            TxScope::Existing => Ok(()),
            TxScope::Auto(xid) => {
                if success {
                    self.x_contract.commit_tx(xid).await
                } else {
                    self.x_contract.abort_tx(xid).await
                }
            }
        }
    }

    pub async fn begin_tx(&self) -> RS<XID> {
        let mut guard = self.tx_state.lock().await;
        if let Some(xid) = *guard {
            return Ok(xid);
        }
        let xid = self.x_contract.begin_tx().await?;
        *guard = Some(xid);
        Ok(xid)
    }

    pub async fn commit_tx(&self) -> RS<()> {
        let mut guard = self.tx_state.lock().await;
        let xid = guard
            .take()
            .ok_or_else(|| m_error!(EC::NoSuchElement, "no active transaction"))?;
        drop(guard);
        self.x_contract.commit_tx(xid).await
    }

    pub async fn rollback_tx(&self) -> RS<()> {
        let mut guard = self.tx_state.lock().await;
        let xid = guard
            .take()
            .ok_or_else(|| m_error!(EC::NoSuchElement, "no active transaction"))?;
        drop(guard);
        self.x_contract.abort_tx(xid).await
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
