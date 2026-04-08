use crate::backend::mudu_conn_core::query_exec_to_rows;
use async_trait::async_trait;
use mudu::common::result::RS;
use mudu_contract::database::result_set::ResultSetAsync;
use mudu_contract::tuple::tuple_field_desc::TupleFieldDesc;
use mudu_contract::tuple::tuple_value::TupleValue;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct MuduResultSetAsync {
    desc: Arc<TupleFieldDesc>,
    inner: Mutex<ResultRows>,
}

struct ResultRows {
    rows: Vec<TupleValue>,
    index: usize,
}

impl MuduResultSetAsync {
    pub async fn from_query_exec(
        exec: Arc<dyn mudu_kernel::contract::query_exec::QueryExec>,
    ) -> RS<Self> {
        let (rows, desc) = query_exec_to_rows(exec).await?;
        Ok(Self {
            desc: Arc::new(desc),
            inner: Mutex::new(ResultRows { rows, index: 0 }),
        })
    }
}

#[async_trait]
impl ResultSetAsync for MuduResultSetAsync {
    async fn next(&self) -> RS<Option<TupleValue>> {
        let mut inner = self.inner.lock().await;
        if inner.index >= inner.rows.len() {
            return Ok(None);
        }
        let index = inner.index;
        let row = inner.rows.remove(index);
        Ok(Some(row))
    }

    fn desc(&self) -> &TupleFieldDesc {
        self.desc.as_ref()
    }
}
