use crate::contract::meta_mgr::MetaMgr;
use crate::contract::table_desc::TableDesc;
use crate::executor::project_tuple_desc;
use mudu::common::result::RS;
use mudu::error::ec::EC as ER;
use mudu::m_error;
use mudu_contract::tuple::tuple_field_desc::TupleFieldDesc;
use sql_parser::ast::stmt_type::StmtType;
use std::sync::Arc;

pub struct Describer {
    meta_mgr: Arc<dyn MetaMgr>,
}

impl Describer {
    pub fn new(meta_mgr: Arc<dyn MetaMgr>) -> Self {
        Self { meta_mgr }
    }

    pub async fn describe(&self, stmt: StmtType) -> RS<TupleFieldDesc> {
        match stmt {
            StmtType::Select(stmt) => self.describe_select(stmt).await,
            StmtType::Command(_) => Ok(TupleFieldDesc::new(Vec::new())),
        }
    }

    async fn describe_select(
        &self,
        stmt: sql_parser::ast::stmt_select::StmtSelect,
    ) -> RS<TupleFieldDesc> {
        let table_desc = self.get_table_by_name(stmt.get_table_reference()).await?;
        let select_attrs = self.select_attrs(&table_desc, stmt.get_select_term_list())?;
        Ok(project_tuple_desc(
            &table_desc,
            &crate::x_engine::api::VecSelTerm::new(select_attrs),
        ))
    }

    fn select_attrs(
        &self,
        table_desc: &TableDesc,
        terms: &[sql_parser::ast::select_term::SelectTerm],
    ) -> RS<Vec<usize>> {
        terms
            .iter()
            .map(|term| self.attr_index_by_name(table_desc, term.field().name()))
            .collect()
    }

    fn attr_index_by_name(&self, table_desc: &TableDesc, name: &str) -> RS<usize> {
        let total = table_desc.key_info().len() + table_desc.value_info().len();
        (0..total)
            .find(|attr| table_desc.get_attr(*attr).name() == name)
            .ok_or_else(|| m_error!(ER::NoSuchElement, format!("cannot find column {}", name)))
    }

    async fn get_table_by_name(&self, name: &String) -> RS<Arc<TableDesc>> {
        self.meta_mgr
            .get_table_by_name(name)
            .await?
            .ok_or_else(|| m_error!(ER::NoSuchElement, format!("cannot find table {}", name)))
    }
}
