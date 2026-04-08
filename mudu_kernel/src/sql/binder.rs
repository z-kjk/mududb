use crate::contract::meta_mgr::MetaMgr;
use crate::contract::schema_column::SchemaColumn;
use crate::contract::schema_table::SchemaTable;
use crate::contract::table_desc::TableDesc;
use crate::executor::project_tuple_desc;
use crate::sql::bound_stmt::{
    BoundCommand, BoundCopyFrom, BoundCopyTo, BoundCreateTable, BoundDelete, BoundDropTable,
    BoundInsert, BoundPredicate, BoundQuery, BoundSelect, BoundStmt, BoundUpdate,
};
use crate::sql::copy_layout::CopyLayout;
use crate::sql::value_codec::ValueCodec;
use mudu::common::result::RS;
use mudu::error::ec::EC as ER;
use mudu::m_error;
use mudu_contract::database::sql_params::SQLParams;
use mudu_type::dt_info::DTInfo;
use sql_parser::ast::expr_compare::ExprCompare;
use sql_parser::ast::expr_item::{ExprItem, ExprValue};
use sql_parser::ast::expr_operator::ValueCompare;
use sql_parser::ast::stmt_create_table::StmtCreateTable;
use sql_parser::ast::stmt_delete::StmtDelete;
use sql_parser::ast::stmt_drop_table::StmtDropTable;
use sql_parser::ast::stmt_insert::StmtInsert;
use sql_parser::ast::stmt_type::{StmtCommand, StmtType};
use sql_parser::ast::stmt_update::{AssignedValue, StmtUpdate};
use std::ops::Bound;
use std::sync::Arc;

pub struct Binder {
    meta_mgr: Arc<dyn MetaMgr>,
}

impl Binder {
    pub fn new(meta_mgr: Arc<dyn MetaMgr>) -> Self {
        Self { meta_mgr }
    }

    pub async fn bind(&self, stmt: StmtType, params: &dyn SQLParams) -> RS<BoundStmt> {
        match stmt {
            StmtType::Select(stmt) => Ok(BoundStmt::Query(BoundQuery::Select(
                self.bind_select(stmt, params).await?,
            ))),
            StmtType::Command(command) => Ok(BoundStmt::Command(
                self.bind_command(command, params).await?,
            )),
        }
    }

    async fn bind_command(&self, command: StmtCommand, params: &dyn SQLParams) -> RS<BoundCommand> {
        match command {
            StmtCommand::CreateTable(stmt) => {
                Ok(BoundCommand::CreateTable(self.bind_create_table(stmt)?))
            }
            StmtCommand::DropTable(stmt) => {
                Ok(BoundCommand::DropTable(self.bind_drop_table(stmt).await?))
            }
            StmtCommand::Insert(stmt) => {
                Ok(BoundCommand::Insert(self.bind_insert(stmt, params).await?))
            }
            StmtCommand::Update(stmt) => {
                Ok(BoundCommand::Update(self.bind_update(stmt, params).await?))
            }
            StmtCommand::Delete(stmt) => {
                Ok(BoundCommand::Delete(self.bind_delete(stmt, params).await?))
            }
            StmtCommand::CopyFrom(stmt) => {
                Ok(BoundCommand::CopyFrom(self.bind_copy_from(stmt).await?))
            }
            StmtCommand::CopyTo(stmt) => Ok(BoundCommand::CopyTo(self.bind_copy_to(stmt).await?)),
        }
    }

    async fn bind_select(
        &self,
        stmt: sql_parser::ast::stmt_select::StmtSelect,
        params: &dyn SQLParams,
    ) -> RS<BoundSelect> {
        let table_desc = self.get_table_by_name(stmt.get_table_reference()).await?;
        let select_attrs = self.select_attrs(&table_desc, stmt.get_select_term_list())?;
        let tuple_desc = project_tuple_desc(
            &table_desc,
            &crate::x_engine::api::VecSelTerm::new(select_attrs.clone()),
        );
        let predicate = self.bind_predicate(&table_desc, stmt.get_where_predicate(), params)?;
        Ok(BoundSelect {
            table_id: table_desc.id(),
            select_attrs,
            tuple_desc,
            predicate,
        })
    }

    fn bind_create_table(&self, mut stmt: StmtCreateTable) -> RS<BoundCreateTable> {
        stmt.assign_index_for_columns();
        let key_columns = stmt
            .primary_columns()
            .iter()
            .map(Self::schema_column_from_ast)
            .collect::<RS<Vec<_>>>()?;
        let value_columns = stmt
            .non_primary_columns()
            .iter()
            .map(Self::schema_column_from_ast)
            .collect::<RS<Vec<_>>>()?;
        Ok(BoundCreateTable {
            schema: SchemaTable::new(stmt.table_name().clone(), key_columns, value_columns),
        })
    }

    async fn bind_drop_table(&self, stmt: StmtDropTable) -> RS<BoundDropTable> {
        match self
            .meta_mgr
            .get_table_by_name(&stmt.table_name().to_string())
            .await?
        {
            Some(table_desc) => Ok(BoundDropTable {
                table_id: table_desc.id(),
            }),
            None if stmt.drop_if_exists() => Err(m_error!(
                ER::NoSuchElement,
                "drop if exists is not implemented"
            )),
            None => Err(m_error!(
                ER::NoSuchElement,
                format!("cannot find table {}", stmt.table_name())
            )),
        }
    }

    async fn bind_insert(&self, stmt: StmtInsert, params: &dyn SQLParams) -> RS<BoundInsert> {
        let table_desc = self.get_table_by_name(stmt.table_name()).await?;
        if stmt.values_list().len() != 1 {
            return Err(m_error!(
                ER::NotImplemented,
                "multi-row insert is not implemented"
            ));
        }

        let columns = if stmt.columns().is_empty() {
            let total = table_desc.key_info().len() + table_desc.value_info().len();
            (0..total)
                .map(|attr| table_desc.get_attr(attr).name().clone())
                .collect::<Vec<_>>()
        } else {
            stmt.columns().clone()
        };

        let values = &stmt.values_list()[0];
        if columns.len() != values.len() {
            return Err(m_error!(ER::IOErr, "insert column size mismatch"));
        }

        let mut param_index = 0;
        let mut key = vec![];
        let mut value = vec![];
        for (name, expr) in columns.iter().zip(values.iter()) {
            let attr = self.attr_index_by_name(&table_desc, name)?;
            let field = table_desc.get_attr(attr);
            let binary =
                ValueCodec::binary_from_expr(expr, field.type_desc(), params, &mut param_index)?;
            if field.is_primary() {
                key.push((attr, binary));
            } else {
                value.push((attr, binary));
            }
        }

        Ok(BoundInsert {
            table_id: table_desc.id(),
            key,
            value,
        })
    }

    async fn bind_copy_from(
        &self,
        stmt: sql_parser::ast::stmt_copy_from::StmtCopyFrom,
    ) -> RS<BoundCopyFrom> {
        let table_desc = self.get_table_by_name(stmt.copy_to_table_name()).await?;
        let layout = CopyLayout::new(&table_desc, stmt.table_columns())?;
        Ok(BoundCopyFrom {
            file_path: stmt.copy_from_file_path().clone(),
            table_id: table_desc.id(),
            key_index: layout.key_index().to_vec(),
            value_index: layout.value_index().to_vec(),
        })
    }

    async fn bind_copy_to(
        &self,
        stmt: sql_parser::ast::stmt_copy_to::StmtCopyTo,
    ) -> RS<BoundCopyTo> {
        let table_desc = self.get_table_by_name(stmt.copy_from_table_name()).await?;
        let layout = CopyLayout::new(&table_desc, stmt.table_columns())?;
        Ok(BoundCopyTo {
            file_path: stmt.copy_to_file_path().clone(),
            table_id: table_desc.id(),
            key_indexing: layout.key_index().to_vec(),
            value_indexing: layout.value_index().to_vec(),
        })
    }

    async fn bind_update(&self, stmt: StmtUpdate, params: &dyn SQLParams) -> RS<BoundUpdate> {
        let table_desc = self.get_table_by_name(stmt.get_table_reference()).await?;
        let mut param_index = 0;
        let mut value = Vec::with_capacity(stmt.get_set_values().len());

        for assignment in stmt.get_set_values() {
            let attr = self.attr_index_by_name(&table_desc, assignment.get_column_reference())?;
            let field = table_desc.get_attr(attr);
            if field.is_primary() {
                return Err(m_error!(
                    ER::NotImplemented,
                    "updating primary key columns is not implemented"
                ));
            }
            let AssignedValue::Value(expr) = assignment.get_set_value() else {
                return Err(m_error!(
                    ER::NotImplemented,
                    "expression updates are not implemented"
                ));
            };
            let binary =
                ValueCodec::binary_from_expr(expr, field.type_desc(), params, &mut param_index)?;
            value.push((attr, binary));
        }
        let key = self.bind_exact_key_from(
            &table_desc,
            stmt.get_where_predicate(),
            params,
            &mut param_index,
        )?;

        Ok(BoundUpdate {
            table_id: table_desc.id(),
            key,
            value,
        })
    }

    async fn bind_delete(&self, stmt: StmtDelete, params: &dyn SQLParams) -> RS<BoundDelete> {
        let table_desc = self.get_table_by_name(stmt.get_table_reference()).await?;
        let key = self.bind_exact_key(&table_desc, stmt.get_where_predicate(), params)?;
        Ok(BoundDelete {
            table_id: table_desc.id(),
            key,
        })
    }

    fn bind_predicate(
        &self,
        table_desc: &TableDesc,
        predicates: &[ExprCompare],
        params: &dyn SQLParams,
    ) -> RS<BoundPredicate> {
        let mut param_index = 0;
        self.bind_predicate_from(table_desc, predicates, params, &mut param_index)
    }

    fn bind_predicate_from(
        &self,
        table_desc: &TableDesc,
        predicates: &[ExprCompare],
        params: &dyn SQLParams,
        param_index: &mut usize,
    ) -> RS<BoundPredicate> {
        if predicates.is_empty() {
            return Ok(BoundPredicate::True);
        }

        let mut eq_items = vec![];
        let mut start: Bound<Vec<(usize, Vec<u8>)>> = Bound::Unbounded;
        let mut end: Bound<Vec<(usize, Vec<u8>)>> = Bound::Unbounded;

        for predicate in predicates {
            let (field_name, expr_value, op) =
                self.field_literal_compare(predicate).ok_or_else(|| {
                    m_error!(
                        ER::NotImplemented,
                        "only column/literal predicates are supported"
                    )
                })?;
            let attr = self.attr_index_by_name(table_desc, field_name)?;
            let field = table_desc.get_attr(attr);
            if !field.is_primary() {
                return Err(m_error!(
                    ER::NotImplemented,
                    "non-key predicates are not implemented"
                ));
            }
            let binary =
                ValueCodec::binary_from_expr(&expr_value, field.type_desc(), params, param_index)?;
            match op {
                ValueCompare::EQ => eq_items.push((attr, binary)),
                ValueCompare::GE => start = Bound::Included(vec![(attr, binary)]),
                ValueCompare::GT => start = Bound::Excluded(vec![(attr, binary)]),
                ValueCompare::LE => end = Bound::Included(vec![(attr, binary)]),
                ValueCompare::LT => end = Bound::Excluded(vec![(attr, binary)]),
                ValueCompare::NE => {
                    return Err(m_error!(
                        ER::NotImplemented,
                        "not-equal predicates are not implemented"
                    ))
                }
            }
        }

        if !eq_items.is_empty()
            && matches!(start, Bound::Unbounded)
            && matches!(end, Bound::Unbounded)
        {
            return Ok(BoundPredicate::KeyEq { key: eq_items });
        }

        if !eq_items.is_empty() {
            return Err(m_error!(
                ER::NotImplemented,
                "mixed equality and range predicates are not implemented"
            ));
        }

        Ok(BoundPredicate::KeyRange { start, end })
    }

    fn bind_exact_key(
        &self,
        table_desc: &TableDesc,
        predicates: &[ExprCompare],
        params: &dyn SQLParams,
    ) -> RS<Vec<(usize, Vec<u8>)>> {
        let mut param_index = 0;
        self.bind_exact_key_from(table_desc, predicates, params, &mut param_index)
    }

    fn bind_exact_key_from(
        &self,
        table_desc: &TableDesc,
        predicates: &[ExprCompare],
        params: &dyn SQLParams,
        param_index: &mut usize,
    ) -> RS<Vec<(usize, Vec<u8>)>> {
        match self.bind_predicate_from(table_desc, predicates, params, param_index)? {
            BoundPredicate::KeyEq { mut key } => {
                if key.len() != table_desc.key_info().len() {
                    return Err(m_error!(
                        ER::NotImplemented,
                        "update/delete require a complete primary key predicate"
                    ));
                }
                key.sort_by_key(|(attr, _)| *attr);
                for (index, (attr, _)) in key.iter().enumerate() {
                    if *attr != index {
                        return Err(m_error!(
                            ER::NotImplemented,
                            "update/delete require one equality predicate for each primary key column"
                        ));
                    }
                }
                Ok(key)
            }
            BoundPredicate::True => Err(m_error!(
                ER::NotImplemented,
                "full-table update/delete is not implemented"
            )),
            BoundPredicate::KeyRange { .. } => Err(m_error!(
                ER::NotImplemented,
                "range update/delete is not implemented"
            )),
        }
    }

    fn field_literal_compare<'a>(
        &self,
        predicate: &'a ExprCompare,
    ) -> Option<(&'a String, ExprValue, ValueCompare)> {
        match (predicate.left(), predicate.right()) {
            (ExprItem::ItemName(name), ExprItem::ItemValue(value)) => {
                Some((name.name(), value.clone(), *predicate.op()))
            }
            (ExprItem::ItemValue(value), ExprItem::ItemName(name)) => Some((
                name.name(),
                value.clone(),
                Self::reverse_compare(*predicate.op()),
            )),
            _ => None,
        }
    }

    fn reverse_compare(op: ValueCompare) -> ValueCompare {
        match op {
            ValueCompare::EQ => ValueCompare::EQ,
            ValueCompare::LE => ValueCompare::GT,
            ValueCompare::LT => ValueCompare::GE,
            ValueCompare::GE => ValueCompare::LT,
            ValueCompare::GT => ValueCompare::LE,
            ValueCompare::NE => ValueCompare::NE,
        }
    }

    fn schema_column_from_ast(column: &sql_parser::ast::column_def::ColumnDef) -> RS<SchemaColumn> {
        let ty = column.data_type().clone().uni_to()?;
        let mut schema_column = SchemaColumn::new(
            column.column_name().clone(),
            ty.dat_type_id(),
            DTInfo::from_opt_object(&ty),
        );
        schema_column.set_primary(column.is_primary_key());
        schema_column.set_index(column.column_index());
        Ok(schema_column)
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
