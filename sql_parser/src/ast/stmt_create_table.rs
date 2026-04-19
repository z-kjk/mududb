use crate::ast::ast_node::ASTNode;
use crate::ast::column_def::ColumnDef;
use crate::ast::stmt_table_partition::StmtTablePartition;
use mudu::common::id::AttrIndex;
use std::fmt::Debug;

#[derive(Clone, Debug)]
pub struct StmtCreateTable {
    table_name: String,
    column_def: Vec<ColumnDef>,
    primary_key_column_def: Vec<AttrIndex>,
    non_primary_key_column_def: Vec<AttrIndex>,
    partition: Option<StmtTablePartition>,
}

impl StmtCreateTable {
    pub fn new(table_name: String) -> StmtCreateTable {
        Self {
            table_name,
            column_def: vec![],
            primary_key_column_def: vec![],
            non_primary_key_column_def: vec![],
            partition: None,
        }
    }

    pub fn table_name(&self) -> &String {
        &self.table_name
    }

    pub fn column_def(&self) -> &Vec<ColumnDef> {
        &self.column_def
    }

    pub fn add_column_def(&mut self, def: ColumnDef) {
        let mut _def = def;
        _def.set_index(self.column_def.len());
        self.column_def.push(_def)
    }

    pub fn mutable_column_def(&mut self) -> &mut Vec<ColumnDef> {
        &mut self.column_def
    }

    pub fn column_def_by_index(&self, index: AttrIndex) -> &ColumnDef {
        &self.column_def[index]
    }

    pub fn non_primary_column_indices(&self) -> &Vec<AttrIndex> {
        &self.non_primary_key_column_def
    }

    pub fn primary_column_indices(&self) -> &Vec<AttrIndex> {
        &self.primary_key_column_def
    }

    pub fn non_primary_columns(&self) -> Vec<&ColumnDef> {
        self.non_primary_key_column_def
            .iter()
            .map(|index| &self.column_def[*index])
            .collect()
    }

    pub fn primary_columns(&self) -> Vec<&ColumnDef> {
        self.primary_key_column_def
            .iter()
            .map(|index| &self.column_def[*index])
            .collect()
    }

    pub fn partition(&self) -> Option<&StmtTablePartition> {
        self.partition.as_ref()
    }

    pub fn set_partition(&mut self, partition: StmtTablePartition) {
        self.partition = Some(partition);
    }

    pub fn assign_index_for_columns(&mut self) {
        self.primary_key_column_def.clear();
        self.non_primary_key_column_def.clear();

        for (index, c) in self.column_def.iter_mut().enumerate() {
            if c.is_primary_key() {
                self.primary_key_column_def.push(index);
            } else {
                self.non_primary_key_column_def.push(index);
            }
        }
        self.primary_key_column_def
            .sort_by_key(|index| self.column_def[*index].expect_primary_key_index());
    }
}

impl ASTNode for StmtCreateTable {}
