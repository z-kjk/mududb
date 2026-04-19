use crate::ast::ast_node::ASTNode;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StmtTablePartition {
    rule_name: String,
    reference_columns: Vec<String>,
}

impl StmtTablePartition {
    pub fn new(rule_name: String, reference_columns: Vec<String>) -> Self {
        Self {
            rule_name,
            reference_columns,
        }
    }

    pub fn rule_name(&self) -> &str {
        &self.rule_name
    }

    pub fn reference_columns(&self) -> &[String] {
        &self.reference_columns
    }
}

impl ASTNode for StmtTablePartition {}
