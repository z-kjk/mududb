use crate::ast::ast_node::ASTNode;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StmtPartitionPlacementItem {
    partition_name: String,
    worker_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StmtCreatePartitionPlacement {
    rule_name: String,
    placements: Vec<StmtPartitionPlacementItem>,
}

impl StmtPartitionPlacementItem {
    pub fn new(partition_name: String, worker_id: String) -> Self {
        Self {
            partition_name,
            worker_id,
        }
    }

    pub fn partition_name(&self) -> &str {
        &self.partition_name
    }

    pub fn worker_id(&self) -> &str {
        &self.worker_id
    }
}

impl StmtCreatePartitionPlacement {
    pub fn new(rule_name: String, placements: Vec<StmtPartitionPlacementItem>) -> Self {
        Self {
            rule_name,
            placements,
        }
    }

    pub fn rule_name(&self) -> &str {
        &self.rule_name
    }

    pub fn placements(&self) -> &[StmtPartitionPlacementItem] {
        &self.placements
    }
}

impl ASTNode for StmtCreatePartitionPlacement {}
