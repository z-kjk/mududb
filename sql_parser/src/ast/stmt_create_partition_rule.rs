use crate::ast::ast_node::ASTNode;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StmtPartitionBound {
    Unbounded,
    Value(Vec<Vec<u8>>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StmtRangePartition {
    name: String,
    start: StmtPartitionBound,
    end: StmtPartitionBound,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StmtCreatePartitionRule {
    rule_name: String,
    partitions: Vec<StmtRangePartition>,
}

impl StmtRangePartition {
    pub fn new(name: String, start: StmtPartitionBound, end: StmtPartitionBound) -> Self {
        Self { name, start, end }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn start(&self) -> &StmtPartitionBound {
        &self.start
    }

    pub fn end(&self) -> &StmtPartitionBound {
        &self.end
    }
}

impl StmtCreatePartitionRule {
    pub fn new(rule_name: String, partitions: Vec<StmtRangePartition>) -> Self {
        Self {
            rule_name,
            partitions,
        }
    }

    pub fn rule_name(&self) -> &str {
        &self.rule_name
    }

    pub fn partitions(&self) -> &[StmtRangePartition] {
        &self.partitions
    }
}

impl ASTNode for StmtCreatePartitionRule {}
