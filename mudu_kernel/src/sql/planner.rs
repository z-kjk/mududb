use crate::command::create_partition_placement::CreatePartitionPlacement;
use crate::command::create_partition_rule::CreatePartitionRule;
use crate::command::create_table::CreateTable;
use crate::command::delete_key_value::DeleteKeyValue;
use crate::command::drop_table::DropTable;
use crate::command::insert_key_value::InsertKeyValue;
use crate::command::load_from_file::LoadFromFile;
use crate::command::save_to_file::SaveToFile;
use crate::command::update_key_value::UpdateKeyValue;
use crate::contract::cmd_exec::CmdExec;
use crate::contract::query_exec::QueryExec;
use crate::sql::bound_stmt::{
    BoundCommand, BoundCopyFrom, BoundCopyTo, BoundCreatePartitionPlacement,
    BoundCreatePartitionRule, BoundCreateTable, BoundDelete, BoundDropTable, BoundInsert,
    BoundPredicate, BoundQuery, BoundSelect, BoundUpdate,
};
use crate::sql::plan_ctx::PlanCtx;
use crate::x_engine::api::{OptRead, Predicate, RangeData, VecDatum, VecSelTerm};
use crate::x_engine::x_param::{
    PAccessKey, PAccessRange, PCreatePartitionPlacement, PCreatePartitionRule, PCreateTable,
    PDeleteKeyValue, PDropTable, PInsertKeyValue, PUpdateKeyValue,
};
use mudu::common::result::RS;
use std::sync::Arc;

pub struct Planner {
    ctx: PlanCtx,
}

impl Planner {
    pub fn new(ctx: PlanCtx) -> Self {
        Self { ctx }
    }

    pub async fn plan_query(&self, query: BoundQuery) -> RS<Arc<dyn QueryExec>> {
        match query {
            BoundQuery::Select(select) => self.plan_select(select).await,
        }
    }

    pub async fn plan_command(&self, command: BoundCommand) -> RS<Arc<dyn CmdExec>> {
        match command {
            BoundCommand::CreatePartitionPlacement(stmt) => {
                Ok(Arc::new(self.plan_create_partition_placement(stmt)))
            }
            BoundCommand::CreatePartitionRule(stmt) => {
                Ok(Arc::new(self.plan_create_partition_rule(stmt)))
            }
            BoundCommand::CreateTable(stmt) => Ok(Arc::new(self.plan_create_table(stmt))),
            BoundCommand::DropTable(stmt) => Ok(Arc::new(self.plan_drop_table(stmt))),
            BoundCommand::Insert(stmt) => Ok(Arc::new(self.plan_insert(stmt))),
            BoundCommand::Update(stmt) => Ok(Arc::new(self.plan_update(stmt))),
            BoundCommand::Delete(stmt) => Ok(Arc::new(self.plan_delete(stmt))),
            BoundCommand::CopyFrom(stmt) => Ok(Arc::new(self.plan_copy_from(stmt))),
            BoundCommand::CopyTo(stmt) => Ok(Arc::new(self.plan_copy_to(stmt))),
        }
    }

    async fn plan_select(&self, stmt: BoundSelect) -> RS<Arc<dyn QueryExec>> {
        let select = VecSelTerm::new(stmt.select_attrs.clone());
        match stmt.predicate {
            BoundPredicate::True => {
                let exec = crate::executor::index_access_range::IndexAccessRange::new(
                    PAccessRange {
                        tx_mgr: self.ctx.tx_mgr.clone(),
                        table_id: stmt.table_id,
                        pred_key: RangeData::new(
                            std::ops::Bound::Unbounded,
                            std::ops::Bound::Unbounded,
                        ),
                        pred_non_key: Predicate::CNF(Vec::new()),
                        select,
                        opt_read: OptRead::default(),
                    },
                    self.ctx.x_contract.clone(),
                    self.ctx.meta_mgr.clone(),
                )
                .await?;
                Ok(Arc::new(exec))
            }
            BoundPredicate::KeyEq { key } => {
                let exec = crate::executor::index_access_key::IndexAccessKey::new(
                    PAccessKey {
                        tx_mgr: self.ctx.tx_mgr.clone(),
                        table_id: stmt.table_id,
                        pred_key: VecDatum::new(key),
                        select,
                        opt_read: OptRead::default(),
                    },
                    self.ctx.x_contract.clone(),
                    self.ctx.meta_mgr.clone(),
                )
                .await?;
                Ok(Arc::new(exec))
            }
            BoundPredicate::KeyRange { start, end } => {
                let exec = crate::executor::index_access_range::IndexAccessRange::new(
                    PAccessRange {
                        tx_mgr: self.ctx.tx_mgr.clone(),
                        table_id: stmt.table_id,
                        pred_key: RangeData::new(start, end),
                        pred_non_key: Predicate::CNF(Vec::new()),
                        select,
                        opt_read: OptRead::default(),
                    },
                    self.ctx.x_contract.clone(),
                    self.ctx.meta_mgr.clone(),
                )
                .await?;
                Ok(Arc::new(exec))
            }
        }
    }

    fn plan_create_partition_placement(
        &self,
        stmt: BoundCreatePartitionPlacement,
    ) -> CreatePartitionPlacement {
        CreatePartitionPlacement::new(
            PCreatePartitionPlacement {
                tx_mgr: self.ctx.tx_mgr.clone(),
                placements: stmt.placements,
            },
            self.ctx.meta_mgr.clone(),
        )
    }

    fn plan_create_partition_rule(&self, stmt: BoundCreatePartitionRule) -> CreatePartitionRule {
        CreatePartitionRule::new(
            PCreatePartitionRule {
                tx_mgr: self.ctx.tx_mgr.clone(),
                rule: stmt.rule,
            },
            self.ctx.meta_mgr.clone(),
        )
    }

    fn plan_create_table(&self, stmt: BoundCreateTable) -> CreateTable {
        CreateTable::new(
            PCreateTable {
                tx_mgr: self.ctx.tx_mgr.clone(),
                schema: stmt.schema,
                partition_binding: stmt.partition_binding,
            },
            self.ctx.x_contract.clone(),
            self.ctx.meta_mgr.clone(),
        )
    }

    fn plan_drop_table(&self, stmt: BoundDropTable) -> DropTable {
        DropTable::new(
            PDropTable {
                tx_mgr: self.ctx.tx_mgr.clone(),
                oid: stmt.oid,
            },
            self.ctx.x_contract.clone(),
            self.ctx.meta_mgr.clone(),
        )
    }

    fn plan_insert(&self, stmt: BoundInsert) -> InsertKeyValue {
        InsertKeyValue::new(
            PInsertKeyValue {
                tx_mgr: self.ctx.tx_mgr.clone(),
                table_id: stmt.table_id,
                rows: stmt
                    .rows
                    .into_iter()
                    .map(|row| (VecDatum::new(row.key), VecDatum::new(row.value)))
                    .collect(),
            },
            self.ctx.x_contract.clone(),
            self.ctx.meta_mgr.clone(),
        )
    }

    fn plan_update(&self, stmt: BoundUpdate) -> UpdateKeyValue {
        UpdateKeyValue::new(
            PUpdateKeyValue {
                tx_mgr: self.ctx.tx_mgr.clone(),
                table_id: stmt.table_id,
                key: VecDatum::new(stmt.key),
                value: VecDatum::new(stmt.value),
            },
            self.ctx.x_contract.clone(),
            self.ctx.meta_mgr.clone(),
        )
    }

    fn plan_delete(&self, stmt: BoundDelete) -> DeleteKeyValue {
        DeleteKeyValue::new(
            PDeleteKeyValue {
                tx_mgr: self.ctx.tx_mgr.clone(),
                table_id: stmt.table_id,
                key: VecDatum::new(stmt.key),
            },
            self.ctx.x_contract.clone(),
            self.ctx.meta_mgr.clone(),
        )
    }

    fn plan_copy_from(&self, stmt: BoundCopyFrom) -> LoadFromFile {
        LoadFromFile::new(
            stmt.file_path,
            self.ctx.tx_mgr.clone(),
            stmt.table_id,
            stmt.key_index,
            stmt.value_index,
            self.ctx.x_contract.clone(),
            self.ctx.meta_mgr.clone(),
        )
    }

    fn plan_copy_to(&self, stmt: BoundCopyTo) -> SaveToFile {
        SaveToFile::new(
            stmt.file_path,
            self.ctx.tx_mgr.clone(),
            stmt.table_id,
            stmt.key_indexing,
            stmt.value_indexing,
            self.ctx.x_contract.clone(),
            self.ctx.meta_mgr.clone(),
        )
    }
}
