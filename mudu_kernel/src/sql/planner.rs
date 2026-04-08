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
    BoundCommand, BoundCopyFrom, BoundCopyTo, BoundCreateTable, BoundDelete, BoundDropTable,
    BoundInsert, BoundPredicate, BoundQuery, BoundSelect, BoundUpdate,
};
use crate::sql::plan_ctx::PlanCtx;
use crate::x_engine::api::{OptRead, Predicate, RangeData, VecDatum, VecSelTerm};
use crate::x_engine::x_param::{
    PAccessKey, PAccessRange, PCreateTable, PDeleteKeyValue, PDropTable, PInsertKeyValue,
    PUpdateKeyValue,
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
                        xid: self.ctx.xid,
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
                        xid: self.ctx.xid,
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
                        xid: self.ctx.xid,
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

    fn plan_create_table(&self, stmt: BoundCreateTable) -> CreateTable {
        CreateTable::new(
            PCreateTable {
                xid: self.ctx.xid,
                schema: stmt.schema,
            },
            self.ctx.x_contract.clone(),
            self.ctx.meta_mgr.clone(),
        )
    }

    fn plan_drop_table(&self, stmt: BoundDropTable) -> DropTable {
        DropTable::new(
            PDropTable {
                xid: self.ctx.xid,
                oid: Some(stmt.table_id),
            },
            self.ctx.x_contract.clone(),
            self.ctx.meta_mgr.clone(),
        )
    }

    fn plan_insert(&self, stmt: BoundInsert) -> InsertKeyValue {
        InsertKeyValue::new(
            PInsertKeyValue {
                xid: self.ctx.xid,
                table_id: stmt.table_id,
                key: VecDatum::new(stmt.key),
                value: VecDatum::new(stmt.value),
            },
            self.ctx.x_contract.clone(),
            self.ctx.meta_mgr.clone(),
        )
    }

    fn plan_update(&self, stmt: BoundUpdate) -> UpdateKeyValue {
        UpdateKeyValue::new(
            PUpdateKeyValue {
                xid: self.ctx.xid,
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
                xid: self.ctx.xid,
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
            self.ctx.xid,
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
            self.ctx.xid,
            stmt.table_id,
            stmt.key_indexing,
            stmt.value_indexing,
            self.ctx.x_contract.clone(),
            self.ctx.meta_mgr.clone(),
        )
    }
}
