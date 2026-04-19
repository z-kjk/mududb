use crate::contract::cmd_exec::CmdExec;
use crate::contract::meta_mgr::MetaMgr;
use crate::x_engine::api::{OptInsert, VecDatum, XContract};
use crate::x_engine::tx_mgr::TxMgr;
use async_std::fs::File;
use async_trait::async_trait;
use csv_async::StringRecord;
use futures::StreamExt;
use mudu::common::buf::Buf;
use mudu::common::id::OID;
use mudu::common::result::RS;
use mudu::error::ec::EC as ER;
use mudu::m_error;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct LoadFromFile {
    inner: Arc<Mutex<_LoadFromFile>>,
}

struct _LoadFromFile {
    csv_file: String,
    tx_mgr: Arc<dyn TxMgr>,
    table_id: OID,
    key_index: Vec<usize>,
    value_index: Vec<usize>,
    x_contract: Arc<dyn XContract>,
    meta_mgr: Arc<dyn MetaMgr>,
    affected_rows: u64,
}

impl LoadFromFile {
    pub fn new(
        csv_file: String,
        tx_mgr: Arc<dyn TxMgr>,
        table_id: OID,
        key_index: Vec<usize>,
        value_index: Vec<usize>,
        x_contract: Arc<dyn XContract>,
        meta_mgr: Arc<dyn MetaMgr>,
    ) -> Self {
        Self {
            inner: Arc::new(Mutex::new(_LoadFromFile::new(
                csv_file,
                tx_mgr,
                table_id,
                key_index,
                value_index,
                x_contract,
                meta_mgr,
            ))),
        }
    }
}

impl _LoadFromFile {
    fn new(
        csv_file: String,
        tx_mgr: Arc<dyn TxMgr>,
        table_id: OID,
        key_index: Vec<usize>,
        value_index: Vec<usize>,
        x_contract: Arc<dyn XContract>,
        meta_mgr: Arc<dyn MetaMgr>,
    ) -> Self {
        Self {
            csv_file,
            tx_mgr,
            table_id,
            key_index,
            value_index,
            x_contract,
            meta_mgr,
            affected_rows: 0,
        }
    }

    async fn prepare(&self) -> RS<()> {
        let table_desc = self.meta_mgr.get_table_by_id(self.table_id).await?;
        if self.key_index.len() != table_desc.key_info().len()
            || self.value_index.len() != table_desc.value_info().len()
        {
            return Err(m_error!(ER::IOErr, "column size error"));
        }
        Ok(())
    }

    async fn load_table(&self) -> RS<u64> {
        let table_desc = self.meta_mgr.get_table_by_id(self.table_id).await?;
        let file = File::open(self.csv_file.clone()).await.map_err(|e| {
            m_error!(
                ER::IOErr,
                format!("load failed, open csv file {} error, {}", self.csv_file, e)
            )
        })?;
        let mut reader = csv_async::AsyncReader::from_reader(file);
        let mut records = reader.records();
        let mut rows = 0;
        while let Some(record) = records.next().await {
            let record = record.map_err(|e| {
                m_error!(
                    ER::IOErr,
                    format!("load failed, csv file {} error, {}", self.csv_file, e)
                )
            })?;
            let field_num = self.key_index.len() + self.value_index.len();
            if field_num != record.len() {
                return Err(m_error!(
                    ER::IOErr,
                    format!(
                        "load failed, table column size {} not equal to csv column count {}",
                        field_num,
                        record.len()
                    )
                ));
            }

            let key = Self::build_datum_from_line(
                &record,
                &self.key_index,
                table_desc.key_indices(),
                &table_desc,
            )?;
            let value = Self::build_datum_from_line(
                &record,
                &self.value_index,
                table_desc.value_indices(),
                &table_desc,
            )?;
            self.x_contract
                .insert(
                    self.tx_mgr.clone(),
                    self.table_id,
                    &key,
                    &value,
                    &OptInsert::default(),
                )
                .await?;
            rows += 1;
        }
        Ok(rows)
    }

    fn set_affected_rows(&mut self, rows: u64) {
        self.affected_rows = rows;
    }

    fn get_affected_rows(&self) -> u64 {
        self.affected_rows
    }

    fn build_datum_from_line(
        record: &StringRecord,
        csv_index: &[usize],
        attr_indices: &[usize],
        table_desc: &crate::contract::table_desc::TableDesc,
    ) -> RS<VecDatum> {
        let mut datum = Vec::with_capacity(csv_index.len());
        for (position, csv_col) in csv_index.iter().enumerate() {
            let textual = record
                .get(*csv_col)
                .ok_or_else(|| m_error!(ER::IndexOutOfRange))?;
            let attr_index = attr_indices[position];
            let field = table_desc.get_attr(attr_index);
            let dat_type = field.type_desc();
            let dat_id = dat_type.dat_type_id();
            let internal = dat_id.fn_input()(textual, dat_type)
                .map_err(|e| m_error!(ER::TypeBaseErr, "convert printable to internal error", e))?;
            let binary: Buf = dat_id.fn_send()(&internal, dat_type)
                .map_err(|e| m_error!(ER::TypeBaseErr, "converting internal to binary error", e))?
                .into();
            datum.push((attr_index, binary));
        }
        Ok(VecDatum::new(datum))
    }
}

#[async_trait]
impl CmdExec for LoadFromFile {
    async fn prepare(&self) -> RS<()> {
        let inner = self.inner.lock().await;
        inner.prepare().await
    }

    async fn run(&self) -> RS<()> {
        let mut inner = self.inner.lock().await;
        let rows = inner.load_table().await?;
        inner.set_affected_rows(rows);
        Ok(())
    }

    async fn affected_rows(&self) -> RS<u64> {
        let inner = self.inner.lock().await;
        Ok(inner.get_affected_rows())
    }
}
