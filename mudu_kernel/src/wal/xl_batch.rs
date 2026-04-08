use crate::wal::xl_entry::XLEntry;
use serde::{Deserialize, Serialize};

/// A transaction-log batch in WAL.
///
/// Each [`XLBatch`] contains one or more transaction log entries that describe
/// transaction-level CRUD operations and transaction control records such as
/// begin, commit, and abort.
#[derive(Serialize, Deserialize, Debug, Eq, PartialEq, Clone)]
pub struct XLBatch {
    pub entries: Vec<XLEntry>,
}

pub use crate::wal::xl_batch_worker_log::{
    append_xl_batch, append_xl_batch_async, decode_xl_batches, decode_xl_batches_with_pending,
    deserialize_batch, new_xl_batch_worker_log, new_xl_batch_writer, serialize_batch,
    NoopXLBatchRecoveryHandler, XLBatchWorkerLog,
};
