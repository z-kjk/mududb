use mudu::common::id::{AttrIndex, OID};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum RpcBound {
    Included(Vec<u8>),
    Excluded(Vec<u8>),
    Unbounded,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum PartitionRpcRequest {
    ReadKey {
        table_id: OID,
        partition_id: OID,
        key: Vec<u8>,
        select: Vec<AttrIndex>,
    },
    ReadRange {
        table_id: OID,
        partition_id: OID,
        start: RpcBound,
        end: RpcBound,
        select: Vec<AttrIndex>,
    },
    Insert {
        table_id: OID,
        partition_id: OID,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Delete {
        table_id: OID,
        partition_id: OID,
        key: Vec<u8>,
    },
    Update {
        table_id: OID,
        partition_id: OID,
        key: Vec<u8>,
        values: Vec<(AttrIndex, Vec<u8>)>,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum PartitionRpcResponse {
    ReadKey(Option<Vec<Vec<u8>>>),
    ReadRange(Vec<Vec<Vec<u8>>>),
    Insert,
    Delete(usize),
    Update(usize),
    Err(String),
}
