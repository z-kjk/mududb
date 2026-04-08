use mudu::common::id::{AttrIndex, OID};

use crate::contract::field_info::FieldInfo;
use mudu_contract::tuple::tuple_binary_desc::TupleBinaryDesc as TupleDesc;
use std::collections::HashMap;

pub struct TableDesc {
    name: String,
    oid: OID,
    key_oid: Vec<OID>,
    value_oid: Vec<OID>,

    // use AttrIndex index to access key/value
    // [0     -- N ] , key datum, if 0 <= AttrIndex < N, this index would be  key
    // [N + 1 -- M ] , value datum, if N <= AttrIndex < M, this index would be value
    key_desc: TupleDesc,
    value_desc: TupleDesc,
    key_info: Vec<FieldInfo>,
    value_info: Vec<FieldInfo>,
    name2oid: HashMap<String, OID>,
    oid2col: HashMap<OID, FieldInfo>,
    column_oid: Vec<OID>,
}

impl TableDesc {
    pub fn new(
        name: String,
        oid: OID,
        key_oid: Vec<OID>,
        value_oid: Vec<OID>,
        key_desc: TupleDesc,
        value_desc: TupleDesc,
        name2oid: HashMap<String, OID>,
        oid2col: HashMap<OID, FieldInfo>,
    ) -> Self {
        let mut vec: Vec<(&OID, &FieldInfo)> = oid2col.iter().collect();
        vec.sort_by(|a, b| a.1.column_index().cmp(&b.1.column_index()));
        let column_oid: Vec<OID> = vec.iter().map(|(id, _)| *(*id)).collect();
        let mut key_info: Vec<_> = Vec::new();
        let mut value_info: Vec<_> = Vec::new();
        key_info.resize(key_desc.field_count(), FieldInfo::default());
        value_info.resize(value_desc.field_count(), FieldInfo::default());
        for (_oid, field) in oid2col.iter() {
            if field.is_primary() {
                key_info[field.column_index()] = field.clone();
            } else {
                value_info[field.column_index()] = field.clone();
            }
        }
        Self {
            name,
            oid,
            key_oid,
            value_oid,
            key_desc,
            value_desc,
            key_info,
            value_info,
            oid2col,
            name2oid,
            column_oid,
        }
    }

    pub fn key_field_oid(&self) -> &Vec<OID> {
        &self.key_oid
    }

    pub fn value_field_oid(&self) -> &Vec<OID> {
        &self.value_oid
    }

    pub fn get_attr(&self, index: AttrIndex) -> &FieldInfo {
        if index < self.key_info.len() {
            &self.key_info[index]
        } else {
            &self.value_info[index - self.key_info.len()]
        }
    }
    pub fn key_info(&self) -> &Vec<FieldInfo> {
        &self.key_info
    }
    pub fn key_desc(&self) -> &TupleDesc {
        &self.key_desc
    }

    pub fn value_info(&self) -> &Vec<FieldInfo> {
        &self.value_info
    }
    pub fn value_desc(&self) -> &TupleDesc {
        &self.value_desc
    }

    pub fn name2oid(&self) -> &HashMap<String, OID> {
        &self.name2oid
    }
    pub fn oid2col(&self) -> &HashMap<OID, FieldInfo> {
        &self.oid2col
    }

    pub fn name(&self) -> &String {
        &self.name
    }

    pub fn id(&self) -> OID {
        self.oid
    }

    pub fn original_column_oid(&self) -> &Vec<OID> {
        &self.column_oid
    }
}
