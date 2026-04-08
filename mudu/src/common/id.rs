use uuid::Uuid;

/// unique object id
pub type OID = u128;

// Nth attribute index of data tuple
pub type AttrIndex = usize;

pub type TupleID = u64;
pub type ThdID = u64;

pub const INVALID_OID: OID = 0;

pub fn gen_oid() -> OID {
    let id = Uuid::new_v4();
    id.as_u128()
}

pub fn oid_cast_to_u32(n: u128) -> u32 {
    n as u32
}
