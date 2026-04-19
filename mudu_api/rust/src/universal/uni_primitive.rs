#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    serde_repr::Serialize_repr,
    serde_repr::Deserialize_repr,
)]
#[repr(u32)]
pub enum UniPrimitive {
    Bool = 0,

    U8 = 1,

    I8 = 2,

    U16 = 3,

    I16 = 4,

    U32 = 5,

    I32 = 6,

    U64 = 7,

    U128 = 8,

    I64 = 9,

    I128 = 10,

    F32 = 11,

    F64 = 12,

    Char = 13,

    String = 14,

    Blob = 15,
}

impl Default for UniPrimitive {
    fn default() -> Self {
        Self::Bool
    }
}
