use crate::universal::uni_primitive::UniPrimitive;
use mudu::common::into_result::ToResult;
use mudu::common::result::RS;
use mudu::common::result_from::ResultFrom;
use mudu::error::ec::EC;
use mudu::m_error;
use mudu_type::dat_type::DatType;
use mudu_type::dat_type_id::DatTypeID;

impl UniPrimitive {
    pub fn uni_to(self) -> RS<DatType> {
        let ty = match self {
            UniPrimitive::Bool => {
                return Err(m_error!(EC::TypeErr, "primitive bool is not supported"));
            }
            UniPrimitive::U8 => return Err(m_error!(EC::TypeErr, "primitive u8 is not supported")),
            UniPrimitive::I8 => return Err(m_error!(EC::TypeErr, "primitive i8 is not supported")),
            UniPrimitive::U16 => {
                return Err(m_error!(EC::TypeErr, "primitive u16 is not supported"));
            }
            UniPrimitive::I16 => {
                return Err(m_error!(EC::TypeErr, "primitive i16 is not supported"));
            }
            UniPrimitive::U32 => {
                return Err(m_error!(EC::TypeErr, "primitive u32 is not supported"));
            }
            UniPrimitive::I32 => DatType::default_for(DatTypeID::I32),
            UniPrimitive::U64 => {
                return Err(m_error!(EC::TypeErr, "primitive u64 is not supported"));
            }
            UniPrimitive::U128 => DatType::default_for(DatTypeID::U128),
            UniPrimitive::I64 => DatType::default_for(DatTypeID::I64),
            UniPrimitive::I128 => DatType::default_for(DatTypeID::I128),
            UniPrimitive::F32 => DatType::default_for(DatTypeID::F32),
            UniPrimitive::F64 => DatType::default_for(DatTypeID::F64),
            UniPrimitive::Char => {
                return Err(m_error!(EC::TypeErr, "primitive char is not supported"));
            }
            UniPrimitive::String => DatType::default_for(DatTypeID::String),
            UniPrimitive::Blob => DatType::default_for(DatTypeID::Binary),
        };
        Ok(ty)
    }

    pub fn uni_from(ty: DatType) -> RS<Self> {
        let uni_prim = match ty.dat_type_id() {
            DatTypeID::I32 => Self::I32,
            DatTypeID::I64 => Self::I64,
            DatTypeID::I128 => Self::I128,
            DatTypeID::U128 => Self::U128,
            DatTypeID::F32 => Self::F32,
            DatTypeID::F64 => Self::F64,
            DatTypeID::String => Self::String,
            DatTypeID::Array => {
                return Err(m_error!(EC::TypeErr, "array type is not primitive"));
            }
            DatTypeID::Record => {
                return Err(m_error!(EC::TypeErr, "record type is not primitive"));
            }
            DatTypeID::Binary => Self::Blob,
        };
        Ok(uni_prim)
    }
}

impl ToResult<DatType> for UniPrimitive {
    fn to(self) -> RS<DatType> {
        self.uni_to()
    }
}

impl ResultFrom<DatType> for UniPrimitive {
    fn from(value: DatType) -> RS<Self> {
        Self::uni_from(value)
    }
}
