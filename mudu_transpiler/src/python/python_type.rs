use mudu::common::result::RS;
use mudu::error::ec::EC;
use mudu::m_error;
use mudu_binding::universal::uni_type_desc::UniTypeDesc;
use mudu_type::dat_type::DatType;
use mudu_type::dat_type_id::DatTypeID;
use mudu_type::dtp_array::DTPArray;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PythonType {
    Primitive(String),
    Generic(String, Vec<PythonType>),
    Tuple(Vec<PythonType>),
    Union(Vec<PythonType>),
    Custom(String),
    Any, //不支持落地
    NoneType, //可以作为无返回值使用
}

impl PythonType {
    pub fn is_bytes(&self) -> bool {
        matches!(self, PythonType::Primitive(inner) if inner == "bytes")
    }

    pub fn as_ret_type(&self) -> Vec<PythonType> {
        match self {
            PythonType::Tuple(items) => items.clone(),
            PythonType::NoneType => vec![],
            _ => vec![self.clone()],
        }
    }

    pub fn to_type_str(&self) -> String {
        match self {
            PythonType::Primitive(name) => name.clone(),
            PythonType::Any => "Any".to_string(),
            PythonType::NoneType => "None".to_string(),
            PythonType::Generic(name, args) => {
                let args_str: Vec<String> = args.iter().map(|t| t.to_type_str()).collect();
                format!("{}[{}]", name, args_str.join(", "))
            }
            PythonType::Tuple(items) => {
                let items_str: Vec<String> = items.iter().map(|t| t.to_type_str()).collect();
                format!("tuple[{}]", items_str.join(", "))
            }
            PythonType::Union(items) => {
                let items_str: Vec<String> = items.iter().map(|t| t.to_type_str()).collect();
                format!("Union[{}]", items_str.join(", "))
            }
            PythonType::Custom(name) => name.clone(),
        }
    }

    pub fn to_ret_type_str(&self) -> Vec<String> {
        self.as_ret_type().iter().map(|t| t.to_type_str()).collect()
    }

    pub fn to_dat_type(&self, custom_types: &UniTypeDesc) -> RS<DatType> {
        match self {
            PythonType::Primitive(s) => match s.as_str() {
                "int" => Ok(DatType::default_for(DatTypeID::I64)),
                "float" => Ok(DatType::default_for(DatTypeID::F64)),
                "bool" => Ok(DatType::default_for(DatTypeID::I64)),
                "str" => Ok(DatType::default_for(DatTypeID::String)),
                "bytes" => Ok(DatType::default_for(DatTypeID::Binary)),
                _ => Err(m_error!(EC::TypeErr, format!("not support primitive type {}", s))),
            },

            PythonType::Custom(s) => {
                let ty = custom_types.types.get(s).map_or_else(
                    || Err(m_error!(EC::NoneErr, format!("no such type name:{}", s))),
                    |t| Ok(t),
                )?;
                ty.clone().uni_to()
            }

            PythonType::Generic(ident, args) => match (ident.as_str(), args.as_slice()) {
                ("List", [inner]) | ("list", [inner]) => {
                    let array = DTPArray::new(inner.to_dat_type(custom_types)?);
                    Ok(DatType::from_array(array))
                }
                ("Optional", [inner]) | ("optional", [inner]) => {
                    inner.to_dat_type(custom_types)
                }
                _ => Err(m_error!(EC::TypeErr, format!("not support generic type {:?}", self))),
            },

            PythonType::Any => Err(m_error!(EC::TypeErr, "Any is not supported now")),

            PythonType::Union(_) => {
                Err(m_error!(EC::TypeErr, format!("not support union type {:?}", self)))
            }

            PythonType::Tuple(_) => {
                Err(m_error!(EC::TypeErr, format!("tuple cannot directly convert to DatType: {:?}", self)))
            }

            PythonType::NoneType => {
                Err(m_error!(EC::TypeErr, "NoneType cannot convert to DatType directly"))
            }
        }
    }
}
