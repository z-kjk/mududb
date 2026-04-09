use mudu::common::result::RS;
use mudu::error::ec::EC;
use mudu::m_error;
use mudu_binding::universal::uni_type_desc::UniTypeDesc;
use mudu_type::dat_type::DatType;
use mudu_type::dat_type_id::DatTypeID;
use mudu_type::dtp_array::DTPArray;

#[derive(Debug, Clone)]
pub enum PythonType {
    Primitive(String),
    Generic(String, Vec<PythonType>),
    Dict(Box<PythonType>, Box<PythonType>),
    Tuple(Vec<PythonType>),
    Union(Vec<PythonType>),
    Custom(String),
    Any,
    None,
}

impl PythonType {
    pub fn is_bytes(&self) -> bool {
        matches!(self, PythonType::Primitive(inner) if inner == "bytes")
    }

    // 【核心 1】：解包逻辑，专门用于处理返回值。
    // 如果是 Tuple，就把里面的元素拆成多个独立的返回值；否则当成一个返回值。
    pub fn as_ret_type(&self) -> Vec<PythonType> {
        match self {
            PythonType::Tuple(items) => items.clone(),
            PythonType::None => vec![], // 如果返回 None，相当于没有返回值
            _ => vec![self.clone()],
        }
    }

    pub fn to_type_str(&self) -> String {
        match self {
            PythonType::Primitive(name) => name.clone(),
            PythonType::Any => "Any".to_string(),
            PythonType::None => "None".to_string(),
            PythonType::Generic(name, args) => {
                let args_str: Vec<String> = args.iter().map(|t| t.to_type_str()).collect();
                format!("{}[{}]", name, args_str.join(", "))
            }
            PythonType::Dict(k, v) => format!("Dict[{}, {}]", k.to_type_str(), v.to_type_str()),
            PythonType::Tuple(items) => {
                let items_str: Vec<String> = items.iter().map(|t| t.to_type_str()).collect();
                format!("Tuple[{}]", items_str.join(", "))
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
                // 1. 整数：Python int -> I64
                "int" => Ok(DatType::default_for(DatTypeID::I64)),

                // 2. 浮点：Python float -> F64
                "float" => Ok(DatType::default_for(DatTypeID::F64)),

                // 3. 布尔：Python bool -> I64 (0 or 1)
                // 注意：因为列表里没有 Bool，所以这里强制转换为 I64
                "bool" => Ok(DatType::default_for(DatTypeID::I64)),

                // 4. 字符串：Python str -> String
                "str" => Ok(DatType::default_for(DatTypeID::String)),

                // 5. 二进制：Python bytes -> Binary
                "bytes" => Ok(DatType::default_for(DatTypeID::Binary)),

                // 其他 Python 原生类型暂不支持
                _ => Err(m_error!(EC::TypeErr, format!("not support primitive type {}", s))),
            },

            // 处理自定义类型
            PythonType::Custom(s) => {
                let ty = custom_types.types.get(s).map_or_else(
                    || Err(m_error!(EC::NoneErr, format!("no such type name:{}", s))),
                    |t| Ok(t),
                )?;
                ty.clone().uni_to() // 调用底层的统一转换
            },

            // 处理 List 泛型：Python List[int] -> Array
            PythonType::Generic(ident, vec) => {
                if ident == "List" && vec.len() == 1 {
                    // List 里面的元素类型，也要递归调用 to_dat_type 来确定
                    let array = DTPArray::new(vec[0].to_dat_type(custom_types)?);
                    Ok(DatType::from_array(array))
                } else {
                    Err(m_error!(EC::TypeErr, format!("not support generic type {:?}", self)))
                }
            },

            // 其他复杂类型暂不支持
            _ => Err(m_error!(EC::TypeErr, format!("not support type {:?}", self))),
        }
    }
}