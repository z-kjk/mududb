use mudu::common::result::RS;
use mudu::error::ec::EC;
use mudu::m_error;
use mudu_binding::universal::uni_type_desc::UniTypeDesc;
use mudu_contract::procedure::proc_desc::ProcDesc;
use mudu_contract::tuple::datum_desc::DatumDesc;
use mudu_contract::tuple::tuple_field_desc::TupleFieldDesc;
use crate::python::python_type::PythonType;       // 引入刚才定义的 PythonType

#[derive(Debug, Clone)]
pub struct PyFunction {
    pub name: String,
    pub arg_list: Vec<(String, PythonType)>, // 参数列表：(参数名, 类型)
    pub return_type: Option<PythonType>,     // 返回值类型
    pub is_async: bool,                      // 是否是 async def
}

impl PyFunction {
    // 【核心 3】：生成统一的 ProcDesc 说明书
    pub fn to_proc_desc(&self, module_name: &String, custom_types: &UniTypeDesc) -> RS<ProcDesc> {

        // 1. 强制校验：RPC 函数至少需要一个 OID (在 Python 类方法中通常是 'self')
        if self.arg_list.is_empty() {
            return Err(m_error!(
                EC::InternalErr,
                "procedure must have at least one OID (self) argument"
            ));
        }

        // 2. 处理参数：跳过第 0 个参数 (self/OID)，只保留实际的业务参数
        let mut params = Vec::with_capacity(self.arg_list.len() - 1);
        for (name, arg_ty) in self.arg_list[1..].iter() {
            let desc = DatumDesc::new(name.clone(), arg_ty.to_dat_type(custom_types)?);
            params.push(desc);
        }

        // 3. 处理返回值：使用 as_ret_type 自动“脱壳”
        // 如果 return_type 是 Tuple[int, str]，会被拆成 [int, str] 两个返回值
        // 如果 return_type 是 int，就是 [int] 一个返回值
        let rets = if let Some(ty) = &self.return_type {
            let ret_types = ty.as_ret_type(); // 这里完成了脱壳
            let mut rets = Vec::with_capacity(ret_types.len());
            for (i, r) in ret_types.iter().enumerate() {
                // 给返回值按顺序编号 "0", "1", "2"...
                let desc = DatumDesc::new(i.to_string(), r.to_dat_type(custom_types)?);
                rets.push(desc);
            }
            rets
        } else {
            vec![]
        };

        // 4. 组装成最终的中间标准
        Ok(ProcDesc::new(
            module_name.clone(),
            self.name.clone(),
            TupleFieldDesc::new(params),
            TupleFieldDesc::new(rets),
            self.is_async,
        ))
    }
}