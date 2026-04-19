use mudu::common::result::RS;
use mudu_binding::universal::uni_type_desc::UniTypeDesc;
use mudu_contract::procedure::proc_desc::ProcDesc;
use mudu_contract::tuple::datum_desc::DatumDesc;
use mudu_contract::tuple::tuple_field_desc::TupleFieldDesc;
use crate::python::python_type::PythonType;

#[derive(Debug, Clone)]
pub struct PyFunction {
    pub name: String,
    pub arg_list: Vec<(String, PythonType)>,
    pub return_type: Option<PythonType>,
    pub is_async: bool,
}

impl PyFunction {
    pub fn to_proc_desc(&self, module_name: &String, custom_types: &UniTypeDesc) -> RS<ProcDesc> {
        let start_idx = if let Some((name, _)) = self.arg_list.first() {
            if name == "self" { 1 } else { 0 }
        } else {
            0
        };

        let mut params = Vec::with_capacity(self.arg_list.len().saturating_sub(start_idx));
        for (name, arg_ty) in self.arg_list[start_idx..].iter() {
            let desc = DatumDesc::new(name.clone(), arg_ty.to_dat_type(custom_types)?);
            params.push(desc);
        }

        let rets = if let Some(ty) = &self.return_type {
            let ret_types = ty.as_ret_type();
            let mut rets = Vec::with_capacity(ret_types.len());
            for (i, r) in ret_types.iter().enumerate() {
                let desc = DatumDesc::new(i.to_string(), r.to_dat_type(custom_types)?);
                rets.push(desc);
            }
            rets
        } else {
            vec![]
        };

        Ok(ProcDesc::new(
            module_name.clone(),
            self.name.clone(),
            TupleFieldDesc::new(params),
            TupleFieldDesc::new(rets),
            self.is_async,
        ))
    }
}
