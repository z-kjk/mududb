use crate::dat_type::DatType;
use crate::dat_type_id::DatTypeID;
use crate::dat_value::DatValue;
use crate::dt_fn_arbitrary::FnArbitrary;
use crate::dt_impl::dt_create::create_object_type;
use crate::dt_impl::fn_object::fn_object_out;
use crate::type_error::TyErr;
use arbitrary::Arbitrary;
use arbitrary::Unstructured;

const OBJECT_FIELD_TYPE_IDS: [DatTypeID; 9] = [
    DatTypeID::I32,
    DatTypeID::I64,
    DatTypeID::F32,
    DatTypeID::F64,
    DatTypeID::String,
    DatTypeID::U128,
    DatTypeID::I128,
    DatTypeID::Binary,
    DatTypeID::Array,
];

fn arbitrary_name(u: &mut Unstructured, prefix: &str, index: usize) -> arbitrary::Result<String> {
    let len = (u8::arbitrary(u)? as usize % 8) + 1;
    let mut s = String::with_capacity(prefix.len() + len + 8);
    s.push_str(prefix);
    s.push('_');
    s.push_str(&index.to_string());
    s.push('_');
    for _ in 0..len {
        let ch = (u8::arbitrary(u)? % 26) + b'a';
        s.push(ch as char);
    }
    Ok(s)
}

fn arbitrary_field_type(u: &mut Unstructured) -> arbitrary::Result<DatType> {
    let index = (u8::arbitrary(u)? as usize) % OBJECT_FIELD_TYPE_IDS.len();
    let type_id = OBJECT_FIELD_TYPE_IDS[index];
    if type_id.has_param() {
        type_id.fn_arb_param()(u)
    } else {
        Ok(DatType::default_for(type_id))
    }
}

fn to_arb_err(e: TyErr) -> arbitrary::Error {
    let _ = e;
    arbitrary::Error::IncorrectFormat
}

pub fn fn_object_arb_typed(
    u: &mut Unstructured,
    dat_type: &DatType,
) -> arbitrary::Result<DatValue> {
    let param = dat_type.expect_record_param();
    let mut fields = Vec::with_capacity(param.fields().len());
    for (_, field_ty) in param.fields() {
        let value = field_ty.dat_type_id().fn_arb_internal()(u, field_ty)?;
        fields.push(value);
    }
    Ok(DatValue::from_record(fields))
}

pub fn fn_object_arb_printable(
    u: &mut Unstructured,
    dat_type: &DatType,
) -> arbitrary::Result<String> {
    let value = fn_object_arb_typed(u, dat_type)?;
    let textual = fn_object_out(&value, dat_type).map_err(to_arb_err)?;
    Ok(textual.into())
}

pub fn fn_object_arb_dt_param(u: &mut Unstructured) -> arbitrary::Result<DatType> {
    let field_count = (u8::arbitrary(u)? as usize % 4) + 1;
    let name = arbitrary_name(u, "record", 0)?;
    let mut fields = Vec::with_capacity(field_count);
    for idx in 0..field_count {
        let field_name = arbitrary_name(u, "field", idx)?;
        let field_ty = arbitrary_field_type(u)?;
        fields.push((field_name, field_ty));
    }
    Ok(create_object_type(name, fields))
}

pub const FN_OBJECT_ARBITRARY: FnArbitrary = FnArbitrary {
    param: fn_object_arb_dt_param,
    value_object: fn_object_arb_typed,
    value_print: fn_object_arb_printable,
};
