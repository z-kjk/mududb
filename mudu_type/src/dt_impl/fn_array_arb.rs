use crate::dat_textual::DatTextual;
use crate::dat_type::DatType;
use crate::dat_type_id::DatTypeID;
use crate::dat_value::DatValue;
use crate::dt_fn_arbitrary::FnArbitrary;
use crate::dtp_array::DTPArray;
use arbitrary::{Arbitrary, Unstructured};

const ARRAY_INNER_TYPE_IDS: [DatTypeID; 8] = [
    DatTypeID::I32,
    DatTypeID::I64,
    DatTypeID::F32,
    DatTypeID::F64,
    DatTypeID::String,
    DatTypeID::U128,
    DatTypeID::I128,
    DatTypeID::Binary,
];

pub fn fn_array_arb_object(
    u: &mut Unstructured,
    dat_type: &DatType,
) -> arbitrary::Result<DatValue> {
    let n = u8::arbitrary(u)? as usize;
    let param = dat_type.expect_array_param();
    let inner_type = param.dat_type();
    let mut vec = Vec::with_capacity(n);
    for _ in 0..n {
        let dat = inner_type.dat_type_id().fn_arb_internal()(u, inner_type)?;
        vec.push(dat);
    }
    Ok(DatValue::from_array(vec))
}

pub fn fn_array_arb_printable(
    u: &mut Unstructured,
    dat_type: &DatType,
) -> arbitrary::Result<String> {
    let object = fn_array_arb_object(u, dat_type)?;
    let printable: DatTextual = DatTypeID::Array.fn_output()(&object, dat_type).unwrap();
    Ok(printable.into())
}

pub fn fn_array_arb_dt_param(u: &mut Unstructured) -> arbitrary::Result<DatType> {
    let n = (u8::arbitrary(u)? as usize) % ARRAY_INNER_TYPE_IDS.len();
    let dat_type_id = ARRAY_INNER_TYPE_IDS[n];
    let inner_type = if dat_type_id.has_param() {
        dat_type_id.fn_arb_param()(u)?
    } else {
        DatType::default_for(dat_type_id)
    };
    let param = DTPArray::new(inner_type);
    let dat_type = DatType::from_array(param);
    Ok(dat_type)
}

pub const FN_ARRAY_ARBITRARY: FnArbitrary = FnArbitrary {
    param: fn_array_arb_dt_param,
    value_object: fn_array_arb_object,
    value_print: fn_array_arb_printable,
};
