use crate::dat_type::DatType;
use crate::dat_type_id::DatTypeID;
use crate::dat_value::DatValue;
use crate::dt_fn_arbitrary::FnArbitrary;
use arbitrary::{Arbitrary, Unstructured};

fn arb_finite_f64(u: &mut Unstructured) -> arbitrary::Result<f64> {
    let value = f64::arbitrary(u)?;
    Ok(if value.is_finite() { value } else { 0.0 })
}

pub fn fn_f64_arb_val(u: &mut Unstructured, _: &DatType) -> arbitrary::Result<DatValue> {
    Ok(DatValue::from_f64(arb_finite_f64(u)?))
}

pub fn fn_f64_arb_printable(u: &mut Unstructured, _: &DatType) -> arbitrary::Result<String> {
    Ok(arb_finite_f64(u)?.to_string())
}

pub fn fn_f64_arb_dt_param(_u: &mut Unstructured) -> arbitrary::Result<DatType> {
    Ok(DatType::new_no_param(DatTypeID::F64))
}

pub const FN_F64_ARBITRARY: FnArbitrary = FnArbitrary {
    param: fn_f64_arb_dt_param,
    value_object: fn_f64_arb_val,
    value_print: fn_f64_arb_printable,
};
