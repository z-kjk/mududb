use crate::dat_type::DatType;
use crate::dat_type_id::DatTypeID;
use crate::dat_value::DatValue;
use crate::dt_fn_arbitrary::FnArbitrary;
use arbitrary::{Arbitrary, Unstructured};

pub fn fn_u128_arb_val(u: &mut Unstructured, _: &DatType) -> arbitrary::Result<DatValue> {
    Ok(DatValue::from_u128(u128::arbitrary(u)?))
}

pub fn fn_u128_arb_printable(u: &mut Unstructured, _: &DatType) -> arbitrary::Result<String> {
    Ok(format!("\"{}\"", u128::arbitrary(u)?))
}

pub fn fn_u128_arb_dt_param(_u: &mut Unstructured) -> arbitrary::Result<DatType> {
    Ok(DatType::new_no_param(DatTypeID::U128))
}

pub const FN_OID_ARBITRARY: FnArbitrary = FnArbitrary {
    param: fn_u128_arb_dt_param,
    value_object: fn_u128_arb_val,
    value_print: fn_u128_arb_printable,
};
