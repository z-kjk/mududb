use crate::dat_binary::DatBinary;
use crate::dat_json::DatJson;
use crate::dat_textual::DatTextual;
use crate::dat_type::DatType;
use crate::dat_value::DatValue;
use crate::dt_fn_compare::{ErrCompare, FnCompare};
use crate::dt_fn_convert::FnBase;
use crate::type_error::{TyEC, TyErr};
use mudu::common::endian;
use mudu::utils::json::{JsonValue, from_json_str};
use mudu::utils::msg_pack::{MsgPackUtf8String, MsgPackValue};
use std::cmp::Ordering;
use std::hash::Hasher;
use std::str::FromStr;

fn parse_u128_str(value: &str) -> Result<u128, TyErr> {
    u128::from_str(value).map_err(|e| TyErr::new(TyEC::TypeConvertFailed, e.to_string()))
}

fn parse_u128_json(value: &JsonValue) -> Result<u128, TyErr> {
    if let Some(s) = value.as_str() {
        return parse_u128_str(s);
    }
    if let Some(n) = value.as_u64() {
        return Ok(n as u128);
    }
    Err(TyErr::new(
        TyEC::TypeConvertFailed,
        format!("cannot convert json {} to oid", value),
    ))
}

fn fn_u128_in_textual(v: &str, dt: &DatType) -> Result<DatValue, TyErr> {
    let json = from_json_str::<JsonValue>(v)
        .map_err(|e| TyErr::new(TyEC::TypeConvertFailed, e.to_string()))?;
    fn_u128_in_json(&json, dt)
}

fn fn_u128_out_textual(v: &DatValue, dt: &DatType) -> Result<DatTextual, TyErr> {
    let json = fn_u128_out_json(v, dt)?;
    Ok(DatTextual::from(json.to_string()))
}

fn fn_u128_in_json(v: &JsonValue, _: &DatType) -> Result<DatValue, TyErr> {
    Ok(DatValue::from_u128(parse_u128_json(v)?))
}

fn fn_u128_out_json(v: &DatValue, _: &DatType) -> Result<DatJson, TyErr> {
    Ok(DatJson::from(JsonValue::String(v.to_oid().to_string())))
}

fn fn_u128_in_msgpack(msg_pack: &MsgPackValue, _: &DatType) -> Result<DatValue, TyErr> {
    if let Some(s) = msg_pack.as_str() {
        return Ok(DatValue::from_u128(parse_u128_str(s)?));
    }
    if let Some(n) = msg_pack.as_u64() {
        return Ok(DatValue::from_u128(n as u128));
    }
    Err(TyErr::new(
        TyEC::TypeConvertFailed,
        "cannot convert msg pack to oid".to_string(),
    ))
}

fn fn_u128_out_msgpack(v: &DatValue, _: &DatType) -> Result<MsgPackValue, TyErr> {
    Ok(MsgPackValue::String(MsgPackUtf8String::from(
        v.to_oid().to_string(),
    )))
}

fn fn_u128_len(_: &DatType) -> Result<Option<u32>, TyErr> {
    Ok(Some(size_of::<u128>() as u32))
}

fn fn_u128_dat_output_len(_: &DatValue, ty: &DatType) -> Result<u32, TyErr> {
    Ok(fn_u128_len(ty)?.unwrap())
}

fn fn_u128_send(v: &DatValue, _: &DatType) -> Result<DatBinary, TyErr> {
    let oid = v.to_oid();
    let mut buf = vec![0; size_of::<u128>()];
    endian::write_u128(&mut buf, oid);
    Ok(DatBinary::from(buf))
}

fn fn_u128_send_to(v: &DatValue, _: &DatType, buf: &mut [u8]) -> Result<u32, TyErr> {
    if buf.len() < size_of::<u128>() {
        return Err(TyErr::new(
            TyEC::InsufficientSpace,
            "insufficient space".to_string(),
        ));
    }
    endian::write_u128(buf, v.to_oid());
    Ok(size_of::<u128>() as u32)
}

fn fn_u128_recv(buf: &[u8], _: &DatType) -> Result<(DatValue, u32), TyErr> {
    if buf.len() < size_of::<u128>() {
        return Err(TyErr::new(
            TyEC::InsufficientSpace,
            "insufficient space".to_string(),
        ));
    }
    Ok((
        DatValue::from_u128(endian::read_u128(buf)),
        size_of::<u128>() as u32,
    ))
}

fn fn_u128_default(_: &DatType) -> Result<DatValue, TyErr> {
    Ok(DatValue::from_u128(u128::default()))
}

fn fn_u128_order(v1: &DatValue, v2: &DatValue) -> Result<Ordering, ErrCompare> {
    Ok(v1.to_oid().cmp(&v2.to_oid()))
}

fn fn_u128_equal(v1: &DatValue, v2: &DatValue) -> Result<bool, ErrCompare> {
    Ok(v1.to_oid() == v2.to_oid())
}

fn fn_u128_hash(v: &DatValue, hasher: &mut dyn Hasher) -> Result<(), ErrCompare> {
    hasher.write_u128(v.to_oid());
    Ok(())
}

pub const FN_OID_COMPARE: FnCompare = FnCompare {
    order: fn_u128_order,
    equal: fn_u128_equal,
    hash: fn_u128_hash,
};

pub const FN_OID_CONVERT: FnBase = FnBase {
    input_textual: fn_u128_in_textual,
    output_textual: fn_u128_out_textual,
    input_json: fn_u128_in_json,
    output_json: fn_u128_out_json,
    input_msg_pack: fn_u128_in_msgpack,
    output_msg_pack: fn_u128_out_msgpack,
    type_len: fn_u128_len,
    data_len: fn_u128_dat_output_len,
    receive: fn_u128_recv,
    send: fn_u128_send,
    send_to: fn_u128_send_to,
    default: fn_u128_default,
};
