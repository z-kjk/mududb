use crate::dat_binary::DatBinary;
use crate::dat_json::DatJson;
use crate::dat_textual::DatTextual;
use crate::dat_type::DatType;
use crate::dat_value::DatValue;
use crate::dt_fn_compare::{ErrCompare, FnCompare};
use crate::dt_fn_convert::FnBase;
use crate::type_error::{TyEC, TyErr};
use byteorder::ByteOrder;
use mudu::common::endian::Endian;
use mudu::utils::json::{JsonValue, from_json_str};
use mudu::utils::msg_pack::{MsgPackUtf8String, MsgPackValue};
use std::cmp::Ordering;
use std::hash::Hasher;
use std::str::FromStr;

fn parse_i128_str(value: &str) -> Result<i128, TyErr> {
    i128::from_str(value).map_err(|e| TyErr::new(TyEC::TypeConvertFailed, e.to_string()))
}

fn parse_i128_json(value: &JsonValue) -> Result<i128, TyErr> {
    if let Some(s) = value.as_str() {
        return parse_i128_str(s);
    }
    if let Some(n) = value.as_i64() {
        return Ok(n as i128);
    }
    if let Some(n) = value.as_u64() {
        return Ok(n as i128);
    }
    Err(TyErr::new(
        TyEC::TypeConvertFailed,
        format!("cannot convert json {} to i128", value),
    ))
}

fn fn_i128_in_textual(v: &str, dt: &DatType) -> Result<DatValue, TyErr> {
    let json = from_json_str::<JsonValue>(v)
        .map_err(|e| TyErr::new(TyEC::TypeConvertFailed, e.to_string()))?;
    fn_i128_in_json(&json, dt)
}

fn fn_i128_out_textual(v: &DatValue, dt: &DatType) -> Result<DatTextual, TyErr> {
    let json = fn_i128_out_json(v, dt)?;
    Ok(DatTextual::from(json.to_string()))
}

fn fn_i128_in_json(v: &JsonValue, _: &DatType) -> Result<DatValue, TyErr> {
    Ok(DatValue::from_i128(parse_i128_json(v)?))
}

fn fn_i128_out_json(v: &DatValue, _: &DatType) -> Result<DatJson, TyErr> {
    Ok(DatJson::from(JsonValue::String(v.to_i128().to_string())))
}

fn fn_i128_in_msgpack(msg_pack: &MsgPackValue, _: &DatType) -> Result<DatValue, TyErr> {
    if let Some(s) = msg_pack.as_str() {
        return Ok(DatValue::from_i128(parse_i128_str(s)?));
    }
    if let Some(n) = msg_pack.as_i64() {
        return Ok(DatValue::from_i128(n as i128));
    }
    if let Some(n) = msg_pack.as_u64() {
        return Ok(DatValue::from_i128(n as i128));
    }
    Err(TyErr::new(
        TyEC::TypeConvertFailed,
        "cannot convert msg pack to i128".to_string(),
    ))
}

fn fn_i128_out_msgpack(v: &DatValue, _: &DatType) -> Result<MsgPackValue, TyErr> {
    Ok(MsgPackValue::String(MsgPackUtf8String::from(
        v.to_i128().to_string(),
    )))
}

fn fn_i128_len(_: &DatType) -> Result<Option<u32>, TyErr> {
    Ok(Some(size_of::<i128>() as u32))
}

fn fn_i128_dat_output_len(_: &DatValue, ty: &DatType) -> Result<u32, TyErr> {
    Ok(fn_i128_len(ty)?.unwrap())
}

fn fn_i128_send(v: &DatValue, _: &DatType) -> Result<DatBinary, TyErr> {
    let value = v.to_i128();
    let mut buf = vec![0; size_of::<i128>()];
    Endian::write_i128(&mut buf, value);
    Ok(DatBinary::from(buf))
}

fn fn_i128_send_to(v: &DatValue, _: &DatType, buf: &mut [u8]) -> Result<u32, TyErr> {
    if buf.len() < size_of::<i128>() {
        return Err(TyErr::new(
            TyEC::InsufficientSpace,
            "insufficient space".to_string(),
        ));
    }
    Endian::write_i128(buf, v.to_i128());
    Ok(size_of::<i128>() as u32)
}

fn fn_i128_recv(buf: &[u8], _: &DatType) -> Result<(DatValue, u32), TyErr> {
    if buf.len() < size_of::<i128>() {
        return Err(TyErr::new(
            TyEC::InsufficientSpace,
            "insufficient space".to_string(),
        ));
    }
    Ok((
        DatValue::from_i128(Endian::read_i128(buf)),
        size_of::<i128>() as u32,
    ))
}

fn fn_i128_default(_: &DatType) -> Result<DatValue, TyErr> {
    Ok(DatValue::from_i128(i128::default()))
}

fn fn_i128_order(v1: &DatValue, v2: &DatValue) -> Result<Ordering, ErrCompare> {
    Ok(v1.to_i128().cmp(&v2.to_i128()))
}

fn fn_i128_equal(v1: &DatValue, v2: &DatValue) -> Result<bool, ErrCompare> {
    Ok(v1.to_i128() == v2.to_i128())
}

fn fn_i128_hash(v: &DatValue, hasher: &mut dyn Hasher) -> Result<(), ErrCompare> {
    hasher.write_i128(v.to_i128());
    Ok(())
}

pub const FN_I128_COMPARE: FnCompare = FnCompare {
    order: fn_i128_order,
    equal: fn_i128_equal,
    hash: fn_i128_hash,
};

pub const FN_I128_CONVERT: FnBase = FnBase {
    input_textual: fn_i128_in_textual,
    output_textual: fn_i128_out_textual,
    input_json: fn_i128_in_json,
    output_json: fn_i128_out_json,
    input_msg_pack: fn_i128_in_msgpack,
    output_msg_pack: fn_i128_out_msgpack,
    type_len: fn_i128_len,
    data_len: fn_i128_dat_output_len,
    receive: fn_i128_recv,
    send: fn_i128_send,
    send_to: fn_i128_send_to,
    default: fn_i128_default,
};
