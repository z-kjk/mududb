pub mod latest;

use super::{Frame, FrameHeader};
use mudu::common::result::RS;
use mudu::error::ec::EC;
use mudu::m_error;

pub use latest::HEADER_LEN;

pub fn encode_latest(frame: &Frame) -> Vec<u8> {
    latest::encode(frame)
}

pub fn decode(buf: &[u8]) -> RS<Frame> {
    match peek_version(buf)? {
        1 => latest::decode(buf),
        version => Err(m_error!(
            EC::ParseErr,
            format!("unsupported frame version {}", version)
        )),
    }
}

pub fn decode_header_bytes(buf: &[u8]) -> RS<FrameHeader> {
    match peek_version(buf)? {
        1 => latest::decode_header_bytes(buf),
        version => Err(m_error!(
            EC::ParseErr,
            format!("unsupported frame version {}", version)
        )),
    }
}

fn peek_version(buf: &[u8]) -> RS<u32> {
    if buf.len() < HEADER_LEN {
        return Err(m_error!(EC::ParseErr, "frame header is incomplete"));
    }
    Ok(u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]))
}
