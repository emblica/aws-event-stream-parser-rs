//! aws_event_stream_parser is a Rust implementation of the `vnd.amazon.event-stream`-binary format
//! It's used in some AWS APIs, this library has been created to be used with AWS Kinesis **SubscribeToShard**-endpoint
//!

#[cfg(test)]
#[macro_use]
extern crate hex_literal;
#[macro_use]
extern crate nom;
extern crate byteorder;
extern crate bytes;
extern crate chrono;
extern crate crc;
extern crate tokio_util;
extern crate uuid;
use byteorder::BigEndian;
use byteorder::ByteOrder;
use bytes::BufMut;
use chrono::prelude::*;
use crc::crc32;
use crc::Hasher32;
use nom::number::streaming::{be_u16, be_u32, be_u64, be_u8};
use nom::Err::Error;
use nom::Err::Failure;
use nom::Err::Incomplete;
use nom::IResult;
use std::convert::From;
use uuid::Uuid;
const PRELUDE_SIZE: u32 = 12;
const CHECKSUM_SIZE: u32 = 4;

use nom::bytes::streaming::take;

fn u32_to_u8(x: u32) -> Vec<u8> {
    let mut buf = [0u8; 4];
    BigEndian::write_u32(&mut buf, x);
    buf.to_vec()
}

fn u64_to_u8(x: u64) -> Vec<u8> {
    let mut buf = [0u8; 8];
    BigEndian::write_u64(&mut buf, x);
    buf.to_vec()
}

fn u16_to_u8(x: u16) -> Vec<u8> {
    let mut buf = [0u8; 2];
    BigEndian::write_u16(&mut buf, x);
    buf.to_vec()
}

#[derive(Debug, PartialEq)]
pub enum HeaderValue {
    Boolean(bool),
    Bytes(Vec<u8>),
    Short(u16),
    Integer(u32),
    Long(u64),
    Byte(u8),
    String(String),
    Timestamp(DateTime<Utc>),
    Uuid(Uuid),
    Unknown,
}

impl HeaderValue {
    pub fn type_of(&self) -> u8 {
        match self {
            HeaderValue::Boolean(true) => 0,
            HeaderValue::Boolean(false) => 1,
            HeaderValue::Byte(_) => 2,
            HeaderValue::Short(_) => 3,
            HeaderValue::Integer(_) => 4,
            HeaderValue::Long(_) => 5,
            HeaderValue::Bytes(_) => 6,
            HeaderValue::String(_) => 7,
            HeaderValue::Timestamp(_) => 8,
            HeaderValue::Uuid(_) => 9,
            HeaderValue::Unknown => 10,
        }
    }

    pub fn as_buffer(&self) -> Vec<u8> {
        match self {
            HeaderValue::Byte(n) => vec![*n],
            HeaderValue::Short(n) => u16_to_u8(*n),
            HeaderValue::Integer(n) => u32_to_u8(*n),
            HeaderValue::Long(n) => u64_to_u8(*n),
            HeaderValue::Bytes(b) => {
                let mut buf = vec![];
                buf.extend(u16_to_u8(b.len() as u16));
                buf.extend(b);
                buf
            }
            HeaderValue::String(s) => {
                let mut buf = vec![];
                buf.extend(u16_to_u8(s.len() as u16));
                buf.extend(s.as_bytes());
                buf
            }
            HeaderValue::Timestamp(ts) => u64_to_u8(ts.timestamp() as u64),
            HeaderValue::Uuid(uuid) => uuid.as_bytes().to_vec(),
            // Boolean fields don't contain content
            _ => vec![],
        }
    }
}

impl From<bool> for HeaderValue {
    fn from(item: bool) -> Self {
        HeaderValue::Boolean(item)
    }
}
impl From<Vec<u8>> for HeaderValue {
    fn from(item: Vec<u8>) -> Self {
        HeaderValue::Bytes(item)
    }
}
impl From<&[u8]> for HeaderValue {
    fn from(item: &[u8]) -> Self {
        HeaderValue::Bytes(item.to_vec())
    }
}
impl From<u16> for HeaderValue {
    fn from(item: u16) -> Self {
        HeaderValue::Short(item)
    }
}
impl From<u32> for HeaderValue {
    fn from(item: u32) -> Self {
        HeaderValue::Integer(item)
    }
}
impl From<u64> for HeaderValue {
    fn from(item: u64) -> Self {
        HeaderValue::Long(item)
    }
}
impl From<u8> for HeaderValue {
    fn from(item: u8) -> Self {
        HeaderValue::Byte(item)
    }
}
impl From<String> for HeaderValue {
    fn from(item: String) -> Self {
        HeaderValue::String(item)
    }
}

impl From<&str> for HeaderValue {
    fn from(item: &str) -> Self {
        HeaderValue::String(item.to_string())
    }
}
impl From<DateTime<Utc>> for HeaderValue {
    fn from(item: DateTime<Utc>) -> Self {
        HeaderValue::Timestamp(item)
    }
}
impl From<Uuid> for HeaderValue {
    fn from(item: Uuid) -> Self {
        HeaderValue::Uuid(item)
    }
}

#[derive(Debug, PartialEq)]
pub struct PreludeBlock {
    pub total_length: u32,
    pub headers_length: u32,
    pub checksum: u32,
}

impl PreludeBlock {
    pub fn new(total_length: u32, headers_length: u32) -> PreludeBlock {
        let crc = PreludeBlock::_calculate_crc(total_length, headers_length);
        PreludeBlock {
            total_length,
            headers_length,
            checksum: crc,
        }
    }
    fn _calculate_crc(total_length: u32, headers_length: u32) -> u32 {
        let mut digest = crc32::Digest::new(crc32::IEEE);
        digest.write(&u32_to_u8(total_length));
        digest.write(&u32_to_u8(headers_length));
        digest.sum32()
    }
    pub fn calculate_crc(&self) -> u32 {
        PreludeBlock::_calculate_crc(self.total_length, self.headers_length)
    }

    pub fn valid(&self) -> bool {
        self.calculate_crc() == self.checksum
    }

    pub fn as_buffer(&self) -> Vec<u8> {
        let mut buf = vec![];
        buf.extend(u32_to_u8(self.total_length).iter());
        buf.extend(u32_to_u8(self.headers_length).iter());
        buf.extend(u32_to_u8(self.checksum).iter());
        buf
    }
}

#[derive(Debug, PartialEq)]
pub struct Header {
    pub key: String,
    pub value: HeaderValue,
}

impl Header {
    pub fn new(key: String, value: HeaderValue) -> Header {
        Header { key, value }
    }
    pub fn from_pair(key: impl Into<String>, value: impl Into<HeaderValue>) -> Header {
        Header {
            key: key.into(),
            value: value.into(),
        }
    }
    pub fn as_buffer(&self) -> Vec<u8> {
        let mut buf: Vec<u8> = vec![self.key.len() as u8];
        buf.extend(self.key.as_bytes());
        buf.push(self.value.type_of());
        buf.extend(self.value.as_buffer());
        buf
    }
}

#[derive(Debug, PartialEq)]
pub struct HeaderBlock {
    pub headers: Vec<Header>,
}
impl HeaderBlock {
    pub fn as_buffer(&self) -> Vec<u8> {
        let v: Vec<Vec<u8>> = self
            .headers
            .iter()
            .map(|h: &Header| h.as_buffer())
            .collect();
        v.concat()
    }
}

#[derive(Debug, PartialEq)]
pub struct Message {
    pub prelude: PreludeBlock,
    pub headers: HeaderBlock,
    pub body: Vec<u8>,
    pub checksum: u32,
}

impl Message {
    pub fn new(prelude: PreludeBlock, headers: HeaderBlock, body: Vec<u8>) -> Message {
        let crc = Message::_calculate_crc_from_values(&prelude, &headers, &body);
        Message {
            prelude,
            headers,
            body,
            checksum: crc,
        }
    }

    pub fn build(headers: HeaderBlock, body: Vec<u8>) -> Message {
        let headers_length = headers.as_buffer().len() as u32;
        let total_length = headers_length + body.len() as u32 + PRELUDE_SIZE + CHECKSUM_SIZE;
        let prelude = PreludeBlock::new(total_length, headers_length);
        let crc = Message::_calculate_crc_from_values(&prelude, &headers, &body);
        Message {
            prelude,
            headers,
            body,
            checksum: crc,
        }
    }

    pub fn calculate_crc(&self) -> u32 {
        Message::_calculate_crc_from_values(&self.prelude, &self.headers, &self.body)
    }
    fn _calculate_crc_from_values(
        prelude: &PreludeBlock,
        headers: &HeaderBlock,
        body: &[u8],
    ) -> u32 {
        let mut digest = crc32::Digest::new(crc32::IEEE);
        digest.write(&prelude.as_buffer());
        digest.write(&headers.as_buffer());
        digest.write(&body);
        digest.sum32()
    }
    pub fn as_buffer(&self) -> BytesMut {
        let mut buf = BytesMut::with_capacity(self.prelude.total_length as usize);
        buf.put_slice(&self.prelude.as_buffer());
        buf.put_slice(&self.headers.as_buffer());
        buf.put_slice(&self.body);
        buf.put_slice(&u32_to_u8(self.checksum));
        buf
    }
    pub fn valid(&self) -> bool {
        self.calculate_crc() == self.checksum
            && self.prelude.valid()
            && self.as_buffer().len() == self.prelude.total_length as usize
    }
}

named!(parse_byte<&[u8], HeaderValue>, do_parse!(
    value: be_u8 >>
    (HeaderValue::Byte(value))
));

named!(parse_short<&[u8], HeaderValue>, do_parse!(
    value: be_u16 >>
    (HeaderValue::Short(value))
));

named!(parse_integer<&[u8], HeaderValue>, do_parse!(
    value: be_u32 >>
    (HeaderValue::Integer(value))
));

named!(parse_long<&[u8], HeaderValue>, do_parse!(
    value: be_u64 >>
    (HeaderValue::Long(value))
));

named!(parse_bytes<&[u8], HeaderValue>, do_parse!(
    length: be_u16 >>
    value: take!(length) >>
    (HeaderValue::Bytes(value.into()))
));

named!(parse_string<&[u8], HeaderValue>, do_parse!(
    length: be_u16 >>
    value: take_str!(length) >>
    (HeaderValue::String(value.to_string()))
));

named!(parse_timestamp<&[u8], HeaderValue>, do_parse!(
    epoch: be_u64 >>
    (HeaderValue::Timestamp(Utc.timestamp_opt(epoch as i64, 0).unwrap()))
));

named!(parse_uuid<&[u8], HeaderValue>, do_parse!(
    vals: take!(16) >>
    (HeaderValue::Uuid(Uuid::from_slice(&vals).unwrap()))
));

named!(parse_prelude<&[u8], PreludeBlock>, do_parse!(
    message_total_length: be_u32 >>
    headers_length: be_u32 >>
    prelude_checksum: be_u32 >>
    (PreludeBlock {
        total_length: message_total_length,
        headers_length: headers_length,
        checksum: prelude_checksum
     })
));

// 0 => HeaderValue::BooleanTrue,
// 1 => HeaderValue::BooleanFalse,
// 2 => HeaderValue::Byte,
// 3 => HeaderValue::Short,
// 4 => HeaderValue::Integer,
// 5 => HeaderValue::Long,
// 6 => HeaderValue::Bytes,
// 7 => HeaderValue::String,
// 8 => HeaderValue::Timestamp,
// 9 => HeaderValue::Uuid,
// _ => HeaderValue::Unknown,

named!(parse_header<&[u8], Header>, do_parse!(
    header_key_length: be_u8 >>
    header_key: take_str!(header_key_length) >>
    value: switch!(be_u8,
        0 => value!(HeaderValue::Boolean(true)) |
        1 => value!(HeaderValue::Boolean(false)) |
        2 => call!(parse_byte) |
        3 => call!(parse_short) |
        4 => call!(parse_integer) |
        5 => call!(parse_long) |
        6 => call!(parse_bytes) |
        7 => call!(parse_string) |
        8 => call!(parse_timestamp) |
        9 => call!(parse_uuid) |
        _ => value!(HeaderValue::Unknown)
    ) >>
    (Header {
        key: header_key.to_string(),
        value: value
     })
));

fn parse_header_block(input: &[u8], block_length: u32) -> IResult<&[u8], HeaderBlock> {
    let (input, header_bytes) = take(block_length)(input)?;

    let mut buffer = header_bytes;
    let mut headers = Vec::new();
    while !buffer.is_empty() {
        let (header_bytes, header) = parse_header(buffer)?;
        buffer = header_bytes;
        headers.push(header);
    }

    Ok((input, HeaderBlock { headers }))
}

fn calculate_body_length(prelude: &PreludeBlock) -> usize {
    prelude
        .total_length
        .checked_sub(PRELUDE_SIZE)
        .and_then(|l| l.checked_sub(prelude.headers_length))
        .and_then(|l| l.checked_sub(CHECKSUM_SIZE))
        .map(|l| l as usize)
        .unwrap_or(0)
}

named!(pub parse_message<&[u8], Message>, do_parse!(
    prelude: parse_prelude >>
    headers: call!(parse_header_block, prelude.headers_length) >>
    body: take!(calculate_body_length(&prelude)) >>
    checksum: be_u32 >>
    (Message {
        prelude: prelude,
        headers: headers,
        body: body.into(),
        checksum: checksum
     })
));

use bytes::BytesMut;
use std::{io, usize};
use tokio_util::codec::{Decoder, Encoder};

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Default)]
pub struct EventStreamCodec;


impl Decoder for EventStreamCodec {
    type Item = Message;
    type Error = std::io::Error;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Message>, Self::Error> {
        match parse_message(buf) {
            Ok((rest_bytes, message)) => {
                *buf = BytesMut::from(rest_bytes);
                Ok(Some(message))
            }
            Err(Incomplete(_)) => Ok(None),
            Err(Error(_)) => Ok(None),
            Err(Failure((_, e))) => {
                Err(io::Error::new(io::ErrorKind::InvalidData, e.description()))
            }
        }
    }
}

impl Encoder<Message> for EventStreamCodec {
    // type Item = Message;
    type Error = io::Error;

    fn encode(&mut self, msg: Message, buf: &mut BytesMut) -> Result<(), io::Error> {
        let msg_buf = msg.as_buffer();
        buf.reserve(msg_buf.len());
        buf.put(msg_buf);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_prelude_total_length() {
        let res = parse_prelude(&hex!("0000001500000001ba25f70d03666f6f013aa3e0d6"))
            .unwrap()
            .1;
        assert_eq!(res.total_length, 21);
    }

    #[test]
    fn test_parse_prelude_headers_length() {
        let res = parse_prelude(&hex!("0000001500000001ba25f70d03666f6f013aa3e0d6"))
            .unwrap()
            .1;
        assert_eq!(res.headers_length, 1);
        assert_eq!(res.calculate_crc(), res.checksum);
    }

    #[test]
    fn test_parse_prelude_headers_checksum() {
        let res = parse_prelude(&hex!("0000001500000001ba25f70d03666f6f013aa3e0d6"))
            .unwrap()
            .1;
        assert_eq!(res.checksum, 3_123_050_253);
        assert_eq!(res.calculate_crc(), res.checksum);
    }

    #[test]
    fn test_parse_prelude_is_valid() {
        let res = parse_prelude(&hex!("0000001500000001ba25f70d03666f6f013aa3e0d6"))
            .unwrap()
            .1;
        assert_eq!(res.valid(), true);
        assert_eq!(res.calculate_crc(), res.checksum);
    }

    #[test]
    fn test_parse_prelude_is_invalid() {
        let res = parse_prelude(&hex!("0000001000000000001020304d98c8ff"))
            .unwrap()
            .1;
        assert_eq!(res.total_length, 16);
        assert_eq!(res.headers_length, 0);
        assert_eq!(res.checksum, 1_056_816);
        assert_eq!(res.valid(), false);
        assert_ne!(res.calculate_crc(), res.checksum);
    }

    #[test]
    fn test_no_headers_no_body() {
        let res = parse_message(&hex!("000000100000000005c248eb7d98c8ff"))
            .unwrap()
            .1;

        assert_eq!(res.prelude.total_length, 16);
        assert_eq!(res.prelude.headers_length, 0);
        assert_eq!(res.prelude.checksum, 96_618_731);
        assert_eq!(res.checksum, 2_107_164_927);
        assert_eq!(res.calculate_crc(), res.checksum);
        assert!(res.valid())
    }

    #[test]
    fn test_false_boolean_header() {
        let res = parse_message(&hex!("0000001500000005bd48331403666f6f01004c70c4"))
            .unwrap()
            .1;

        assert_eq!(res.prelude.total_length, 21);
        assert_eq!(res.prelude.headers_length, 5);
        assert_eq!(res.prelude.checksum, 3_175_625_492);
        assert_eq!(res.prelude.calculate_crc(), res.prelude.checksum);
        assert_eq!(res.calculate_crc(), res.checksum);
        assert_eq!(res.checksum, 5_009_604);
        assert!(res.prelude.valid());
        assert!(res.valid())
    }

    #[test]
    fn test_true_boolean_header() {
        let res = parse_message(&hex!("0000001500000005ba25f70d03666f6f004da4d040"))
            .unwrap()
            .1;

        assert_eq!(res.prelude.total_length, 21);
        assert_eq!(res.prelude.headers_length, 5);
        assert_eq!(res.prelude.checksum, 3_123_050_253);
        assert_eq!(res.checksum, 1_302_646_848);
    }

    #[test]
    fn test_byte_header() {
        let res = parse_message(&hex!("0000001600000006fd858ddd03666f6f02ffa44bfd93"))
            .unwrap()
            .1;

        assert_eq!(res.prelude.total_length, 22);
        assert_eq!(res.prelude.headers_length, 6);
        assert_eq!(res.prelude.checksum, 4_253_388_253);
        assert_eq!(res.checksum, 2_756_443_539);
    }

    #[test]
    fn test_bytes_header() {
        let res = parse_message(&hex!(
            "0000001c0000000cb735957c03666f6f0600050102030405cdda4038"
        ))
        .unwrap()
        .1;

        assert_eq!(res.prelude.total_length, 28);
        assert_eq!(res.prelude.headers_length, 12);
        assert_eq!(res.prelude.checksum, 3_073_742_204);
        assert_eq!(res.checksum, 3_453_632_568);
    }

    #[test]
    fn test_int32_header() {
        let res = parse_message(&hex!("0000002d0000001041c424b80a6576656e742d74797065040000a00c7b27666f6f273a27626172277d36f480a0")).unwrap().1;

        assert_eq!(res.prelude.total_length, 45);
        assert_eq!(res.prelude.headers_length, 16);
        assert_eq!(res.prelude.checksum, 1_103_373_496);
        assert_eq!(res.calculate_crc(), res.checksum);
        assert_eq!(res.checksum, 921_993_376);
        assert!(res.valid())
    }

    #[test]
    fn test_one_str_header() {
        let res = parse_message(&hex!("0000003d0000002007fd83960c636f6e74656e742d747970650700106170706c69636174696f6e2f6a736f6e7b27666f6f273a27626172277d8d9c08b1")).unwrap().1;

        assert_eq!(res.prelude.total_length, 61);
        assert_eq!(res.prelude.headers_length, 32);
        assert_eq!(res.prelude.checksum, 134_054_806);
        assert_eq!(res.calculate_crc(), res.checksum);
        assert_eq!(res.checksum, 2_375_813_297);
        assert!(res.valid())
    }

    #[test]
    fn test_all_headers() {
        let res = parse_message(&hex!("000000cc000000af0fae64ca0a6576656e742d74797065040000a00c0c636f6e74656e742d747970650700106170706c69636174696f6e2f6a736f6e0a626f6f6c2066616c73650109626f6f6c207472756500046279746502cf08627974652062756606001449276d2061206c6974746c6520746561706f74210974696d657374616d70080000000000845fed05696e74313603002a05696e7436340500000000028757b20475756964090102030405060708090a0b0c0d0e0f107b27666f6f273a27626172277daba5f10c")).unwrap().1;

        assert_eq!(res.prelude.total_length, 204);
        assert_eq!(res.prelude.headers_length, 175);
        assert_eq!(res.prelude.checksum, 263_087_306);
        assert_eq!(res.calculate_crc(), res.checksum);
        assert_eq!(res.checksum, 2_879_779_084);
        assert!(res.valid());
        assert_eq!(res.headers.headers.len(), 10);
        let hdr_1 = &res.headers.headers[0];
        let hdr_2 = &res.headers.headers[1];
        assert_eq!(hdr_1.key, "event-type");
        assert_eq!(hdr_2.key, "content-type");
        if let HeaderValue::String(s) = &hdr_2.value {
            assert_eq!(*s, "application/json".to_string());
        }
        assert_eq!(
            res.body,
            [123, 39, 102, 111, 111, 39, 58, 39, 98, 97, 114, 39, 125]
        );
    }

    #[test]
    fn test_symmetric_from_bytes() {
        let original = hex!("000000cc000000af0fae64ca0a6576656e742d74797065040000a00c0c636f6e74656e742d747970650700106170706c69636174696f6e2f6a736f6e0a626f6f6c2066616c73650109626f6f6c207472756500046279746502cf08627974652062756606001449276d2061206c6974746c6520746561706f74210974696d657374616d70080000000000845fed05696e74313603002a05696e7436340500000000028757b20475756964090102030405060708090a0b0c0d0e0f107b27666f6f273a27626172277daba5f10c");
        let res = parse_message(&original).unwrap().1;

        assert_eq!(res.as_buffer().to_vec(), original.to_vec());
    }

    #[test]
    fn test_body_length_calculation() {
        assert_eq!(0, calculate_body_length(&PreludeBlock::new(0, 0)));
        assert_eq!(0, calculate_body_length(&PreludeBlock::new(10, 0)));
        assert_eq!(4, calculate_body_length(&PreludeBlock::new(20, 0)));
        assert_eq!(14, calculate_body_length(&PreludeBlock::new(30, 0)));
    }
    #[test]
    fn test_message_build() {
        let a = Message::new(
            PreludeBlock::new(16, 0),
            HeaderBlock { headers: vec![] },
            vec![],
        );
        let b = Message::build(HeaderBlock { headers: vec![] }, vec![]);
        assert_eq!(a, b);
    }

    #[test]
    fn test_symmetric_from_struct() {
        let original = Message::build(HeaderBlock { headers: vec![] }, vec![]);
        assert!(original.valid());
        let buffer = original.as_buffer().to_vec();
        let res = parse_message(&buffer).unwrap().1;
        assert!(res.valid());
        assert_eq!(res, original);
    }

    #[test]
    fn test_invalid_total_length() {
        let original = Message::new(
            PreludeBlock::new(17, 0),
            HeaderBlock { headers: vec![] },
            vec![],
        );
        assert!(!original.valid());
        let buffer = original.as_buffer().to_vec();
        let res = parse_message(&buffer).is_err();
        assert!(res);
    }

    #[test]
    fn test_throw_if_no_args() {
        let res = parse_message(&hex!(""));
        assert!(res.is_err());
    }

    #[test]
    fn test_none_if_no_buffer_length_match() {
        let res = parse_message(&hex!(
            "0000003d0000002007fd83960c636f6e74656e742d747970650700106170706c69636174696f6e2f6a73"
        ));
        assert!(res.is_err());
    }

    #[test]
    fn test_handles_short_headers() {
        assert_eq!(3, 3);
    }
    #[test]
    fn test_u16_to_u8() {
        assert_eq!(vec![0u8, 0u8], u16_to_u8(0u16));
        assert_eq!(vec![255u8, 255u8], u16_to_u8(65535u16));
    }
    #[test]
    fn test_u32_to_u8() {
        assert_eq!(vec![0u8, 0u8, 0u8, 0u8], u32_to_u8(0u32));
        assert_eq!(vec![0u8, 0u8, 255u8, 255u8], u32_to_u8(65535u32));
        assert_eq!(
            vec![255u8, 255u8, 255u8, 255u8],
            u32_to_u8(4_294_967_295u32)
        );
    }
    #[test]
    fn test_u64_to_u8() {
        assert_eq!(
            vec![0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8],
            u64_to_u8(0u64)
        );
        assert_eq!(
            vec![0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 255u8, 255u8],
            u64_to_u8(65535u64)
        );
        assert_eq!(
            vec![0u8, 0u8, 0u8, 0u8, 255u8, 255u8, 255u8, 255u8],
            u64_to_u8(4_294_967_295u64)
        );
    }

    #[test]
    fn test_headervalue_from_boolean() {
        let a: HeaderValue = true.into();
        let b: HeaderValue = false.into();
        assert_eq!(a, HeaderValue::Boolean(true));
        assert_eq!(b, HeaderValue::Boolean(false));
    }

    #[test]
    fn test_headervalue_from_byte() {
        let a: HeaderValue = 3u8.into();
        assert_eq!(a, HeaderValue::Byte(3));
    }
    #[test]
    fn test_headervalue_from_short() {
        let a: HeaderValue = 4333u16.into();
        assert_eq!(a, HeaderValue::Short(4333));
    }
    #[test]
    fn test_headervalue_from_integer() {
        let a: HeaderValue = 411333u32.into();
        assert_eq!(a, HeaderValue::Integer(411333));
    }
    #[test]
    fn test_headervalue_from_long() {
        let a: HeaderValue = 4333781237812377u64.into();
        assert_eq!(a, HeaderValue::Long(4333781237812377));
    }
    #[test]
    fn test_headervalue_from_string() {
        let a: HeaderValue = "asdadsd&&€".to_string().into();
        assert_eq!(a, HeaderValue::String("asdadsd&&€".to_string()));
    }
    #[test]
    fn test_headervalue_from_str() {
        let a: HeaderValue = "asdadsd&&€".into();
        assert_eq!(a, HeaderValue::String("asdadsd&&€".to_string()));
    }

    #[test]
    fn test_headervalue_from_bytes() {
        let a: HeaderValue = vec![23u8, 120u8].into();
        assert_eq!(a, HeaderValue::Bytes([23u8, 120u8].to_vec()));
    }

    #[test]
    fn test_headervalue_from_timestamp() {
        let dt = Utc.ymd(2014, 7, 8).and_hms(9, 10, 11);

        let a: HeaderValue = dt.into();
        assert_eq!(a, HeaderValue::Timestamp(dt));
    }
    #[test]
    fn test_headervalue_from_uuid() {
        let code = Uuid::parse_str("936DA01F9ABD4d9d80C702AF85C822A8").unwrap();

        let a: HeaderValue = code.into();
        assert_eq!(a, HeaderValue::Uuid(code));
    }

    #[test]
    fn test_serialize_boolean() {
        //HeaderValue::Boolean(true) => 0,
        //HeaderValue::Boolean(false) => 1,
        assert_eq!(HeaderValue::Boolean(true).as_buffer(), []);
        assert_eq!(HeaderValue::Boolean(false).as_buffer(), []);
    }

    #[test]
    fn test_serialize_byte() {
        for t in 0..255 {
            let buf = HeaderValue::Byte(t).as_buffer();
            assert_eq!(buf, [t]);
            if let HeaderValue::Byte(b) = parse_byte(&buf).unwrap().1 {
                assert_eq!(b, t);
            } else {
                panic!("Byte was not correctly parsed");
            }
        }
    }
    #[test]
    fn test_serialize_short() {
        //HeaderValue::Short(_) => 3,
        for t in 0..255 {
            let buf = HeaderValue::Short(t).as_buffer();
            assert_eq!(buf, u16_to_u8(t));
            if let HeaderValue::Short(b) = parse_short(&buf).unwrap().1 {
                assert_eq!(b, t);
            } else {
                panic!("Short was not correctly parsed");
            }
        }
    }

    #[test]
    fn test_serialize_integer() {
        //HeaderValue::Integer(_) => 4,
        for t in 0..255 {
            let buf = HeaderValue::Integer(t).as_buffer();
            assert_eq!(buf, u32_to_u8(t));
            if let HeaderValue::Integer(b) = parse_integer(&buf).unwrap().1 {
                assert_eq!(b, t);
            } else {
                panic!("Integer was not correctly parsed");
            }
        }
    }

    #[test]
    fn test_serialize_long() {
        //HeaderValue::Long(_) => 5,
        for t in 0..255 {
            let buf = HeaderValue::Long(t).as_buffer();
            assert_eq!(buf, u64_to_u8(t));
            if let HeaderValue::Long(b) = parse_long(&buf).unwrap().1 {
                assert_eq!(b, t);
            } else {
                panic!("Long was not correctly parsed");
            }
        }
    }

    #[test]
    fn test_serialize_bytes() {
        //HeaderValue::Bytes(_) => 6,
        let buf = HeaderValue::Bytes([23u8, 120u8].to_vec()).as_buffer();
        assert_eq!(buf, [0u8, 2u8, 23u8, 120u8]);
        if let HeaderValue::Bytes(b) = parse_bytes(&buf).unwrap().1 {
            assert_eq!(b, [23u8, 120u8]);
        } else {
            panic!("Bytes was not correctly parsed");
        }
        // Not enough data provided (11 bytes, only 1 provided)
        assert!(parse_bytes(&[0u8, 11u8, 104u8]).is_err());
    }

    #[test]
    fn test_serialize_string() {
        //HeaderValue::String(_) => 6,
        let buf = HeaderValue::String("hello world".to_string()).as_buffer();
        assert_eq!(
            buf,
            [
                0u8, 11u8, 104u8, 101u8, 108u8, 108u8, 111u8, 32u8, 119u8, 111u8, 114u8, 108u8,
                100u8
            ]
        );
        if let HeaderValue::String(b) = parse_string(&buf).unwrap().1 {
            assert_eq!(b, "hello world".to_string());
        } else {
            panic!("String was not correctly parsed");
        };
        // Not enough data provided (11 chars, only 1 provided)
        assert!(parse_string(&[0u8, 11u8, 104u8]).is_err());
    }

    #[test]
    fn test_serialize_timestamp() {
        //HeaderValue::Timestamp(_) => 6,
        let dt = Utc.ymd(2014, 7, 8).and_hms(9, 10, 11);
        let buf = HeaderValue::Timestamp(dt).as_buffer();
        assert_eq!(buf, [0, 0, 0, 0, 83, 187, 181, 115]);
        if let HeaderValue::Timestamp(b) = parse_timestamp(&buf).unwrap().1 {
            assert_eq!(b, dt);
        } else {
            panic!("Timestamp was not correctly parsed");
        };
        // Not enough data provided (11 chars, only 1 provided)
        assert!(parse_timestamp(&[11u8, 104u8]).is_err());
    }
    #[test]
    fn test_serialize_uuid() {
        //HeaderValue::Uuid(_) => 6,
        let code = Uuid::parse_str("936DA01F9ABD4d9d80C702AF85C822A8").unwrap();

        let buf = HeaderValue::Uuid(code).as_buffer();
        assert_eq!(
            buf,
            [147, 109, 160, 31, 154, 189, 77, 157, 128, 199, 2, 175, 133, 200, 34, 168]
        );
        if let HeaderValue::Uuid(b) = parse_uuid(&buf).unwrap().1 {
            assert_eq!(b, code);
        } else {
            panic!("UUID was not correctly parsed");
        };
        // Not enough data provided (11 chars, only 1 provided)
        assert!(parse_uuid(&[11u8, 104u8]).is_err());
    }

}
