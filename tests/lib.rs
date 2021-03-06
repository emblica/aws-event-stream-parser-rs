#[macro_use]
extern crate hex_literal;
extern crate bytes;
extern crate tokio_util;
extern crate aws_event_stream_parser;

use bytes::{BufMut, BytesMut};

use aws_event_stream_parser::{parse_message, EventStreamCodec, Header, HeaderBlock, Message};
use tokio_util::codec::{Decoder, Encoder};

#[test]
fn test_parse_multiple_messages() {
    let buf = hex!(
        "
        00000018000000083b698b18047465737403007821ab3883
        0000001d0000000d83e3f0e7047465737407000568656c6c
        6f1afe7100"
    );
    let (b, _) = parse_message(&buf).unwrap();
    let buf2 = hex!(
        "0000001d0000000d83e3f0e7047465
         737407000568656c6c6f1afe7100"
    );
    assert_eq!(b, buf2);
}

#[test]
fn test_encoder() {
    let buf = &mut BytesMut::new();
    buf.reserve(200);
    buf.put_slice(
        &hex!(
            "00000018000000083b698b1804746573
             7403007821ab38830000001d0000000d
             83e3f0e7047465737407000568656c6c
             6f1afe7100"
        )
        .to_vec(),
    );

    let mut codec = EventStreamCodec::default();
    assert_eq!(
        Message::build(
            HeaderBlock {
                headers: vec![Header::from_pair("test", 120u16)]
            },
            vec![]
        ),
        codec.decode(buf).unwrap().unwrap()
    );

    assert_eq!(
        Message::build(
            HeaderBlock {
                headers: vec![Header::from_pair("test", "hello")]
            },
            vec![]
        ),
        codec.decode(buf).unwrap().unwrap()
    );
}
#[test]
fn test_decoder() {
    let mut codec = EventStreamCodec::default();
    let mut buf = BytesMut::new();

    codec
        .encode(
            Message::build(
                HeaderBlock {
                    headers: vec![Header::from_pair("test", 120u16)],
                },
                vec![],
            ),
            &mut buf,
        )
        .unwrap();

    let mut fbuf = BytesMut::new();
    fbuf.put_slice(&hex!(
        "00000018000000083b698b18
         047465737403007821ab3883"
    )
    .to_vec());

    assert_eq!(
        fbuf,
        buf
    );

    codec
        .encode(
            Message::build(
                HeaderBlock {
                    headers: vec![Header::from_pair("test", "hello")],
                },
                vec![],
            ),
            &mut buf,
        )
        .unwrap();
    let mut fbuf = BytesMut::new();
    fbuf.put_slice(&hex!(
        "00000018000000083b698b1
         8047465737403007821ab38
         830000001d0000000d83e3f
         0e704746573740700056865
         6c6c6f1afe7100"
    )
    .to_vec());
    assert_eq!(fbuf, buf);
}
