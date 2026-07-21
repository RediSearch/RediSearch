/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use fork_gc::Frame;
use std::io::{self, Cursor};

// Link both Rust-provided and C-provided symbols
extern crate redisearch_rs;
// Provide Redis allocator shims so the C dict functions can allocate memory.
redis_mock::mock_or_stub_missing_redis_c_symbols!();

#[test]
fn read_returns_data_frame_for_length_prefixed_payload() {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&5usize.to_ne_bytes());
    bytes.extend_from_slice(b"hello");
    let mut src = Cursor::new(bytes);
    assert!(matches!(Frame::decode(&mut src).unwrap(), Frame::Data(d) if &*d == b"hello"));
}

#[test]
fn read_returns_terminator_for_size_max_prefix() {
    let bytes = usize::MAX.to_ne_bytes().to_vec();
    let mut src = Cursor::new(bytes);
    assert!(matches!(
        Frame::decode(&mut src).unwrap(),
        Frame::Terminator
    ));
}

#[test]
fn read_returns_empty_for_zero_prefix() {
    let bytes = 0usize.to_ne_bytes().to_vec();
    let mut src = Cursor::new(bytes);
    assert!(matches!(Frame::decode(&mut src).unwrap(), Frame::Empty));
}

#[test]
fn read_errors_on_truncated_payload() {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&5usize.to_ne_bytes());
    bytes.extend_from_slice(b"hi"); // short
    let mut src = Cursor::new(bytes);
    let err = Frame::decode(&mut src).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
}

#[test]
fn write_data_frame_emits_length_then_payload() {
    let mut sink: Vec<u8> = Vec::new();
    Frame::data(b"hello").encode(&mut sink).unwrap();
    let mut expected = Vec::new();
    expected.extend_from_slice(&5usize.to_ne_bytes());
    expected.extend_from_slice(b"hello");
    assert_eq!(sink, expected);
}

#[test]
fn write_empty_frame_emits_zero_prefix_only() {
    let mut sink: Vec<u8> = Vec::new();
    Frame::data(b"").encode(&mut sink).unwrap();
    assert_eq!(sink, 0usize.to_ne_bytes());
}

#[test]
fn write_terminator_emits_size_max_prefix() {
    let mut sink: Vec<u8> = Vec::new();
    Frame::Terminator.encode(&mut sink).unwrap();
    assert_eq!(sink, usize::MAX.to_ne_bytes());
}

#[test]
fn data_frame_roundtrips() {
    let mut sink: Vec<u8> = Vec::new();
    Frame::data(b"round trip").encode(&mut sink).unwrap();
    match Frame::decode(&mut Cursor::new(sink)).unwrap() {
        Frame::Data(d) => assert_eq!(&*d, b"round trip"),
        other => panic!("expected Data, got {other:?}"),
    }
}

#[test]
fn terminator_frame_roundtrips() {
    let mut sink: Vec<u8> = Vec::new();
    Frame::Terminator.encode(&mut sink).unwrap();
    let frame = Frame::decode(&mut Cursor::new(sink)).unwrap();
    assert!(matches!(frame, Frame::Terminator));
}

#[test]
fn empty_frame_roundtrips() {
    let mut sink: Vec<u8> = Vec::new();
    Frame::Empty.encode(&mut sink).unwrap();
    let frame = Frame::decode(&mut Cursor::new(sink)).unwrap();
    assert!(matches!(frame, Frame::Empty));
}
