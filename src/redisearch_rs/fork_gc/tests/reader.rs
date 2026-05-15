/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use fork_gc::Frame;
use fork_gc::reader::Reader;
use std::io::{self, Cursor, Read};

#[test]
fn recv_fixed_reads_exact_bytes() {
    let mut src = Cursor::new(b"hello world".to_vec());
    let mut pr = Reader::from_reader(&mut src);
    let mut buf = [0u8; 11];
    pr.read_exact(&mut buf).unwrap();
    assert_eq!(&buf, b"hello world");
}

#[test]
fn recv_fixed_surfaces_unexpected_eof_on_short_stream() {
    let mut src = Cursor::new(b"hi".to_vec());
    let mut pr = Reader::from_reader(&mut src);
    let mut buf = [0u8; 5];
    let err = pr.read_exact(&mut buf).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
}

#[test]
fn recv_fixed_reads_less_bytes_than_are_available() {
    let mut src = Cursor::new(b"hello world".to_vec());
    let mut pr = Reader::from_reader(&mut src);
    let mut buf = [0u8; 5];
    pr.read_exact(&mut buf).unwrap();
    assert_eq!(&buf, b"hello");
}

#[test]
fn recv_buffer_reads_length_prefixed_payload() {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&5usize.to_ne_bytes());
    bytes.extend_from_slice(b"hello");
    let mut pr = Reader::from_reader(Cursor::new(bytes));
    match pr.read_frame().unwrap() {
        Frame::Data(d) => assert_eq!(&*d, b"hello"),
        other => panic!("expected Data, got {other:?}"),
    }
}

#[test]
fn recv_buffer_detects_terminator() {
    let bytes = usize::MAX.to_ne_bytes().to_vec();
    let mut pr = Reader::from_reader(Cursor::new(bytes));
    assert!(matches!(pr.read_frame().unwrap(), Frame::Terminator));
}

#[test]
fn recv_buffer_detects_empty() {
    let bytes = 0usize.to_ne_bytes().to_vec();
    let mut pr = Reader::from_reader(Cursor::new(bytes));
    assert!(matches!(pr.read_frame().unwrap(), Frame::Empty));
}

#[test]
fn recv_buffer_eof_on_truncated_payload() {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&5usize.to_ne_bytes());
    bytes.extend_from_slice(b"hi"); // short
    let mut pr = Reader::from_reader(Cursor::new(bytes));
    let err = pr.read_frame().unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
}
