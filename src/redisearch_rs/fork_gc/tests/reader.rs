/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use fork_gc::reader::Reader;
use std::io::{self, Cursor};

#[test]
fn recv_fixed_reads_exact_bytes() {
    let mut src = Cursor::new(b"hello world".to_vec());
    let mut pr = Reader::from_reader(&mut src);
    let mut buf = [0u8; 11];
    pr.recv_fixed(&mut buf).unwrap();
    assert_eq!(&buf, b"hello world");
}

#[test]
fn recv_fixed_surfaces_unexpected_eof_on_short_stream() {
    let mut src = Cursor::new(b"hi".to_vec());
    let mut pr = Reader::from_reader(&mut src);
    let mut buf = [0u8; 5];
    let err = pr.recv_fixed(&mut buf).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
}

#[test]
fn recv_fixed_reads_less_bytes_than_are_available() {
    let mut src = Cursor::new(b"hello world".to_vec());
    let mut pr = Reader::from_reader(&mut src);
    let mut buf = [0u8; 5];
    pr.recv_fixed(&mut buf).unwrap();
    assert_eq!(&buf, b"hello");
}
