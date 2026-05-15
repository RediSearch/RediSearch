/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use fork_gc::{Frame, writer::Writer};
use std::io::{self, Write};

#[test]
fn send_fixed_writes_all_bytes_to_sink() {
    let mut sink: Vec<u8> = Vec::new();
    {
        let mut pw = Writer::from_writer(&mut sink);
        pw.write_all(b"hello world").unwrap();
    }
    assert_eq!(sink, b"hello world");
}

#[test]
fn send_fixed_empty_buffer_is_a_no_op() {
    let mut sink: Vec<u8> = Vec::new();
    {
        let mut pw = Writer::from_writer(&mut sink);
        pw.write_all(&[]).unwrap();
    }
    assert!(sink.is_empty());
}

#[test]
fn send_fixed_propagates_write_failure() {
    // `&mut [u8]` as a `Write` returns `WriteZero` once it's full.
    let mut backing = [0u8; 3];
    let slice: &mut [u8] = &mut backing;
    let mut pw = Writer::from_writer(slice);
    let err = pw.write_all(b"hello").unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::WriteZero);
}

#[test]
fn send_buffer_writes_length_prefix_then_payload() {
    let mut sink: Vec<u8> = Vec::new();
    {
        let mut pw = Writer::from_writer(&mut sink);
        pw.write_frame(Frame::Data(b"hello")).unwrap();
    }

    let mut expected = Vec::new();
    expected.extend_from_slice(&5usize.to_ne_bytes());
    expected.extend_from_slice(b"hello");
    assert_eq!(sink, expected);
}

#[test]
fn send_buffer_empty_writes_zero_length_only() {
    let mut sink: Vec<u8> = Vec::new();
    {
        let mut pw = Writer::from_writer(&mut sink);
        pw.write_frame(Frame::Data(&[])).unwrap();
    }
    assert_eq!(sink, 0usize.to_ne_bytes());
}

#[test]
fn send_terminator_writes_size_max() {
    let mut sink: Vec<u8> = Vec::new();
    {
        let mut pw = Writer::from_writer(&mut sink);
        pw.write_frame(Frame::Terminator).unwrap();
    }
    assert_eq!(sink, usize::MAX.to_ne_bytes());
}
