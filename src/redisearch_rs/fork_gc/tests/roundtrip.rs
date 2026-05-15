/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use fork_gc::reader::{Reader, RecvFrame};
use fork_gc::writer::Writer;
use std::io::Cursor;

#[test]
fn send_buffer_roundtrips_to_recv_buffer_data_frame() {
    let mut sink: Vec<u8> = Vec::new();
    {
        let mut pw = Writer::from_writer(&mut sink);
        pw.send_buffer(b"round trip").unwrap();
    }
    let mut pr = Reader::from_reader(Cursor::new(sink));
    match pr.recv_buffer().unwrap() {
        RecvFrame::Data(d) => assert_eq!(&*d, b"round trip"),
        other => panic!("expected Data, got {other:?}"),
    }
}

#[test]
fn send_terminator_roundtrips_to_recv_buffer_terminator_frame() {
    let mut sink: Vec<u8> = Vec::new();
    {
        let mut pw = Writer::from_writer(&mut sink);
        pw.send_terminator().unwrap();
    }
    let mut pr = Reader::from_reader(Cursor::new(sink));
    let frame = pr.recv_buffer().unwrap();
    assert!(matches!(frame, RecvFrame::Terminator));
}

#[test]
fn send_buffer_empty_roundtrips_to_recv_buffer_empty_frame() {
    let mut sink: Vec<u8> = Vec::new();
    {
        let mut pw = Writer::from_writer(&mut sink);
        pw.send_buffer(&[]).unwrap();
    }
    let mut pr = Reader::from_reader(Cursor::new(sink));
    let frame = pr.recv_buffer().unwrap();
    assert!(matches!(frame, RecvFrame::Empty));
}
