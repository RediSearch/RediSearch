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
use fork_gc::writer::Writer;
use std::io::Cursor;

#[test]
fn send_buffer_roundtrips_to_recv_buffer_data_frame() {
    let mut sink: Vec<u8> = Vec::new();
    {
        let mut pw = Writer::from_writer(&mut sink);
        pw.write_frame(Frame::Data(b"round trip")).unwrap();
    }
    let mut pr = Reader::from_reader(Cursor::new(sink));
    match pr.read_frame().unwrap() {
        Frame::Data(d) => assert_eq!(&*d, b"round trip"),
        other => panic!("expected Data, got {other:?}"),
    }
}

#[test]
fn send_terminator_roundtrips_to_recv_buffer_terminator_frame() {
    let mut sink: Vec<u8> = Vec::new();
    {
        let mut pw = Writer::from_writer(&mut sink);
        pw.write_frame(Frame::Terminator).unwrap();
    }
    let mut pr = Reader::from_reader(Cursor::new(sink));
    let frame = pr.read_frame().unwrap();
    assert!(matches!(frame, Frame::Terminator));
}

#[test]
fn send_buffer_empty_roundtrips_to_recv_buffer_empty_frame() {
    let mut sink: Vec<u8> = Vec::new();
    {
        let mut pw = Writer::from_writer(&mut sink);
        pw.write_frame(Frame::Empty).unwrap();
    }
    let mut pr = Reader::from_reader(Cursor::new(sink));
    let frame = pr.read_frame().unwrap();
    assert!(matches!(frame, Frame::Empty));
}
