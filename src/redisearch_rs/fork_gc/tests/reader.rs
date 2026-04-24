use fork_gc::reader::{Reader, RecvFrame};
use fork_gc::writer::Writer;
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
fn recv_fixed_empty_buffer_is_a_no_op() {
    let mut src = Cursor::new(Vec::<u8>::new());
    let mut pr = Reader::from_reader(&mut src);
    pr.recv_fixed(&mut []).unwrap();
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
fn recv_buffer_reads_length_prefixed_payload() {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&5usize.to_ne_bytes());
    bytes.extend_from_slice(b"hello");
    let mut pr = Reader::from_reader(Cursor::new(bytes));
    match pr.recv_buffer().unwrap() {
        RecvFrame::Data(d) => assert_eq!(d, b"hello"),
        other => panic!("expected Data, got {other:?}"),
    }
}

#[test]
fn recv_buffer_detects_terminator() {
    let bytes = usize::MAX.to_ne_bytes().to_vec();
    let mut pr = Reader::from_reader(Cursor::new(bytes));
    assert!(matches!(pr.recv_buffer().unwrap(), RecvFrame::Terminator));
}

#[test]
fn recv_buffer_detects_empty() {
    let bytes = 0usize.to_ne_bytes().to_vec();
    let mut pr = Reader::from_reader(Cursor::new(bytes));
    assert!(matches!(pr.recv_buffer().unwrap(), RecvFrame::Empty));
}

#[test]
fn recv_buffer_roundtrips_through_send_buffer() {
    // Emit a `send_buffer` frame into a Vec, then parse it back with
    // `recv_buffer` — the two sides agree on the wire format.
    let mut sink: Vec<u8> = Vec::new();
    {
        let mut pw = Writer::from_writer(&mut sink);
        pw.send_buffer(b"round trip").unwrap();
    }
    let mut pr = Reader::from_reader(Cursor::new(sink));
    match pr.recv_buffer().unwrap() {
        RecvFrame::Data(d) => assert_eq!(d, b"round trip"),
        other => panic!("expected Data, got {other:?}"),
    }
}

#[test]
fn recv_buffer_eof_on_truncated_payload() {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&5usize.to_ne_bytes());
    bytes.extend_from_slice(b"hi"); // short
    let mut pr = Reader::from_reader(Cursor::new(bytes));
    let err = pr.recv_buffer().unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
}
