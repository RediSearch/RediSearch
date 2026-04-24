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
