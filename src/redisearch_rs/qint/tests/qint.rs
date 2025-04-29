use std::io::{Cursor, Seek, Write};

use qint::{
    qint_decode2, qint_decode3, qint_decode4, qint_encode2, qint_encode3, qint_encode4
};

#[test]
fn test_qint2() -> Result<(), std::io::Error> {
    let mut buf = [0u8; 64];
    let mut write_cursor = Cursor::new(buf.as_mut());

    // 2 bytes
    let ca = 3333;
    // 1 byte
    let cb = 10;
    let bytes_written = qint_encode2(&mut write_cursor, ca, cb)?;
    let read_buf = write_cursor.into_inner();
    let mut read_cursor = Cursor::new(read_buf.as_ref());
    let (a, b, bytes_read) = qint_decode2(&mut read_cursor)?;

    // Check the number of bytes written (a=2bytes, b=1 byte, 1 leading byte) -> 4 bytes
    assert_eq!(bytes_written, 4);
    assert_eq!(bytes_read, bytes_written);
    assert_eq!(a, ca);
    assert_eq!(b, cb);

    Ok(())
}

#[test]
fn test_qint3() -> Result<(), std::io::Error> {
    let mut buf = [0u8; 64];
    let mut write_cursor = Cursor::new(buf.as_mut());

    let ca = 1_000_000_000; // 4 bytes
    let cb = 70_000; // 3 bytes
    let cc = 20; // 1 byte
    let bytes_written = qint_encode3(&mut write_cursor, ca, cb, cc)?;
    let read_buf = write_cursor.into_inner();
    let mut read_cursor = Cursor::new(read_buf.as_ref());
    let (a, b, c, bytes_read) = qint_decode3(&mut read_cursor)?;

    assert_eq!(bytes_written, 9); // 1 leading byte + 4 bytes + 3 bytes + 1 byte = 9 bytes
    assert_eq!(bytes_read, bytes_written);
    assert_eq!(a, ca);
    assert_eq!(b, cb);
    assert_eq!(c, cc);

    Ok(())
}

#[test]
fn test_qint4() -> Result<(), std::io::Error> {
    let mut buf = [0u8; 64];
    let mut write_cursor = Cursor::new(buf.as_mut());

    let ca = 2_500_000_000; // 4 bytes
    let cb = 90_000; // 3 bytes
    let cc = 0xFF; // 1 byte
    let cd = 1_500_000_000; // 4 byte
    let bytes_written = qint_encode4(&mut write_cursor, ca, cb, cc, cd)?;
    let read_buf = write_cursor.into_inner();
    let mut read_cursor = Cursor::new(read_buf.as_ref());
    let (a, b, c, d, bytes_read) = qint_decode4(&mut read_cursor)?;

    assert_eq!(bytes_read, bytes_written);
    assert_eq!(bytes_written, 13); // 1 leading byte + 4 bytes + 3 bytes + 1 byte + 4 bytes = 13 bytes
    assert_eq!(a, ca);
    assert_eq!(b, cb);
    assert_eq!(c, cc);
    assert_eq!(d, cd);

    Ok(())
}

#[test]
fn test_too_small_decode_buffer() {
    let mut buf = [0u8; 1];
    let mut read_cursor = Cursor::new(buf.as_mut());

    let res = qint_decode2(&mut read_cursor);
    assert_eq!(res.is_err(), true);
    assert_eq!(res.unwrap_err().kind(), std::io::ErrorKind::UnexpectedEof);

    let res = qint_decode3(&mut read_cursor);
    assert_eq!(res.is_err(), true);
    assert_eq!(res.unwrap_err().kind(), std::io::ErrorKind::UnexpectedEof);

    let res = qint_decode4(&mut read_cursor);
    assert_eq!(res.is_err(), true);
    assert_eq!(res.unwrap_err().kind(), std::io::ErrorKind::UnexpectedEof);
}

struct OutOfMemMock;

impl Write for OutOfMemMock {
    fn write(&mut self, _buf: &[u8]) -> std::io::Result<usize> {
        Err(std::io::Error::new(std::io::ErrorKind::OutOfMemory, "Out of memory"))
    }
    
    fn flush(&mut self) -> std::io::Result<()> {
        
        Ok(())
    }
}

impl Seek for OutOfMemMock {
    fn seek(&mut self, _pos: std::io::SeekFrom) -> std::io::Result<u64> {
        Ok(0)
    }
}

#[test]
fn test_out_of_memory_error() {

    let mut write_cursor = OutOfMemMock;

    let ca = 3333;
    let cb = 10;
    let cc = 0xFF;
    let cd = 1_500_000_000;

    let res = qint_encode2(&mut write_cursor, ca, cb);
    assert_eq!(res.is_err(), true);
    assert_eq!(res.unwrap_err().kind(), std::io::ErrorKind::OutOfMemory);

    let res = qint_encode3(&mut write_cursor, ca, cb, cc);
    assert_eq!(res.is_err(), true);
    assert_eq!(res.unwrap_err().kind(), std::io::ErrorKind::OutOfMemory);

    let res = qint_encode4(&mut write_cursor, ca, cb, cc, cd);
    assert_eq!(res.is_err(), true);
    assert_eq!(res.unwrap_err().kind(), std::io::ErrorKind::OutOfMemory);
}