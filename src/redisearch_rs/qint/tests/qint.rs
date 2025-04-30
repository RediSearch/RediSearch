use std::io::{Cursor, Read as _, Seek, Write};

use qint::{qint_encode, qint_decode, QInt2, QInt3, QInt4};

#[test]
fn test_qint2() -> Result<(), std::io::Error> {
    let mut buf = [0u8; 64];
    let mut cursor = Cursor::new(buf.as_mut());

    let v = [3333, 10]; // 2bytes, 1byte
    let bytes_written = qint_encode(&mut cursor, v)?;
    cursor.seek(std::io::SeekFrom::Start(0))?;
    let (out, bytes_read) = qint_decode::<QInt2, _>(&mut cursor)?;

    // Check the number of bytes written 1+(2+1) -> 4 bytes
    assert_eq!(bytes_written, 4);
    assert_eq!(bytes_written, bytes_read);
    assert_eq!(v, out);

    Ok(())
}

#[test]
fn test_qint3() -> Result<(), std::io::Error> {
    let mut buf = [0u8; 64];
    let mut cursor = Cursor::new(buf.as_mut());

    let v = [1_000_000_000, 70_000, 20]; // 4 bytes, 3 bytes, 1 byte
    let bytes_written = qint_encode(&mut cursor, v)?;
    cursor.seek(std::io::SeekFrom::Start(0))?;
    let (out, bytes_read) = qint_decode::<QInt3, _>(&mut cursor)?;

    assert_eq!(bytes_written, 9); // 1+(4+3+1) = 9 bytes
    assert_eq!(bytes_read, bytes_written);
    assert_eq!(v, out);
 
    Ok(())
}

#[test]
fn test_qint4() -> Result<(), std::io::Error> {
    let mut buf = [0u8; 64];
    let mut cursor = Cursor::new(buf.as_mut());

    let v = [2_500_000_000, 90_000, 0xFF, 1_500_000_000]; // 4 bytes, 3 bytes, 1 byte, 4 bytes
    let bytes_written = qint_encode(&mut cursor, v)?;
    cursor.seek(std::io::SeekFrom::Start(0))?;
    let (out, bytes_read) = qint_decode::<QInt4, _>(&mut cursor)?;

    assert_eq!(bytes_read, bytes_written);
    assert_eq!(bytes_written, 13); // 1 leading byte + 4 bytes + 3 bytes + 1 byte + 4 bytes = 13 bytes
    assert_eq!(v, out);
    Ok(())
}

#[test]
fn test_too_small_decode_buffer() {
    let mut buf = [0u8; 1];
    let mut cursor = Cursor::new(buf.as_mut());

    let res = qint_decode::<QInt2, _>(&mut cursor);
    assert_eq!(res.is_err(), true);
    assert_eq!(res.unwrap_err().kind(), std::io::ErrorKind::UnexpectedEof);
}

#[test]
fn test_encode_cursor_pos() {
    let mut buf = [42u8; 64];
    let mut cursor = Cursor::new(buf.as_mut());

    let v = [0xFF, 0xFFFF];
    let bytes_written = qint_encode(&mut cursor, v).unwrap();
    assert_eq!(bytes_written, 4); // 1 (1+2) = 4 bytes
    
    // check that the cursor is at the right position 
    let mut num = 0;
    let mut read_buf = [0u8; 1];
    loop {
        let bytes_read = cursor.read(&mut read_buf).unwrap();
        if bytes_read == 0 {
            break;
        }

        // all the following bytes should be 42
        assert_eq!(read_buf[0], 42);
        num += 1;
    }

    // we should have read 64 - bytes_written bytes
    assert_eq!(num, 64 - bytes_written); 
    
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

    let mut cursor = OutOfMemMock;

    let ca = 3333;
    let cb = 10;

    let res = qint_encode(&mut cursor, [ca, cb]);
    assert_eq!(res.is_err(), true);
    assert_eq!(res.unwrap_err().kind(), std::io::ErrorKind::OutOfMemory);
}

mod property_based {
    //#![cfg(not(miri))]

    //! This module contains property-based tests for the qint encoding and decoding functions.
    //! 
    //! It uses the qint_varlen strategy to generate an equal amount of integers which one up to four 
    //! bytes each. Hereby 0-255 is randomly chosen for each byte.
    //! Based on that the qint1, qint2, qint3 and qint4 strategies are created as building block for
    //! providing a enum PropEncoding that serves as input for the property-based tests.

    use std::io::{Cursor, Seek as _};
    use proptest::{prelude::*, prop_compose, prop_oneof};
    use::qint::{qint_encode, qint_decode};

    #[derive(Debug, Clone)]
    enum PropEncoding {
        QInt2([u32; 2]),
        QInt3([u32; 3]),
        QInt4([u32; 4]),
    }

    prop_compose! {
        fn qint_varlen()(num_bytes in 1..4u32) -> u32 {
            let mut bytes = [0u8; 4];
            for idx in 0..num_bytes {
                bytes[idx as usize] = rand::random::<u8>();
            }
            u32::from_ne_bytes(bytes)
        }
    }

    prop_compose! {
        fn qint2()(a in qint_varlen(), b in qint_varlen()) -> PropEncoding {
            PropEncoding::QInt2([a, b])
        }
    }

    prop_compose! {
        fn qint3()(a in qint_varlen(), b in qint_varlen(), c in qint_varlen()) -> PropEncoding {
            PropEncoding::QInt3([a, b, c])
        }
    }

    prop_compose! {
        fn qint4()(a in qint_varlen(), b in qint_varlen(), c in qint_varlen(), d in qint_varlen()) -> PropEncoding {
            PropEncoding::QInt4([a, b, c, d])
        }
    }
    
    fn qint_encoding() -> BoxedStrategy<PropEncoding> {
        // Generate a random number of integers (2, 3, or 4) in a slice encapsulated in a PropEncoding enum
        prop_oneof![
            qint2(),
            qint3(),
            qint4()
        ].boxed()
    }

    proptest::proptest! {
        #[test]
        fn test_encode_decode_identify(prop_encoding in qint_encoding()) {
            // prepare buffer and cursor
            let mut buf = [42u8; 64];
            let mut cursor = Cursor::new(buf.as_mut());

            // match on the PropEncoding enum to get the value slices and encode them
            let bytes_written = match prop_encoding {
                PropEncoding::QInt2(vals) => qint_encode(&mut cursor, vals).unwrap(),
                PropEncoding::QInt3(vals) => qint_encode(&mut cursor, vals).unwrap(),
                PropEncoding::QInt4(vals) => qint_encode(&mut cursor, vals).unwrap(),
            };

            // move cursor to begin and decode
            cursor.seek(std::io::SeekFrom::Start(0)).unwrap();
            match prop_encoding {
                PropEncoding::QInt2(input) => {
                    let (decoded_values, bytes_read) = qint_decode::<[u32;2], _>(&mut cursor).unwrap();
                    assert_eq!(bytes_written, bytes_read);
                    assert_eq!(input, decoded_values);
                }
                PropEncoding::QInt3(input) => {
                    let (decoded_values, bytes_read) = qint_decode::<[u32;3], _>(&mut cursor).unwrap();
                    assert_eq!(bytes_written, bytes_read);
                    assert_eq!(input, decoded_values);
                }
                PropEncoding::QInt4(input) => {
                    let (decoded_values, bytes_read) = qint_decode::<[u32;4], _>(&mut cursor).unwrap();
                    assert_eq!(bytes_written, bytes_read);
                    assert_eq!(input, decoded_values);
                }
            }
        }
    }
}