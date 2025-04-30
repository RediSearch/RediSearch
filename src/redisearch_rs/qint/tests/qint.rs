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
            let mut buf = [0u8; 64];
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