use std::io::{Cursor, Read, Seek};

use qint::{qint_decode, qint_encode};

// A qint needs a maximum of 1+4*4=17 bytes we round up to 24 bytes for 8 byte alignment
const MAX_QINT_BUFFER_SIZE: usize = 24;

#[test]
fn test_qint2() -> Result<(), std::io::Error> {
    let mut buf = [0u8; MAX_QINT_BUFFER_SIZE];
    let mut cursor = Cursor::new(buf.as_mut());

    let v = [3333, 10]; // 2bytes, 1byte
    let bytes_written = qint_encode(&mut cursor, v)?;
    cursor.seek(std::io::SeekFrom::Start(0))?;
    let (out, bytes_read) = qint_decode::<2, _>(&mut cursor)?;

    // Check the number of bytes written 1+(2+1) -> 4 bytes
    assert_eq!(bytes_written, 4);
    assert_eq!(bytes_written, bytes_read);
    assert_eq!(v, out);

    Ok(())
}

#[test]
fn test_qint3() -> Result<(), std::io::Error> {
    let mut buf = [0u8; MAX_QINT_BUFFER_SIZE];
    let mut cursor = Cursor::new(buf.as_mut());

    let v = [1_000_000_000, 70_000, 20]; // 4 bytes, 3 bytes, 1 byte
    let bytes_written = qint_encode(&mut cursor, v)?;
    cursor.seek(std::io::SeekFrom::Start(0))?;
    let (out, bytes_read) = qint_decode::<3, _>(&mut cursor)?;

    assert_eq!(bytes_written, 9); // 1+(4+3+1) = 9 bytes
    assert_eq!(bytes_read, bytes_written);
    assert_eq!(v, out);

    Ok(())
}

#[test]
fn test_qint4() -> Result<(), std::io::Error> {
    let mut buf = [0u8; MAX_QINT_BUFFER_SIZE];
    let mut cursor = Cursor::new(buf.as_mut());

    let v = [2_500_000_000, 90_000, 0xFF, 1_500_000_000]; // 4 bytes, 3 bytes, 1 byte, 4 bytes
    let bytes_written = qint_encode(&mut cursor, v)?;
    cursor.seek(std::io::SeekFrom::Start(0))?;
    let (out, bytes_read) = qint_decode::<4, _>(&mut cursor)?;

    assert_eq!(bytes_read, bytes_written);
    assert_eq!(bytes_written, 13); // 1 leading byte + 4 bytes + 3 bytes + 1 byte + 4 bytes = 13 bytes
    assert_eq!(v, out);
    Ok(())
}

#[test]
fn test_too_small_decode_buffer() {
    let mut buf = [0u8; 1];
    let mut cursor = Cursor::new(buf.as_mut());

    let res = qint_decode::<2, _>(&mut cursor);
    assert_eq!(res.is_err(), true);
    assert_eq!(res.unwrap_err().kind(), std::io::ErrorKind::UnexpectedEof);
}

#[test]
fn test_encode_cursor_pos() {
    let mut buf = [42u8; MAX_QINT_BUFFER_SIZE];
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

    // we should have read MAX_QINT_BUFFER_SIZE - bytes_written bytes
    assert_eq!(num, MAX_QINT_BUFFER_SIZE - bytes_written);
}

#[test]
fn test_out_of_memory_error() {
    let mut buf = [0u8; MAX_QINT_BUFFER_SIZE];
    let buf = &mut buf[0..1];
    let mut cursor = Cursor::new(buf);
    let res = qint_encode(&mut cursor, [3333, 10]);

    assert_eq!(res.is_err(), true);
    let kind = res.unwrap_err().kind();
    let is_mem_err =
        kind == std::io::ErrorKind::OutOfMemory || kind == std::io::ErrorKind::WriteZero;
    assert_eq!(is_mem_err, true);
}

#[test]
fn proptest_false_positive_bc_of_expected_written_mismatch() {
    // this found an edge case in input generation where randomly a 0 was generated for the last byte and so num_bytes required reduction by 1.
    // the QInt enumeration changed therefore this is not part of qint-proptest-regressions anymore.
    //cc 89c60968333fa0650f6e808183adad48d96770946813b10d43c8343ce3516667 # shrinks to prop_encoding = QInt4(([127, 8106623, 2491134591, 10583097], 13)), buffer_size = 12
    let mut buf = [0u8; MAX_QINT_BUFFER_SIZE];
    let v: [u32; 4] = [127, 8106623, 2491134591, 10583097];
    let buffer_size = 12;
    let buf = &mut buf[0..buffer_size];

    let mut cursor = Cursor::new(buf);
    let res = qint_encode(&mut cursor, v);
    if res.is_err() {
        unreachable!(
            "Generation wrong: QInt4(([127, 8106623, 2491134591, 10583097], 13)), buffer_size = 12<{}: expected_written",
            res.unwrap()
        );
    }
}

mod property_based {
    #![cfg(not(miri))]

    //! This module contains property-based tests for the qint encoding and decoding functions.
    //!
    //! The strategies here return an array of u32 integers and the expected number of bytes that should
    //! be written to the buffer.
    //!
    //! It uses the qint_varlen strategy to generate an equal amount of integers which one up to four
    //! bytes each. Hereby 0-255 is randomly chosen for each byte.
    //! Based on that the qint1, qint2, qint3 and qint4 strategies are created as building block for
    //! providing a enum PropEncoding that serves as input for the property-based tests.

    use ::qint::{qint_decode, qint_encode};
    use proptest::prop_assert_eq;
    use std::io::{Cursor, Seek as _};

    use proptest::{
        prelude::{BoxedStrategy, any},
        prop_compose, prop_oneof,
        strategy::Strategy,
    };
    use rand::{RngCore as _, SeedableRng as _};

    #[derive(Debug, Clone)]
    pub enum PropEncoding {
        QInt2(([u32; 2], [usize; 2])),
        QInt3(([u32; 3], [usize; 3])),
        QInt4(([u32; 4], [usize; 4])),
    }

    impl PropEncoding {
        pub fn expected_written(&self) -> usize {
            match self {
                PropEncoding::QInt2((_, expected_size)) => expected_size.iter().sum::<usize>() + 1,
                PropEncoding::QInt3((_, expected_size)) => expected_size.iter().sum::<usize>() + 1,
                PropEncoding::QInt4((_, expected_size)) => expected_size.iter().sum::<usize>() + 1,
            }
        }

        pub fn leading_byte(&self) -> u8 {
            let mut leading_byte = 0b00000000u8;
            match &self {
                PropEncoding::QInt2((_, expected_size)) => {
                    leading_byte |= (expected_size[0] - 1) as u8;
                    leading_byte |= ((expected_size[1] - 1) << 2) as u8;
                }
                PropEncoding::QInt3((_, expected_size)) => {
                    leading_byte |= (expected_size[0] - 1) as u8;
                    leading_byte |= ((expected_size[1] - 1) << 2) as u8;
                    leading_byte |= ((expected_size[2] - 1) << 4) as u8;
                }
                PropEncoding::QInt4((_, expected_size)) => {
                    leading_byte |= (expected_size[0] - 1) as u8;
                    leading_byte |= ((expected_size[1] - 1) << 2) as u8;
                    leading_byte |= ((expected_size[2] - 1) << 4) as u8;
                    leading_byte |= ((expected_size[3] - 1) << 6) as u8;
                }
            }
            leading_byte
        }
    }

    prop_compose! {
        // Generate a random number of bytes (1, 2, 3 or 4) f
        fn qint_varlen()(num_bytes in 1..=4usize, seed in any::<u64>()) -> (u32, usize) {
            let mut bytes = [0u8; 4];
            let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
            let mut forward_size = num_bytes;
            for item in bytes.iter_mut().take(num_bytes) {
                *item = (rng.next_u32() & 0x000000FF) as u8;
            }
            for idx in (1..num_bytes).rev() {
                if bytes[idx] == 0 {
                    forward_size -= 1;
                } else {
                    break;
                }
            }
            (u32::from_ne_bytes(bytes), forward_size)
        }
    }

    prop_compose! {
        fn qint2()(a in qint_varlen(), b in qint_varlen()) -> PropEncoding {
            PropEncoding::QInt2(([a.0, b.0], [a.1, b.1]))
        }
    }

    prop_compose! {
        fn qint3()(a in qint_varlen(), b in qint_varlen(), c in qint_varlen()) -> PropEncoding {
            PropEncoding::QInt3(([a.0, b.0, c.0], [a.1, b.1, c.1]))
        }
    }

    prop_compose! {
        fn qint4()(a in qint_varlen(), b in qint_varlen(), c in qint_varlen(), d in qint_varlen()) -> PropEncoding {
            PropEncoding::QInt4(([a.0, b.0, c.0, d.0], [a.1, b.1, c.1, d.1]))
        }
    }

    pub fn qint_encoding() -> BoxedStrategy<PropEncoding> {
        // Generate a random number of integers (2, 3, or 4) in a slice encapsulated in a PropEncoding enum
        prop_oneof![qint2(), qint3(), qint4()].boxed()
    }

    use crate::MAX_QINT_BUFFER_SIZE;

    proptest::proptest! {
        // tests for working conditions
        #[test]
        fn test_encode_decode_identify(prop_encoding in qint_encoding()) {
            // prepare buffer and cursor
            let mut buf = [0u8; MAX_QINT_BUFFER_SIZE];
            let mut cursor = Cursor::new(buf.as_mut());

            // match on the PropEncoding enum to get the value slices and encode them
            let bytes_written = match prop_encoding {
                PropEncoding::QInt2((vals, _)) => qint_encode(&mut cursor, vals).unwrap(),
                PropEncoding::QInt3((vals, _)) => qint_encode(&mut cursor, vals).unwrap(),
                PropEncoding::QInt4((vals, _)) => qint_encode(&mut cursor, vals).unwrap(),
            };

            // move cursor to begin and decode
            cursor.seek(std::io::SeekFrom::Start(0)).unwrap();
            match prop_encoding {
                PropEncoding::QInt2((input, _)) => {
                    let (decoded_values, bytes_read) = qint_decode::<2, _>(&mut cursor).unwrap();
                    prop_assert_eq!(bytes_written, bytes_read);
                    prop_assert_eq!(input, decoded_values);
                }
                PropEncoding::QInt3((input, _)) => {
                    let (decoded_values, bytes_read) = qint_decode::<3, _>(&mut cursor).unwrap();
                    prop_assert_eq!(bytes_written, bytes_read);
                    prop_assert_eq!(input, decoded_values);
                }
                PropEncoding::QInt4((input, _)) => {
                    let (decoded_values, bytes_read) = qint_decode::<4, _>(&mut cursor).unwrap();
                    prop_assert_eq!(bytes_written, bytes_read);
                    prop_assert_eq!(input, decoded_values);
                }
            }
        }
    }

    macro_rules! match_qint_encoding {
        ($($variant:ident),* => $cursor:expr, $prop_encoding:expr, $buffer_size:expr) => {
            let expected_size = $prop_encoding.expected_written();
            match $prop_encoding {
                $(
                    PropEncoding::$variant((v, _)) => {
                        let res = qint_encode(&mut $cursor, v);
                        if expected_size > $buffer_size {
                            prop_assert_eq!(res.is_err(), true);
                            let kind = res.unwrap_err().kind();
                            let is_mem_err = kind == std::io::ErrorKind::OutOfMemory || kind == std::io::ErrorKind::WriteZero;
                            prop_assert_eq!(is_mem_err, true);
                        } else {
                            prop_assert_eq!(res.is_ok(), true);
                        }
                    }
                ),*
            }
        };
    }

    proptest::proptest! {
        // tests for error conditions related to buffer size
        #[test]
        fn test_encoding_with_varied_buffer(prop_encoding in qint_encoding(), buffer_size in 1..=17usize) {
            let mut buf = [0u8; MAX_QINT_BUFFER_SIZE];
            let buf = &mut buf[0..buffer_size];
            let mut cursor = Cursor::new(buf);

            match_qint_encoding!(QInt2, QInt3, QInt4 => cursor, prop_encoding, buffer_size);
        }
    }

    proptest::proptest! {
        // tests for error conditions related to buffer size
        #[test]
        fn test_decoding_with_varied_buffer(prop_encoding in qint_encoding(), buffer_size in 1..=17usize) {
            let mut buf = [0u8; MAX_QINT_BUFFER_SIZE];
            let buf = &mut buf[0..buffer_size];
            let expected_written = prop_encoding.expected_written();
            let fails_with_buffer_size = expected_written > buffer_size;

            // we mock the encoding by writing the leading byte into buffer and the rest remains zero.
            // so we can test what happens if the decoding buffer is smaller than the expected size
            let leading_byte = prop_encoding.leading_byte();
            buf[0] = leading_byte;
            for i in 1..buffer_size {
                buf[i] = (rand::random::<u8>()) as u8;
            }

            let mut cursor = Cursor::new(buf);
            match prop_encoding {
                PropEncoding::QInt2((_, _)) => {
                    let res = qint_decode::<2, _>(&mut cursor);
                    if fails_with_buffer_size {
                        prop_assert_eq!(res.is_err(), true);
                        let kind = res.unwrap_err().kind();
                        let is_mem_err = kind == std::io::ErrorKind::UnexpectedEof;
                        prop_assert_eq!(is_mem_err, true);
                    } else {
                        prop_assert_eq!(res.is_ok(), true);
                    }
                }
                PropEncoding::QInt3((_, _)) => {
                    let res = qint_decode::<3, _>(&mut cursor);
                    if fails_with_buffer_size {
                        prop_assert_eq!(res.is_err(), true);
                        let kind = res.unwrap_err().kind();
                        let is_mem_err = kind == std::io::ErrorKind::UnexpectedEof;
                        prop_assert_eq!(is_mem_err, true);
                    } else {
                        prop_assert_eq!(res.is_ok(), true);
                    }
                }
                PropEncoding::QInt4((_, _)) => {
                    let res = qint_decode::<4, _>(&mut cursor);
                    if fails_with_buffer_size {
                        prop_assert_eq!(res.is_err(), true);
                        let kind = res.unwrap_err().kind();
                        let is_mem_err = kind == std::io::ErrorKind::UnexpectedEof;
                        prop_assert_eq!(is_mem_err, true);
                    } else {
                        prop_assert_eq!(res.is_ok(), true);
                    }
                }
            };

        }
    }
}
