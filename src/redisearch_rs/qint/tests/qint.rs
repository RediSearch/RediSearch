/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use qint::{qint_decode, qint_encode};
use std::io::{Cursor, Read, Seek};

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
fn test_qint_zeros() -> Result<(), std::io::Error> {
    let mut buf = [0u8; MAX_QINT_BUFFER_SIZE];
    let mut cursor = Cursor::new(buf.as_mut());

    let v = [0, 0, 0, 0]; // 1+1+1+1=4 bytes
    let bytes_written = qint_encode(&mut cursor, v)?;
    cursor.seek(std::io::SeekFrom::Start(0))?;
    let (out, bytes_read) = qint_decode::<4, _>(&mut cursor)?;

    // Check the number of bytes written 1+(1+1+1+1) -> 5 bytes
    assert_eq!(bytes_written, 5);
    assert_eq!(bytes_written, bytes_read);
    assert_eq!(v, out);

    Ok(())
}

#[test]
fn test_multiple_qints_in_a_buffer() -> Result<(), std::io::Error> {
    let v2 = [3333, 10]; // 2bytes, 1byte
    let v3 = [1_000_000_000, 70_000, 20]; // 4 bytes, 3 bytes, 1 byte
    let v4 = [2_500_000_000, 90_000, 0xFF, 1_500_000_000]; // 4 bytes, 3 bytes, 1 byte, 4 bytes

    let mut buf = [0u8; 1024];
    let mut cursor = Cursor::new(buf.as_mut());
    let mut bytes_written = vec![];
    bytes_written.push(qint_encode(&mut cursor, v2)?);
    bytes_written.push(qint_encode(&mut cursor, v3)?);
    bytes_written.push(qint_encode(&mut cursor, v4)?);

    // test we wrote the right number of bytes
    assert_eq!(bytes_written[0], 4); // 1+(2+1) = 4 bytes
    assert_eq!(bytes_written[1], 9); // 1+(4+3+1) = 9 bytes
    assert_eq!(bytes_written[2], 13); // 1+(4+3+1+4) = 13 bytes

    // use decode for identity test
    cursor.seek(std::io::SeekFrom::Start(0))?;
    let (out2, br2) = qint_decode::<2, _>(&mut cursor)?;
    let (out3, br3) = qint_decode::<3, _>(&mut cursor)?;
    let (out4, br4) = qint_decode::<4, _>(&mut cursor)?;
    let bytes_read = vec![br2, br3, br4];

    // check that we read the right number of bytes
    assert_eq!(bytes_written, bytes_read);
    // check that we read the right values
    assert_eq!(v2, out2);
    assert_eq!(v3, out3);
    assert_eq!(v4, out4);

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
    assert_eq!(kind, std::io::ErrorKind::WriteZero);
}

mod property_based {
    #![cfg(not(miri))]
    //! This module contains property-based tests for the qint encoding and decoding functions.
    //!
    //! The [PropEncoding] enum represents different configurations of integers and their expected sizes for encoding.
    //! Each variant corresponds to a specific number of integers (2, 3, or 4) and their respective sizes in bytes.
    //!
    //! The module defines strategies for generating random test data: [qint_varlen], [qint2], [qint3], [qint4], and [qint_encoding].
    //! These strategies produce arrays of `u32` integers along with their expected sizes in bytes, which are used to test the encoding and decoding logic.
    //!
    //! The [qint_varlen] strategy generates random integers, where each integer is 1 to 4 bytes long.
    //! Each byte is assigned a random value between 0 and 255. This way we get the same distribution of 1-4 bytes as in the encoding
    //! whereas a normal random u32 would strongly bias the distribution towards 4 bytes.
    //! If we would create a u32 from random bytes we would have a strong bias towards 4 bytes as the probability of getting a 3 byte
    //! integer value is already 2^8 times smaller than that of a 4 byte value.
    //!
    //! Using [qint_varlen], the [qint2], [qint3], and [qint4] strategies build [PropEncoding] variants.
    //! For example, `PropEncoding::QInt3(([100, 2000, 30000], [1, 2, 3]))` represents three integers with sizes of 1, 2, and 3 bytes, respectively.
    //! These variants serve as input for property-based tests.
    //!
    //! The strategy [qint_encoding_with_to_small_buffer_base] is used in the property tests [test_encoding_with_too_small_buffer] and
    //! [test_decoding_with_too_small_buffer] with prop_filter to ensure only buffers that are too small are used.
    //!
    //! ## How to handle failures of Property-based tests
    //!
    //! When a property-based test fails, it will print the input that caused the failure. The property-based test framework will try to minimize the
    //! input size to find a minimal failing case. In our case a [qint2] is considered smaller than a [qint3] and so on. This is decided by the ordering
    //! in the [PropEncoding] enum as an implementation detail of the property-based test framework. For integers like num_bytes=1..=4 the framework
    //! will try to minimize the number of bytes.
    //!
    //! It is advisable to use that input to write a unit test for the failure.
    //!
    //! In case of a failure you get an error message like this:
    //! ```text
    //! proptest: Saving this and future failures in .../RediSearch/src/redisearch_rs/qint/tests/qint.proptest-regressions
    //! proptest: If this test was run on a CI system, you may wish to add the following line to your copy of the file. (You may need to create it.)
    //! cc 14694d891a3112acab7bf19e0b77a965d75e90fb417cca8273871d5c2a8739a9
    //!
    //! thread 'property_based::test_encoding_with_varied_buffer' panicked at qint/tests/qint.rs:342:5:
    //! Test failed: assertion failed: `(left == right)`
    //! left: `false`,
    //! right: `true` at qint/tests/qint.rs:350.
    //! minimal failing input: prop_encoding = QInt2(
    //!   (
    //!       [
    //!           8106623,
    //!           8185929,
    //!       ],
    //!       [
    //!           3,
    //!           4,
    //!       ],
    //!   ),
    //! ), buffer_size = 7
    //!       successes: 190
    //!       local rejects: 0
    //!       global rejects: 0
    //! ```
    //!
    //! The property test framework provides information the cc 14694d891a3112acab7bf19e0b77a965d75e90fb417cca8273871d5c2a8739a9
    //! which is a hash of the input. This hash can be used to reproduce the test case and is stored in the file `qint.proptest-regressions`.
    //!
    //! The line `minimal failing input: prop_encoding = QInt2(...)` and the following lines shows the input that caused the failure and is helpful
    //! to rewrite the test case as a unit test. The input is a [PropEncoding] enum with the values that caused the failure. `buffer_size` is the size
    //! of the buffer that is used internally by the proptest to test both succeeding and failing cases.
    //!
    //! successes are the number of tests that passed before the failing test run. Local rejects are input filters implement in input strategies.
    //! Global rejects are the number serve a similar purpose but are encoded at test level with the `prop_assume` macro. Both local and global
    //! rejects are helpful to write specialized tests, e.g. tests where the `buffer_size` is always too small to fit the encoded integers as we
    //! do in the tests [test_encoding_with_too_small_buffer] and [test_decoding_with_too_small_buffer].

    use ::qint::{qint_decode, qint_encode};
    use proptest::prop_assert_eq;
    use std::io::{Cursor, Seek as _};

    use proptest::{
        prelude::{BoxedStrategy, any},
        prop_compose, prop_oneof,
        strategy::Strategy,
    };
    use rand::{Rng as _, SeedableRng as _};

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
                *item = rng.random_range(0..=255);
            }

            // we use a repair step instead of local or global rejects because we want to
            // change the random distribution and have no simple filter case here.
            // If we would create a u32 from random bytes we would have a strong bias towards 4 bytes
            // as the probability of getting a 3 byte already 2^8 times smaller than a 4 byte.
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

    prop_compose! {
        // Generate a random number of integers (2, 3, or 4) in a slice encapsulated in a PropEncoding enum
        fn qint_encoding_base()(prop_encoding in prop_oneof![qint2(), qint3(), qint4()]) -> PropEncoding {
            prop_encoding
        }
    }

    prop_compose! {
        fn qint_encoding_with_to_small_buffer_base()(enc in qint_encoding_base(), buffer_size in 1..=17usize) -> (PropEncoding, usize) {
            // Generate a random number of integers (2, 3, or 4) in a slice encapsulated in a PropEncoding enum
            (enc, buffer_size)
        }
    }

    pub fn qint_encoding() -> BoxedStrategy<PropEncoding> {
        // Generate a random number of integers (2, 3, or 4) in a slice encapsulated in a PropEncoding enum
        qint_encoding_base().boxed()
    }

    pub fn qint_encoding_with_buffer_size() -> BoxedStrategy<(PropEncoding, usize)> {
        // Generate a random number of integers (2, 3, or 4) in a slice encapsulated in a PropEncoding enum
        qint_encoding_with_to_small_buffer_base().boxed()
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

    macro_rules! match_qint_encoding_buf_too_small {
        ($($variant:ident),* => $cursor:expr, $prop_encoding:expr) => {
            match $prop_encoding {
                $(
                    PropEncoding::$variant((v, _)) => {
                        let res = qint_encode(&mut $cursor, v);
                        prop_assert_eq!(res.is_err(), true);
                        let kind = res.unwrap_err().kind();
                        prop_assert_eq!(kind, std::io::ErrorKind::WriteZero);
                    }
                ),*
            }
        };
    }

    proptest::proptest! {
        // tests for error conditions related to buffer size
        #[test]
        fn test_encoding_with_too_small_buffer((prop_encoding, buffer_size) in qint_encoding_with_buffer_size()
            .prop_filter("only use buffers that are too small", |(enc, buf_size_candidate)| enc.expected_written() > *buf_size_candidate)) {
            let mut buf = [0u8; MAX_QINT_BUFFER_SIZE];
            let buf = &mut buf[0..buffer_size];
            let mut cursor = Cursor::new(buf);

            match_qint_encoding_buf_too_small!(QInt2, QInt3, QInt4 => cursor, prop_encoding);
        }
    }

    macro_rules! match_qint_decoding_buf_too_small {
        ($($variant:ident, $number:literal),* => $cursor:expr, $prop_encoding:expr) => {
            match $prop_encoding {
                $(
                    PropEncoding::$variant(_) => {
                        let res = qint_decode::<$number, _>(&mut $cursor);
                        prop_assert_eq!(res.is_err(), true);
                        let kind = res.unwrap_err().kind();
                        prop_assert_eq!(kind, std::io::ErrorKind::UnexpectedEof);
                    }
                ),*
            }
        }
    }

    proptest::proptest! {
        // tests for error conditions related to buffer size
        #[test]
        fn test_decoding_with_too_small_buffer((prop_encoding, buffer_size) in qint_encoding_with_buffer_size()
            .prop_filter("only use buffers that are too small", |(enc, buf_size_candidate)| enc.expected_written() > *buf_size_candidate)) {
            let mut buf = [0u8; MAX_QINT_BUFFER_SIZE];
            let buf = &mut buf[0..buffer_size];

            // we mock the encoding by writing the leading byte into buffer and the rest remains zero.
            // so we can test what happens if the decoding buffer is smaller than the expected size
            let leading_byte = prop_encoding.leading_byte();
            buf[0] = leading_byte;
            for i in 1..buffer_size {
                buf[i] = (rand::random::<u8>()) as u8;
            }

            let mut cursor = Cursor::new(buf);
            match_qint_decoding_buf_too_small!(QInt2, 2, QInt3, 3, QInt4, 4 => cursor, prop_encoding);
        }
    }
}
