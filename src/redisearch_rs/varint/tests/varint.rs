/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::io::Cursor;

use varint::*;

#[test]
fn test_u32() {
    let values: [u32; 12] = [123456789, 987654321, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
    let expected_lens = [4, 5, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1];

    for (i, value) in values.into_iter().enumerate() {
        let mut buf = Vec::new();
        value.write_as_varint(&mut buf).unwrap();
        assert_eq!(buf.len(), expected_lens[i]);
        let decoded: u32 = varint::read(&mut Cursor::new(buf)).unwrap();
        assert_eq!(decoded, value);
    }
}

#[test]
fn test_u64() {
    let values: [u64; 12] = [123456789, 987654321, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
    let expected_lens = [4, 5, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1];

    for (i, value) in values.into_iter().enumerate() {
        let mut buf = Vec::new();
        value.write_as_varint(&mut buf).unwrap();
        assert_eq!(buf.len(), expected_lens[i]);
        let decoded: u64 = varint::read(&mut Cursor::new(buf)).unwrap();
        assert_eq!(decoded, value);
    }
}

#[test]
fn test_writer_error() {
    // The buffer is too small to accommodate the encoded value.
    let mut buf = [0u8; 1];
    let error = u32::write_as_varint(128, buf.as_mut_slice()).unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::WriteZero);
}

#[test]
fn test_empty_reader() {
    let error = u32::read_as_varint(&mut Cursor::new([])).unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::UnexpectedEof);
}

#[test]
fn test_truncated_encoding() {
    let mut buf = [0u8; 2];
    let n_written_bytes = u32::write_as_varint(128, buf.as_mut_slice()).unwrap();
    assert_eq!(n_written_bytes, 2);

    let mut truncated = Cursor::new(&buf[..1]);
    let error = u32::read_as_varint(&mut truncated).unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::UnexpectedEof);
}

#[test]
/// Try to decode a number that's larger than the maximum size of the expected type.
///
/// The decoding process won't panic, but it'll output a non-sensical number.
fn test_size_confusion() {
    let mut buf = [0u8; 24];
    let input = u128::MAX;
    let n_written_bytes = input.write_as_varint(buf.as_mut_slice()).unwrap();
    assert_eq!(n_written_bytes, 19);

    let output = u32::read_as_varint(&mut Cursor::new(buf)).unwrap();
    assert_eq!(output, u32::MAX);
    assert_ne!(output as u128, input);
}

#[test]
fn test_u32_encoded_bytes() {
    // Test specific values against their expected encoded byte sequences.
    let test_cases = [
        (0u32, vec![0x00]),
        (1, vec![0x01]),
        (127, vec![0x7F]),
        (128, vec![0x80, 0x00]),
        (129, vec![0x80, 0x01]),
        (255, vec![0x80, 0x7F]),
        (256, vec![0x81, 0x00]),
        (16383, vec![0xFE, 0x7F]),
        (16384, vec![0xFF, 0x00]),
        (16511, vec![0xFF, 0x7F]),
        // 3-byte encoding boundary.
        (16512, vec![0x80, 0x80, 0x00]),
        (2097151, vec![0xFE, 0xFE, 0x7F]),
        (2097152, vec![0xFE, 0xFF, 0x00]),
        // 4-byte encoding boundary.
        (268435455, vec![0xFE, 0xFE, 0xFE, 0x7F]),
        (268435456, vec![0xFE, 0xFE, 0xFF, 0x00]),
        // Maximum u32 value (5-byte encoding).
        (u32::MAX, vec![0x8E, 0xFE, 0xFE, 0xFE, 0x7F]),
    ];

    for (value, expected_bytes) in test_cases {
        let mut buf = Vec::new();
        value.write_as_varint(&mut buf).unwrap();
        assert_eq!(
            buf, expected_bytes,
            "Encoded bytes for value {value} don't match expected: got {buf:?}, expected {expected_bytes:?}"
        );

        // Verify round-trip decoding still works.
        assert_eq!(u32::read_as_varint(&mut Cursor::new(buf)).unwrap(), value);
    }
}

#[test]
fn test_u64_encoded_bytes() {
    // Test specific values against their expected encoded byte sequences.
    let test_cases = [
        (0u64, vec![0x00]),
        (1, vec![0x01]),
        (127, vec![0x7F]),
        (128, vec![0x80, 0x00]),
        (129, vec![0x80, 0x01]),
        (255, vec![0x80, 0x7F]),
        (256, vec![0x81, 0x00]),
        (16383, vec![0xFE, 0x7F]),
        (16384, vec![0xFF, 0x00]),
        (16511, vec![0xFF, 0x7F]),
        // 3-byte encoding boundary.
        (16512, vec![0x80, 0x80, 0x00]),
        (2097151, vec![0xFE, 0xFE, 0x7F]),
        (2097152, vec![0xFE, 0xFF, 0x00]),
        // 4-byte encoding boundary.
        (268435455, vec![0xFE, 0xFE, 0xFE, 0x7F]),
        (268435456, vec![0xFE, 0xFE, 0xFF, 0x00]),
        // Maximum u32 value (5-byte encoding).
        (u32::MAX as _, vec![0x8E, 0xFE, 0xFE, 0xFE, 0x7F]),
        // Values just beyond u32::MAX.
        (u32::MAX as u64 + 1, vec![0x8E, 0xFE, 0xFE, 0xFF, 0x00]),
        (u32::MAX as u64 + 100, vec![0x8E, 0xFE, 0xFE, 0xFF, 0x63]),
        // Large u64 value.
        (
            0x1FFFFFFFFFFFFF,
            vec![0x8E, 0xFE, 0xFE, 0xFE, 0xFE, 0xFE, 0xFE, 0x7F],
        ),
        // Maximum u64 value.
        (
            u64::MAX,
            vec![0x80, 0xFE, 0xFE, 0xFE, 0xFE, 0xFE, 0xFE, 0xFE, 0xFE, 0x7F],
        ),
    ];

    for (value, expected_bytes) in test_cases {
        let mut buf = Vec::new();
        value.write_as_varint(&mut buf).unwrap();
        assert_eq!(
            buf, expected_bytes,
            "Encoded bytes for field mask {value} don't match expected: got {buf:?}, expected {expected_bytes:?}"
        );

        // Verify round-trip decoding still works.
        assert_eq!(u64::read_as_varint(&mut Cursor::new(buf)).unwrap(), value);
    }
}

#[test]
fn test_u128_encoded_bytes() {
    // Test specific values against their expected encoded byte sequences.
    let test_cases = [
        (0u128, vec![0x00]),
        (1, vec![0x01]),
        (127, vec![0x7F]),
        (128, vec![0x80, 0x00]),
        (129, vec![0x80, 0x01]),
        (255, vec![0x80, 0x7F]),
        (256, vec![0x81, 0x00]),
        (16383, vec![0xFE, 0x7F]),
        (16384, vec![0xFF, 0x00]),
        (16511, vec![0xFF, 0x7F]),
        // 3-byte encoding boundary.
        (16512, vec![0x80, 0x80, 0x00]),
        (2097151, vec![0xFE, 0xFE, 0x7F]),
        (2097152, vec![0xFE, 0xFF, 0x00]),
        // 4-byte encoding boundary.
        (268435455, vec![0xFE, 0xFE, 0xFE, 0x7F]),
        (268435456, vec![0xFE, 0xFE, 0xFF, 0x00]),
        // Maximum u32 value (5-byte encoding).
        (u32::MAX as _, vec![0x8E, 0xFE, 0xFE, 0xFE, 0x7F]),
        // Values just beyond u32::MAX.
        (u32::MAX as u128 + 1, vec![0x8E, 0xFE, 0xFE, 0xFF, 0x00]),
        (u32::MAX as u128 + 100, vec![0x8E, 0xFE, 0xFE, 0xFF, 0x63]),
        // Large u64 value.
        (
            0x1FFFFFFFFFFFFF,
            vec![0x8E, 0xFE, 0xFE, 0xFE, 0xFE, 0xFE, 0xFE, 0x7F],
        ),
        // Maximum u64 value.
        (
            u64::MAX as _,
            vec![0x80, 0xFE, 0xFE, 0xFE, 0xFE, 0xFE, 0xFE, 0xFE, 0xFE, 0x7F],
        ),
        // Values just beyond u64::MAX.
        (
            u64::MAX as u128 + 1,
            vec![0x80, 0xFE, 0xFE, 0xFE, 0xFE, 0xFE, 0xFE, 0xFE, 0xFF, 0x00],
        ),
        (
            u64::MAX as u128 + 100,
            vec![0x80, 0xFE, 0xFE, 0xFE, 0xFE, 0xFE, 0xFE, 0xFE, 0xFF, 0x63],
        ),
        // Large u128 value.
        (
            0x1FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF,
            vec![
                0xBE, 0xFE, 0xFE, 0xFE, 0xFE, 0xFE, 0xFE, 0xFE, 0xFE, 0xFE, 0xFE, 0xFE, 0xFE, 0xFE,
                0xFE, 0xFE, 0xFE, 0x7F,
            ],
        ),
        // Maximum u128 value.
        (
            u128::MAX,
            vec![
                0x82, 0xFE, 0xFE, 0xFE, 0xFE, 0xFE, 0xFE, 0xFE, 0xFE, 0xFE, 0xFE, 0xFE, 0xFE, 0xFE,
                0xFE, 0xFE, 0xFE, 0xFE, 0x7F,
            ],
        ),
    ];

    for (value, expected_bytes) in test_cases {
        let mut buf = Vec::new();
        value.write_as_varint(&mut buf).unwrap();
        assert_eq!(
            buf, expected_bytes,
            "Encoded bytes for {value} don't match expected: got {buf:?}, expected {expected_bytes:?}"
        );

        // Verify round-trip decoding still works.
        assert_eq!(u128::read_as_varint(&mut Cursor::new(buf)).unwrap(), value);
    }
}

mod property_based {
    //! Round-trip tests with randomly-generated values of different sizes.
    #![cfg(not(miri))]
    use super::*;

    fn roundtrip_value<T: VarintEncode + std::fmt::Debug + PartialEq + Eq + Copy>(value: T) {
        let mut buf = Vec::new();
        value.write_as_varint(&mut buf).unwrap();
        let decoded: T = varint::read(&mut Cursor::new(buf)).unwrap();
        assert_eq!(decoded, value);
    }

    proptest::proptest! {
        #[test]
        fn test_u32_roundtrip(v: u32) {
            roundtrip_value(v);
        }

        #[test]
        fn test_u64_roundtrip(v: u64) {
            roundtrip_value(v);
        }

        #[test]
        fn test_u128_roundtrip(v: u128) {
            roundtrip_value(v);
        }
    }
}
