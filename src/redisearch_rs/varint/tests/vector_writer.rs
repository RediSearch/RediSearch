/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::io::Cursor;
use varint::VectorWriter;

#[test]
fn test_new_vector_writer() {
    let writer = VectorWriter::new(10);
    assert_eq!(writer.bytes().len(), 0);
    assert_eq!(writer.bytes_len(), 0);
    assert_eq!(writer.capacity(), 10);
    assert_eq!(writer.count(), 0);
}
#[test]
fn test_reset() {
    let mut writer = VectorWriter::new(100);

    // Write some values
    writer.write(10).unwrap();
    writer.write(20).unwrap();
    writer.write(30).unwrap();

    assert_eq!(writer.count(), 3);
    assert!(writer.bytes_len() > 0);

    // Reset the writer
    writer.reset();

    assert_eq!(writer.count(), 0);
    assert_eq!(writer.bytes_len(), 0);
    assert_eq!(writer.bytes().len(), 0);

    // Write new values after reset
    writer.write(100).unwrap();
    assert_eq!(writer.count(), 1);

    // Verify the new value
    let mut cursor = Cursor::new(writer.bytes());
    let decoded: u32 = varint::read(&mut cursor).unwrap();
    assert_eq!(decoded, 100);
}

#[test]
fn test_bytes_and_bytes_mut() {
    let mut writer = VectorWriter::new(10);
    writer.write(42).unwrap();

    // Test immutable access
    let bytes = writer.bytes();
    assert!(!bytes.is_empty());

    // Test mutable access
    let bytes_mut = writer.bytes_mut();
    let original_len = bytes_mut.len();

    // Manually append a byte
    bytes_mut.push(0xFF);
    assert_eq!(writer.bytes().len(), original_len + 1);
}

#[test]
fn test_shrink_to_fit() {
    let mut writer = VectorWriter::new(1000);
    writer.write(42).unwrap();
    // The capacity is larger than needed
    assert!(writer.capacity() > writer.bytes_len());

    let new_capacity = writer.shrink_to_fit();
    assert_eq!(new_capacity, writer.bytes_len());
}

#[test]
fn test_write_multiple_values_ascending() {
    roundtrip_values(&[10, 20, 30, 40, 50]);
}

#[test]
fn test_write_multiple_values_descending() {
    roundtrip_values(&[100, 90, 80, 70, 60]);
}

#[test]
fn test_write_with_wrapping() {
    roundtrip_values(&[u32::MAX - 10, 5]);
}

#[test]
fn test_identical_values() {
    roundtrip_values(&[100, 100, 100, 100]);
}

#[test]
fn test_maximum_values() {
    roundtrip_values(&[u32::MAX, u32::MAX]);
}

#[test]
fn test_alternating_pattern() {
    roundtrip_values(&[1000, 10, 2000, 20, 3000, 30]);
}

#[test]
fn test_edge_case_deltas() {
    roundtrip_values(&[
        0, 1, 127,     // Maximum single-byte varint
        128,     // Minimum two-byte varint
        16383,   // Maximum two-byte varint
        16384,   // Minimum three-byte varint
        2097151, // Maximum three-byte varint
        2097152, // Minimum four-byte varint
    ]);
}

/// Test that a sequence of values can be written and read back correctly.
fn roundtrip_values(values: &[u32]) {
    let mut writer = VectorWriter::new(0);

    // Write all values
    for &value in values {
        writer.write(value).unwrap();
    }

    assert_eq!(writer.count(), values.len());

    // Decode and verify all values
    let mut cursor = Cursor::new(writer.bytes());
    let mut last_value = 0u32;

    for expected in values {
        let delta: u32 = varint::read(&mut cursor).unwrap();
        let decoded = last_value.wrapping_add(delta);
        assert_eq!(decoded, *expected);
        last_value = decoded;
    }
}

mod property_based {
    //! Property-based tests using random values
    #![cfg(not(miri))]
    use super::*;

    proptest::proptest! {
        #[test]
        fn test_roundtrip_random_values(values: Vec<u32>) {
            roundtrip_values(&values);
        }
    }
}
