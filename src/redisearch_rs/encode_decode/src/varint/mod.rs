/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! This module provides functions to read and write varints (variable-length integers) in a
//! compact format. Varints are used to encode integers in a way that uses fewer bytes for small
//! values and more bytes for larger values. This is useful for reducing the size of data when
//! transmitting or storing integers.
//!
//! The module includes functions to read and write varints from and to a byte buffer, as well as
//! functions to read and write field masks, which are a specific type of varint used in the
//! Redisearch project. The module also includes a [`VectorWriter`] struct that allows for writing
//! multiple varints to a vector in a more efficient way.

mod vector_writer;
pub use vector_writer::VectorWriter;

use super::FieldMask;

use std::{
    io::{self, Read, Write},
    ops::{Deref, DerefMut},
};

/// Read an encoded integer from the given reader.
#[inline(always)]
pub fn read<R>(mut read: R) -> io::Result<u32>
where
    R: Read,
{
    let mut buffer = [0; 1];
    read.read_exact(&mut buffer)?;
    let mut c = buffer[0];

    let mut val = (c & 127) as u32;
    while c >> 7 != 0 {
        val += 1;
        read.read_exact(&mut buffer)?;
        c = buffer[0];
        val = (val << 7) | ((c & 127) as u32);
    }

    Ok(val)
}

/// Same as `read` but reads the value into a `FieldMask`.
// FIXME: The logic is identical to `read` so we can use a macro here to avoid code duplication.
#[inline(always)]
pub fn read_field_mask<R>(mut read: R) -> io::Result<FieldMask>
where
    R: Read,
{
    let mut buffer = [0; 1];
    read.read_exact(&mut buffer)?;
    let mut c = buffer[0];

    let mut val = (c & 127) as FieldMask;
    while c >> 7 != 0 {
        val += 1;
        read.read_exact(&mut buffer)?;
        c = buffer[0];
        val = (val << 7) | ((c & 127) as FieldMask);
    }

    Ok(val)
}

/// Encode an integer into a varint format and write it to the given writer.
///
/// # Return Value
///
/// The number of bytes written.
#[inline(always)]
pub fn write<W>(value: u32, mut write: W) -> io::Result<usize>
where
    W: Write,
{
    let mut variant = VarintBuf::new();
    let pos = encode(value, &mut variant);
    write.write_all(&variant[pos..])?;

    Ok(variant[pos..].len())
}

/// Encode a [`FieldMask`] into a varint format and write it to the given writer.
///
/// # Return Value
///
/// The number of bytes written.
#[inline(always)]
pub fn write_field_mask<W>(value: FieldMask, mut write: W) -> io::Result<usize>
where
    W: Write,
{
    let mut variant = VarintBuf::new();
    let pos = encode_field_mask(value, &mut variant);
    write.write_all(&variant[pos..])?;

    Ok(variant[pos..].len())
}

#[inline(always)]
fn encode(mut value: u32, vbuf: &mut VarintBuf) -> usize {
    let mut pos = vbuf.len() - 1;
    vbuf[pos] = (value & 127) as u8;
    value >>= 7;
    while value != 0 {
        pos -= 1;
        value -= 1;
        vbuf[pos] = 128 | ((value & 127) as u8);
        value >>= 7;
    }

    pos
}

// FIXME: The logic is identical to `encode` so we can use a macro here to avoid code duplication.
fn encode_field_mask(mut value: FieldMask, vbuf: &mut VarintBuf) -> usize {
    let mut pos = vbuf.len() - 1;
    vbuf[pos] = (value & 127) as u8;
    value >>= 7;
    while value != 0 {
        pos -= 1;
        value -= 1;
        vbuf[pos] = 128 | ((value & 127) as u8);
        value >>= 7;
    }

    pos
}

/// A buffer used for encoding varints.
#[repr(transparent)]
struct VarintBuf([u8; 24]);

impl VarintBuf {
    fn new() -> Self {
        Self([0; 24])
    }
}

impl Deref for VarintBuf {
    type Target = [u8; 24];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for VarintBuf {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_varint() {
        let values = [123456789, 987654321, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
        let expected_lens = [4, 5, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1];

        for (i, value) in values.iter().enumerate() {
            let mut buf = Vec::new();
            write(*value, &mut buf).unwrap();
            assert_eq!(buf.len(), expected_lens[i]);
            assert_eq!(read(&buf[..]).unwrap(), *value);
        }
    }

    #[test]
    fn test_field_mask() {
        let values = [123456789, 987654321, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
        let expected_lens = [4, 5, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1];

        for (i, value) in values.iter().enumerate() {
            let mut buf = Vec::new();
            write_field_mask(*value, &mut buf).unwrap();
            assert_eq!(buf.len(), expected_lens[i]);
            assert_eq!(read_field_mask(&buf[..]).unwrap(), *value);
        }
    }

    #[test]
    fn test_varint_encoded_bytes() {
        // Test specific values and their expected encoded byte sequences.
        let test_cases = [
            (0u32, vec![0x00]),
            (1u32, vec![0x01]),
            (127u32, vec![0x7F]),
            (128u32, vec![0x80, 0x00]),
            (129u32, vec![0x80, 0x01]),
            (255u32, vec![0x80, 0x7F]),
            (256u32, vec![0x81, 0x00]),
            (16383u32, vec![0xFE, 0x7F]),
            (16384u32, vec![0xFF, 0x00]),
            // 3-byte encoding boundary.
            (2097151u32, vec![0xFE, 0xFE, 0x7F]),
            (2097152u32, vec![0xFE, 0xFF, 0x00]),
            // 4-byte encoding boundary.
            (268435455u32, vec![0xFE, 0xFE, 0xFE, 0x7F]),
            (268435456u32, vec![0xFE, 0xFE, 0xFF, 0x00]),
            // Maximum u32 value (5-byte encoding).
            (u32::MAX, vec![0x8E, 0xFE, 0xFE, 0xFE, 0x7F]),
        ];

        for (value, expected_bytes) in test_cases {
            let mut buf = Vec::new();
            write(value, &mut buf).unwrap();
            assert_eq!(
                buf, expected_bytes,
                "Encoded bytes for value {} don't match expected: got {:?}, expected {:?}",
                value, buf, expected_bytes
            );

            // Verify round-trip decoding still works.
            assert_eq!(read(&buf[..]).unwrap(), value);
        }
    }

    #[test]
    fn test_field_mask_encoded_bytes() {
        // Test specific field mask values and their expected encoded byte sequences.
        let test_cases: &[(FieldMask, Vec<u8>)] = &[
            (0, vec![0x00]),
            (1, vec![0x01]),
            (127, vec![0x7F]),
            (128, vec![0x80, 0x00]),
            (129, vec![0x80, 0x01]),
            (255, vec![0x80, 0x7F]),
            (256, vec![0x81, 0x00]),
            (16383, vec![0xFE, 0x7F]),
            (16384, vec![0xFF, 0x00]),
            // 3-byte encoding boundary.
            (2097151, vec![0xFE, 0xFE, 0x7F]),
            (2097152, vec![0xFE, 0xFF, 0x00]),
            // 4-byte encoding boundary.
            (268435455, vec![0xFE, 0xFE, 0xFE, 0x7F]),
            (268435456, vec![0xFE, 0xFE, 0xFF, 0x00]),
            // Maximum u32 value (5-byte encoding).
            (u32::MAX as FieldMask, vec![0x8E, 0xFE, 0xFE, 0xFE, 0x7F]),
        ];

        for (value, expected_bytes) in test_cases {
            let mut buf = Vec::new();
            write_field_mask(*value, &mut buf).unwrap();
            assert_eq!(
                &buf, expected_bytes,
                "Encoded bytes for field mask {} don't match expected: got {:?}, expected {:?}",
                value, buf, expected_bytes
            );

            // Verify round-trip decoding still works.
            assert_eq!(read_field_mask(&buf[..]).unwrap(), *value);
        }

        // Platform-specific test cases for extended FieldMask ranges.
        #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
        {
            // On platforms where FieldMask is u64, test values beyond u32::MAX.
            let u64_test_cases: &[(FieldMask, Vec<u8>)] = &[
                // Values just beyond u32::MAX.
                (
                    u32::MAX as FieldMask + 1,
                    vec![0x8E, 0xFE, 0xFE, 0xFF, 0x00],
                ),
                (
                    u32::MAX as FieldMask + 100,
                    vec![0x8E, 0xFE, 0xFE, 0xFF, 0x63],
                ),
                // Large u64 value.
                (
                    0x1FFFFFFFFFFFFF,
                    vec![0x8E, 0xFE, 0xFE, 0xFE, 0xFE, 0xFE, 0xFE, 0x7F],
                ),
                // Maximum u64 value.
                (
                    u64::MAX as FieldMask,
                    vec![0x80, 0xFE, 0xFE, 0xFE, 0xFE, 0xFE, 0xFE, 0xFE, 0xFE, 0x7F],
                ),
            ];

            for &(value, ref expected_bytes) in u64_test_cases {
                let mut buf = Vec::new();
                write_field_mask(value, &mut buf).unwrap();
                assert_eq!(
                    buf, *expected_bytes,
                    "Encoded bytes for large field mask {} don't match expected: got {:?}, expected {:?}",
                    value, buf, expected_bytes
                );

                // Verify round-trip decoding still works.
                assert_eq!(read_field_mask(&buf[..]).unwrap(), value);
            }
        }

        #[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
        {
            // On platforms where FieldMask is u128, test values beyond u64::MAX.
            let u128_test_cases: &[(FieldMask, Vec<u8>)] = &[
                // Values just beyond u64::MAX.
                (
                    u64::MAX as FieldMask + 1,
                    vec![0x80, 0xFE, 0xFE, 0xFE, 0xFE, 0xFE, 0xFE, 0xFE, 0xFF, 0x00],
                ),
                (
                    u64::MAX as FieldMask + 100,
                    vec![0x80, 0xFE, 0xFE, 0xFE, 0xFE, 0xFE, 0xFE, 0xFE, 0xFF, 0x63],
                ),
                // Large u128 value.
                (
                    0x1FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF,
                    vec![
                        0xBE, 0xFE, 0xFE, 0xFE, 0xFE, 0xFE, 0xFE, 0xFE, 0xFE, 0xFE, 0xFE, 0xFE,
                        0xFE, 0xFE, 0xFE, 0xFE, 0xFE, 0x7F,
                    ],
                ),
                // Maximum u128 value.
                (
                    u128::MAX,
                    vec![
                        0x82, 0xFE, 0xFE, 0xFE, 0xFE, 0xFE, 0xFE, 0xFE, 0xFE, 0xFE, 0xFE, 0xFE,
                        0xFE, 0xFE, 0xFE, 0xFE, 0xFE, 0xFE, 0x7F,
                    ],
                ),
            ];

            for &(value, ref expected_bytes) in u128_test_cases {
                let mut buf = Vec::new();
                write_field_mask(value, &mut buf).unwrap();
                assert_eq!(
                    buf, *expected_bytes,
                    "Encoded bytes for large field mask {} don't match expected: got {:?}, expected {:?}",
                    value, buf, expected_bytes
                );

                // Verify round-trip decoding still works.
                assert_eq!(read_field_mask(&buf[..]).unwrap(), value);
            }
        }
    }
}
