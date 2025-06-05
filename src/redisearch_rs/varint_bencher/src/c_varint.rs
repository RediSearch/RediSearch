/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Implementation of C varint wrapper types that provide safe Rust interfaces
//! to the C varint functions for benchmarking comparisons.

use crate::FieldMask;
use crate::ffi;

/// A safe wrapper around the C VarintVectorWriter implementation.
#[repr(transparent)]
pub struct CVarintVectorWriter(*mut ffi::VarintVectorWriter);

impl CVarintVectorWriter {
    /// Create a new C-backed varint vector writer with the given capacity.
    #[inline(always)]
    pub fn new(cap: usize) -> Self {
        let ptr = unsafe { ffi::NewVarintVectorWriter(cap) };
        assert!(!ptr.is_null(), "Failed to create C VarintVectorWriter");
        Self(ptr)
    }

    /// Write a varint to the vector writer.
    /// Returns the number of bytes written.
    #[inline(always)]
    pub fn write(&mut self, value: u32) -> usize {
        unsafe { ffi::VVW_Write(self.0, value) }
    }

    /// Get the number of values written to the vector.
    #[inline(always)]
    pub fn count(&self) -> usize {
        unsafe { (*self.0).nmemb }
    }

    /// Get the total number of bytes written.
    #[inline(always)]
    pub fn bytes_len(&self) -> usize {
        unsafe { (*self.0).buf.offset }
    }

    /// Get a pointer to the internal byte data.
    /// The data is valid until the next write operation or until the writer is dropped.
    #[inline(always)]
    pub fn bytes_data(&self) -> *const u8 {
        unsafe { (*self.0).buf.data as *const u8 }
    }

    /// Truncate the vector writer.
    /// Returns the new capacity.
    #[inline(always)]
    pub fn shrink_to_fit(&mut self) -> usize {
        unsafe { ffi::VVW_Truncate(self.0) }
    }
}

impl Drop for CVarintVectorWriter {
    fn drop(&mut self) {
        if !self.0.is_null() {
            unsafe {
                ffi::VVW_Free(self.0);
            }
        }
    }
}

/// Helper functions for C varint operations that match the Rust API patterns.
pub mod c_varint_ops {
    use super::*;

    /// Encode a varint using the C implementation into the provided buffer.
    /// Returns the number of bytes written.
    #[inline(always)]
    pub fn write(value: u32, buffer: &mut ffi::Buffer) -> usize {
        unsafe {
            // Set up C BufferWriter structure
            let mut writer = ffi::BufferWriter {
                buf: buffer,
                pos: buffer.data.add(buffer.offset),
            };

            // Use the C WriteVarint function
            let bytes_written = ffi::WriteVarint(value, &mut writer);

            // Update buffer offset
            buffer.offset += bytes_written;

            bytes_written
        }
    }

    /// Encode a field mask using the C implementation into the provided buffer.
    /// Returns the number of bytes written.
    #[inline(always)]
    pub fn write_field_mask(value: FieldMask, buffer: &mut ffi::Buffer) -> usize {
        unsafe {
            // Set up C BufferWriter structure
            let mut writer = ffi::BufferWriter {
                buf: buffer,
                pos: buffer.data.add(buffer.offset),
            };

            // Use the C WriteVarintFieldMask function
            let bytes_written = ffi::WriteVarintFieldMask(value, &mut writer);

            // Update buffer offset
            buffer.offset += bytes_written;

            bytes_written
        }
    }

    /// Convenience wrapper for single varint encoding that returns Vec<u8>.
    /// Used by main.rs for memory analysis.
    #[inline(always)]
    pub fn write_to_vec(value: u32) -> Vec<u8> {
        unsafe {
            // Create buffer for single varint (max 5 bytes)
            let initial_capacity = 16;
            let data_ptr = crate::RedisModule_Alloc.unwrap()(initial_capacity);
            if data_ptr.is_null() {
                return Vec::new();
            }

            // Set up C Buffer structure
            let mut buffer = ffi::Buffer {
                data: data_ptr as *mut i8,
                offset: 0,
                cap: initial_capacity,
            };

            // Use the buffer-based write function
            write(value, &mut buffer);

            // Copy the result to a Rust Vec
            let result_data = std::slice::from_raw_parts(buffer.data as *const u8, buffer.offset);
            let result = result_data.to_vec();

            // Free the C buffer
            crate::RedisModule_Free.unwrap()(data_ptr);

            result
        }
    }

    /// Convenience wrapper for field mask encoding that returns Vec<u8>.
    /// Used by main.rs for memory analysis.
    #[inline(always)]
    pub fn write_field_mask_to_vec(value: FieldMask) -> Vec<u8> {
        unsafe {
            // Create initial buffer using C allocation so it can be grown by Buffer_Grow
            let initial_capacity = 32;
            let data_ptr = crate::RedisModule_Alloc.unwrap()(initial_capacity);
            if data_ptr.is_null() {
                return Vec::new();
            }

            // Set up C Buffer structure
            let mut buffer = ffi::Buffer {
                data: data_ptr as *mut i8,
                offset: 0,
                cap: initial_capacity,
            };

            // Use the buffer-based write function
            write_field_mask(value, &mut buffer);

            // Copy the result to a Rust Vec
            let result_data = std::slice::from_raw_parts(buffer.data as *const u8, buffer.offset);
            let result = result_data.to_vec();

            // Free the C buffer
            crate::RedisModule_Free.unwrap()(data_ptr);

            result
        }
    }

    /// Decode a varint using the actual C ReadVarint function with BufferReader.
    /// This matches the real-world usage pattern in the C codebase.
    #[inline(always)]
    pub fn read(data: &[u8]) -> u32 {
        unsafe {
            // Create a Buffer structure pointing to our data.
            let mut buffer = ffi::Buffer {
                data: data.as_ptr() as *mut i8,
                offset: data.len(),
                cap: data.len(),
            };

            // Create a BufferReader.
            let mut reader = ffi::NewBufferReader(&mut buffer);

            // Use the actual C ReadVarint function.
            ffi::ReadVarintNonInline(&mut reader)
        }
    }

    /// Decode a field mask using the actual C ReadVarintFieldMask function with BufferReader.
    /// This matches the real-world usage pattern in the C codebase.
    #[inline(always)]
    pub fn read_field_mask(data: &[u8]) -> FieldMask {
        unsafe {
            // Create a Buffer structure pointing to our data.
            let mut buffer = ffi::Buffer {
                data: data.as_ptr() as *mut i8,
                offset: data.len(),
                cap: data.len(),
            };

            // Create a BufferReader.
            let mut reader = ffi::NewBufferReader(&mut buffer);

            // Use the actual C ReadVarintFieldMask function.
            ffi::ReadVarintFieldMaskNonInline(&mut reader)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buffer_reader_decode_matches_raw_decode() {
        let test_values = [
            0,
            1,
            127,
            128,
            16383,
            16384,
            2097151,
            2097152,
            268435455,
            268435456,
            u32::MAX,
        ];

        for &value in &test_values {
            // Encode using C implementation.
            let encoded = c_varint_ops::write_to_vec(value);

            // Decode using both methods.
            let decoded_raw = c_varint_ops::read(&encoded);
            let decoded_buffer_reader = c_varint_ops::read(&encoded);

            // They should match.
            assert_eq!(
                decoded_raw, decoded_buffer_reader,
                "Mismatch for value {}: raw={}, buffer_reader={}",
                value, decoded_raw, decoded_buffer_reader
            );

            // Both should match the original value.
            assert_eq!(value, decoded_raw);
            assert_eq!(value, decoded_buffer_reader);
        }
    }

    #[test]
    fn test_buffer_reader_field_mask_decode_matches_raw_decode() {
        let test_values: Vec<FieldMask> = [
            0,
            1,
            127,
            128,
            16383,
            16384,
            2097151,
            2097152,
            268435455,
            268435456,
            u32::MAX as FieldMask,
        ]
        .to_vec();

        for &value in &test_values {
            // Encode using C implementation.
            let encoded = c_varint_ops::write_field_mask_to_vec(value);

            // Decode using both methods.
            let decoded_raw = c_varint_ops::read_field_mask(&encoded);
            let decoded_buffer_reader = c_varint_ops::read_field_mask(&encoded);

            // They should match.
            assert_eq!(
                decoded_raw, decoded_buffer_reader,
                "Mismatch for field mask {}: raw={}, buffer_reader={}",
                value, decoded_raw, decoded_buffer_reader
            );

            // Both should match the original value.
            assert_eq!(value, decoded_raw);
            assert_eq!(value, decoded_buffer_reader);
        }
    }
}
