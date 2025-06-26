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
        // SAFETY: NewVarintVectorWriter is a valid C function that creates a new vector writer
        let ptr = unsafe { ffi::NewVarintVectorWriter(cap) };
        assert!(!ptr.is_null(), "Failed to create C VarintVectorWriter");
        Self(ptr)
    }

    /// Write a varint to the vector writer.
    /// Returns the number of bytes written.
    #[inline(always)]
    pub fn write(&mut self, value: u32) -> usize {
        // SAFETY: VVW_Write is a valid C function and self.0 is a valid VarintVectorWriter pointer
        unsafe { ffi::VVW_Write(self.0, value) }
    }

    /// Get the total number of bytes written.
    #[inline(always)]
    pub fn bytes_len(&self) -> usize {
        // SAFETY: self.0 is a valid VarintVectorWriter pointer, accessing buf.offset field
        unsafe { (*self.0).buf.offset }
    }

    /// Get the actual encoded bytes.
    #[inline(always)]
    pub fn bytes(&self) -> &[u8] {
        // SAFETY: self.0 is a valid VarintVectorWriter pointer
        unsafe {
            let data = (*self.0).buf.data as *const u8;
            let len = (*self.0).buf.offset;
            std::slice::from_raw_parts(data, len)
        }
    }
}

impl Drop for CVarintVectorWriter {
    fn drop(&mut self) {
        if !self.0.is_null() {
            // SAFETY: VVW_Free is a valid C function and self.0 is a valid VarintVectorWriter pointer
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
        // SAFETY: WriteVarint is a valid C function, buffer is a valid Buffer pointer
        unsafe {
            // Set up C BufferWriter structure
            let mut writer = ffi::BufferWriter {
                buf: buffer,
                pos: buffer.offset,
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
        // SAFETY: WriteVarintFieldMask is a valid C function, buffer is a valid Buffer pointer
        unsafe {
            // Set up C BufferWriter structure
            let mut writer = ffi::BufferWriter {
                buf: buffer,
                pos: buffer.offset,
            };

            // Use the C WriteVarintFieldMask function
            let bytes_written = ffi::WriteVarintFieldMask(value, &mut writer);

            // Update buffer offset
            buffer.offset += bytes_written;

            bytes_written
        }
    }

    /// Encode a varint using pre-allocated buffer and return the encoded bytes.
    /// This avoids allocation overhead in benchmarks.
    #[inline(always)]
    pub fn write_to_buffer(value: u32, buffer: &mut ffi::Buffer) -> &[u8] {
        // SAFETY: buffer is a valid Buffer with valid data pointer and offset
        unsafe {
            buffer.offset = 0; // Reset buffer
            write(value, buffer);
            std::slice::from_raw_parts(buffer.data as *const u8, buffer.offset)
        }
    }

    /// Convenience wrapper for single varint encoding that returns Vec<u8>.
    /// Used by main.rs for memory analysis.
    #[inline(always)]
    pub fn write_to_vec(value: u32) -> Vec<u8> {
        // SAFETY: RedisModule_Alloc is a valid function pointer provided by Redis module system
        let alloc_fn = unsafe { crate::RedisModule_Alloc.unwrap() };

        // SAFETY: Calling Redis allocator with valid size parameter
        let data_ptr = unsafe { alloc_fn(16) };
        if data_ptr.is_null() {
            return Vec::new();
        }

        // Set up C Buffer structure
        let mut buffer = ffi::Buffer {
            data: data_ptr as *mut i8,
            offset: 0,
            cap: 16,
        };

        // Use the buffer-based write function
        let encoded = write_to_buffer(value, &mut buffer);
        let result = encoded.to_vec();

        // SAFETY: RedisModule_Free is a valid function pointer provided by Redis module system
        let free_fn = unsafe { crate::RedisModule_Free.unwrap() };
        // SAFETY: Freeing buffer that was allocated by RedisModule_Alloc
        unsafe { free_fn(data_ptr) };

        result
    }

    /// Encode a field mask using pre-allocated buffer and return the encoded bytes.
    /// This avoids allocation overhead in benchmarks.
    #[inline(always)]
    pub fn write_field_mask_to_buffer(value: FieldMask, buffer: &mut ffi::Buffer) -> &[u8] {
        // SAFETY: buffer is a valid Buffer with valid data pointer and offset
        unsafe {
            buffer.offset = 0; // Reset buffer
            write_field_mask(value, buffer);
            std::slice::from_raw_parts(buffer.data as *const u8, buffer.offset)
        }
    }

    /// Convenience wrapper for field mask encoding that returns Vec<u8>.
    /// Used by main.rs for memory analysis.
    #[inline(always)]
    pub fn write_field_mask_to_vec(value: FieldMask) -> Vec<u8> {
        // SAFETY: RedisModule_Alloc is a valid function pointer provided by Redis module system
        let alloc_fn = unsafe { crate::RedisModule_Alloc.unwrap() };

        // SAFETY: Calling Redis allocator with valid size parameter
        let data_ptr = unsafe { alloc_fn(32) };
        if data_ptr.is_null() {
            return Vec::new();
        }

        // Set up C Buffer structure
        let mut buffer = ffi::Buffer {
            data: data_ptr as *mut i8,
            offset: 0,
            cap: 32,
        };

        // Use the buffer-based write function
        let encoded = write_field_mask_to_buffer(value, &mut buffer);
        let result = encoded.to_vec();

        // SAFETY: RedisModule_Free is a valid function pointer provided by Redis module system
        let free_fn = unsafe { crate::RedisModule_Free.unwrap() };
        // SAFETY: Freeing buffer that was allocated by RedisModule_Alloc
        unsafe { free_fn(data_ptr) };

        result
    }

    /// Decode a varint using the actual C ReadVarint function with BufferReader.
    /// This matches the real-world usage pattern in the C codebase.
    /// Buffer must already be set up to point to the data to decode.
    #[inline(always)]
    pub fn read(buffer: &mut ffi::Buffer) -> u32 {
        // SAFETY: NewBufferReader is a valid C function that creates a BufferReader from a Buffer
        let mut reader = unsafe { ffi::NewBufferReader(buffer) };

        // SAFETY: ReadVarintNonInline is a valid C function that reads from a BufferReader
        unsafe { ffi::ReadVarintNonInline(&mut reader) }
    }

    /// Decode a field mask using the actual C ReadVarintFieldMask function with BufferReader.
    /// This matches the real-world usage pattern in the C codebase.
    /// Buffer must already be set up to point to the data to decode.
    #[inline(always)]
    pub fn read_field_mask(buffer: &mut ffi::Buffer) -> FieldMask {
        // SAFETY: NewBufferReader is a valid C function that creates a BufferReader from a Buffer
        let mut reader = unsafe { ffi::NewBufferReader(buffer) };

        // SAFETY: ReadVarintFieldMaskNonInline is a valid C function that reads from a BufferReader
        unsafe { ffi::ReadVarintFieldMaskNonInline(&mut reader) }
    }

    /// Convenience wrapper for single varint decoding that returns u32.
    /// Used by tests and non-performance-critical code.
    pub fn read_to_u32(data: &[u8]) -> u32 {
        let mut temp_buffer = ffi::Buffer {
            data: data.as_ptr() as *mut i8,
            offset: data.len(),
            cap: data.len(),
        };
        read(&mut temp_buffer)
    }

    /// Convenience wrapper for single field mask decoding that returns FieldMask.
    /// Used by tests and non-performance-critical code.
    pub fn read_to_field_mask(data: &[u8]) -> FieldMask {
        let mut temp_buffer = ffi::Buffer {
            data: data.as_ptr() as *mut i8,
            offset: data.len(),
            cap: data.len(),
        };
        read_field_mask(&mut temp_buffer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buffer_reader_decode_round_trip() {
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

            // Decode using BufferReader-based function.
            let decoded = c_varint_ops::read_to_u32(&encoded);

            // Should match the original value.
            assert_eq!(value, decoded, "Round-trip failed for value {}", value);
        }
    }

    #[test]
    fn test_buffer_reader_field_mask_decode_round_trip() {
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

            // Decode using BufferReader-based function.
            let decoded = c_varint_ops::read_to_field_mask(&encoded);

            // Should match the original value.
            assert_eq!(value, decoded, "Round-trip failed for field mask {}", value);
        }
    }
}
