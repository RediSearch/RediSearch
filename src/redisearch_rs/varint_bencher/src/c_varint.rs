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
}
