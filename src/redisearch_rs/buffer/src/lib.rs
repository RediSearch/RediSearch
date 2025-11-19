/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! The `Buffer` API Rust wrappers.
//!
//! This crate provides Rust wrappers for the `Buffer`, `BufferReader` and `BufferWriter` structs
//! from `buffer.h`.

use ffi::Buffer_Grow;
use std::{ptr::NonNull, slice};

pub use reader::BufferReader;
pub use writer::BufferWriter;

mod reader;
mod writer;

/// Thin wrapper around the `Buffer` struct from `buffer.h`.
#[repr(transparent)]
pub struct Buffer(pub ffi::Buffer);

impl Buffer {
    /// Creates a new `Buffer` with the given pointer, length, and capacity.
    ///
    /// # Safety
    ///
    /// * `data` must be a valid pointer to a memory region of at least `capacity` bytes.
    /// * `len` must be less than or equal to `capacity`.
    /// * The memory region must remain valid for the lifetime of the Buffer.
    /// * The first `len` bytes of the memory region must be initialized.
    ///
    /// # Panics
    ///
    /// Panics if `len` is greater than `capacity`.
    pub unsafe fn new(data: NonNull<u8>, len: usize, capacity: usize) -> Self {
        debug_assert!(len <= capacity, "len must not exceed capacity");
        Self(ffi::Buffer {
            data: data.as_ptr().cast(),
            offset: len,
            cap: capacity,
        })
    }

    /// Returns the initialized portion of the buffer as a slice.
    pub const fn as_slice(&self) -> &[u8] {
        // Safety: We assume `self.len` is a valid length within the allocated memory.
        // Creates a slice referencing the initialized portion of the buffer.
        unsafe { slice::from_raw_parts(self.0.data as *const u8, self.len()) }
    }

    /// Returns the initialized portion of the buffer as a mutable slice.
    pub const fn as_mut_slice(&mut self) -> &mut [u8] {
        // Safety: We assume `self.0.offset` is a valid length within the allocated memory.
        unsafe { slice::from_raw_parts_mut(self.0.data as *mut u8, self.len()) }
    }

    /// Returns the length of the buffer (number of initialized bytes).
    pub const fn len(&self) -> usize {
        self.0.offset
    }

    /// Returns true if the buffer is empty (length is zero).
    pub const fn is_empty(&self) -> bool {
        self.0.offset == 0
    }

    /// Returns the total capacity of the buffer (maximum number of bytes it can hold).
    pub const fn capacity(&self) -> usize {
        self.0.cap
    }

    /// Returns the remaining capacity of the buffer (capacity - length).
    pub const fn remaining_capacity(&self) -> usize {
        self.0.cap - self.0.offset
    }

    /// Ensure that the buffer has enough capacity to store `additional_capacity` values.
    ///
    /// If necessary, grow the buffer by reallocating.
    ///
    /// After calling this function, all previously held pointers into the buffer data
    /// must be considered invalid.
    pub fn reserve(&mut self, additional_capacity: usize) {
        #[cfg(debug_assertions)]
        {
            let Some(new_length) = self.len().checked_add(additional_capacity) else {
                panic!("The requested buffer capacity would overflow usize::MAX")
            };
            if new_length > isize::MAX as usize {
                panic!("The requested buffer capacity would overflow isize::MAX")
            }
        }
        // We have enough space, no need to resize.
        if additional_capacity <= self.remaining_capacity() {
            return;
        }
        // Safety: `Buffer_Grow` is a C function that increases the buffer's capacity. It
        // expects a valid buffer pointer and returns the number of bytes added to capacity.
        // This number can be 0 if the buffer is already large enough.
        unsafe { Buffer_Grow(&mut self.0 as *mut _, additional_capacity) };
    }

    /// Advance the buffer by `n` bytes.
    ///
    /// This increases the buffer's length by `n` bytes, effectively marking more of the buffer as
    /// "initialized" without actually writing any data. Typically used after directly writing to
    /// the buffer's memory.
    ///
    /// # Safety
    ///
    /// * `n` must not exceed the remaining capacity of the buffer.
    /// * The new bytes added by this call must be initialized.
    pub unsafe fn advance(&mut self, n: usize) {
        debug_assert!(n <= self.remaining_capacity());
        self.0.offset += n;
    }
}

impl std::fmt::Debug for Buffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut debug = f.debug_struct("Buffer");
        let debug = debug
            .field("len", &self.len())
            .field("capacity", &self.capacity());
        // We don't want to accidentally output huge or sensitive data in production code.
        #[cfg(debug_assertions)]
        let debug = debug.field("data", &self.as_slice());
        debug.finish()
    }
}
