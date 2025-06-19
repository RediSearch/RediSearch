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

use std::{
    cmp,
    ptr::{NonNull, copy_nonoverlapping},
    slice,
};

/// Thin wrapper around the `Buffer` struct from `buffer.h`.
#[repr(transparent)]
pub struct Buffer(ffi::Buffer);

/// Thin wrapper around the `BufferReader` struct from `buffer.h`
///
/// Provides read functionality over a Buffer with position tracking.
///
/// # Safety Invariants
///
/// * `ffi::BufferReader::buf` must point to a valid, properly initialized `Buffer`.
/// * `ffi::BufferReader::pos` must not exceed the length of the Buffer.
/// * The `BufferReader` does not own the `Buffer`, only borrows it.
///
/// It's best to use this through the `std::io::Read` implementation.
#[repr(transparent)]
pub struct BufferReader(ffi::BufferReader);

impl std::io::Read for BufferReader {
    fn read(&mut self, dest_buf: &mut [u8]) -> std::io::Result<usize> {
        // Safety: We assume `buf` is a valid pointer to a properly initialized Buffer.
        let buffer = unsafe { &*self.0.buf };

        debug_assert!(self.0.pos <= buffer.offset);
        // Safety: We assume that `self.pos` is within the limits of the buffer's initialized range
        // so this pointer arithmetic is safe.
        let src = unsafe { buffer.data.add(self.0.pos) } as *const u8;

        // Safety: We just verified that `src` points to valid memory within the buffer.
        let dest = dest_buf.as_mut_ptr();
        let len = cmp::min(dest_buf.len(), buffer.offset - self.0.pos);

        // Safety:
        // * `src` points to initialized memory in the buffer.
        // * `dest` is a valid pointer to the destination slice.
        // * We've verified there are at least `dest_buf.len()` bytes available in the source.
        // * The memory regions don't overlap (copy_nonoverlapping requirement).
        unsafe { copy_nonoverlapping(src, dest, len) };

        // Update position after successful read
        self.0.pos += len;

        Ok(len)
    }
}

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
    pub fn as_slice(&self) -> &[u8] {
        // Safety: We assume `self.len` is a valid length within the allocated memory.
        // Creates a slice referencing the initialized portion of the buffer.
        unsafe { slice::from_raw_parts(self.0.data as *const u8, self.len()) }
    }

    /// Returns the initialized portion of the buffer as a mutable slice.
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        // Safety: We assume `self.0.offset` is a valid length within the allocated memory.
        unsafe { slice::from_raw_parts_mut(self.0.data as *mut u8, self.len()) }
    }

    /// Returns the length of the buffer (number of initialized bytes).
    pub fn len(&self) -> usize {
        self.0.offset
    }

    /// Returns true if the buffer is empty (length is zero).
    pub fn is_empty(&self) -> bool {
        self.0.offset == 0
    }

    /// Returns the total capacity of the buffer (maximum number of bytes it can hold).
    pub fn capacity(&self) -> usize {
        self.0.cap
    }

    /// Returns the remaining capacity of the buffer (capacity - length).
    pub fn remaining_capacity(&self) -> usize {
        self.0.cap - self.0.offset
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

/// Redefines the `BufferWriter` struct from `buffer.h`
///
/// A writer for appending data to a `Buffer`. Provides a mechanism to write bytes to a `Buffer`
/// while tracking the current write position.
///
/// # Safety Invariants
///
/// * `ffi::BufferWriter::buf` must point to a valid, properly initialized `Buffer`.
/// * `ffi::BufferWriter::pos` must be lower than or equal to the offset of the `Buffer`.
/// * The `BufferWriter` does not own the `Buffer`, only borrows it mutably.
/// * If the buffer is grown (implicitly) as part of a write operation, all previously obtained
///   pointers into the buffer are invalidated.
///
/// # Usage
///
/// It's best to use this through the `std::io::Write` implementation, which handles
/// buffer growth automatically.
#[repr(transparent)]
#[derive(Debug, Clone, Copy)]
pub struct BufferWriter(ffi::BufferWriter);

impl BufferWriter {
    /// Create a new `BufferWriter` for the given buffer.
    ///
    /// # Safety
    ///
    /// `buffer` must not be written to or invalidated throughout the
    /// lifetime of the `BufferWriter` returned by this call.
    pub unsafe fn for_buffer(buffer: &mut Buffer) -> Self {
        // Safety: We assume `buf` is a valid pointer to a properly initialized `Buffer`.
        let current_length = buffer.0.offset;
        Self(ffi::BufferWriter {
            // Safety: We assume `buf` is a valid pointer to a properly initialized `Buffer`.
            buf: (&mut buffer.0) as *mut _,
            pos: current_length,
        })
    }

    /// Get a reference to the underlying buffer.
    ///
    /// # Safety
    ///
    /// We assume `buf` is a valid pointer to a properly initialized `Buffer`.
    pub unsafe fn buffer(&self) -> &Buffer {
        // Safety: We assume `buf` is a valid pointer to a properly initialized `Buffer`.
        unsafe { &*(self.0.buf as *const Buffer) }
    }

    /// Get a mutable reference to the underlying buffer.
    ///
    /// # Safety
    ///
    /// We assume `buf` is a valid pointer to a properly initialized `Buffer`.
    pub unsafe fn buffer_mut(&mut self) -> &mut Buffer {
        // Safety: We assume `buf` is a valid pointer to a properly initialized `Buffer`.
        unsafe { &mut *(self.0.buf as *mut Buffer) }
    }
}

impl std::io::Write for BufferWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        // Safety: We assume `buf` is a valid pointer to a properly initialized Buffer.
        let buffer = unsafe { self.buffer_mut() };

        // Check if we need to grow the buffer to accommodate the new data
        debug_assert!(
            buffer.len().checked_add(bytes.len()).is_some(),
            "Buffer overflow"
        );
        if buffer.len() + bytes.len() > buffer.capacity() {
            // Safety: `Buffer_Grow` is a C function that increases the buffer's capacity. It
            // expects a valid buffer pointer and returns the number of bytes added to capacity.
            // This number can be 0 if the buffer is already large enough but since it reallocates
            // unconditionally, we need to update our cursor in either case.
            //
            // Note that this may invalidate previously held pointers into the buffer.
            unsafe { Buffer_Grow(self.0.buf, bytes.len()) };
        }

        // Safety: We assume `buf` is a valid pointer to a properly initialized Buffer.
        let buffer = unsafe { self.buffer_mut() };
        // Now copy the bytes into the buffer
        let src = bytes.as_ptr();
        // Safety:
        // * The position is always in bounds for the underlying buffer.
        let dest = unsafe { buffer.0.data.add(self.0.pos) };

        // Safety:
        // * `src` is a valid pointer to the input slice.
        // * `dest` is a valid pointer within the buffer (upheld by our invariants).
        // * We've either verified or ensured that the buffer has enough capacity.
        // * The regions don't overlap (copy_nonoverlapping requirement).
        unsafe { copy_nonoverlapping(src, dest.cast(), bytes.len()) };

        // Safety: We assume `buf` is a valid pointer to a properly initialized Buffer.
        let buffer = unsafe { self.buffer_mut() };
        // Update the buffer length.
        // Safety: We've ensured that the buffer has enough capacity.
        unsafe { buffer.advance(bytes.len()) };

        // Unlikely that we overflow the `isize::MAX` but let's ensure in debug mode.
        debug_assert!(bytes.len() < isize::MAX as usize);
        debug_assert!(self.0.pos.saturating_add(bytes.len()) < isize::MAX as usize);
        // Update the cursor position.
        self.0.pos += bytes.len();

        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        // BufferWriter is unbuffered, so flush is a no-op
        Ok(())
    }
}

#[cfg(not(test))]
use ffi::Buffer_Grow;

#[cfg(test)]
mod mock;
#[cfg(test)]
use mock::Buffer_Grow;

#[cfg(test)]
mod tests;
