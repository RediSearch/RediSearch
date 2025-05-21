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
    ptr::{NonNull, copy_nonoverlapping},
    slice,
};

/// Redefines the `Buffer` struct from `buffer.h`
#[repr(C)]
pub struct Buffer {
    /// A pointer to the underlying data buffer. This is typically allocated by C and hence should
    /// not be freed by Rust code.
    data: NonNull<u8>,
    /// The capacity of the buffer (i-e allocated size)
    capacity: usize,
    /// The length of the buffer (i-e the total used memory from allocated memory)
    len: usize,
}

/// Redefines the `BufferReader` struct from `buffer.h`
///
/// Provides read functionality over a Buffer with position tracking.
///
/// # Safety Invariants
///
/// * `buf` must point to a valid, properly initialized `Buffer`.
/// * `pos` must not exceed the length of the Buffer.
/// * The `BufferReader` does not own the `Buffer`, only borrows it.
///
/// It's best to use this through the `std::io::Read` implementation.
#[repr(C)]
pub struct BufferReader {
    /// A pointer to a [`Buffer`]. If the `BufferReader` is coming from the C side, we must not
    /// free or move it.
    pub buf: NonNull<Buffer>,
    /// The read position in the `buf` pointer.
    pub pos: usize,
}

impl std::io::Read for BufferReader {
    fn read(&mut self, dest_buf: &mut [u8]) -> std::io::Result<usize> {
        // Safety: We assume `buf` is a valid pointer to a properly initialized Buffer.
        let buffer = unsafe { self.buf.as_mut() };

        // Check if there's enough data to read
        if self.pos + dest_buf.len() > buffer.len {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "BufferReader: not enough data",
            ));
        }

        // Safety: `self.pos` was just checked to be within the limits of the buffer's
        // initialized range, so this pointer arithmetic is safe.
        let src = unsafe { buffer.data.add(self.pos) };

        // Safety: We just verified that `src` points to valid memory within the buffer.
        let src = unsafe { src.as_ref() };
        let dest = dest_buf.as_mut_ptr();

        // Safety:
        // * `src` points to initialized memory in the buffer.
        // * `dest` is a valid pointer to the destination slice.
        // * We've verified there are at least `dest_buf.len()` bytes available in the source.
        // * The memory regions don't overlap (copy_nonoverlapping requirement).
        unsafe { copy_nonoverlapping(src, dest, dest_buf.len()) };

        // Update position after successful read
        self.pos += dest_buf.len();

        Ok(dest_buf.len())
    }
}

impl Buffer {
    /// Creates a new `Buffer` with the given pointer, length, and capacity.
    ///
    /// # Safety
    ///
    /// This function is safe to call, but creates a Buffer that maintains unsafe invariants:
    /// * `data` must be a valid pointer to a memory region of at least `capacity` bytes.
    /// * The memory region must remain valid for the lifetime of the Buffer.
    /// * The first `len` bytes of the memory region must be initialized.
    ///
    /// # Panics
    ///
    /// Panics if `len` is greater than `capacity`.
    pub fn new(data: NonNull<u8>, len: usize, capacity: usize) -> Self {
        assert!(len <= capacity, "len must not exceed capacity");
        Self {
            data,
            len,
            capacity,
        }
    }

    /// Returns the initialized portion of the buffer as a slice.
    ///
    /// # Safety
    ///
    /// This method assumes the safety invariants of `Buffer` are upheld:
    /// * `data` points to a valid memory region of at least `capacity` bytes.
    /// * The first `len` bytes of that memory are initialized.
    ///
    /// If these invariants are violated, this method may return an invalid slice,
    /// leading to undefined behavior.
    pub fn as_slice(&self) -> &[u8] {
        // Safety: We assume `self.data` is a valid pointer as per `Buffer`'s invariants.
        let data = unsafe { self.data.as_ref() };

        // Safety: We assume `self.len` is a valid length within the allocated memory.
        // Creates a slice referencing the initialized portion of the buffer.
        unsafe { slice::from_raw_parts(data, self.len) }
    }

    /// Returns the initialized portion of the buffer as a mutable slice.
    ///
    /// # Safety
    ///
    /// This method assumes the safety invariants of Buffer are upheld:
    /// * `data` points to a valid memory region of at least `capacity` bytes.
    /// * The first `len` bytes of that memory are initialized.
    /// * The caller has exclusive access to the buffer (no aliasing).
    ///
    /// If these invariants are violated, this method may return an invalid mutable slice,
    /// leading to undefined behavior.
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        // Safety: We assume `self.data` is a valid pointer as per `Buffer`'s invariants
        // and that we have exclusive access to the buffer.
        let data = unsafe { self.data.as_mut() };

        // Safety: We assume `self.len` is a valid length within the allocated memory.
        unsafe { slice::from_raw_parts_mut(data, self.len) }
    }

    /// Returns the length of the buffer (number of initialized bytes).
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns true if the buffer is empty (length is zero).
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns the total capacity of the buffer (maximum number of bytes it can hold).
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Returns the remaining capacity of the buffer (capacity - length).
    pub fn remaining_capacity(&self) -> usize {
        self.capacity - self.len
    }

    /// Advance the buffer by `n` bytes.
    ///
    /// This increases the buffer's length by `n` bytes, effectively marking
    /// more of the buffer as "initialized" without actually writing any data.
    /// Typically used after directly writing to the buffer's memory.
    ///
    /// # Panics
    ///
    /// Panics if `n` exceeds the remaining capacity of the buffer.
    pub fn advance(&mut self, n: usize) {
        assert!(n <= self.remaining_capacity());
        self.len += n;
    }
}

/// Redefines the `BufferWriter` struct from `buffer.h`
///
/// A writer for appending data to a `Buffer`. Provides a mechanism to write bytes to a `Buffer`
/// while tracking the current write position with a cursor.
///
/// # Safety Invariants
///
/// * `buf` must point to a valid, properly initialized `Buffer`.
/// * `cursor` must point to a valid position within the Buffer's allocated memory.
/// * The `BufferWriter` does not own the `Buffer`, only borrows it.
/// * If the buffer is grown (implicitly) as part of a write operation, all previously obtained
///   pointers into the buffer are invalidated.
///
/// # Usage
///
/// It's best to use this through the `std::io::Write` implementation, which handles
/// buffer growth and cursor position management automatically.
///
/// ```no_run
/// # use std::io::Write;
/// # use buffer::{Buffer, BufferWriter};
/// # use std::ptr::NonNull;
/// let buffer_ptr: NonNull<Buffer> = todo!();
/// let mut writer = unsafe { BufferWriter::for_buffer(buffer_ptr) };
///
/// // Write data to the buffer
/// writer.write(b"Hello, world!").unwrap();
/// ```
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct BufferWriter {
    pub buf: NonNull<Buffer>,
    pub cursor: NonNull<u8>,
}

impl BufferWriter {
    /// Create a new `BufferWriter` for the given buffer.
    ///
    /// # Safety
    ///
    /// We assume `buf` is a valid pointer to a properly initialized `Buffer`.
    pub unsafe fn for_buffer(buffer: NonNull<Buffer>) -> Self {
        Self {
            buf: buffer,
            // Safety: We assume `buf` is a valid pointer to a properly initialized `Buffer`.
            cursor: unsafe { buffer.as_ref() }.data,
        }
    }
}

impl std::io::Write for BufferWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        // Safety: We assume `buf` is a valid pointer to a properly initialized Buffer.
        let buffer = unsafe { self.buf.as_mut() };

        // Check if we need to grow the buffer to accommodate the new data
        if buffer.len + bytes.len() > buffer.capacity {
            // Safety: `Buffer_Grow` is a C function that increases the buffer's capacity. It
            // expects a valid buffer pointer and returns the number of bytes added to capacity. If
            // it returns non-zero, the buffer was successfully grown.
            //
            // Note that this may invalidate previously held pointers into the buffer.
            if unsafe { Buffer_Grow(self.buf, bytes.len()) } != 0 {
                // The buffer has been grown and potentially relocated, so we need to update our cursor.
                // Safety: After growth, `buffer.len()` points just past the end of the valid data,
                // which is a valid position to write to.
                self.cursor = unsafe { buffer.data.add(buffer.len) };
            }
        }

        // Now copy the bytes into the buffer
        let src = bytes.as_ptr();
        let dest = self.cursor.as_ptr();

        // Safety:
        // * `src` is a valid pointer to the input slice.
        // * `dest` is a valid pointer within the buffer (upheld by our invariants).
        // * We've either verified or ensured that the buffer has enough capacity.
        // * The regions don't overlap (copy_nonoverlapping requirement).
        unsafe { copy_nonoverlapping(src, dest, bytes.len()) };

        // Update the buffer length.
        buffer.len += bytes.len();

        // Update the cursor position.
        // Safety: The cursor is moved forward by the number of bytes written, which is still within
        // the valid buffer memory.
        self.cursor = unsafe { self.cursor.add(bytes.len()) };

        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        // BufferWriter is unbuffered, so flush is a no-op
        Ok(())
    }
}

#[cfg(not(test))]
unsafe extern "C" {
    /// Ensure that at least extraLen new bytes can be added to the buffer.
    ///
    /// This function grows the buffer's capacity if needed to accommodate at least `extraLen`
    /// additional bytes beyond the current length. If the buffer already has enough capacity, no
    /// action is taken.
    ///
    /// # Safety
    ///
    /// This function must be called with a valid buffer pointer. It may reallocate the buffer's
    /// memory, which invalidates any existing pointers into the buffer. After calling this
    /// function, any previously obtained pointers or references to the buffer's data may be
    /// invalid.
    ///
    /// The caller must ensure:
    /// * `b` points to a valid `Buffer` struct.
    /// * The `Buffer`'s data pointer is valid and was allocated by the C side.
    /// * After calling this function, update any pointers that may have been invalidated.
    ///
    /// # Returns
    ///
    /// Returns the number of bytes added to the capacity, or 0 if the buffer
    /// already had enough capacity.
    fn Buffer_Grow(b: NonNull<Buffer>, extraLen: usize) -> usize;
}

#[cfg(test)]
mod mock;
#[cfg(test)]
use mock::Buffer_Grow;

#[cfg(test)]
mod tests;
