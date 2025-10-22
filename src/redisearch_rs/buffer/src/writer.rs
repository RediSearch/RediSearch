/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::Buffer;

/// A cursor to write data into a `Buffer`.
///
/// # Usage
///
/// It's best to use this through the `std::io::Write` implementation, which handles
/// buffer growth and cursor position management automatically.
///
/// # Invariants
///
/// 1. Position is smaller than or equal to the length of the buffer we're writing into.
/// 2. `BufferWriter` has the same memory layout as [`ffi::BufferWriter`].
#[repr(C)]
pub struct BufferWriter<'a> {
    buffer: &'a mut Buffer,
    position: usize,
}

impl<'a> BufferWriter<'a> {
    /// Create a new writer for the given buffer.
    ///
    /// The writer will append new data at the end of the buffer,
    /// growing its capacity if necessary.
    pub const fn new(buffer: &'a mut Buffer) -> Self {
        let position = buffer.len();
        Self { buffer, position }
    }

    /// Create a new writer for the given buffer.
    ///
    /// The writer will write new data starting from the specified
    /// position.
    /// If the position is strictly smaller than the offset of the buffer,
    /// it will overwrite existing data.
    ///
    /// # Panic
    ///
    /// Panics if the specified position is greater than the offset
    /// of the buffer.
    pub fn new_at(buffer: &'a mut Buffer, position: usize) -> Self {
        if position > buffer.0.offset {
            panic!(
                "Position out of bounds: {} is greater than the buffer offset {}",
                position, buffer.0.offset
            );
        }
        Self { buffer, position }
    }

    /// The current position of the writer.
    pub const fn position(&self) -> usize {
        self.position
    }

    /// A reference to the buffer we're writing into.
    pub const fn buffer(&mut self) -> &mut Buffer {
        self.buffer
    }

    /// Cast to a raw pointer on [`ffi::BufferWriter`].
    pub const fn as_mut_ptr(&mut self) -> *mut ffi::BufferWriter {
        // Safety: `BufferWriter` has the same memory layout as [`ffi::BufferWriter`]
        // so we can safely cast one into the other.
        self as *const _ as *mut _
    }
}

impl<'a> std::io::Write for BufferWriter<'a> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        // Ensure we have enough capacity to write the new elements.
        self.buffer.reserve(bytes.len());

        let n_written_bytes = bytes.len();
        // Safety:
        // - The offsetted pointer is in bounds thanks to `BufferWriter`'s invariant 1.
        let unint_buffer_ptr = unsafe { self.buffer.0.data.add(self.buffer.len()) };
        // Safety:
        // - The two slices don't overlap.
        // - We have reserved enough capacity above to ensure that we won't write beyond
        //   the end of the destination buffer.
        // - We have exclusive access to the destination buffer.
        unsafe {
            std::ptr::copy_nonoverlapping(bytes.as_ptr(), unint_buffer_ptr.cast(), n_written_bytes)
        };

        // Update the buffer length.
        // Safety: We've initialized all elements with the write above.
        unsafe { self.buffer.advance(n_written_bytes) };

        // Unlikely that we overflow the `isize::MAX` but let's ensure in debug mode.
        debug_assert!(n_written_bytes < isize::MAX as usize);
        debug_assert!(self.position.saturating_add(n_written_bytes) < isize::MAX as usize);
        // Update the cursor position.
        self.position += n_written_bytes;

        Ok(n_written_bytes)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        // BufferWriter is unbuffered, so flush is a no-op
        Ok(())
    }
}

// Check, at compile-time, that `BufferWriter` and `ffi::BufferWriter` have the same representation.
// This check will alert us if the C or the Rust definition changed without a corresponding patch
// to the representation in the other language.
// In that scenario, you should make sure to align both on the same memory layout.
const _: () = {
    use std::mem::offset_of;

    // Size and alignment check
    const SIZE_MATCHES: bool = size_of::<BufferWriter>() == size_of::<ffi::BufferWriter>();
    const ALIGN_MATCHES: bool = align_of::<BufferWriter>() == align_of::<ffi::BufferWriter>();

    // Field offset checks
    const BUFFER_OFFSET_MATCHES: bool =
        offset_of!(BufferWriter<'static>, buffer) == offset_of!(ffi::BufferWriter, buf);
    const POSITION_OFFSET_MATCHES: bool =
        offset_of!(BufferWriter<'static>, position) == offset_of!(ffi::BufferWriter, pos);

    // Conditional compilation failure on mismatch for BufferWriter
    if !SIZE_MATCHES {
        panic!("Size mismatch between BufferWriter and ffi::BufferWriter");
    }
    if !ALIGN_MATCHES {
        panic!("Alignment mismatch between BufferWriter and ffi::BufferWriter");
    }
    if !BUFFER_OFFSET_MATCHES {
        panic!("Field 'buffer' does not match offset of 'buf' in BufferWriter");
    }
    if !POSITION_OFFSET_MATCHES {
        panic!("Field 'position' does not match offset of 'pos' in BufferWriter");
    }
};
