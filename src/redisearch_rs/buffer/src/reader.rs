/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::Buffer;

/// A cursor to read from a [`Buffer`].
///
/// It's best to use this through the `std::io::Read` implementation.
///
/// # Invariants
///
/// 1. Position is smaller than or equal to the length of the buffer we're reading from.
/// 2. `BufferReader` has the same memory layout as [`ffi::BufferReader`].
#[repr(C)]
pub struct BufferReader<'a> {
    buffer: &'a Buffer,
    position: usize,
}

impl<'a> BufferReader<'a> {
    /// Create a new cursor, reading from the beginning of the buffer.
    pub const fn new(buffer: &'a Buffer) -> Self {
        Self {
            buffer,
            position: 0,
        }
    }

    /// Create a new cursor, reading from the given position inside the buffer.
    ///
    /// # Panic
    ///
    /// Panics if `position` is out of bounds—i.e. if it's greater than the
    /// buffer offset.
    pub fn new_at(buffer: &'a Buffer, position: usize) -> Self {
        if position > buffer.0.offset {
            panic!(
                "Position out of bounds: {} is greater than the buffer offset {}",
                position, buffer.0.offset
            );
        }
        Self { buffer, position }
    }

    /// The current position of the reader.
    pub const fn position(&self) -> usize {
        self.position
    }

    /// A reference to the buffer we're reading from.
    pub const fn buffer(&self) -> &Buffer {
        self.buffer
    }

    /// Cast to a raw pointer on [`ffi::BufferReader`].
    pub const fn as_mut_ptr(&mut self) -> *mut ffi::BufferReader {
        // Safety: `BufferReader` has the same memory layout as [`ffi::BufferReader`]
        // so we can safely cast one into the other.
        self as *const _ as *mut _
    }
}

impl<'a> std::io::Read for BufferReader<'a> {
    fn read(&mut self, mut dest_buf: &mut [u8]) -> std::io::Result<usize> {
        use std::io::Write as _;

        debug_assert!(self.position <= self.buffer.0.offset);
        // No out-of-bounds risk, thanks to `BufferReader`'s invariant 1.
        let n_bytes = dest_buf.write(&self.buffer.as_slice()[self.position..])?;
        self.position += n_bytes;

        Ok(n_bytes)
    }
}

// Check, at compile-time, that `BufferReader` and `ffi::BufferReader` have the same representation.
// This check will alert us if the C or the Rust definition changed without a corresponding patch
// to the representation in the other language.
// In that scenario, you should make sure to align both on the same memory layout.
const _: () = {
    use std::mem::offset_of;

    // Size and alignment check
    const SIZE_MATCHES: bool = size_of::<BufferReader>() == size_of::<ffi::BufferReader>();
    const ALIGN_MATCHES: bool = align_of::<BufferReader>() == align_of::<ffi::BufferReader>();

    // Field offset checks
    const BUFFER_OFFSET_MATCHES: bool =
        offset_of!(BufferReader<'static>, buffer) == offset_of!(ffi::BufferReader, buf);
    const POSITION_OFFSET_MATCHES: bool =
        offset_of!(BufferReader<'static>, position) == offset_of!(ffi::BufferReader, pos);

    // Conditional compilation failure on mismatch
    if !SIZE_MATCHES {
        panic!("Size mismatch between BufferReader and ffi::BufferReader");
    }
    if !ALIGN_MATCHES {
        panic!("Alignment mismatch between BufferReader and ffi::BufferReader");
    }
    if !BUFFER_OFFSET_MATCHES {
        panic!("Field 'buffer' does not match offset of 'buf'");
    }
    if !POSITION_OFFSET_MATCHES {
        panic!("Field 'position' does not match offset of 'pos'");
    }
};
