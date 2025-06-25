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
    pub fn new(buffer: &'a mut Buffer) -> Self {
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
    pub fn position(&self) -> usize {
        self.position
    }

    /// A reference to the buffer we're writing into.
    pub fn buffer(&mut self) -> &mut Buffer {
        self.buffer
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

impl<'a> std::io::Seek for BufferWriter<'a> {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        let new_position = match pos {
            std::io::SeekFrom::Start(offset) => {
                // Check for overflow when converting to usize
                if offset > usize::MAX as u64 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "Seek offset too large",
                    ));
                }
                offset as usize
            }
            std::io::SeekFrom::End(offset) => {
                let buffer_len = self.buffer.len();

                // Handle both positive and negative offsets from end
                if offset >= 0 {
                    buffer_len.saturating_add(offset as usize)
                } else {
                    match buffer_len.checked_sub(offset.unsigned_abs() as usize) {
                        Some(pos) => pos,
                        None => {
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::InvalidInput,
                                "Seek position would underflow",
                            ));
                        }
                    }
                }
            }
            std::io::SeekFrom::Current(offset) => {
                let current_pos = self.position;
                if offset >= 0 {
                    current_pos.saturating_add(offset as usize)
                } else {
                    match current_pos.checked_sub(offset.unsigned_abs() as usize) {
                        Some(pos) => pos,
                        None => {
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::InvalidInput,
                                "Seek position would underflow",
                            ));
                        }
                    }
                }
            }
        };

        let buffer_cap = self.buffer.capacity();

        // Grow buffer if seeking beyond current size
        if new_position > buffer_cap {
            let additional_bytes = new_position - buffer_cap;

            // Fill extra space with zeros.
            let bytes_to_fill = vec![0u8; additional_bytes];
            std::io::Write::write_all(self, &bytes_to_fill)?;
        }

        // Update the cursor position.
        self.position = new_position;

        Ok(self.position as u64)
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
