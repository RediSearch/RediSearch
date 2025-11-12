/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use super::VarintEncode;

#[derive(Debug)]
/// A structure to encode multiple integers into a single byte buffer,
/// trying to minimize the size of the encoded data.
///
/// # Delta Encoding
///
/// Rather than encoding each integer individually, we rely on **delta encoding**.
/// We encode the difference between the current value and the previous value.
/// This approach can significantly reduce the size of the encoded data,
/// under the assumption that values are of a similar magnitude.
///
/// The delta is encoded using **variable-length integer encoding** (VarInt).
pub struct VectorWriter {
    buffer: Vec<u8>,
    /// Track the number of encoded values.
    n_members: usize,
    /// The last encoded value, used to calculate the delta of the next value.
    last_value: u32,
}

impl VectorWriter {
    /// Create a new `VectorWriter` with the given capacity.
    pub fn new(cap: usize) -> Self {
        Self {
            buffer: Vec::with_capacity(cap),
            last_value: 0,
            n_members: 0,
        }
    }

    /// Write an integer into the vector.
    ///
    /// # Return Value
    ///
    /// The number of bytes written to the vector.
    pub fn write(&mut self, value: u32) -> std::io::Result<usize> {
        // If the value we're trying to encode is smaller than the last value,
        // we wrap around rather than underflowing.
        let diff = value.wrapping_sub(self.last_value);
        let size = diff.write_as_varint(&mut self.buffer)?;
        self.n_members += 1;
        self.last_value = value;

        Ok(size)
    }

    /// Get a reference to the internal byte buffer.
    #[inline(always)]
    pub fn bytes(&self) -> &[u8] {
        &self.buffer
    }

    /// The capacity of the internal byte buffer.
    pub const fn capacity(&self) -> usize {
        self.buffer.capacity()
    }

    /// Get a mutable reference to the internal byte buffer.
    #[inline(always)]
    pub const fn bytes_mut(&mut self) -> &mut Vec<u8> {
        &mut self.buffer
    }

    /// Reset the vector writer.
    ///
    /// All encoded values are dropped, but the buffer capacity is preserved.
    pub fn reset(&mut self) {
        self.buffer.clear();
        self.last_value = 0;
        self.n_members = 0;
    }

    /// The number of bytes written to the vector.
    #[inline(always)]
    pub const fn bytes_len(&self) -> usize {
        self.buffer.len()
    }

    /// The number of members written to the vector.
    #[inline(always)]
    pub const fn count(&self) -> usize {
        self.n_members
    }

    /// Resize the vector, dropping any excess capacity.
    ///
    /// # Return value
    ///
    /// The new capacity of the vector.
    pub fn shrink_to_fit(&mut self) -> usize {
        self.buffer.shrink_to_fit();
        self.buffer.capacity()
    }
}
