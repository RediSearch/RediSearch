/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use super::write;

#[derive(Debug)]
pub struct VectorWriter {
    buffer: Vec<u8>,
    n_members: usize,
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

    /// Write an integer to the vector.
    ///
    /// # Return Value
    ///
    /// The number of bytes written to the vector.
    pub fn write(&mut self, value: u32) -> std::io::Result<usize> {
        self.buffer.reserve_exact(16);
        let diff = value.wrapping_sub(self.last_value);
        let size = write(diff, &mut self.buffer)?;
        self.n_members += 1;
        self.last_value = value;

        Ok(size)
    }

    /// reference to the internal byte buffer.
    #[inline(always)]
    pub fn bytes(&self) -> &[u8] {
        &self.buffer
    }

    /// reference to the internal byte buffer as mutable.
    #[inline(always)]
    pub fn bytes_mut(&mut self) -> &mut Vec<u8> {
        &mut self.buffer
    }

    /// Reset the vector writer (dropping all bytes).
    pub fn reset(&mut self) {
        self.buffer.clear();
        self.last_value = 0;
        self.n_members = 0;
    }

    /// The number of bytes written to the vector.
    #[inline(always)]
    pub fn bytes_len(&self) -> usize {
        self.buffer.len()
    }

    /// The number of members written to the vector.
    #[inline(always)]
    pub fn count(&self) -> usize {
        self.n_members
    }

    /// Truncate the vector.
    pub fn shrink_to_fit(&mut self) -> usize {
        self.buffer.shrink_to_fit();

        self.buffer.capacity()
    }
}
