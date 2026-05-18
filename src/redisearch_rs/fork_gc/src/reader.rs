/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::io::{self, Read};

/// Reader over a Fork GC pipe endpoint.
///
/// Constructed via [`ForkGC::reader`](crate::ForkGC::reader) in
/// production, or directly via [`from_reader`](Self::from_reader) in
/// tests. Exposes [`recv_fixed`](Self::recv_fixed) as an inherent method;
/// callers go through that rather than through [`Read`] directly.
///
/// Any borrow-lifetime relationship with the owning `ForkGC` is encoded
/// in the inner Reader `R`, not in `Reader` itself.
pub struct Reader<R: Read> {
    reader: R,
}

impl<R: Read> Reader<R> {
    /// Wrap any [`Read`] impl as a `Reader`.
    pub const fn from_reader(reader: R) -> Self {
        Self { reader }
    }

    /// Read exactly `buf.len()` bytes from the reader into `buf`.
    pub fn recv_fixed(&mut self, buf: &mut [u8]) -> io::Result<()> {
        self.reader.read_exact(buf)
    }
}
