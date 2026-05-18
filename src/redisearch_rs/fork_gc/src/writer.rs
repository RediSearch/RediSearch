/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Pipe I/O primitives used by the Fork GC child/parent protocol.

use std::io::{self, Write};

/// Writer over a Fork GC pipe endpoint.
///
/// Constructed via [`ForkGC::writer`](crate::ForkGC::writer) in
/// production, or directly via [`from_writer`](Self::from_writer) in
/// tests. Exposes the Fork GC protocol primitives
/// ([`send_fixed`](Self::send_fixed),
/// [`send_buffer`](Self::send_buffer),
/// [`send_terminator`](Self::send_terminator)) as inherent methods;
/// callers go through those rather than through [`Write`] directly.
///
/// Any borrow-lifetime relationship with the owning `ForkGC` is encoded
/// in the inner writer `W`, not in `Writer` itself.
pub struct Writer<W: Write> {
    writer: W,
}

impl<W: Write> Writer<W> {
    /// Wrap any [`Write`] impl as a `Writer`.
    pub const fn from_writer(writer: W) -> Self {
        Self { writer }
    }

    /// Write all bytes of `buf` to the pipe.
    ///
    /// Equivalent to [`Write::write_all`].
    pub fn send_fixed(&mut self, buf: &[u8]) -> io::Result<()> {
        self.writer.write_all(buf)
    }

    /// Write a length-prefixed buffer frame.
    ///
    /// Wire format: the native-endian byte representation of `buf.len()`
    /// (a `usize`) followed by `buf.len()` payload bytes. Parent and
    /// child share the same process image post-fork, so native-endian
    /// encoding is sound.
    pub fn send_buffer(&mut self, buf: &[u8]) -> io::Result<()> {
        self.send_fixed(&buf.len().to_ne_bytes())?;
        self.send_fixed(buf)
    }

    /// Write the end-of-stream sentinel.
    ///
    /// Emits a single `usize::MAX`, signalling to
    /// the parent reader that no more buffers will follow.
    pub fn send_terminator(&mut self) -> io::Result<()> {
        self.send_fixed(&usize::MAX.to_ne_bytes())
    }
}
