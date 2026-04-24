/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Pipe I/O primitives used by the Fork GC child/parent protocol.

use crate::util::exit_on_write_error;
use std::io::{self, Write};

/// Writer over a Fork GC pipe endpoint.
///
/// Constructed via [`ForkGC::pipe_write`](crate::ForkGC::pipe_write) in
/// production, or directly via [`from_writer`](Self::from_writer) in
/// tests. Exposes the Fork GC protocol primitives
/// ([`send_fixed`](Self::send_fixed),
/// [`send_buffer`](Self::send_buffer),
/// [`send_terminator`](Self::send_terminator)) as inherent methods;
/// callers go through those rather than through [`Write`] directly.
///
/// Any borrow-lifetime relationship with the owning `ForkGC` is encoded
/// in the inner writer `W`, not in `PipeWriter` itself.
pub struct Writer<W: Write> {
    writer: W,
}

impl<W: Write> Writer<W> {
    /// Wrap any [`Write`] impl as a `PipeWriter`.
    ///
    /// Kept `pub(crate)` so external callers must go through
    /// [`ForkGC::pipe_write`](crate::ForkGC::pipe_write); tests inside
    /// the crate use this directly with a `Vec<u8>` or `&mut [u8]` sink.
    pub fn from_writer(writer: W) -> Self {
        Self { writer }
    }

    /// Write all bytes of `buf` to the pipe.
    ///
    /// Equivalent to [`Write::write_all`]. Returns any [`io::Error`]
    /// surfaced by the underlying fd; use
    /// [`send_fixed_or_exit`](Self::send_fixed_or_exit) inside the
    /// forked child where a broken pipe is unrecoverable.
    pub fn send_fixed(&mut self, buf: &[u8]) -> io::Result<()> {
        self.writer.write_all(buf)
    }

    /// Write a length-prefixed buffer frame.
    ///
    /// Wire format: the native-endian byte representation of `buf.len()`
    /// (a `size_t`) followed by `buf.len()` payload bytes. Parent and
    /// child share the same process image post-fork, so native-endian
    /// encoding is sound.
    pub fn send_buffer(&mut self, buf: &[u8]) -> io::Result<()> {
        self.send_fixed(&buf.len().to_ne_bytes())?;
        self.send_fixed(buf)
    }

    /// Write the end-of-stream sentinel.
    ///
    /// Emits a single `size_t`-sized value of `usize::MAX`, signalling to
    /// the parent reader that no more buffers will follow.
    pub fn send_terminator(&mut self) -> io::Result<()> {
        self.send_fixed(&usize::MAX.to_ne_bytes())
    }

    /// Like [`send_fixed`](Self::send_fixed), but terminate the child
    /// process on failure via `RedisModule_ExitFromChild`.
    pub fn send_fixed_or_exit(&mut self, buf: &[u8]) {
        if let Err(err) = self.send_fixed(buf) {
            exit_on_write_error(err);
        }
    }

    /// Like [`send_buffer`](Self::send_buffer), but terminate the child
    /// process on failure.
    pub fn send_buffer_or_exit(&mut self, buf: &[u8]) {
        if let Err(err) = self.send_buffer(buf) {
            exit_on_write_error(err);
        }
    }

    /// Like [`send_terminator`](Self::send_terminator), but terminate the
    /// child process on failure.
    pub fn send_terminator_or_exit(&mut self) {
        if let Err(err) = self.send_terminator() {
            exit_on_write_error(err);
        }
    }
}
