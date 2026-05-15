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

use crate::{EMPTY, Frame, TERMINATOR};

/// Writer over a Fork GC pipe endpoint.
///
/// Constructed via [`ForkGC::writer`](crate::ForkGC::writer) in
/// production, or directly via [`from_writer`](Self::from_writer) in
/// tests. Implements [`Write`] by delegating to the inner `W`, and exposes
/// the higher-level protocol primitive
/// [`write_frame`](Self::write_frame).
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

    /// Write a frame to the pipe.
    ///
    /// The payload slice is borrowed from the caller — no allocation or copy
    /// is made. Counterpart of [`Reader::read_frame`](crate::reader::Reader::read_frame).
    pub fn write_frame(&mut self, frame: Frame<&[u8]>) -> io::Result<()> {
        match frame {
            Frame::Terminator => self.write_all(&TERMINATOR.to_ne_bytes()),
            Frame::Empty => self.write_all(&EMPTY.to_ne_bytes()),
            Frame::Data(data) => {
                self.write_all(&data.len().to_ne_bytes())?;
                self.write_all(data)
            }
        }
    }
}

impl<W: Write> Write for Writer<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.writer.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}
