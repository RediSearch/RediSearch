/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::io::{self, Read};

use crate::frame::{EMPTY, Frame, FrameData, TERMINATOR};

/// Reader over a Fork GC pipe endpoint.
///
/// Constructed via [`ForkGC::reader`](crate::ForkGC::reader) in
/// production, or directly via [`from_reader`](Self::from_reader) in
/// tests. Implements [`Read`] by delegating to the inner `R`, and exposes
/// the higher-level protocol methods
/// [`read_frame`](Self::read_frame) and
/// [`read_frame_and_id`](Self::read_frame_and_id).
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

    /// Read a length-prefixed buffer frame.
    ///
    /// Counterpart of [`Writer::write_frame`](crate::writer::Writer::write_frame).
    /// Reads a native-endian `size_t` prefix, then:
    ///
    /// - `usize::MAX` → [`Frame::Terminator`] (end-of-stream sentinel; no payload follows).
    /// - `0` → [`Frame::Empty`] (no payload).
    /// - otherwise → [`Frame::Data`] containing exactly that many payload bytes.
    pub fn read_frame(&mut self) -> io::Result<Frame<Box<[u8]>>> {
        let mut len_bytes = [0u8; size_of::<usize>()];
        self.read_exact(&mut len_bytes)?;
        let len = usize::from_ne_bytes(len_bytes);

        if len == TERMINATOR {
            return Ok(Frame::Terminator);
        }
        if len == EMPTY {
            return Ok(Frame::Empty);
        }

        let mut data = vec![0u8; len].into_boxed_slice();
        self.read_exact(&mut data)?;

        // SAFETY: len is neither TERMINATOR nor EMPTY.
        let frame_data = unsafe { FrameData::new_unchecked(data) };

        Ok(Frame::Data(frame_data))
    }
}

impl<R: Read> Read for Reader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.reader.read(buf)
    }
}
