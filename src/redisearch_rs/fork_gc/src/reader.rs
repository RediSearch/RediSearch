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

    /// Read a length-prefixed buffer frame.
    ///
    /// Counterpart of [`Writer::send_buffer`](crate::writer::Writer::send_buffer) and
    /// [`Writer::send_terminator`](crate::writer::Writer::send_terminator). Reads a native-endian `size_t`
    /// prefix, then:
    ///
    /// - `usize::MAX` → [`RecvFrame::Terminator`] (end-of-stream
    ///   sentinel; no payload follows).
    /// - `0` → [`RecvFrame::Empty`] (no payload).
    /// - otherwise → [`RecvFrame::Data`] containing exactly that many
    ///   payload bytes.
    pub fn recv_buffer(&mut self) -> io::Result<RecvFrame> {
        let mut len_bytes = [0u8; size_of::<usize>()];
        self.recv_fixed(&mut len_bytes)?;
        let len = usize::from_ne_bytes(len_bytes);

        if len == usize::MAX {
            return Ok(RecvFrame::Terminator);
        }
        if len == 0 {
            return Ok(RecvFrame::Empty);
        }

        let mut data = vec![0u8; len].into_boxed_slice();
        self.recv_fixed(&mut data)?;
        Ok(RecvFrame::Data(data))
    }

    /// Read a length-prefixed buffer frame followed by a native-endian `u64` field id.
    ///
    /// Extends [`recv_buffer`](Self::recv_buffer) with a trailing id read.
    /// On [`RecvFrame::Terminator`] the id is not present on the wire; `0` is
    /// returned as a placeholder and the caller should discard it.
    pub fn recv_buffer_and_id(&mut self) -> io::Result<(RecvFrame, u64)> {
        let frame = self.recv_buffer()?;

        if matches!(frame, RecvFrame::Terminator) {
            return Ok((frame, 0));
        }

        let mut id_bytes = [0u8; size_of::<u64>()];
        self.recv_fixed(&mut id_bytes)?;
        let id = u64::from_ne_bytes(id_bytes);

        Ok((frame, id))
    }
}

/// A frame decoded by [`Reader::recv_buffer`].
///
/// The three variants correspond to the three possible length prefixes
/// of the Fork GC buffer protocol: `usize::MAX` (end of stream), `0`
/// (empty), or a positive payload length.
#[derive(Debug)]
pub enum RecvFrame {
    /// End-of-stream sentinel — the writer called
    /// [`Writer::send_terminator`](crate::writer::Writer::send_terminator).
    Terminator,
    /// Zero-length frame — the writer called
    /// [`Writer::send_buffer`](crate::writer::Writer::send_buffer) with an empty slice.
    Empty,
    /// A frame carrying `data.len()` payload bytes.
    Data(Box<[u8]>),
}
