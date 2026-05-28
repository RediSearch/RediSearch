/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{
    io::{self, Read, Write},
    ops::Deref,
};

pub(crate) const EMPTY: usize = 0;
pub(crate) const TERMINATOR: usize = usize::MAX;

/// A frame in the Fork GC buffer protocol.
///
/// The three variants correspond to the three possible length prefixes on the
/// wire: `usize::MAX` (end of stream), `0` (empty), or a positive payload
/// length.
///
/// `D` is the data container for the [`Frame::Data`] variant. Use `Box<[u8]>`
/// on the read path (as returned by [`Frame::decode`]) or `&[u8]` on the write
/// path (passed to [`Frame::encode`]) to avoid copying.
#[derive(Debug, Clone)]
pub enum Frame<D> {
    /// End-of-stream sentinel; no payload follows.
    ///
    /// On the wire: a single native-endian `usize::MAX` prefix.
    Terminator,
    /// Zero-length frame; no payload follows.
    ///
    /// On the wire: a single native-endian `0` prefix. Produced by
    /// [`Frame::data`] when called with an empty slice.
    Empty,
    /// A frame carrying `data.len()` payload bytes.
    ///
    /// On the wire: a native-endian length prefix followed by the payload.
    /// Produced by [`Frame::data`] when called with a non-empty slice.
    Data(FrameData<D>),
}

impl Frame<Box<[u8]>> {
    /// Read one frame from `reader`.
    ///
    /// Reads a native-endian `usize` length prefix, then:
    ///
    /// - `usize::MAX` → [`Frame::Terminator`] (no further bytes consumed)
    /// - `0` → [`Frame::Empty`] (no further bytes consumed)
    /// - `n` → reads exactly `n` payload bytes into a [`Box<[u8]>`] and
    ///   returns [`Frame::Data`]
    ///
    /// # Errors
    ///
    /// Propagates any [`io::Error`] from `reader`, including
    /// [`io::ErrorKind::UnexpectedEof`] if the stream ends before the length
    /// prefix or payload are fully read.
    pub fn decode(reader: &mut impl Read) -> io::Result<Self> {
        let mut len_bytes = [0u8; size_of::<usize>()];
        reader.read_exact(&mut len_bytes)?;
        let len = usize::from_ne_bytes(len_bytes);

        if len == TERMINATOR {
            return Ok(Frame::Terminator);
        }
        if len == EMPTY {
            return Ok(Frame::Empty);
        }

        let mut data = vec![0u8; len].into_boxed_slice();
        reader.read_exact(&mut data)?;

        Ok(Frame::Data(FrameData(data)))
    }
}

impl<'a> Frame<&'a [u8]> {
    /// Construct a `Frame` from `data`.
    ///
    /// - `data.is_empty()` → [`Frame::Empty`]
    /// - otherwise → [`Frame::Data`]
    ///
    /// # Panics
    ///
    /// Panics if `data.len() == usize::MAX`, as that value is reserved for
    /// the [`Frame::Terminator`] wire encoding.
    pub fn data(data: &'a [u8]) -> Self {
        match data.len() {
            EMPTY => Self::Empty,
            TERMINATOR => {
                panic!("data length usize::MAX is reserved for the Frame::Terminator wire encoding")
            }
            _ => Self::Data(FrameData(data)),
        }
    }

    /// Write this frame to `writer`.
    ///
    /// Encodes the frame as a native-endian `usize` length prefix followed by
    /// the payload (if any):
    ///
    /// - [`Frame::Terminator`] → writes `usize::MAX`; no payload
    /// - [`Frame::Empty`] → writes `0`; no payload
    /// - [`Frame::Data`] → writes the payload length, then the payload bytes
    ///
    /// # Errors
    ///
    /// Propagates any [`io::Error`] from `writer`.
    pub fn encode(self, writer: &mut impl Write) -> io::Result<()> {
        match self {
            Frame::Terminator => writer.write_all(&TERMINATOR.to_ne_bytes()),
            Frame::Empty => writer.write_all(&EMPTY.to_ne_bytes()),
            Frame::Data(data) => {
                writer.write_all(&data.len().to_ne_bytes())?;
                writer.write_all(&data)
            }
        }
    }
}

/// Payload of a [`Frame::Data`] frame.
///
/// A transparent wrapper around `D` that upholds the invariant that the
/// underlying byte slice is neither empty (which encodes as [`Frame::Empty`]
/// on the wire) nor `usize::MAX` bytes long (which encodes as
/// [`Frame::Terminator`]).
///
/// Implements [`Deref<Target = [u8]>`](std::ops::Deref) when
/// `D: Deref<Target = [u8]>`, allowing slice operations directly on the
/// wrapper.
#[derive(Debug, Clone)]
pub struct FrameData<D>(D);

impl<D> FrameData<D> {
    /// Unwrap and return the inner `D`, consuming `self`.
    pub fn into_inner(self) -> D {
        self.0
    }
}

impl<D: Deref<Target = [u8]>> Deref for FrameData<D> {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.0
    }
}
