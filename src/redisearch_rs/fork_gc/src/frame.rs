/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ops::Deref;

pub(crate) const EMPTY: usize = 0;
pub(crate) const TERMINATOR: usize = usize::MAX;

/// A frame in the Fork GC buffer protocol.
///
/// The three variants correspond to the three possible length prefixes on the
/// wire: `usize::MAX` (end of stream), `0` (empty), or a positive payload
/// length.
///
/// `D` is the data container for the [`Frame::Data`] variant. Use `Box<[u8]>`
/// on the read path (as returned by
/// [`Reader::read_frame`](crate::reader::Reader::read_frame)) or `&[u8]` on
/// the write path (passed to
/// [`Writer::write_frame`](crate::writer::Writer::write_frame)) to avoid
/// copying.
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

impl<D: AsRef<[u8]>> Frame<D> {
    /// Construct a `Frame` from `data`.
    ///
    /// - `data.as_ref().is_empty()` → [`Frame::Empty`]
    /// - otherwise → [`Frame::Data`]
    ///
    /// # Panics
    ///
    /// Panics if `data.as_ref().len() == usize::MAX`, as that value is
    /// reserved for the [`Frame::Terminator`] wire encoding.
    pub fn data(data: D) -> Self {
        match data.as_ref().len() {
            EMPTY => Self::Empty,
            TERMINATOR => {
                panic!("data length usize::MAX is reserved for the Frame::Terminator wire encoding")
            }
            _ => Self::Data(FrameData(data)),
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
    /// Wrap `data` without checking the length invariant.
    ///
    /// # Safety
    ///
    /// The caller must ensure the underlying slice length is neither `0`
    /// (reserved for [`Frame::Empty`]) nor `usize::MAX` (reserved for
    /// [`Frame::Terminator`]).
    pub(crate) const unsafe fn new_unchecked(data: D) -> Self {
        Self(data)
    }

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
