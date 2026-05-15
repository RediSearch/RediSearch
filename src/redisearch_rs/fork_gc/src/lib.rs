/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Safe Rust implementation of RediSearch's Fork GC.
//!
//! This crate hosts the protocol- and algorithm-level logic that runs in
//! the forked child and the parent process. Redis-facing concerns (the
//! `FGC_*` C ABI, `RedisModule_*` API calls, process-exit handling) live
//! in the `fork_gc_ffi` crate, which is a thin trampoline on top of this
//! one.

pub mod fork_gc;
pub mod io_result_ext;
pub mod reader;
pub mod util;
pub mod writer;

pub use fork_gc::ForkGC;

const EMPTY: usize = 0;
const TERMINATOR: usize = usize::MAX;

/// A frame in the Fork GC buffer protocol.
///
/// The three variants correspond to the three possible length prefixes on the
/// wire: `Terminator`, `Empty`, or a positive payload length.
///
/// `D` is the data container for the [`Frame::Data`] variant. Use `Box<[u8]>`
/// on the read path (as returned by
/// [`Reader::read_frame`](crate::reader::Reader::read_frame)) or `&[u8]` on
/// the write path (passed to
/// [`Writer::write_frame`](crate::writer::Writer::write_frame)) to avoid
/// copying.
#[derive(Debug)]
pub enum Frame<D> {
    /// End-of-stream sentinel; no payload follows.
    ///
    /// On the wire: a single native-endian `usize::MAX` prefix.
    Terminator,
    /// Zero-length frame; no payload follows.
    ///
    /// On the wire: a single native-endian `0` prefix.
    Empty,
    /// A frame carrying `data.len()` payload bytes.
    ///
    /// On the wire: a native-endian length prefix followed by the payload.
    Data(D),
}
