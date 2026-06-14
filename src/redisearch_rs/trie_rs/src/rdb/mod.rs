/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! RDB serialization for trie maps.
//!
//! Mirrors the wire format produced by the C functions `TrieType_GenericSave`
//! and `TrieType_GenericLoad`. This parent module owns the shared substrate —
//! the [`TrieEntry`] value type, the [`RdbWrite`] / [`RdbRead`] IO traits,
//! [`RdbOpts`], [`RdbError`], and the NUL-framing helpers. The two concrete
//! serializers live alongside it:
//!
//! - [`byte`] — for the byte-keyed [`crate::TrieMap<TrieEntry>`].
//! - [`mod@str`] — for the UTF-8-keyed [`crate::str_trie_map::StrTrieMap<TrieEntry>`],
//!   a thin wrapper that delegates to [`byte`] and is byte-identical on the wire.
//!
//! # Wire format
//!
//! ```text
//! u64  count                            // map.n_unique_keys()
//! [ bytes(key + '\0')
//!   f64  score
//!   bytes(payload + '\0')               // only if RdbOpts::payloads
//!   u64  num_docs                       // only if RdbOpts::num_docs
//! ] * count
//! ```
//!
//! The diagram lists the framed primitives passed to [`RdbWrite`]; the actual
//! on-wire bytes include length prefixes added by `RedisModule_Save*`, which
//! are opaque to this layer.
//!
//! # Trailing-NUL framing
//!
//! Both keys and payloads are written with a trailing NUL byte (so the saved
//! buffer is `key.len() + 1` bytes long, matching C's
//! `SaveStringBuffer(..., len + 1)`) and the loader strips one byte. Buffers
//! that do not end in NUL are rejected with [`RdbError::MissingTrailingNul`].
//! NUL framing is applied in the algorithm body via [`save_nul_terminated`]
//! and [`load_nul_terminated`]; the [`RdbWrite`] / [`RdbRead`] trait surface
//! stays neutral — it just writes and reads raw length-prefixed buffers. A
//! single scratch [`Vec<u8>`] is reused across every entry of a save, so the
//! per-key NUL padding adds zero allocations after the first.
//!
//! # Empty-payload normalization
//!
//! When `RdbOpts::payloads` is `true`, both `payload: None` and
//! `payload: Some(vec![])` emit a single-NUL buffer (`"\0"`) and load back as
//! `None`. This mirrors the C-side collapse `payload.len ? &payload : NULL`.
//!
//! # IO model
//!
//! Save is infallible at the Rust API level, matching the void-returning C
//! `RedisModule_Save*` primitives. Errors only surface on load through
//! [`RdbError`].

pub mod byte;
pub mod str;

/// Write `b` followed by one trailing NUL byte as a single length-prefixed
/// record, reusing `scratch` as the temporary contiguous buffer. The saved
/// buffer is `b.len() + 1` bytes long, matching the C wire format
/// (`SaveStringBuffer(s, len + 1)`).
///
/// `scratch` is borrowed from the caller so one allocation can amortize
/// across an entire save loop.
pub(crate) fn save_nul_terminated<W: RdbWrite>(writer: &mut W, scratch: &mut Vec<u8>, b: &[u8]) {
    scratch.clear();
    scratch.reserve(b.len() + 1);
    scratch.extend_from_slice(b);
    scratch.push(0);
    writer.save_bytes(scratch);
}

/// Read one length-prefixed buffer that is expected to end in a NUL byte
/// and return its contents with the trailing NUL stripped. Returns
/// [`RdbError::MissingTrailingNul`] when the wire buffer is empty or does
/// not end in `0x00`.
pub(crate) fn load_nul_terminated<R: RdbRead>(reader: &mut R) -> Result<Vec<u8>, RdbError> {
    let mut buf = reader.load_bytes()?;
    if buf.pop() != Some(0) {
        return Err(RdbError::MissingTrailingNul);
    }
    Ok(buf)
}

/// Controls which optional fields are present on the wire.
///
/// The same value must be used at save and load time. Mismatches misalign
/// the wire layout, so subsequent reads either fail with an [`RdbError`] or
/// silently parse the wrong bytes as the next field.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RdbOpts {
    /// Persist each entry's payload (with trailing NUL).
    pub payloads: bool,
    /// Persist each entry's `num_docs`.
    pub num_docs: bool,
}

/// One trie entry: score, optional opaque payload, and a per-entry counter.
///
/// `payload: None` and `payload: Some(vec![])` are wire-indistinguishable
/// when payloads are persisted — both round-trip as `None`. See
/// [`RdbOpts::payloads`].
#[derive(Clone, Debug, PartialEq)]
pub struct TrieEntry {
    /// Score associated with the entry. Semantics are caller-defined (e.g.
    /// suggestion weight, or a constant for index-term tries) and may be
    /// mutated by callers after the initial insert. The C trie stores this
    /// as `float`; the RDB wire format widens it to `f64` (via
    /// `RedisModule_SaveDouble`).
    pub score: f64,
    /// Optional opaque payload bytes.
    pub payload: Option<Vec<u8>>,
    /// Per-entry counter, persisted only when [`RdbOpts::num_docs`] is set.
    /// Semantics are caller-defined (e.g. document frequency for an index's
    /// term trie); this type does not enforce a meaning. Loads with
    /// `num_docs = false` materialize this as `0`.
    pub num_docs: u64,
}

/// Sink for the typed RDB save primitives.
///
/// One method per primitive type — `RedisModule_Save*` is a typed framing
/// API (length-prefixed buffers, fixed-width numbers) rather than a byte
/// stream, so [`std::io::Write`] would not be a faithful abstraction.
pub trait RdbWrite {
    /// Write a 64-bit unsigned integer.
    fn save_u64(&mut self, v: u64);
    /// Write a 64-bit IEEE-754 double.
    fn save_f64(&mut self, v: f64);
    /// Write `b` as a single length-prefixed buffer. Any NUL padding the
    /// wire format requires is applied by the caller before this is
    /// invoked (see [`save_nul_terminated`]).
    fn save_bytes(&mut self, b: &[u8]);
}

/// Source for the typed RDB load primitives.
///
/// Counterpart to [`RdbWrite`]. Every primitive may fail with [`RdbError`].
pub trait RdbRead {
    /// Read a 64-bit unsigned integer.
    fn load_u64(&mut self) -> Result<u64, RdbError>;
    /// Read a 64-bit IEEE-754 double.
    fn load_f64(&mut self) -> Result<f64, RdbError>;
    /// Read one length-prefixed buffer and return its raw bytes. The caller
    /// applies any trailing-NUL stripping the wire format requires (see
    /// [`load_nul_terminated`]).
    fn load_bytes(&mut self) -> Result<Vec<u8>, RdbError>;
}

/// Errors that can occur while reading a trie RDB payload.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RdbError {
    /// The underlying RDB read failed (EOF, corrupted stream, etc.).
    #[error("rdb io error")]
    Io,
    /// A bytes buffer expected to end with a NUL terminator did not.
    #[error("rdb bytes buffer missing trailing NUL")]
    MissingTrailingNul,
    /// A key buffer was not valid UTF-8 when loaded through the
    /// [`crate::rdb::str`] wrapper that requires UTF-8 keys.
    #[error("rdb key bytes not valid UTF-8")]
    InvalidUtf8,
}

/// In-memory [`RdbWrite`] / [`RdbRead`] mocks shared by the byte-keyed and
/// str-keyed RDB test suites. Lives here (rather than inside either test
/// module) so both [`crate::rdb::byte`]'s and [`crate::rdb::str`]'s tests
/// import the same `Op` enum — keeps the wire-shape assertions cross-checkable
/// against one canonical representation.
#[cfg(test)]
pub(crate) mod test_helpers {
    use super::*;

    /// One typed call against [`RdbWrite`] / [`RdbRead`]. The wire-shape
    /// tests assert against `Vec<Op>` traces directly.
    #[derive(Debug, Clone, PartialEq)]
    pub(crate) enum Op {
        U64(u64),
        F64(f64),
        Bytes(Vec<u8>),
    }

    /// [`RdbWrite`] impl that records every call as an [`Op`]. The trace
    /// is exposed via the tuple field so tests can both inspect it
    /// (`rec.0`) and move it into a [`Replayer`].
    #[derive(Default)]
    pub(crate) struct Recorder(pub(crate) Vec<Op>);
    impl RdbWrite for Recorder {
        fn save_u64(&mut self, v: u64) {
            self.0.push(Op::U64(v));
        }
        fn save_f64(&mut self, v: f64) {
            self.0.push(Op::F64(v));
        }
        fn save_bytes(&mut self, b: &[u8]) {
            self.0.push(Op::Bytes(b.to_vec()));
        }
    }

    /// [`RdbRead`] impl that replays a recorded [`Op`] trace. Optionally
    /// short-circuits with [`RdbError::Io`] after `n` calls to exercise
    /// mid-stream IO failure paths.
    pub(crate) struct Replayer {
        ops: std::vec::IntoIter<Op>,
        fail_after: Option<usize>,
        calls: usize,
    }

    impl Replayer {
        pub(crate) fn new(ops: Vec<Op>) -> Self {
            Self {
                ops: ops.into_iter(),
                fail_after: None,
                calls: 0,
            }
        }

        pub(crate) fn fail_after(ops: Vec<Op>, n: usize) -> Self {
            Self {
                ops: ops.into_iter(),
                fail_after: Some(n),
                calls: 0,
            }
        }

        fn step(&mut self) -> Result<Op, RdbError> {
            if let Some(n) = self.fail_after
                && self.calls >= n
            {
                return Err(RdbError::Io);
            }
            self.calls += 1;
            self.ops.next().ok_or(RdbError::Io)
        }
    }

    impl RdbRead for Replayer {
        fn load_u64(&mut self) -> Result<u64, RdbError> {
            match self.step()? {
                Op::U64(v) => Ok(v),
                op => panic!("mock: expected U64, got {op:?}"),
            }
        }
        fn load_f64(&mut self) -> Result<f64, RdbError> {
            match self.step()? {
                Op::F64(v) => Ok(v),
                op => panic!("mock: expected F64, got {op:?}"),
            }
        }
        fn load_bytes(&mut self) -> Result<Vec<u8>, RdbError> {
            match self.step()? {
                Op::Bytes(v) => Ok(v),
                op => panic!("mock: expected Bytes, got {op:?}"),
            }
        }
    }
}
