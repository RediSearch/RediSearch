/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! RDB serialization for the [`trie_rs`] trie maps.
//!
//! Mirrors the wire format produced by the C functions `TrieType_GenericSave`
//! and `TrieType_GenericLoad`. This crate owns the shared substrate —
//! [`RdbOpts`], [`RdbError`], the
//! NUL-framing helpers, and the [`read_entries`] entry-stream reader — plus
//! the [`TrieEntry`] value type the wire fields are modeled on. The two
//! serializer flavors live alongside it:
//!
//! - [`byte`] — for the byte-keyed [`trie_rs::TrieMap`].
//! - [`mod@str`] — for the UTF-8-keyed [`trie_rs::str_trie_map::StrTrieMap`],
//!   a thin wrapper that delegates to [`byte`] and is byte-identical on the wire.
//!
//! Each flavor is generic over the map's payload type (`save_with` /
//! `load_with`, with a per-entry mapping to and from the wire fields) and
//! offers `save` / `load` shorthands for maps that store [`TrieEntry`]
//! itself.
//!
//! IO is abstracted behind the [`RdbIO`] trait so this crate
//! carries no Redis dependency: the C entrypoints implement it over
//! `RedisModuleIO` (in the `trie_rdb_ffi` crate), and pure-Rust callers can
//! implement it over any buffer.
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
//! The diagram lists the framed primitives passed to [`RdbIO`]; the actual
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
//! and [`load_nul_terminated`]; the [`RdbIO`] trait surface
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
pub mod entry;
pub mod str;

use std::io;

use rdb_io::RdbIO;

pub use entry::{EntryFields, TrieEntry};

/// Read the entry stream shared by both key flavors and feed each decoded
/// entry to `insert`.
///
/// Owns the count framing and the per-entry field layout (key, score, and
/// the optional payload / `num_docs` selected by `opts`) so the byte- and
/// str-keyed loaders cannot drift apart. The two flavors differ only in how
/// raw key bytes become a key: `key_from_bytes` maps the NUL-stripped buffer
/// into the caller's key type (identity for bytes, UTF-8 validation for str),
/// and `insert` places the finished `(key, entry)` into the caller's map.
pub(crate) fn read_entries<IO: RdbIO, K>(
    reader: &mut IO,
    opts: RdbOpts,
    mut key_from_bytes: impl FnMut(Vec<u8>) -> Result<K, RdbError>,
    mut insert: impl FnMut(K, TrieEntry),
) -> Result<(), RdbError> {
    let count = reader.read_u64()?;
    for _ in 0..count {
        let key = key_from_bytes(load_nul_terminated(reader)?)?;
        let score = reader.read_f64()?;
        let payload = opts
            .payloads
            .then(|| load_nul_terminated(reader))
            .transpose()?
            .filter(|b| !b.is_empty());
        let num_docs = if opts.num_docs { reader.read_u64()? } else { 0 };
        insert(
            key,
            TrieEntry {
                score,
                payload,
                num_docs,
            },
        );
    }
    Ok(())
}

/// Write `b` followed by one trailing NUL byte as a single length-prefixed
/// record, reusing `scratch` as the temporary contiguous buffer. The saved
/// buffer is `b.len() + 1` bytes long, matching the C wire format
/// (`SaveStringBuffer(s, len + 1)`).
///
/// `scratch` is borrowed from the caller so one allocation can amortize
/// across an entire save loop.
pub(crate) fn save_nul_terminated<IO: RdbIO>(writer: &mut IO, scratch: &mut Vec<u8>, b: &[u8]) {
    scratch.clear();
    scratch.reserve(b.len() + 1);
    scratch.extend_from_slice(b);
    scratch.push(0);
    writer.write_buffer(scratch);
}

/// Read one length-prefixed buffer that is expected to end in a NUL byte
/// and return its contents with the trailing NUL stripped. Returns
/// [`RdbError::MissingTrailingNul`] when the wire buffer is empty or does
/// not end in `0x00`.
pub(crate) fn load_nul_terminated<IO: RdbIO>(reader: &mut IO) -> Result<Vec<u8>, RdbError> {
    let mut buf = reader.read_buffer()?;
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

/// Errors that can occur while reading a trie RDB payload.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RdbError {
    /// The underlying RDB read failed (EOF, corrupted stream, etc.).
    ///
    /// The originating [`std::io::Error`] is intentionally not retained: the
    /// only consumer (the C load entry point) collapses every failure to a
    /// NULL return, so the detail is discarded regardless. Dropping it keeps
    /// [`RdbError`] `Clone + PartialEq + Eq`, which the wire-shape tests rely
    /// on to assert exact error values.
    #[error("rdb io error")]
    Io,
    /// A bytes buffer expected to end with a NUL terminator did not.
    #[error("rdb bytes buffer missing trailing NUL")]
    MissingTrailingNul,
    /// A key buffer was not valid UTF-8 when loaded through the
    /// [`crate::str`] wrapper that requires UTF-8 keys.
    #[error("rdb key bytes not valid UTF-8")]
    InvalidUtf8,
}

impl From<io::Error> for RdbError {
    /// Lift an IO failure from the [`RdbIO`] load primitives into the framing
    /// error type, so `?` threads `io::Result` through the framing helpers.
    fn from(_: io::Error) -> Self {
        RdbError::Io
    }
}

/// In-memory [`RdbIO`] mock shared by the byte-keyed and str-keyed RDB test
/// suites. Lives here (rather than inside either test module) so both
/// [`crate::byte`]'s and [`crate::str`]'s tests import the same `Op` enum —
/// keeps the wire-shape assertions cross-checkable against one canonical
/// representation.
#[cfg(test)]
pub(crate) mod test_helpers {
    use super::*;

    /// One typed call against [`RdbIO`]. The wire-shape tests assert against
    /// `Vec<Op>` traces directly.
    #[derive(Debug, Clone, PartialEq)]
    pub(crate) enum Op {
        U64(u64),
        F64(f64),
        Bytes(Vec<u8>),
    }

    /// Round-trip [`RdbIO`] mock: `save_*` append to `ops`; `load_*` replay
    /// them in order from an internal read cursor. One buffer, so saving into
    /// the mock and then loading it back reproduces the production save→load
    /// path against a single endpoint — the shape the real `RedisModuleIO`
    /// handle has. `ops` is public so wire-shape tests can assert the exact
    /// recorded trace.
    #[derive(Default)]
    pub(crate) struct RdbMock {
        pub(crate) ops: Vec<Op>,
        read_pos: usize,
        fail_after: Option<usize>,
        read_calls: usize,
    }

    impl RdbMock {
        /// Preload the mock with a known op stream, for load-only tests that
        /// feed a hand-built (possibly malformed) trace rather than saving one.
        pub(crate) fn from_ops(ops: Vec<Op>) -> Self {
            Self {
                ops,
                ..Self::default()
            }
        }

        /// Short-circuit `load_*` with an [`std::io::Error`] after `n`
        /// successful reads, to exercise mid-stream IO failure paths.
        pub(crate) fn fail_after(mut self, n: usize) -> Self {
            self.fail_after = Some(n);
            self
        }

        fn next_read(&mut self) -> io::Result<Op> {
            if let Some(n) = self.fail_after
                && self.read_calls >= n
            {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "mock: injected io failure",
                ));
            }
            self.read_calls += 1;
            let op = self.ops.get(self.read_pos).cloned().ok_or_else(|| {
                io::Error::new(io::ErrorKind::UnexpectedEof, "mock: op stream exhausted")
            })?;
            self.read_pos += 1;
            Ok(op)
        }
    }

    // The trie RDB wire format only ever uses u64/f64/buffer; the `i64`/`f32`
    // methods of the shared `RdbIO` trait exist for other consumers (e.g. RSE's
    // vecsim) and are never reached by `trie_rdb`'s serializers, so the mock
    // asserts that invariant rather than modeling them.
    impl RdbIO for RdbMock {
        fn write_u64(&mut self, v: u64) {
            self.ops.push(Op::U64(v));
        }
        fn write_f64(&mut self, v: f64) {
            self.ops.push(Op::F64(v));
        }
        fn write_buffer(&mut self, b: &[u8]) {
            self.ops.push(Op::Bytes(b.to_vec()));
        }
        fn write_i64(&mut self, _v: i64) {
            unreachable!("trie_rdb never serializes i64");
        }
        fn write_f32(&mut self, _v: f32) {
            unreachable!("trie_rdb never serializes f32");
        }

        fn read_u64(&mut self) -> io::Result<u64> {
            match self.next_read()? {
                Op::U64(v) => Ok(v),
                op => panic!("mock: expected U64, got {op:?}"),
            }
        }
        fn read_f64(&mut self) -> io::Result<f64> {
            match self.next_read()? {
                Op::F64(v) => Ok(v),
                op => panic!("mock: expected F64, got {op:?}"),
            }
        }
        fn read_buffer(&mut self) -> io::Result<Vec<u8>> {
            match self.next_read()? {
                Op::Bytes(v) => Ok(v),
                op => panic!("mock: expected Bytes, got {op:?}"),
            }
        }
        fn read_i64(&mut self) -> io::Result<i64> {
            unreachable!("trie_rdb never deserializes i64");
        }
        fn read_f32(&mut self) -> io::Result<f32> {
            unreachable!("trie_rdb never deserializes f32");
        }
    }
}
