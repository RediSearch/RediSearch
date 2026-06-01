/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! RDB serialization for [`TrieMap<TrieEntry>`].
//!
//! Mirrors the wire format produced by the C functions `TrieType_GenericSave`
//! and `TrieType_GenericLoad`, but for a Rust [`TrieMap<TrieEntry>`].
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
//! NUL framing is applied here in the algorithm body via [`save_nul_terminated`]
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

use crate::TrieMap;

/// Serialize a [`TrieMap<TrieEntry>`] to `writer` in the trie RDB wire format.
///
/// Iterates entries in lexicographic key order. Keys, and payloads when
/// [`RdbOpts::payloads`] is set, are written with a trailing NUL byte to
/// match the C wire format (the loader strips it back off). One scratch
/// buffer is reused across all entries so the per-key NUL padding costs
/// at most one allocation per save call.
pub fn save<W: RdbWrite>(map: &TrieMap<TrieEntry>, writer: &mut W, opts: RdbOpts) {
    writer.save_u64(map.n_unique_keys() as u64);
    let mut scratch = Vec::new();
    for (key, entry) in map.iter() {
        save_nul_terminated(writer, &mut scratch, &key);
        writer.save_f64(entry.score);
        if opts.payloads {
            save_nul_terminated(
                writer,
                &mut scratch,
                entry.payload.as_deref().unwrap_or(&[]),
            );
        }
        if opts.num_docs {
            writer.save_u64(entry.num_docs);
        }
    }
}

/// Deserialize a [`TrieMap<TrieEntry>`] from `reader`.
///
/// `opts` must match the [`RdbOpts`] used at save time.
///
/// The trailing NUL byte is stripped from every key (and every payload
/// when [`RdbOpts::payloads`] is set). An empty payload (i.e. a single-NUL
/// buffer, `"\0"`) is normalized to `payload: None`.
pub fn load<R: RdbRead>(reader: &mut R, opts: RdbOpts) -> Result<TrieMap<TrieEntry>, RdbError> {
    let count = reader.load_u64()?;
    let mut map = TrieMap::new();
    for _ in 0..count {
        let key = load_nul_terminated(reader)?;
        let score = reader.load_f64()?;
        let payload = opts
            .payloads
            .then(|| load_nul_terminated(reader))
            .transpose()?
            .filter(|b| !b.is_empty());
        let num_docs = if opts.num_docs {
            reader.load_u64()?
        } else {
            0
        };
        map.insert(
            &key,
            TrieEntry {
                score,
                payload,
                num_docs,
            },
        );
    }
    Ok(map)
}

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
    /// [`crate::str::rdb`] wrapper that requires UTF-8 keys.
    #[error("rdb key bytes not valid UTF-8")]
    InvalidUtf8,
}

/// In-memory [`RdbWrite`] / [`RdbRead`] mocks shared by the byte-keyed and
/// str-keyed RDB test suites. Lives here (rather than inside either test
/// module) so both `crate::rdb::tests` and `crate::str::rdb::tests` import
/// the same `Op` enum — keeps the wire-shape assertions cross-checkable
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

#[cfg(test)]
mod tests {
    use super::*;
    use test_helpers::{Op, Recorder, Replayer};

    fn entry(score: f64, payload: Option<&[u8]>, num_docs: u64) -> TrieEntry {
        TrieEntry {
            score,
            payload: payload.map(<[u8]>::to_vec),
            num_docs,
        }
    }

    fn round_trip(map: &TrieMap<TrieEntry>, opts: RdbOpts) -> TrieMap<TrieEntry> {
        let mut rec = Recorder::default();
        save(map, &mut rec, opts);
        let mut rep = Replayer::new(rec.0);
        load(&mut rep, opts).expect("load should succeed")
    }

    #[test]
    fn save_empty_map() {
        let map: TrieMap<TrieEntry> = TrieMap::new();
        let mut rec = Recorder::default();
        save(&map, &mut rec, RdbOpts::default());
        assert_eq!(rec.0, vec![Op::U64(0)]);
    }

    #[test]
    fn save_protocol_shape_keys_only() {
        let mut map = TrieMap::new();
        map.insert(b"alpha", entry(1.0, None, 0));
        map.insert(b"beta", entry(2.5, None, 0));
        let mut rec = Recorder::default();
        save(&map, &mut rec, RdbOpts::default());
        assert_eq!(
            rec.0,
            vec![
                Op::U64(2),
                Op::Bytes(b"alpha\0".to_vec()),
                Op::F64(1.0),
                Op::Bytes(b"beta\0".to_vec()),
                Op::F64(2.5),
            ]
        );
    }

    #[test]
    fn save_protocol_shape_with_all_opts() {
        let mut map = TrieMap::new();
        map.insert(b"x", entry(1.0, Some(b"pay"), 7));
        let mut rec = Recorder::default();
        save(
            &map,
            &mut rec,
            RdbOpts {
                payloads: true,
                num_docs: true,
            },
        );
        assert_eq!(
            rec.0,
            vec![
                Op::U64(1),
                Op::Bytes(b"x\0".to_vec()),
                Op::F64(1.0),
                Op::Bytes(b"pay\0".to_vec()),
                Op::U64(7),
            ]
        );
    }

    #[test]
    fn roundtrip_no_opts() {
        let mut map = TrieMap::new();
        map.insert(b"a", entry(1.0, None, 0));
        map.insert(b"b", entry(2.0, None, 0));
        let loaded = round_trip(&map, RdbOpts::default());
        assert_eq!(loaded.n_unique_keys(), 2);
        assert_eq!(loaded.find(b"a"), Some(&entry(1.0, None, 0)));
        assert_eq!(loaded.find(b"b"), Some(&entry(2.0, None, 0)));
    }

    #[test]
    fn roundtrip_payloads_only() {
        let mut map = TrieMap::new();
        // num_docs is set but not persisted by the opts; it must come back as 0.
        map.insert(b"foo", entry(1.0, Some(b"payload"), 99));
        let opts = RdbOpts {
            payloads: true,
            num_docs: false,
        };
        let loaded = round_trip(&map, opts);
        assert_eq!(loaded.find(b"foo"), Some(&entry(1.0, Some(b"payload"), 0)));
    }

    #[test]
    fn roundtrip_num_docs_only() {
        let mut map = TrieMap::new();
        // Payload is set but not persisted; it must come back as None.
        map.insert(b"foo", entry(1.0, Some(b"ignored"), 42));
        let opts = RdbOpts {
            payloads: false,
            num_docs: true,
        };
        let loaded = round_trip(&map, opts);
        assert_eq!(loaded.find(b"foo"), Some(&entry(1.0, None, 42)));
    }

    #[test]
    fn roundtrip_both() {
        let mut map = TrieMap::new();
        map.insert(b"foo", entry(3.5, Some(b"pay"), 11));
        map.insert(b"bar", entry(0.5, Some(b"x"), 1));
        let opts = RdbOpts {
            payloads: true,
            num_docs: true,
        };
        let loaded = round_trip(&map, opts);
        assert_eq!(loaded.find(b"foo"), Some(&entry(3.5, Some(b"pay"), 11)));
        assert_eq!(loaded.find(b"bar"), Some(&entry(0.5, Some(b"x"), 1)));
    }

    #[test]
    fn empty_trie_roundtrip() {
        let map: TrieMap<TrieEntry> = TrieMap::new();
        let loaded = round_trip(&map, RdbOpts::default());
        assert_eq!(loaded.n_unique_keys(), 0);
    }

    #[test]
    fn lex_order_preserved() {
        let mut map = TrieMap::new();
        for key in [b"zebra".as_slice(), b"apple", b"mango", b"banana"] {
            map.insert(key, entry(1.0, None, 0));
        }
        let mut rec = Recorder::default();
        save(&map, &mut rec, RdbOpts::default());
        let keys: Vec<Vec<u8>> = rec
            .0
            .into_iter()
            .filter_map(|op| match op {
                Op::Bytes(mut b) => {
                    b.pop();
                    Some(b)
                }
                _ => None,
            })
            .collect();
        assert_eq!(
            keys,
            vec![
                b"apple".to_vec(),
                b"banana".to_vec(),
                b"mango".to_vec(),
                b"zebra".to_vec(),
            ]
        );
    }

    #[test]
    fn empty_payload_normalizes_to_none() {
        let mut from_empty = TrieMap::new();
        from_empty.insert(b"k", entry(1.0, Some(b""), 0));
        let mut from_none = TrieMap::new();
        from_none.insert(b"k", entry(1.0, None, 0));

        let opts = RdbOpts {
            payloads: true,
            num_docs: false,
        };
        let mut rec_empty = Recorder::default();
        let mut rec_none = Recorder::default();
        save(&from_empty, &mut rec_empty, opts);
        save(&from_none, &mut rec_none, opts);
        assert_eq!(
            rec_empty.0, rec_none.0,
            "empty Vec and None must match on the wire"
        );

        let loaded = load(&mut Replayer::new(rec_empty.0), opts).unwrap();
        assert_eq!(loaded.find(b"k").unwrap().payload, None);
    }

    #[test]
    fn trailing_nul_on_every_bytes_op() {
        let mut map = TrieMap::new();
        map.insert(b"abc", entry(1.0, Some(b"def"), 1));
        let mut rec = Recorder::default();
        save(
            &map,
            &mut rec,
            RdbOpts {
                payloads: true,
                num_docs: true,
            },
        );
        for op in &rec.0 {
            if let Op::Bytes(b) = op {
                assert_eq!(b.last(), Some(&0), "bytes op missing trailing NUL: {b:?}");
            }
        }
    }

    #[test]
    fn io_error_propagates() {
        let mut map = TrieMap::new();
        map.insert(b"a", entry(1.0, None, 0));
        let mut rec = Recorder::default();
        save(&map, &mut rec, RdbOpts::default());
        // Ops: U64(1), Bytes("a\0"), F64(1.0). Inject an error after the count read.
        let mut rep = Replayer::fail_after(rec.0, 1);
        let err = load(&mut rep, RdbOpts::default()).unwrap_err();
        assert_eq!(err, RdbError::Io);
    }

    #[test]
    fn multibyte_utf8_keys_roundtrip() {
        let mut map = TrieMap::new();
        let k1 = "héllo".as_bytes();
        let k2 = "日本語".as_bytes();
        map.insert(k1, entry(1.0, None, 0));
        map.insert(k2, entry(2.0, None, 0));
        let loaded = round_trip(&map, RdbOpts::default());
        assert_eq!(loaded.find(k1), Some(&entry(1.0, None, 0)));
        assert_eq!(loaded.find(k2), Some(&entry(2.0, None, 0)));
    }

    #[test]
    fn missing_trailing_nul_errors() {
        let ops = vec![
            Op::U64(1),
            Op::Bytes(b"abc".to_vec()), // missing trailing NUL
            Op::F64(1.0),
        ];
        let err = load(&mut Replayer::new(ops), RdbOpts::default()).unwrap_err();
        assert_eq!(err, RdbError::MissingTrailingNul);
    }

}
