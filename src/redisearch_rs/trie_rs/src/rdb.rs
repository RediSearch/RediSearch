/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! RDB serialization for the lex-mode trie.
//!
//! Mirrors the wire format produced by the C functions `TrieType_GenericSave`
//! and `TrieType_GenericLoad` in `src/trie/trie.c`, but for a Rust
//! [`TrieMap<TrieEntry>`].
//!
//! # Wire format
//!
//! ```text
//! u64  count                            // map.n_unique_keys()
//! [ bytes(key + '\0')
//!   f64  score
//!   bytes(payload + '\0')               // only if SaveOpts::save_payloads
//!   u64  num_docs                       // only if SaveOpts::save_num_docs
//! ] * count
//! ```
//!
//! # Trailing-NUL framing
//!
//! Both keys and payloads are written with a trailing NUL byte (so the wire
//! bytes are `key.len() + 1` long, matching C's `SaveStringBuffer(..., len + 1)`)
//! and the loader strips one byte. Streams whose buffers do not end in NUL
//! are rejected with [`RdbError::MissingTrailingNul`].
//!
//! # Empty-payload normalization
//!
//! When `SaveOpts::save_payloads` is `true`, both `payload: None` and
//! `payload: Some(vec![])` emit the wire bytes `"\0"` and load back as
//! `None`. This mirrors the C-side collapse `payload.len ? &payload : NULL`
//! at `src/trie/trie.c:415`.
//!
//! # IO model
//!
//! Save is infallible at the Rust API level, matching the void-returning C
//! `RedisModule_Save*` primitives. Errors only surface on load through
//! [`RdbError`].

use crate::TrieMap;

/// One trie entry: insertion score, optional opaque payload, and number of
/// documents indexed under the key.
///
/// `payload: None` and `payload: Some(vec![])` are wire-indistinguishable
/// when payloads are persisted — both round-trip as `None`. See
/// [`SaveOpts::save_payloads`].
#[derive(Clone, Debug, PartialEq)]
pub struct TrieEntry {
    /// Insertion score. The C trie stores this as `float`; the RDB wire
    /// format widens it to `f64` (via `RedisModule_SaveDouble`).
    pub score: f64,
    /// Optional opaque payload bytes.
    pub payload: Option<Vec<u8>>,
    /// Number of documents currently indexed under this key.
    pub num_docs: u64,
}

/// Controls which optional fields are written by [`save`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SaveOpts {
    /// Write each entry's payload (with trailing NUL) to the stream.
    pub save_payloads: bool,
    /// Write each entry's `num_docs` to the stream.
    pub save_num_docs: bool,
}

/// Controls which optional fields are read by [`load`].
///
/// Must match the [`SaveOpts`] used at save time. Mismatches produce
/// [`RdbError`]s or silently parse following bytes as the wrong field.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LoadOpts {
    /// Read each entry's payload (with trailing NUL) from the stream.
    pub load_payloads: bool,
    /// Read each entry's `num_docs` from the stream.
    pub load_num_docs: bool,
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
    /// Write a length-prefixed byte buffer.
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
    /// Read a length-prefixed byte buffer.
    fn load_bytes(&mut self) -> Result<Vec<u8>, RdbError>;
}

/// Errors that can occur while reading a trie RDB payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RdbError {
    /// The underlying RDB read failed (EOF, corrupted stream, etc.).
    Io(String),
    /// The next primitive in the stream had a different type than expected.
    TypeMismatch {
        /// Expected primitive type.
        expected: &'static str,
        /// Actual primitive type encountered.
        got: &'static str,
    },
    /// A bytes buffer expected to end with a NUL terminator did not.
    MissingTrailingNul,
}

impl std::fmt::Display for RdbError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(msg) => write!(f, "rdb io error: {msg}"),
            Self::TypeMismatch { expected, got } => {
                write!(f, "rdb type mismatch: expected {expected}, got {got}")
            }
            Self::MissingTrailingNul => f.write_str("rdb bytes buffer missing trailing NUL"),
        }
    }
}

impl std::error::Error for RdbError {}

/// Serialize a [`TrieMap<TrieEntry>`] to `writer` in the lex-mode RDB format.
///
/// Iterates entries in lexicographic key order. Keys, and payloads when
/// [`SaveOpts::save_payloads`] is set, are written with a trailing NUL
/// byte to match the C wire format (the loader strips it back off).
pub fn save<W: RdbWrite>(map: &TrieMap<TrieEntry>, writer: &mut W, opts: SaveOpts) {
    writer.save_u64(map.n_unique_keys() as u64);
    for (key, entry) in map.iter() {
        write_bytes_with_nul(writer, &key);
        writer.save_f64(entry.score);
        if opts.save_payloads {
            let bytes = entry.payload.as_deref().unwrap_or(&[]);
            write_bytes_with_nul(writer, bytes);
        }
        if opts.save_num_docs {
            writer.save_u64(entry.num_docs);
        }
    }
}

/// Deserialize a [`TrieMap<TrieEntry>`] from `reader`.
///
/// `opts` must match the [`SaveOpts`] used at save time.
///
/// The trailing NUL byte is stripped from every key (and every payload
/// when [`LoadOpts::load_payloads`] is set). An empty payload (i.e. the
/// wire bytes `"\0"`) is normalized to `payload: None`.
pub fn load<R: RdbRead>(reader: &mut R, opts: LoadOpts) -> Result<TrieMap<TrieEntry>, RdbError> {
    let count = reader.load_u64()?;
    let mut map = TrieMap::new();
    for _ in 0..count {
        let key = read_bytes_strip_nul(reader)?;
        let score = reader.load_f64()?;
        let payload = if opts.load_payloads {
            let bytes = read_bytes_strip_nul(reader)?;
            if bytes.is_empty() { None } else { Some(bytes) }
        } else {
            None
        };
        let num_docs = if opts.load_num_docs {
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

fn write_bytes_with_nul<W: RdbWrite>(w: &mut W, b: &[u8]) {
    let mut buf = Vec::with_capacity(b.len() + 1);
    buf.extend_from_slice(b);
    buf.push(0);
    w.save_bytes(&buf);
}

fn read_bytes_strip_nul<R: RdbRead>(r: &mut R) -> Result<Vec<u8>, RdbError> {
    let mut buf = r.load_bytes()?;
    if buf.pop() != Some(0) {
        return Err(RdbError::MissingTrailingNul);
    }
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, PartialEq)]
    enum Op {
        U64(u64),
        F64(f64),
        Bytes(Vec<u8>),
    }

    impl Op {
        const fn kind(&self) -> &'static str {
            match self {
                Self::U64(_) => "u64",
                Self::F64(_) => "f64",
                Self::Bytes(_) => "bytes",
            }
        }
    }

    #[derive(Default)]
    struct Recorder(Vec<Op>);
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

    struct Replayer {
        ops: std::vec::IntoIter<Op>,
        fail_after: Option<usize>,
        calls: usize,
    }

    impl Replayer {
        fn new(ops: Vec<Op>) -> Self {
            Self {
                ops: ops.into_iter(),
                fail_after: None,
                calls: 0,
            }
        }

        fn fail_after(ops: Vec<Op>, n: usize) -> Self {
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
                return Err(RdbError::Io("injected".into()));
            }
            self.calls += 1;
            self.ops.next().ok_or_else(|| RdbError::Io("eof".into()))
        }

        fn mismatch<T>(expected: &'static str, op: &Op) -> Result<T, RdbError> {
            Err(RdbError::TypeMismatch {
                expected,
                got: op.kind(),
            })
        }
    }

    impl RdbRead for Replayer {
        fn load_u64(&mut self) -> Result<u64, RdbError> {
            let op = self.step()?;
            match op {
                Op::U64(v) => Ok(v),
                _ => Self::mismatch("u64", &op),
            }
        }
        fn load_f64(&mut self) -> Result<f64, RdbError> {
            let op = self.step()?;
            match op {
                Op::F64(v) => Ok(v),
                _ => Self::mismatch("f64", &op),
            }
        }
        fn load_bytes(&mut self) -> Result<Vec<u8>, RdbError> {
            let op = self.step()?;
            match op {
                Op::Bytes(v) => Ok(v),
                _ => Self::mismatch("bytes", &op),
            }
        }
    }

    fn entry(score: f64, payload: Option<&[u8]>, num_docs: u64) -> TrieEntry {
        TrieEntry {
            score,
            payload: payload.map(<[u8]>::to_vec),
            num_docs,
        }
    }

    fn round_trip(
        map: &TrieMap<TrieEntry>,
        save_opts: SaveOpts,
        load_opts: LoadOpts,
    ) -> TrieMap<TrieEntry> {
        let mut rec = Recorder::default();
        save(map, &mut rec, save_opts);
        let mut rep = Replayer::new(rec.0);
        load(&mut rep, load_opts).expect("load should succeed")
    }

    #[test]
    fn save_empty_map() {
        let map: TrieMap<TrieEntry> = TrieMap::new();
        let mut rec = Recorder::default();
        save(&map, &mut rec, SaveOpts::default());
        assert_eq!(rec.0, vec![Op::U64(0)]);
    }

    #[test]
    fn save_protocol_shape_keys_only() {
        let mut map = TrieMap::new();
        map.insert(b"alpha", entry(1.0, None, 0));
        map.insert(b"beta", entry(2.5, None, 0));
        let mut rec = Recorder::default();
        save(&map, &mut rec, SaveOpts::default());
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
            SaveOpts {
                save_payloads: true,
                save_num_docs: true,
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
        let loaded = round_trip(&map, SaveOpts::default(), LoadOpts::default());
        assert_eq!(loaded.n_unique_keys(), 2);
        assert_eq!(loaded.find(b"a"), Some(&entry(1.0, None, 0)));
        assert_eq!(loaded.find(b"b"), Some(&entry(2.0, None, 0)));
    }

    #[test]
    fn roundtrip_payloads_only() {
        let mut map = TrieMap::new();
        // num_docs is set but not persisted by the opts; it must come back as 0.
        map.insert(b"foo", entry(1.0, Some(b"payload"), 99));
        let save_opts = SaveOpts {
            save_payloads: true,
            save_num_docs: false,
        };
        let load_opts = LoadOpts {
            load_payloads: true,
            load_num_docs: false,
        };
        let loaded = round_trip(&map, save_opts, load_opts);
        assert_eq!(loaded.find(b"foo"), Some(&entry(1.0, Some(b"payload"), 0)));
    }

    #[test]
    fn roundtrip_num_docs_only() {
        let mut map = TrieMap::new();
        // Payload is set but not persisted; it must come back as None.
        map.insert(b"foo", entry(1.0, Some(b"ignored"), 42));
        let save_opts = SaveOpts {
            save_payloads: false,
            save_num_docs: true,
        };
        let load_opts = LoadOpts {
            load_payloads: false,
            load_num_docs: true,
        };
        let loaded = round_trip(&map, save_opts, load_opts);
        assert_eq!(loaded.find(b"foo"), Some(&entry(1.0, None, 42)));
    }

    #[test]
    fn roundtrip_both() {
        let mut map = TrieMap::new();
        map.insert(b"foo", entry(3.5, Some(b"pay"), 11));
        map.insert(b"bar", entry(0.5, Some(b"x"), 1));
        let save_opts = SaveOpts {
            save_payloads: true,
            save_num_docs: true,
        };
        let load_opts = LoadOpts {
            load_payloads: true,
            load_num_docs: true,
        };
        let loaded = round_trip(&map, save_opts, load_opts);
        assert_eq!(loaded.find(b"foo"), Some(&entry(3.5, Some(b"pay"), 11)));
        assert_eq!(loaded.find(b"bar"), Some(&entry(0.5, Some(b"x"), 1)));
    }

    #[test]
    fn empty_trie_roundtrip() {
        let map: TrieMap<TrieEntry> = TrieMap::new();
        let loaded = round_trip(&map, SaveOpts::default(), LoadOpts::default());
        assert_eq!(loaded.n_unique_keys(), 0);
    }

    #[test]
    fn lex_order_preserved() {
        let mut map = TrieMap::new();
        for key in [b"zebra".as_slice(), b"apple", b"mango", b"banana"] {
            map.insert(key, entry(1.0, None, 0));
        }
        let mut rec = Recorder::default();
        save(&map, &mut rec, SaveOpts::default());
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

        let opts = SaveOpts {
            save_payloads: true,
            save_num_docs: false,
        };
        let mut rec_empty = Recorder::default();
        let mut rec_none = Recorder::default();
        save(&from_empty, &mut rec_empty, opts);
        save(&from_none, &mut rec_none, opts);
        assert_eq!(
            rec_empty.0, rec_none.0,
            "empty Vec and None must match on the wire"
        );

        let load_opts = LoadOpts {
            load_payloads: true,
            load_num_docs: false,
        };
        let loaded = load(&mut Replayer::new(rec_empty.0), load_opts).unwrap();
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
            SaveOpts {
                save_payloads: true,
                save_num_docs: true,
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
        save(&map, &mut rec, SaveOpts::default());
        // Ops: U64(1), Bytes("a\0"), F64(1.0). Inject an error after the count read.
        let mut rep = Replayer::fail_after(rec.0, 1);
        let err = load(&mut rep, LoadOpts::default()).unwrap_err();
        assert_eq!(err, RdbError::Io("injected".into()));
    }

    #[test]
    fn multibyte_utf8_keys_roundtrip() {
        let mut map = TrieMap::new();
        let k1 = "héllo".as_bytes();
        let k2 = "日本語".as_bytes();
        map.insert(k1, entry(1.0, None, 0));
        map.insert(k2, entry(2.0, None, 0));
        let loaded = round_trip(&map, SaveOpts::default(), LoadOpts::default());
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
        let err = load(&mut Replayer::new(ops), LoadOpts::default()).unwrap_err();
        assert_eq!(err, RdbError::MissingTrailingNul);
    }

    #[test]
    fn type_mismatch_surfaces() {
        // Loader expects U64 for the count, but the stream begins with Bytes.
        let ops = vec![Op::Bytes(b"\0".to_vec())];
        let err = load(&mut Replayer::new(ops), LoadOpts::default()).unwrap_err();
        assert_eq!(
            err,
            RdbError::TypeMismatch {
                expected: "u64",
                got: "bytes",
            }
        );
    }
}
