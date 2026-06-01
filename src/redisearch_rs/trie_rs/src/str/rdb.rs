/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! RDB serialization for [`StrTrieMap<TrieEntry>`].
//!
//! Wraps the byte-keyed [`crate::rdb`] surface for callers whose keys are
//! UTF-8 by type. The wire format is byte-identical to the byte-keyed API:
//! save just delegates after handing over the inner [`crate::TrieMap`], and
//! load funnels each key buffer through [`std::str::from_utf8`] before
//! inserting via [`StrTrieMap::insert`]. Non-UTF-8 input surfaces as
//! [`RdbError::InvalidUtf8`] rather than silently materializing as an
//! ill-formed `String`.

use crate::rdb::{self, RdbError, RdbOpts, RdbRead, RdbWrite, TrieEntry, load_nul_terminated};
use crate::str::StrTrieMap;

/// Serialize a [`StrTrieMap<TrieEntry>`] to `writer` in the trie RDB wire
/// format.
///
/// Delegates to [`crate::rdb::save`] on the inner byte-keyed
/// [`crate::TrieMap`]; the wire output is byte-identical.
pub fn save<W: RdbWrite>(map: &StrTrieMap<TrieEntry>, writer: &mut W, opts: RdbOpts) {
    rdb::save(map.byte_trie(), writer, opts);
}

/// Deserialize a [`StrTrieMap<TrieEntry>`] from `reader`.
///
/// `opts` must match the [`RdbOpts`] used at save time. Each loaded key
/// buffer is UTF-8 validated; on failure the load aborts with
/// [`RdbError::InvalidUtf8`].
pub fn load<R: RdbRead>(reader: &mut R, opts: RdbOpts) -> Result<StrTrieMap<TrieEntry>, RdbError> {
    let count = reader.load_u64()?;
    let mut map = StrTrieMap::new();
    for _ in 0..count {
        let key_bytes = load_nul_terminated(reader)?;
        let key = std::str::from_utf8(&key_bytes).map_err(|_| RdbError::InvalidUtf8)?;
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
            key,
            TrieEntry {
                score,
                payload,
                num_docs,
            },
        );
    }
    Ok(map)
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
    }
    impl Replayer {
        fn new(ops: Vec<Op>) -> Self {
            Self {
                ops: ops.into_iter(),
            }
        }
        fn step(&mut self) -> Result<Op, RdbError> {
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

    fn entry(score: f64, payload: Option<&[u8]>, num_docs: u64) -> TrieEntry {
        TrieEntry {
            score,
            payload: payload.map(<[u8]>::to_vec),
            num_docs,
        }
    }

    #[test]
    fn roundtrip_str_keys_with_all_opts() {
        let mut map = StrTrieMap::new();
        map.insert("alpha", entry(1.0, Some(b"p"), 3));
        map.insert("héllo", entry(2.5, None, 7));
        let opts = RdbOpts {
            payloads: true,
            num_docs: true,
        };
        let mut rec = Recorder::default();
        save(&map, &mut rec, opts);
        let loaded = load(&mut Replayer::new(rec.0), opts).expect("load should succeed");
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded.get("alpha"), Some(&entry(1.0, Some(b"p"), 3)));
        assert_eq!(loaded.get("héllo"), Some(&entry(2.5, None, 7)));
    }

    #[test]
    fn invalid_utf8_key_errors() {
        // count=1, key=<two stray high bytes + NUL>, score=1.0
        let ops = vec![
            Op::U64(1),
            Op::Bytes(b"\xff\xfe\0".to_vec()),
            Op::F64(1.0),
        ];
        // Use `match` rather than `unwrap_err`: `StrTrieMap` doesn't impl
        // `Debug`, and adding it just to satisfy the test would force a
        // `Data: Debug` bound on every map instantiation.
        match load(&mut Replayer::new(ops), RdbOpts::default()) {
            Err(RdbError::InvalidUtf8) => {}
            Err(other) => panic!("expected InvalidUtf8, got {other:?}"),
            Ok(_) => panic!("expected InvalidUtf8 error, got Ok"),
        }
    }

    #[test]
    fn save_wire_matches_byte_api() {
        // Sanity-check: the wrapper produces the same Op trace as the byte API
        // for an ASCII key set, since it delegates to crate::rdb::save.
        let mut str_map = StrTrieMap::new();
        str_map.insert("x", entry(1.0, Some(b"pay"), 7));

        let mut byte_map = crate::TrieMap::new();
        byte_map.insert(b"x", entry(1.0, Some(b"pay"), 7));

        let opts = RdbOpts {
            payloads: true,
            num_docs: true,
        };
        let mut rec_str = Recorder::default();
        let mut rec_bytes = Recorder::default();
        save(&str_map, &mut rec_str, opts);
        rdb::save(&byte_map, &mut rec_bytes, opts);
        assert_eq!(rec_str.0, rec_bytes.0);
    }
}
