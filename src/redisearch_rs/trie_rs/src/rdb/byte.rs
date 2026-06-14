/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! RDB serialization for the byte-keyed [`TrieMap<TrieEntry>`].
//!
//! This is the canonical serializer; the UTF-8-keyed [`crate::rdb::str`]
//! wrapper delegates to it. The wire format and framing rules are documented
//! on the parent module [`crate::rdb`].

use super::{RdbError, RdbOpts, RdbRead, RdbWrite, load_with, save_nul_terminated};
use crate::{TrieEntry, TrieMap};

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
    let mut map = TrieMap::new();
    load_with(reader, opts, Ok, |key, entry| {
        map.insert(&key, entry);
    })?;
    Ok(map)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rdb::test_helpers::{Op, Recorder, Replayer};

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
