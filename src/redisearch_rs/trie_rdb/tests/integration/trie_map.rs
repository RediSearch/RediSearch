/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use trie_rdb::test_utils::{MockRdbIO, Op};
use trie_rdb::trie_map::{load, load_with, save, save_with};
use trie_rdb::{MAX_KEY_BYTES, MAX_KEY_RUNES, RdbError, RdbOpts, SaveError, TrieEntry, WireFields};
use trie_rs::TrieMap;

fn entry(score: f64, payload: Option<&[u8]>, num_docs: u64) -> TrieEntry {
    TrieEntry {
        score,
        payload: payload.map(<[u8]>::to_vec),
        num_docs,
    }
}

fn round_trip(map: &TrieMap<TrieEntry>, opts: RdbOpts) -> TrieMap<TrieEntry> {
    let mut mock = MockRdbIO::default();
    save(map, &mut mock, opts).expect("save should succeed");
    load(&mut mock, opts).expect("load should succeed")
}

#[test]
fn save_empty_map() {
    let map: TrieMap<TrieEntry> = TrieMap::new();
    let mut mock = MockRdbIO::default();
    save(&map, &mut mock, RdbOpts::default()).expect("save should succeed");
    assert_eq!(mock.ops, vec![Op::U64(0)]);
}

#[test]
fn save_protocol_shape_keys_only() {
    let mut map = TrieMap::new();
    map.insert(b"alpha", entry(1.0, None, 0));
    map.insert(b"beta", entry(2.5, None, 0));
    let mut mock = MockRdbIO::default();
    save(&map, &mut mock, RdbOpts::default()).expect("save should succeed");
    assert_eq!(
        mock.ops,
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
    let mut mock = MockRdbIO::default();
    save(
        &map,
        &mut mock,
        RdbOpts {
            payloads: true,
            num_docs: true,
        },
    )
    .expect("save should succeed");
    assert_eq!(
        mock.ops,
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
fn roundtrip_interior_nul_payload() {
    // Payloads are length-framed, so a NUL inside the bytes must survive
    // the round trip untouched; only the appended terminator is stripped.
    let mut map = TrieMap::new();
    map.insert(b"k", entry(1.0, Some(b"ab\0cd"), 0));
    let opts = RdbOpts {
        payloads: true,
        num_docs: false,
    };
    let loaded = round_trip(&map, opts);
    assert_eq!(loaded.find(b"k"), Some(&entry(1.0, Some(b"ab\0cd"), 0)));
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
    let mut mock = MockRdbIO::default();
    save(&map, &mut mock, RdbOpts::default()).expect("save should succeed");
    let keys: Vec<Vec<u8>> = mock
        .ops
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
    let mut rec_empty = MockRdbIO::default();
    let mut rec_none = MockRdbIO::default();
    save(&from_empty, &mut rec_empty, opts).expect("save should succeed");
    save(&from_none, &mut rec_none, opts).expect("save should succeed");
    assert_eq!(
        rec_empty.ops, rec_none.ops,
        "empty Vec and None must match on the wire"
    );

    let loaded = load(&mut rec_empty, opts).unwrap();
    assert_eq!(loaded.find(b"k").unwrap().payload, None);
}

#[test]
fn trailing_nul_on_every_bytes_op() {
    let mut map = TrieMap::new();
    map.insert(b"abc", entry(1.0, Some(b"def"), 1));
    let mut mock = MockRdbIO::default();
    save(
        &map,
        &mut mock,
        RdbOpts {
            payloads: true,
            num_docs: true,
        },
    )
    .expect("save should succeed");
    for op in &mock.ops {
        if let Op::Bytes(b) = op {
            assert_eq!(b.last(), Some(&0), "bytes op missing trailing NUL: {b:?}");
        }
    }
}

#[test]
fn io_error_propagates() {
    let mut map = TrieMap::new();
    map.insert(b"a", entry(1.0, None, 0));
    let mut rec = MockRdbIO::default();
    save(&map, &mut rec, RdbOpts::default()).expect("save should succeed");
    // Recorded ops are U64(1), Bytes("a\0"), F64(1.0). Letting exactly one
    // read succeed puts the failure on the key buffer, mid-entry, rather
    // than on the count that precedes the loop.
    let mut mock = MockRdbIO::from_ops(rec.ops).fail_after(1);
    let err = load(&mut mock, RdbOpts::default()).unwrap_err();
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
fn unit_payload_save_matches_trie_entry_wire() {
    // A `()` payload mapped to constant fields must be wire-identical to
    // a `TrieEntry` map holding the same constants.
    let mut unit_map = TrieMap::new();
    unit_map.insert(b"a", ());
    unit_map.insert(b"b", ());
    let mut entry_map = TrieMap::new();
    entry_map.insert(b"a", entry(1.0, None, 0));
    entry_map.insert(b"b", entry(1.0, None, 0));

    let mut rec_unit = MockRdbIO::default();
    let mut rec_entry = MockRdbIO::default();
    save_with(&unit_map, &mut rec_unit, RdbOpts::default(), |()| {
        WireFields {
            score: 1.0,
            payload: None,
            num_docs: 0,
        }
    })
    .expect("save should succeed");
    save(&entry_map, &mut rec_entry, RdbOpts::default()).expect("save should succeed");

    assert_eq!(rec_unit.ops, rec_entry.ops);
}

#[test]
fn unit_payload_roundtrip_discards_wire_fields() {
    let mut map = TrieMap::new();
    map.insert(b"k", ());
    let mut mock = MockRdbIO::default();
    save_with(&map, &mut mock, RdbOpts::default(), |()| WireFields {
        score: 1.0,
        payload: None,
        num_docs: 0,
    })
    .expect("save should succeed");

    let loaded = load_with(&mut mock, RdbOpts::default(), |_| ()).expect("load should succeed");

    assert_eq!(loaded.n_unique_keys(), 1);
    assert_eq!(loaded.find(b"k"), Some(&()));
}

#[test]
fn missing_trailing_nul_errors() {
    let ops = vec![
        Op::U64(1),
        Op::Bytes(b"abc".to_vec()), // missing trailing NUL
        Op::F64(1.0),
    ];
    let err = load(&mut MockRdbIO::from_ops(ops), RdbOpts::default()).unwrap_err();
    assert_eq!(err, RdbError::MissingTrailingNul);
}

#[test]
fn payload_terminator_value_is_ignored() {
    let opts = RdbOpts {
        payloads: true,
        num_docs: false,
    };
    let ops = vec![
        Op::U64(1),
        Op::Bytes(b"k\0".to_vec()),
        Op::F64(1.0),
        Op::Bytes(b"pay\xAA".to_vec()),
    ];

    let loaded = load(&mut MockRdbIO::from_ops(ops), opts).expect("load should succeed");

    assert_eq!(
        loaded.find(b"k").unwrap().payload.as_deref(),
        Some(&b"pay"[..])
    );
}

#[test]
fn empty_payload_buffer_errors() {
    let opts = RdbOpts {
        payloads: true,
        num_docs: false,
    };
    let ops = vec![
        Op::U64(1),
        Op::Bytes(b"k\0".to_vec()),
        Op::F64(1.0),
        Op::Bytes(Vec::new()), // no terminator slot at all
    ];

    let err = load(&mut MockRdbIO::from_ops(ops), opts).unwrap_err();

    assert_eq!(err, RdbError::MissingTrailingNul);
}

#[test]
fn save_rejects_empty_key() {
    let mut map = TrieMap::new();
    map.insert(b"", entry(1.0, None, 0));
    let mut mock = MockRdbIO::default();

    let err = save(&map, &mut mock, RdbOpts::default()).unwrap_err();

    assert_eq!(err, SaveError::EmptyKey);
}

#[test]
fn save_rejects_key_over_byte_limit() {
    let mut map = TrieMap::new();
    // Multi-byte, so the key trips the byte limit rather than the rune one.
    let key = "é".repeat(MAX_KEY_BYTES / 2 + 1).into_bytes();
    map.insert(&key, entry(1.0, None, 0));
    let mut mock = MockRdbIO::default();

    let err = save(&map, &mut mock, RdbOpts::default()).unwrap_err();

    assert_eq!(err, SaveError::KeyTooLong { bytes: key.len() });
}

#[test]
fn save_rejects_key_over_rune_limit() {
    let mut map = TrieMap::new();
    map.insert(&vec![b'a'; MAX_KEY_RUNES + 1], entry(1.0, None, 0));
    let mut mock = MockRdbIO::default();

    let err = save(&map, &mut mock, RdbOpts::default()).unwrap_err();

    assert_eq!(
        err,
        SaveError::TooManyRunes {
            runes: MAX_KEY_RUNES + 1
        }
    );
}

#[test]
fn save_rejects_key_with_embedded_nul() {
    let mut map = TrieMap::new();
    map.insert(b"a\0b", entry(1.0, None, 0));
    let mut mock = MockRdbIO::default();

    let err = save(&map, &mut mock, RdbOpts::default()).unwrap_err();

    assert_eq!(err, SaveError::DecodesToNul);
}

#[test]
fn save_rejects_key_decoding_to_a_zero_codepoint_without_a_nul_byte() {
    // Both keys hold no NUL byte, yet C's decoder reads a zero codepoint out
    // of them: the continuation pair as the whole key (C drops the entry),
    // the overlong encoding mid-key (C truncates the key at it).
    for key in [b"\x80\x80".as_slice(), b"a\xC0\x80b"] {
        let mut map = TrieMap::new();
        map.insert(key, entry(1.0, None, 0));
        let mut mock = MockRdbIO::default();

        let err = save(&map, &mut mock, RdbOpts::default()).unwrap_err();

        assert_eq!(err, SaveError::DecodesToNul, "key {key:?}");
        assert!(mock.ops.is_empty(), "key {key:?}");
    }
}

#[test]
fn save_rejects_key_ending_in_a_truncated_sequence() {
    let mut map = TrieMap::new();
    map.insert(b"ab\xF0", entry(1.0, None, 0));
    let mut mock = MockRdbIO::default();

    let err = save(&map, &mut mock, RdbOpts::default()).unwrap_err();

    assert_eq!(err, SaveError::TruncatedSequence);
}

#[test]
fn save_accepts_key_at_both_limits() {
    // Astral characters occupy four bytes each, so a key can sit exactly on
    // the byte limit while staying well inside the rune limit — the case
    // that separates rune counting from byte counting.
    let mut map = TrieMap::new();
    let astral = "😀".repeat(MAX_KEY_BYTES / 4);
    map.insert(astral.as_bytes(), entry(1.0, None, 0));
    map.insert(&vec![b'a'; MAX_KEY_RUNES], entry(2.0, None, 0));
    let mut mock = MockRdbIO::default();

    save(&map, &mut mock, RdbOpts::default()).expect("both keys are in domain");

    assert_eq!(mock.ops.first(), Some(&Op::U64(2)));
}

#[test]
fn a_rejected_key_suppresses_the_whole_stream() {
    let mut map = TrieMap::new();
    map.insert(b"fine", entry(1.0, None, 0));
    map.insert(b"", entry(1.0, None, 0));
    let mut mock = MockRdbIO::default();

    save(&map, &mut mock, RdbOpts::default()).unwrap_err();

    assert!(
        mock.ops.is_empty(),
        "validation must precede the count write: {:?}",
        mock.ops
    );
}

#[test]
fn key_terminator_value_is_still_checked_with_payloads_on() {
    let opts = RdbOpts {
        payloads: true,
        num_docs: false,
    };
    let ops = vec![Op::U64(1), Op::Bytes(b"k\xAA".to_vec())];

    let err = load(&mut MockRdbIO::from_ops(ops), opts).unwrap_err();

    assert_eq!(err, RdbError::MissingTrailingNul);
}
