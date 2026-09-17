/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use trie_rdb::str_trie_map::{load, load_with, save, save_with};
use trie_rdb::test_utils::{MockRdbIO, Op};
use trie_rdb::{MAX_KEY_RUNES, RdbError, RdbOpts, SaveError, TrieEntry, WireFields, trie_map};
use trie_rs::str_trie_map::StrTrieMap;

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
    let mut mock = MockRdbIO::default();
    save(&map, &mut mock, opts).expect("save should succeed");
    let loaded = load(&mut mock, opts).expect("load should succeed");
    assert_eq!(loaded.len(), 2);
    assert_eq!(loaded.get("alpha"), Some(&entry(1.0, Some(b"p"), 3)));
    assert_eq!(loaded.get("héllo"), Some(&entry(2.5, None, 7)));
}

#[test]
fn invalid_utf8_key_errors() {
    // count=1, key=<two stray high bytes + NUL>, score=1.0
    let ops = vec![Op::U64(1), Op::Bytes(b"\xff\xfe\0".to_vec()), Op::F64(1.0)];
    // Use `match` rather than `unwrap_err`: `StrTrieMap` doesn't impl
    // `Debug`, and adding it just to satisfy the test would force a
    // `Data: Debug` bound on every map instantiation.
    match load(&mut MockRdbIO::from_ops(ops), RdbOpts::default()) {
        Err(RdbError::InvalidUtf8) => {}
        Err(other) => panic!("expected InvalidUtf8, got {other:?}"),
        Ok(_) => panic!("expected InvalidUtf8 error, got Ok"),
    }
}

#[test]
fn unit_payload_roundtrip_str_keys() {
    let mut map = StrTrieMap::new();
    map.insert("héllo", ());
    map.insert("world", ());
    let mut mock = MockRdbIO::default();
    save_with(&map, &mut mock, RdbOpts::default(), |()| WireFields {
        score: 1.0,
        payload: None,
        num_docs: 0,
    })
    .expect("save should succeed");

    let loaded = load_with(&mut mock, RdbOpts::default(), |_| ()).expect("load should succeed");

    assert_eq!(loaded.len(), 2);
    assert_eq!(loaded.get("héllo"), Some(&()));
    assert_eq!(loaded.get("world"), Some(&()));
}

#[test]
fn save_rejects_out_of_domain_key() {
    // A `String` key is UTF-8 but still not necessarily storable by the C
    // trie; the wrapper inherits the byte flavor's domain check.
    let mut map = StrTrieMap::new();
    map.insert(&"a".repeat(MAX_KEY_RUNES + 1), entry(1.0, None, 0));
    let mut mock = MockRdbIO::default();

    let err = save(&map, &mut mock, RdbOpts::default()).unwrap_err();

    assert_eq!(
        err,
        SaveError::TooManyRunes {
            runes: MAX_KEY_RUNES + 1
        }
    );
    assert!(mock.ops.is_empty());
}

#[test]
fn save_rejects_key_with_embedded_nul() {
    // Valid UTF-8 can still carry a literal NUL, the one zero-codepoint
    // encoding the str flavor's keys can reach.
    let mut map = StrTrieMap::new();
    map.insert("a\0b", entry(1.0, None, 0));
    let mut mock = MockRdbIO::default();

    let err = save(&map, &mut mock, RdbOpts::default()).unwrap_err();

    assert_eq!(err, SaveError::DecodesToNul);
    assert!(mock.ops.is_empty());
}

#[test]
fn save_wire_matches_trie_map_api() {
    // Sanity-check: the wrapper produces the same Op trace as the
    // byte-keyed trie_map API for an ASCII key set, since it delegates to
    // crate::trie_map::save.
    let mut str_map = StrTrieMap::new();
    str_map.insert("x", entry(1.0, Some(b"pay"), 7));

    let mut byte_map = trie_rs::TrieMap::new();
    byte_map.insert(b"x", entry(1.0, Some(b"pay"), 7));

    let opts = RdbOpts {
        payloads: true,
        num_docs: true,
    };
    let mut rec_str = MockRdbIO::default();
    let mut rec_bytes = MockRdbIO::default();
    save(&str_map, &mut rec_str, opts).expect("save should succeed");
    trie_map::save(&byte_map, &mut rec_bytes, opts).expect("save should succeed");
    assert_eq!(rec_str.ops, rec_bytes.ops);
}
