/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{collections::BTreeMap, ffi::c_void};

use proptest::prelude::*;
use trie_rdb::{
    RdbError,
    test_utils::{MockRdbIO, Op},
};
use triemap_ffi::{TrieMap, rdb, tm_len_t};

redis_mock::mock_or_stub_missing_redis_c_symbols!();

fn count_map(entries: &BTreeMap<Vec<u8>, usize>) -> TrieMap {
    let mut map = TrieMap(trie_rs::TrieMap::new());
    for (key, &count) in entries {
        map.insert(key, std::ptr::without_provenance_mut::<c_void>(count));
    }
    map
}

fn entry_ops(key: Vec<u8>, count: u64) -> Vec<Op> {
    vec![Op::U64(1), Op::Bytes(key), Op::F64(0.0), Op::U64(count)]
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: if cfg!(miri) { 4 } else { 64 },
        failure_persistence: None,
        ..ProptestConfig::default()
    })]

    #[test]
    fn count_dictionary_round_trips(
        entries in proptest::collection::btree_map(
            proptest::collection::vec(any::<u8>(), 0..64),
            1usize..=usize::MAX,
            0..24,
        ),
    ) {
        let map = count_map(&entries);
        let mut io = MockRdbIO::default();
        rdb::save(&map, &mut io);
        let restored = rdb::load(&mut io).unwrap();
        prop_assert_eq!(restored.0.n_unique_keys(), entries.len());
        for (key, count) in entries {
            prop_assert_eq!(restored.find(&key).unwrap().as_ptr().addr(), count);
        }
    }
}

#[test]
fn empty_dictionary_has_only_the_entry_count() {
    let map = count_map(&BTreeMap::new());
    let mut io = MockRdbIO::default();
    rdb::save(&map, &mut io);
    assert_eq!(io.ops, [Op::U64(0)]);
    assert_eq!(rdb::load(&mut io).unwrap().0.n_unique_keys(), 0);
}

#[test]
fn saves_counts_in_the_existing_num_docs_field() {
    let map = count_map(&BTreeMap::from([(b"tag".to_vec(), 7)]));
    let mut io = MockRdbIO::default();
    rdb::save(&map, &mut io);
    assert_eq!(io.ops, entry_ops(b"tag\0".to_vec(), 7));
}

#[test]
fn maximum_key_length_and_pointer_count_round_trip() {
    let key = vec![b'x'; usize::from(tm_len_t::MAX)];
    let map = count_map(&BTreeMap::from([(key.clone(), usize::MAX)]));
    let mut io = MockRdbIO::default();
    rdb::save(&map, &mut io);
    let restored = rdb::load(&mut io).unwrap();
    assert_eq!(restored.find(&key).unwrap().as_ptr().addr(), usize::MAX);
}

#[test]
fn malformed_entries_are_rejected() {
    let mut oversized = vec![b'x'; usize::from(tm_len_t::MAX) + 1];
    oversized.push(0);
    let cases = [
        (
            "no terminator slot",
            entry_ops(vec![], 1),
            RdbError::MissingTrailingNul,
        ),
        (
            "non-NUL terminator",
            entry_ops(b"tagX".to_vec(), 1),
            RdbError::MissingTrailingNul,
        ),
        (
            "zero count",
            entry_ops(b"tag\0".to_vec(), 0),
            RdbError::InvalidEntry,
        ),
        (
            "oversized key",
            entry_ops(oversized, 1),
            RdbError::InvalidEntry,
        ),
    ];
    for (name, ops, expected) in cases {
        let mut io = MockRdbIO::from_ops(ops);
        assert_eq!(rdb::load(&mut io).err(), Some(expected), "{name}");
    }
}

#[test]
fn interrupted_loads_discard_partially_built_dictionaries() {
    let map = count_map(&BTreeMap::from([
        (b"first".to_vec(), 1),
        (b"second".to_vec(), 2),
    ]));
    let mut saved = MockRdbIO::default();
    rdb::save(&map, &mut saved);
    for successful_reads in 0..saved.ops.len() {
        let mut io = MockRdbIO::from_ops(saved.ops.clone()).fail_after(successful_reads);
        assert_eq!(
            rdb::load(&mut io).err(),
            Some(RdbError::Io),
            "after {successful_reads} reads"
        );
    }
}
