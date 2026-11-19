/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Tests for [`KeyedNumericIndex`].

use index_result::RSIndexResult;
use inverted_index::IndexReader as _;
use keyed_numeric_index::KeyedNumericIndex;
use numeric_range_tree::{NumericFilter, NumericRangeTree};

// Link both Rust-provided and C-provided symbols
extern crate redisearch_rs;
// Mock or stub the ones that aren't provided by the line above
redis_mock::mock_or_stub_missing_redis_c_symbols!();

const MAX_DEPTH_RANGE: usize = 0;

/// Every `(doc_id, value)` of `tree` with a value in `[min, max]`, sorted.
///
/// A tree returns whole ranges, which overshoot the filter, so the values are
/// filtered again here.
fn matches(tree: &NumericRangeTree, min: f64, max: f64) -> Vec<(u64, f64)> {
    let filter = NumericFilter {
        min,
        max,
        ..Default::default()
    };
    let mut found = Vec::new();
    for range in tree.find(&filter) {
        let mut reader = range.reader();
        let mut result = RSIndexResult::build_numeric(0.0).build();
        while reader.next_record(&mut result).unwrap_or(false) {
            // SAFETY: a numeric index only ever holds numeric records
            let value = unsafe { result.as_numeric_unchecked() };
            if (min..=max).contains(&value) {
                found.push((result.doc_id, value));
            }
        }
    }
    found.sort_by(|a, b| a.partial_cmp(b).unwrap());
    found
}

fn add(index: &mut KeyedNumericIndex, doc_id: u64, key: &[u8], value: f64) {
    index.add(doc_id, key, value, false, MAX_DEPTH_RANGE);
}

#[test]
fn new_index_is_empty() {
    let index = KeyedNumericIndex::new(false);
    assert_eq!(index.num_keys(), 0);
    assert!(index.get(b"rev").is_none());
    assert_eq!(index.memory_usage(), index.computed_memory_usage());
}

#[test]
fn unique_ids_differ() {
    let a = KeyedNumericIndex::new(false);
    let b = KeyedNumericIndex::new(false);
    assert_ne!(u32::from(a.unique_id()), u32::from(b.unique_id()));
}

#[test]
fn keys_are_independent() {
    let mut index = KeyedNumericIndex::new(false);
    add(&mut index, 1, b"rev", 10.0);
    add(&mut index, 1, b"cost", 99.0);
    add(&mut index, 2, b"rev", 20.0);

    assert_eq!(index.num_keys(), 2);
    let rev = index.get(b"rev").unwrap();
    assert_eq!(
        matches(rev, f64::NEG_INFINITY, f64::INFINITY),
        [(1, 10.0), (2, 20.0)]
    );
    let cost = index.get(b"cost").unwrap();
    assert_eq!(matches(cost, f64::NEG_INFINITY, f64::INFINITY), [(1, 99.0)]);
    assert!(index.get(b"missing").is_none());
}

#[test]
fn empty_key_is_a_valid_key() {
    let mut index = KeyedNumericIndex::new(false);
    add(&mut index, 1, b"", 5.0);
    assert_eq!(matches(index.get(b"").unwrap(), 0.0, 10.0), [(1, 5.0)]);
}

#[test]
fn memory_usage_tracks_adds() {
    let mut index = KeyedNumericIndex::new(false);
    let mut last = index.memory_usage();
    for doc_id in 1..=5_000u64 {
        let key = [b"a".as_slice(), b"b", b"c"][doc_id as usize % 3];
        add(&mut index, doc_id, key, doc_id as f64);
        assert_eq!(index.memory_usage(), index.computed_memory_usage());
        assert!(index.memory_usage() >= last, "adds never shrink the index");
        last = index.memory_usage();
    }
}

#[test]
fn remove_empty_keys_drops_only_empty_trees() {
    let mut index = KeyedNumericIndex::new(false);
    add(&mut index, 1, b"kept", 1.0);
    add(&mut index, 1, b"emptied", 2.0);
    add(&mut index, 2, b"emptied", 3.0);
    let with_both = index.memory_usage();

    assert_eq!(index.remove_empty_keys(), 0, "no tree is empty yet");
    assert_eq!(index.memory_usage(), with_both);

    // Doc 1 and 2 are gone from "emptied"; "kept" still holds doc 1.
    index
        .update_tree(b"emptied", |tree| {
            *tree = NumericRangeTree::new(false);
        })
        .expect("the key exists");
    assert_eq!(index.memory_usage(), index.computed_memory_usage());

    assert_eq!(index.remove_empty_keys(), 1);
    assert_eq!(index.num_keys(), 1);
    assert!(index.get(b"emptied").is_none());
    assert!(index.get(b"kept").is_some());
    assert!(index.memory_usage() < with_both);
    assert_eq!(index.memory_usage(), index.computed_memory_usage());

    // A removed key comes back as a fresh tree.
    add(&mut index, 3, b"emptied", 4.0);
    assert_eq!(
        matches(index.get(b"emptied").unwrap(), 0.0, 10.0),
        [(3, 4.0)]
    );
    assert_eq!(index.memory_usage(), index.computed_memory_usage());
}

#[test]
fn update_tree_on_missing_key_does_not_call_the_closure() {
    let mut index = KeyedNumericIndex::new(false);
    assert!(index.update_tree(b"nope", |_| unreachable!()).is_none());
}

#[test]
fn tree_address_is_stable_while_the_map_grows() {
    let mut index = KeyedNumericIndex::new(false);
    add(&mut index, 1, b"first", 1.0);
    let before = index.get(b"first").unwrap() as *const NumericRangeTree;
    for i in 0..1_000u32 {
        add(&mut index, 1, format!("key{i}").as_bytes(), 1.0);
    }
    assert_eq!(
        index.get(b"first").unwrap() as *const NumericRangeTree,
        before
    );
}

#[test]
fn iter_visits_every_key() {
    let mut index = KeyedNumericIndex::new(false);
    for (i, key) in [b"x".as_slice(), b"y", b"z"].into_iter().enumerate() {
        add(&mut index, i as u64 + 1, key, 1.0);
    }
    let mut keys: Vec<_> = index.iter().map(|(key, _)| key.to_vec()).collect();
    keys.sort();
    assert_eq!(keys, [b"x".to_vec(), b"y".to_vec(), b"z".to_vec()]);
}

mod properties {
    use super::*;
    use proptest::prelude::*;

    const KEYS: [&[u8]; 4] = [b"rev:fy0", b"rev:fym1", b"headcount:m2026_06", b""];

    proptest! {
        /// For every key, the range query returns exactly the inserted pairs that fall in the range.
        #[test]
        #[cfg_attr(miri, ignore = "Too slow to run under miri")]
        fn range_results_match_brute_force(
            entries in proptest::collection::vec((1u64..4, 0..KEYS.len(), -500.0f64..500.0), 1..600),
            ranges in proptest::collection::vec((-600.0f64..600.0, 0.0f64..1200.0), 1..8),
        ) {
            let mut index = KeyedNumericIndex::new(false);
            let mut inserted: Vec<(u64, usize, f64)> = Vec::new();
            let mut doc_id = 0;
            for (step, key_idx, value) in entries {
                // Doc ids ascend; a doc carries each key at most once, as a JSON object does.
                doc_id += step;
                index.add(doc_id, KEYS[key_idx], value, false, MAX_DEPTH_RANGE);
                inserted.push((doc_id, key_idx, value));
            }
            prop_assert_eq!(index.memory_usage(), index.computed_memory_usage());

            for (key_idx, key) in KEYS.iter().enumerate() {
                for &(min, width) in &ranges {
                    let max = min + width;
                    let mut expected: Vec<(u64, f64)> = inserted
                        .iter()
                        .filter(|&&(_, k, v)| k == key_idx && (min..=max).contains(&v))
                        .map(|&(d, _, v)| (d, v))
                        .collect();
                    expected.sort_by(|a, b| a.partial_cmp(b).unwrap());
                    let actual = index.get(key).map_or_else(Vec::new, |tree| matches(tree, min, max));
                    prop_assert_eq!(actual, expected);
                }
            }
        }
    }
}
