/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Unit-style tests for [`KeyedNumericIndex`].

use crate::helpers::MAX_DEPTH_RANGE;
use crate::helpers::{add, all, inclusive, matches};
use keyed_numeric_index::{KeyedNumericBatch, KeyedNumericIndex};
use numeric_range_tree::NumericRangeTree;
use numeric_range_tree::test_utils::gc_all_ranges;

fn assert_memory_consistent(index: &KeyedNumericIndex) {
    assert_eq!(index.memory_usage(), index.computed_memory_usage());
}

/// Delete every document of `key`'s tree through the tree's GC delta-apply path.
fn gc_away_every_doc(index: &mut KeyedNumericIndex, key: &[u8]) {
    let id = index.get(key).unwrap().unique_id();
    index
        .update_tree(key, id, |tree| gc_all_ranges(tree, &|_| false))
        .expect("the key exists");
}

#[test]
fn new_index_is_empty() {
    let index = KeyedNumericIndex::new(false);
    assert_eq!(index.num_keys(), 0);
    assert!(index.get(b"rev").is_none());
    assert_memory_consistent(&index);
}

#[test]
fn unique_ids_differ() {
    let a = KeyedNumericIndex::new(false);
    let b = KeyedNumericIndex::new(false);
    assert_ne!(a.unique_id(), b.unique_id());
}

#[test]
fn keys_are_independent() {
    let mut index = KeyedNumericIndex::new(false);
    add(&mut index, 1, b"rev", 10.0);
    add(&mut index, 1, b"cost", 99.0);
    add(&mut index, 2, b"rev", 20.0);

    assert_eq!(index.num_keys(), 2);
    assert_eq!(all(&index, b"rev"), [(1, 10.0), (2, 20.0)]);
    assert_eq!(all(&index, b"cost"), [(1, 99.0)]);
    assert!(index.get(b"missing").is_none());
}

#[test]
fn empty_key_is_a_valid_key() {
    let mut index = KeyedNumericIndex::new(false);
    add(&mut index, 1, b"", 5.0);
    assert_eq!(all(&index, b""), [(1, 5.0)]);
}

#[test]
fn memory_usage_tracks_adds() {
    let mut index = KeyedNumericIndex::new(false);
    let mut last = index.memory_usage();
    for doc_id in 1..=5_000u64 {
        let key = [b"a".as_slice(), b"b", b"c"][doc_id as usize % 3];
        add(&mut index, doc_id, key, doc_id as f64);
        assert_memory_consistent(&index);
        assert!(index.memory_usage() >= last, "adds never shrink the index");
        last = index.memory_usage();
    }
}

#[test]
fn memory_usage_covers_the_tables_growth() {
    let mut index = KeyedNumericIndex::new(false);
    for i in 0..500u32 {
        add(&mut index, 1, format!("key{i}").as_bytes(), 1.0);
    }
    assert_memory_consistent(&index);
    let trees_and_keys: usize = index
        .iter()
        .map(|(key, tree)| key.len() + tree.mem_usage())
        .sum();
    assert!(index.memory_usage() > trees_and_keys + std::mem::size_of::<KeyedNumericIndex>());
}

#[test]
fn a_repeated_document_is_dropped() {
    let mut index = KeyedNumericIndex::new(false);
    add(&mut index, 1, b"rev", 10.0);
    add(&mut index, 2, b"rev", 20.0);
    add(&mut index, 2, b"rev", 30.0);
    assert_eq!(all(&index, b"rev"), [(1, 10.0), (2, 20.0)]);
    assert_memory_consistent(&index);
}

#[test]
fn a_rejected_first_add_creates_no_key() {
    let mut index = KeyedNumericIndex::new(false);
    let before = index.memory_usage();
    // The tree rejects document 0 as it does a repeat.
    add(&mut index, 0, b"rev", 1.0);
    assert_eq!(index.num_keys(), 0);
    assert!(index.get(b"rev").is_none());
    assert_eq!(index.memory_usage(), before);
    assert_memory_consistent(&index);
}

#[test]
fn nan_is_rejected_and_infinities_are_kept() {
    let mut index = KeyedNumericIndex::new(false);
    add(&mut index, 1, b"nan_only", f64::NAN);
    assert_eq!(index.num_keys(), 0, "NaN must not create a key");

    add(&mut index, 1, b"rev", f64::NEG_INFINITY);
    add(&mut index, 2, b"rev", f64::NAN);
    add(&mut index, 3, b"rev", f64::INFINITY);
    assert_eq!(
        all(&index, b"rev"),
        [(1, f64::NEG_INFINITY), (3, f64::INFINITY)]
    );
    assert_memory_consistent(&index);
}

#[test]
fn compressed_floats_are_stored_compressed() {
    let value = 327.479_801_201_175_5;
    let mut plain = KeyedNumericIndex::new(false);
    let mut compressed = KeyedNumericIndex::new(true);
    add(&mut plain, 1, b"k", value);
    add(&mut compressed, 1, b"k", value);

    assert_eq!(all(&plain, b"k"), [(1, value)]);
    let stored = all(&compressed, b"k");
    assert_eq!(stored.len(), 1);
    assert_eq!(stored[0].0, 1);
    assert!((stored[0].1 - value).abs() < 1e-3, "{}", stored[0].1);
    assert_ne!(stored[0].1, value, "the stored value is the lossy one");
    assert_memory_consistent(&compressed);
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

    gc_away_every_doc(&mut index, b"emptied");
    assert_eq!(index.get(b"emptied").unwrap().num_entries(), 0);
    assert_memory_consistent(&index);

    assert_eq!(index.remove_empty_keys(), 1);
    assert_eq!(index.num_keys(), 1);
    assert!(index.get(b"emptied").is_none());
    assert!(index.get(b"kept").is_some());
    assert!(index.memory_usage() < with_both);
    assert_memory_consistent(&index);

    // A removed key comes back as a fresh tree.
    add(&mut index, 3, b"emptied", 4.0);
    assert_eq!(all(&index, b"emptied"), [(3, 4.0)]);
    assert_memory_consistent(&index);
}

#[test]
fn remove_key_if_empty_checks_emptiness_and_identity() {
    let mut index = KeyedNumericIndex::new(false);
    add(&mut index, 1, b"a", 1.0);
    add(&mut index, 1, b"b", 1.0);
    let a_id = index.get(b"a").unwrap().unique_id();
    let b_id = index.get(b"b").unwrap().unique_id();

    assert!(!index.remove_key_if_empty(b"missing", a_id));
    assert!(!index.remove_key_if_empty(b"a", a_id), "a has entries");

    gc_away_every_doc(&mut index, b"a");
    assert!(!index.remove_key_if_empty(b"a", b_id), "wrong identity");
    assert_eq!(index.num_keys(), 2);
    assert!(index.remove_key_if_empty(b"a", a_id));
    assert_eq!(index.num_keys(), 1);
    assert!(index.get(b"b").is_some(), "other keys are untouched");
    assert_memory_consistent(&index);
}

#[test]
fn a_recreated_key_is_a_different_tree() {
    let mut index = KeyedNumericIndex::new(false);
    add(&mut index, 1, b"rev", 1.0);
    let old = index.get(b"rev").unwrap().unique_id();
    assert!(index.get_checked(b"rev", old).is_some());

    gc_away_every_doc(&mut index, b"rev");
    assert!(index.remove_key_if_empty(b"rev", old));
    assert!(index.get_checked(b"rev", old).is_none());

    add(&mut index, 2, b"rev", 2.0);
    let new = index.get(b"rev").unwrap().unique_id();
    assert_ne!(old, new);
    assert!(index.get_checked(b"rev", old).is_none());
    assert!(index.get_checked(b"rev", new).is_some());
    assert!(index.get_checked(b"missing", new).is_none());
}

#[test]
fn update_tree_requires_the_expected_tree() {
    let mut index = KeyedNumericIndex::new(false);
    add(&mut index, 1, b"a", 1.0);
    let other = NumericRangeTree::new(false).unique_id();
    let id = index.get(b"a").unwrap().unique_id();

    assert!(index.update_tree(b"nope", id, |_| unreachable!()).is_none());
    assert!(index.update_tree(b"a", other, |_| unreachable!()).is_none());
    assert_eq!(
        index.update_tree(b"a", id, |tree| tree.num_entries()),
        Some(1)
    );
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

#[test]
fn range_queries_respect_inclusivity() {
    let mut index = KeyedNumericIndex::new(false);
    for (doc, v) in [(1, 1.0), (2, 2.0), (3, 3.0)] {
        add(&mut index, doc, b"k", v);
    }
    let tree = index.get(b"k").unwrap();
    let mut filter = inclusive(1.0, 3.0);
    assert_eq!(matches(tree, &filter).len(), 3);
    filter.min_inclusive = false;
    filter.max_inclusive = false;
    assert_eq!(matches(tree, &filter), [(2, 2.0)]);
}

#[test]
fn memory_usage_stays_exact_across_scattered_removals() {
    let mut index = KeyedNumericIndex::new(false);
    for i in 0..3000u32 {
        add(&mut index, 1, format!("key{i}").as_bytes(), 1.0);
    }
    assert_memory_consistent(&index);
    for i in (0..3000u32).filter(|i| i % 7 != 0) {
        let key = format!("key{i}");
        gc_away_every_doc(&mut index, key.as_bytes());
        assert_memory_consistent(&index);
        let id = index.get(key.as_bytes()).unwrap().unique_id();
        assert!(index.remove_key_if_empty(key.as_bytes(), id));
        assert_memory_consistent(&index);
    }
}

#[test]
fn memory_usage_stays_exact_after_remove_empty_keys() {
    let mut index = KeyedNumericIndex::new(false);
    for i in 0..3000u32 {
        add(&mut index, 1, format!("key{i}").as_bytes(), 1.0);
    }
    for i in (0..3000u32).filter(|i| i % 3 != 0) {
        gc_away_every_doc(&mut index, format!("key{i}").as_bytes());
    }
    assert_memory_consistent(&index);
    assert_eq!(index.remove_empty_keys(), 2000);
    assert_memory_consistent(&index);
}

#[test]
fn add_batch_adds_every_member_and_sums_the_changes() {
    let mut index = KeyedNumericIndex::new(false);
    let mut batch = KeyedNumericBatch::new();
    batch.push(b"rev", 1.0);
    batch.push(b"cost", -2.5);
    assert_eq!(batch.len(), 2);

    let result = index.add_batch(7, &batch, false, MAX_DEPTH_RANGE);

    assert_eq!(result.num_records_delta, 2);
    assert_eq!(all(&index, b"rev"), vec![(7, 1.0)]);
    assert_eq!(all(&index, b"cost"), vec![(7, -2.5)]);
    assert_memory_consistent(&index);
}

#[test]
fn an_empty_batch_changes_nothing() {
    let mut index = KeyedNumericIndex::new(false);
    let batch = KeyedNumericBatch::new();
    assert!(batch.is_empty());

    let result = index.add_batch(1, &batch, false, MAX_DEPTH_RANGE);

    assert_eq!(result, Default::default());
    assert_eq!(index.num_keys(), 0);
}
