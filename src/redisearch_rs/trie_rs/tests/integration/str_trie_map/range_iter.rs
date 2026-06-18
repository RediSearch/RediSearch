/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use trie_rs::str_trie_map::StrTrieMap;
use trie_rs::str_trie_map::iter::{RangeBoundary, RangeFilter};

fn populated() -> StrTrieMap<i32> {
    let mut trie = StrTrieMap::new();
    trie.insert("apple", 1);
    trie.insert("apricot", 2);
    trie.insert("banana", 3);
    trie.insert("cherry", 4);
    trie.insert("date", 5);
    trie
}

fn collect(trie: &StrTrieMap<i32>, filter: RangeFilter<'_>) -> Vec<(String, i32)> {
    trie.range_iter(filter).map(|(k, v)| (k, *v)).collect()
}

#[test]
fn range_all_yields_every_entry_in_order() {
    let trie = populated();
    let hits = collect(&trie, RangeFilter::all());
    assert_eq!(
        hits,
        vec![
            ("apple".to_string(), 1),
            ("apricot".to_string(), 2),
            ("banana".to_string(), 3),
            ("cherry".to_string(), 4),
            ("date".to_string(), 5),
        ],
    );
}

#[test]
fn range_inclusive_both_ends() {
    let trie = populated();
    let hits = collect(
        &trie,
        RangeFilter {
            min: Some(RangeBoundary::included("apricot")),
            max: Some(RangeBoundary::included("cherry")),
        },
    );
    assert_eq!(
        hits,
        vec![
            ("apricot".to_string(), 2),
            ("banana".to_string(), 3),
            ("cherry".to_string(), 4),
        ],
    );
}

#[test]
fn range_exclusive_both_ends() {
    let trie = populated();
    let hits = collect(
        &trie,
        RangeFilter {
            min: Some(RangeBoundary::excluded("apricot")),
            max: Some(RangeBoundary::excluded("cherry")),
        },
    );
    assert_eq!(hits, vec![("banana".to_string(), 3)]);
}

#[test]
fn range_unbounded_min() {
    let trie = populated();
    let hits = collect(
        &trie,
        RangeFilter {
            min: None,
            max: Some(RangeBoundary::included("banana")),
        },
    );
    assert_eq!(
        hits,
        vec![
            ("apple".to_string(), 1),
            ("apricot".to_string(), 2),
            ("banana".to_string(), 3),
        ],
    );
}

#[test]
fn range_unbounded_max() {
    let trie = populated();
    let hits = collect(
        &trie,
        RangeFilter {
            min: Some(RangeBoundary::excluded("banana")),
            max: None,
        },
    );
    assert_eq!(
        hits,
        vec![("cherry".to_string(), 4), ("date".to_string(), 5)],
    );
}

#[test]
fn range_multi_byte_utf8_bound() {
    let mut trie = StrTrieMap::new();
    trie.insert("cafe", 1);
    trie.insert("café", 2);
    trie.insert("cafés", 3);
    trie.insert("zebra", 4);

    let hits = collect(
        &trie,
        RangeFilter {
            min: Some(RangeBoundary::included("café")),
            max: Some(RangeBoundary::included("cafés")),
        },
    );
    assert_eq!(
        hits,
        vec![("café".to_string(), 2), ("cafés".to_string(), 3)],
    );
}
