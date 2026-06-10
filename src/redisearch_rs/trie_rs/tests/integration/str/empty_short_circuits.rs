/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! The wrapper short-circuits three iterators when their input is empty,
//! diverging from the inner [`TrieMap`](trie_rs::TrieMap) (whose
//! [`prefixed_iter(&[])`](trie_rs::TrieMap::prefixed_iter) yields every
//! entry).

use trie_rs::str::StrTrieMap;

fn populated() -> StrTrieMap<i32> {
    let mut trie = StrTrieMap::new();
    trie.insert("apple", 1);
    trie.insert("apricot", 2);
    trie.insert("banana", 3);
    trie
}

#[test]
fn prefixed_iter_empty_prefix_yields_nothing() {
    let trie = populated();
    let hits: Vec<_> = trie.prefixed_iter("").collect();
    assert!(hits.is_empty());
}

#[test]
fn contains_iter_empty_target_yields_nothing() {
    let trie = populated();
    let hits: Vec<_> = trie.contains_iter("").collect();
    assert!(hits.is_empty());
}

#[test]
fn suffixed_iter_empty_suffix_yields_nothing() {
    let trie = populated();
    let hits: Vec<_> = trie.suffixed_iter("").collect();
    assert!(hits.is_empty());
}
