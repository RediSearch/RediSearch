/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Empty search input matches every entry: the empty string is a prefix,
//! suffix, and substring of every key — the same semantics as the inner
//! [`TrieMap`](trie_rs::TrieMap).

use trie_rs::str_trie_map::StrTrieMap;

fn populated() -> StrTrieMap<i32> {
    let mut trie = StrTrieMap::new();
    trie.insert("apple", 1);
    trie.insert("apricot", 2);
    trie.insert("banana", 3);
    trie
}

#[test]
fn prefixed_iter_empty_prefix_yields_every_entry() {
    let trie = populated();
    let hits: Vec<_> = trie.prefixed_iter("").collect();
    assert_eq!(hits.len(), trie.len());
}

#[test]
fn prefixed_values_empty_prefix_yields_every_value() {
    let trie = populated();
    let hits: Vec<_> = trie.prefixed_values("").collect();
    assert_eq!(hits.len(), trie.len());
}

#[test]
fn contains_iter_empty_target_yields_every_entry() {
    let trie = populated();
    let hits: Vec<_> = trie.contains_iter("").collect();
    assert_eq!(hits.len(), trie.len());
}

#[test]
fn suffixed_iter_empty_suffix_yields_every_entry() {
    let trie = populated();
    let hits: Vec<_> = trie.suffixed_iter("").collect();
    assert_eq!(hits.len(), trie.len());
}
