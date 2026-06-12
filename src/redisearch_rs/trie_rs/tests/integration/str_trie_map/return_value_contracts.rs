/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use trie_rs::str_trie_map::StrTrieMap;

#[test]
fn insert_returns_none_on_new_and_some_on_update() {
    let mut trie = StrTrieMap::new();
    assert_eq!(trie.insert("k", 1), None);
    assert_eq!(trie.insert("k", 2), Some(1));
    assert_eq!(trie.get("k"), Some(&2));
    assert_eq!(trie.len(), 1);
}

#[test]
fn insert_with_callback_receives_existing_value_on_reinsert() {
    let mut trie = StrTrieMap::<i32>::new();

    trie.insert_with("k", |prev| {
        assert!(prev.is_none());
        10
    });
    trie.insert_with("k", |prev| {
        assert_eq!(prev, Some(10));
        prev.unwrap() + 5
    });

    assert_eq!(trie.get("k"), Some(&15));
    assert_eq!(trie.len(), 1);
}

#[test]
fn remove_returns_value_on_hit_and_none_on_miss() {
    let mut trie = StrTrieMap::new();
    trie.insert("k", 42);

    assert_eq!(trie.remove("absent"), None);
    assert_eq!(trie.remove("k"), Some(42));
    assert_eq!(trie.remove("k"), None);
    assert!(trie.is_empty());
}

#[test]
fn get_mut_mutates_stored_value() {
    let mut trie = StrTrieMap::new();
    trie.insert("counter", 0_i32);

    *trie.get_mut("counter").unwrap() += 1;
    *trie.get_mut("counter").unwrap() += 1;

    assert_eq!(trie.get("counter"), Some(&2));
    assert!(trie.get_mut("absent").is_none());
}
