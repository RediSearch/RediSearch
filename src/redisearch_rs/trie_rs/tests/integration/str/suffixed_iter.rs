/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use trie_rs::str::StrTrieMap;

#[test]
fn suffixed_iter_matches_keys_ending_with_target() {
    let mut trie = StrTrieMap::new();
    trie.insert("apple", 1);
    trie.insert("pineapple", 2);
    trie.insert("banana", 3);
    trie.insert("application", 4);

    let mut hits: Vec<(String, i32)> = trie.suffixed_iter("apple").map(|(k, v)| (k, *v)).collect();
    hits.sort();
    assert_eq!(
        hits,
        vec![("apple".to_string(), 1), ("pineapple".to_string(), 2)],
    );
}
