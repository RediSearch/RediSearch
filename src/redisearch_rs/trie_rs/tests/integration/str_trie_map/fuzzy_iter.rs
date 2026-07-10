/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use trie_rs::str_trie_map::StrTrieMap;

fn collect(trie: &StrTrieMap<i32>, needle: &str, max_dist: u32) -> Vec<String> {
    trie.fuzzy_iter(needle, max_dist).map(|(k, _)| k).collect()
}

#[test]
fn distance_bounds_matches() {
    let mut trie = StrTrieMap::new();
    trie.insert("hello", 1);
    trie.insert("hell", 2);
    trie.insert("help", 3);
    trie.insert("world", 4);

    assert_eq!(collect(&trie, "hello", 0), vec!["hello"]);
    assert_eq!(collect(&trie, "hello", 1), vec!["hell", "hello"]);
    assert_eq!(collect(&trie, "hello", 2), vec!["hell", "hello", "help"]);
    assert!(collect(&trie, "hello", 2).iter().all(|k| k != "world"));
}

#[test]
fn matching_folds_case_and_yields_stored_case() {
    let mut trie = StrTrieMap::new();
    trie.insert("Hello", 1);
    trie.insert("HELP", 2);

    assert_eq!(collect(&trie, "hellp", 1), vec!["HELP", "Hello"]);
}

#[test]
fn descendants_of_a_match_can_also_match() {
    // "hell" matches at distance 1 and so does its extension "hello" at 0 —
    // the traversal must keep descending past an accepting node.
    let mut trie = StrTrieMap::new();
    trie.insert("hell", 1);
    trie.insert("hello", 2);
    trie.insert("hellos", 3);

    assert_eq!(collect(&trie, "hello", 1), vec!["hell", "hello", "hellos"]);
}

#[test]
fn multibyte_needles_measure_codepoints() {
    let mut trie = StrTrieMap::new();
    trie.insert("café", 1);
    trie.insert("cafe", 2);

    // 'é' → 'e' is a single substitution even though the byte length differs.
    assert_eq!(collect(&trie, "CAFÉ", 1), vec!["cafe", "café"]);
    assert_eq!(collect(&trie, "café", 0), vec!["café"]);
}

#[test]
fn empty_trie_and_no_matches_yield_nothing() {
    assert!(collect(&StrTrieMap::new(), "hello", 3).is_empty());

    let mut trie = StrTrieMap::new();
    trie.insert("completely", 1);
    assert!(collect(&trie, "different", 2).is_empty());
}
