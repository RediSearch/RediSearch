/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use trie_rs::str_trie_map::StrTrieMap;

fn collect(trie: &StrTrieMap<i32>, needle: &str) -> Vec<(String, i32)> {
    trie.case_insensitive_iter(needle)
        .map(|(k, v)| (k, *v))
        .collect()
}

#[test]
fn matches_all_case_variants_of_the_needle() {
    let mut trie = StrTrieMap::new();
    trie.insert("hello", 1);
    trie.insert("Hello", 2);
    trie.insert("HELLO", 3);
    trie.insert("hell", 4);
    trie.insert("hellos", 5);

    let expected = vec![
        ("HELLO".to_string(), 3),
        ("Hello".to_string(), 2),
        ("hello".to_string(), 1),
    ];
    assert_eq!(collect(&trie, "hello"), expected);
    assert_eq!(collect(&trie, "HeLlO"), expected);
}

#[test]
fn no_match_yields_nothing() {
    let mut trie = StrTrieMap::new();
    trie.insert("hello", 1);

    assert!(collect(&trie, "help").is_empty());
    assert!(collect(&trie, "hell").is_empty());
    assert!(collect(&trie, "helloo").is_empty());
    assert!(collect(&trie, "").is_empty());
    assert!(collect(&StrTrieMap::new(), "hello").is_empty());
}

#[test]
fn multibyte_keys_match_case_insensitively() {
    let mut trie = StrTrieMap::new();
    trie.insert("über", 1);
    trie.insert("Über", 2);
    trie.insert("uber", 3);

    assert_eq!(
        collect(&trie, "ÜBER"),
        vec![("Über".to_string(), 2), ("über".to_string(), 1)],
    );
}

#[test]
fn node_splits_inside_a_codepoint_are_handled() {
    // 'è' (C3 A8) and 'é' (C3 A9) share their first byte, so inserting both
    // splits a trie node in the middle of the codepoint. The automaton must
    // carry the partial codepoint across the edge boundary.
    let mut trie = StrTrieMap::new();
    trie.insert("cafè", 1);
    trie.insert("café", 2);

    assert_eq!(collect(&trie, "CAFÉ"), vec![("café".to_string(), 2)]);
    assert_eq!(collect(&trie, "CAFÈ"), vec![("cafè".to_string(), 1)]);
}

#[test]
fn one_to_many_folding_matches_expanded_needle() {
    // 'İ' folds to "i" + U+0307; a needle already in folded form must match
    // the stored uppercase key.
    let mut trie = StrTrieMap::new();
    trie.insert("İstanbul", 1);

    assert_eq!(
        collect(&trie, "i\u{307}stanbul"),
        vec![("İstanbul".to_string(), 1)],
    );
    assert!(collect(&trie, "istanbul").is_empty());
}
