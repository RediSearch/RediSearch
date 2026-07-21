/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use trie_rs::str_trie_map::StrTrieMap;
use trie_rs::str_trie_map::automaton::levenshtein_nfa::MAX_NFA_DIST;

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
fn distance_past_nfa_bound_takes_dp_backend() {
    // `max_dist > MAX_NFA_DIST` exceeds every NFA width, routing
    // `fuzzy_iter` to the DP-row backend.
    let mut trie = StrTrieMap::new();
    trie.insert("hello", 1);
    trie.insert("hellos", 2);
    trie.insert("abcdefghijk", 3);

    // dist("hello", "abcdefghijk") ≥ 6 (length difference alone).
    assert_eq!(
        collect(&trie, "hello", MAX_NFA_DIST + 1),
        vec!["hello", "hellos"],
    );
}

#[test]
fn needle_past_u64_width_takes_u128_backend() {
    // 70 folded codepoints need 71 positions: past u64, within u128.
    let needle = "a".repeat(70);
    let mut trie = StrTrieMap::new();
    trie.insert(&"a".repeat(68), 1);
    trie.insert(&needle, 2);
    trie.insert(&format!("{needle}b"), 3);
    trie.insert(&"b".repeat(70), 4);

    let expected = vec!["a".repeat(68), needle.clone(), format!("{needle}b")];
    assert_eq!(collect(&trie, &needle, 2), expected);
}

#[test]
fn empty_trie_and_no_matches_yield_nothing() {
    assert!(collect(&StrTrieMap::new(), "hello", 3).is_empty());

    let mut trie = StrTrieMap::new();
    trie.insert("completely", 1);
    assert!(collect(&trie, "different", 2).is_empty());
}
