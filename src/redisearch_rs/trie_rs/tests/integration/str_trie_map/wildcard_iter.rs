/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! [`StrTrieMap::wildcard_iter`]: codepoint-semantics wildcard over the trie,
//! cross-checked against a full-scan oracle built from
//! [`CodepointWildcard::matches`].

use trie_rs::str_trie_map::StrTrieMap;
use trie_rs::str_trie_map::automaton::CodepointWildcard;

fn build_trie() -> StrTrieMap<u32> {
    let mut trie = StrTrieMap::new();
    for (i, word) in [
        "",
        "entre",
        "entré",
        "entrée",
        "café",
        "cafe",
        "caffe",
        "日本",
        "日本語",
        "ένταξη",
    ]
    .into_iter()
    .enumerate()
    {
        trie.insert(word, i as u32);
    }
    trie
}

fn oracle(trie: &StrTrieMap<u32>, pattern: &str) -> Vec<String> {
    let parsed = CodepointWildcard::parse(pattern);
    trie.iter()
        .filter(|(k, _)| parsed.matches(k))
        .map(|(k, _)| k)
        .collect()
}

fn specialized(trie: &StrTrieMap<u32>, pattern: &str) -> Vec<String> {
    trie.wildcard_iter(pattern).map(|(k, _)| k).collect()
}

#[test]
fn question_mark_consumes_one_codepoint() {
    let trie = build_trie();

    assert_eq!(specialized(&trie, "entr?"), vec!["entre", "entré"]);
    assert_eq!(specialized(&trie, "entr??"), vec!["entrée"]);
    assert_eq!(specialized(&trie, "日本?"), vec!["日本語"]);
}

#[test]
fn agrees_with_oracle_on_seed_patterns() {
    let trie = build_trie();

    for pattern in [
        "*", "", "entr?", "entr*", "*é", "*é*", "caf?", "caf*", "?????", "日*", "*語", " έ*",
        "*n*", "xyz*",
    ] {
        assert_eq!(
            specialized(&trie, pattern),
            oracle(&trie, pattern),
            "pattern {pattern:?}",
        );
    }
}

#[test]
fn oversized_pattern_routes_to_filter_fallback_and_agrees() {
    let trie = build_trie();

    // > 127 atoms: forced onto the per-key filter backend.
    let no_match = format!("{}*", "?".repeat(200));
    assert_eq!(specialized(&trie, &no_match), oracle(&trie, &no_match));
    assert!(specialized(&trie, &no_match).is_empty());

    let all_match = format!("*{}", "*".repeat(200));
    assert_eq!(specialized(&trie, &all_match), oracle(&trie, &all_match));
    assert_eq!(specialized(&trie, &all_match).len(), trie.len());
}

#[test]
fn empty_trie_yields_nothing() {
    let trie = StrTrieMap::<u32>::new();

    assert!(specialized(&trie, "*").is_empty());
}
