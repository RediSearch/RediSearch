/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use trie_rs::TrieMap;
use wildcard::WildcardPattern;

/// Return all the keys that match the given pattern.
fn matches<Data>(trie: &TrieMap<Data>, pattern: &str) -> Vec<Vec<u8>> {
    let pattern = WildcardPattern::parse(pattern.as_bytes());
    trie.wildcard_iter(pattern).map(|(k, _)| k).collect()
}

#[test]
fn empty_pattern_matches_empty_string() {
    let mut trie = TrieMap::new();
    trie.insert(b"", b"".to_vec());
    trie.insert(b"apple", b"apple".into());

    assert_eq!(matches(&trie, ""), vec![b""]);
}

#[test]
fn empty_trie_does_not_match() {
    let trie = TrieMap::<u64>::new();
    assert!(matches(&trie, "*").is_empty());
}

#[test]
fn wildcard_iter() {
    let mut trie = TrieMap::new();
    trie.insert(b"apple", b"apple".to_vec());
    trie.insert(b"ban", b"ban".into());
    trie.insert(b"banana", b"banana".into());
    trie.insert(b"apricot", b"apricot".into());

    // No non-empty term matches the empty pattern.
    assert!(matches(&trie, "").is_empty());

    // A `*` will match all entries.
    assert_eq!(
        matches(&trie, "*"),
        vec!["apple".as_bytes(), b"apricot", b"ban", b"banana"]
    );

    assert_eq!(matches(&trie, "ap*"), vec!["apple".as_bytes(), b"apricot"]);

    assert_eq!(matches(&trie, "*an*"), vec!["ban".as_bytes(), b"banana"]);

    // If the pattern is a literal that's stored as a term in the trie,
    // it is returned as a valid match for itself.
    assert_eq!(matches(&trie, "apricot"), vec![b"apricot"]);

    // The pattern is ruled out using a prefix search
    assert!(matches(&trie, "peach").is_empty());

    // The pattern is ruled out by examining the first level of trie nodes.
    assert!(matches(&trie, "?ci").is_empty());
}
