/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Cross-check that the streaming-automaton iterators produce the same
//! matches as the existing filter-based [`WildcardIter`].

use proptest::prelude::*;
use trie_rs::TrieMap;
use trie_rs::iter::LargeHeapStateSet;
use wildcard::WildcardPattern;

fn matches_filter<Data>(trie: &TrieMap<Data>, pattern: &str) -> Vec<Vec<u8>> {
    let p = WildcardPattern::parse(pattern.as_bytes());
    trie.wildcard_iter(p).map(|(k, _)| k).collect()
}

fn matches_nfa<Data>(trie: &TrieMap<Data>, pattern: &str) -> Vec<Vec<u8>> {
    // Use the dynamic-length heap bitset so this helper works for patterns
    // of any length; size-dispatch correctness on smaller bitsets is
    // exercised via `matches_specialized`.
    let p = WildcardPattern::parse(pattern.as_bytes());
    trie.wildcard_nfa_iter::<LargeHeapStateSet>(&p)
        .map(|(k, _)| k)
        .collect()
}

fn matches_specialized<Data>(trie: &TrieMap<Data>, pattern: &str) -> Vec<Vec<u8>> {
    let p = WildcardPattern::parse(pattern.as_bytes());
    trie.wildcard_specialized_iter(&p).map(|(k, _)| k).collect()
}

fn build_trie() -> TrieMap<Vec<u8>> {
    let mut trie = TrieMap::new();
    for word in [
        b"apple".as_ref(),
        b"ban",
        b"banana",
        b"apricot",
        b"band",
        b"car",
        b"cart",
        b"cartilage",
        b"",
    ] {
        trie.insert(word, word.to_vec());
    }
    trie
}

#[test]
fn matches_agree_on_seed_patterns() {
    let trie = build_trie();
    let patterns = [
        "", "*", "ap*", "*an*", "apricot", "peach", "?ci", "ban*", "*", "ba?", "ba??", "*a*",
        "*a*a*", "c*t", "?", "??", "*ar*", "ban?", "*ot",
    ];
    for p in patterns {
        let f = matches_filter(&trie, p);
        let n = matches_nfa(&trie, p);
        let s = matches_specialized(&trie, p);
        assert_eq!(f, n, "NFA disagrees on `{p}`");
        assert_eq!(f, s, "Specialized disagrees on `{p}`");
    }
}

#[test]
fn empty_pattern_matches_empty_key_only() {
    let mut trie = TrieMap::new();
    trie.insert(b"", b"".to_vec());
    trie.insert(b"apple", b"apple".to_vec());
    assert_eq!(matches_nfa(&trie, ""), vec![b""]);
    assert_eq!(matches_specialized(&trie, ""), vec![b""]);
}

#[test]
fn empty_trie_yields_nothing() {
    let trie = TrieMap::<u64>::new();
    assert!(matches_nfa(&trie, "*").is_empty());
    assert!(matches_specialized(&trie, "*").is_empty());
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 256,
        ..Default::default()
    })]

    /// Random ASCII keys + random pattern → filter, NFA, and specialized
    /// must agree.
    #[test]
    fn agree_on_random_keys_and_patterns(
        keys in prop::collection::vec("[a-d]{0,6}", 1..30),
        pattern in "[a-d?*]{0,8}",
    ) {
        let mut trie = TrieMap::new();
        for k in &keys {
            trie.insert(k.as_bytes(), ());
        }
        let f = matches_filter(&trie, &pattern);
        let n = matches_nfa(&trie, &pattern);
        let s = matches_specialized(&trie, &pattern);
        prop_assert_eq!(&f, &n, "filter vs nfa, pattern=`{}`", pattern);
        prop_assert_eq!(&f, &s, "filter vs specialized, pattern=`{}`", pattern);
    }

    /// Patterns with literals — exercise the prefix-shortcut path.
    #[test]
    fn agree_with_literal_prefixes(
        keys in prop::collection::vec("[a-z]{0,6}", 1..30),
        prefix in "[a-z]{1,3}",
        suffix in "[a-z?*]{0,4}",
    ) {
        let pattern = format!("{prefix}{suffix}");
        let mut trie = TrieMap::new();
        for k in &keys {
            trie.insert(k.as_bytes(), ());
        }
        let f = matches_filter(&trie, &pattern);
        let n = matches_nfa(&trie, &pattern);
        let s = matches_specialized(&trie, &pattern);
        prop_assert_eq!(&f, &n, "filter vs nfa, pattern=`{}`", pattern);
        prop_assert_eq!(&f, &s, "filter vs specialized, pattern=`{}`", pattern);
    }

    /// Patterns with no `*` — exercise the fixed-length specialization
    /// dominantly. Pattern grammar excludes `*` so the specialized iter
    /// always takes its `Fixed` branch.
    #[test]
    fn fixed_length_patterns_agree(
        keys in prop::collection::vec("[a-d]{0,6}", 1..30),
        pattern in "[a-d?]{1,8}",
    ) {
        let mut trie = TrieMap::new();
        for k in &keys {
            trie.insert(k.as_bytes(), ());
        }
        let f = matches_filter(&trie, &pattern);
        let s = matches_specialized(&trie, &pattern);
        prop_assert_eq!(&f, &s, "filter vs specialized (fixed), pattern=`{}`", pattern);
    }
}

/// Patterns longer than the inline `u64` bitset (63 atoms) exercise the
/// [`StateSet`]'s heap-spill path. The streaming automaton must yield the
/// same matches as the filter-based iterator without panicking or losing
/// correctness.
#[test]
fn long_literal_patterns_via_spilled_bitset() {
    // A 70-char literal — over the 63-bit inline-bitset cap, so the
    // underlying StateSet allocates a second word on the heap.
    let prefix = "a".repeat(70);
    let pattern_long_literal = format!("{prefix}*");
    let pattern_long_fixed = prefix.clone();

    let mut trie = TrieMap::new();
    let matching_key = format!("{prefix}suffix");
    trie.insert(matching_key.as_bytes(), 1u32);
    trie.insert(b"unrelated", 2);
    trie.insert(prefix.as_bytes(), 3);

    // Variable-length pattern: NFA path spills the bitset to the heap.
    let f = matches_filter(&trie, &pattern_long_literal);
    let n = matches_nfa(&trie, &pattern_long_literal);
    let s = matches_specialized(&trie, &pattern_long_literal);
    assert_eq!(f, n, "filter vs nfa, long literal + `*`");
    assert_eq!(f, s, "filter vs specialized, long literal + `*`");

    // Fixed-length pattern: FixedWildcardIter handles arbitrary length too.
    let f = matches_filter(&trie, &pattern_long_fixed);
    let s = matches_specialized(&trie, &pattern_long_fixed);
    assert_eq!(f, s, "filter vs specialized, long literal alone");
}
