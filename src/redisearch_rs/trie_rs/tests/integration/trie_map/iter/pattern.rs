/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::time::Instant;

use lending_iterator::LendingIterator;
use trie_rs::{
    TrieMap,
    iter::{PatternLendingIter, PatternMode},
};

/// Drain `iter`, returning the keys it yields.
fn keys<Data>(mut iter: PatternLendingIter<'_, '_, Data>) -> Vec<Vec<u8>> {
    let mut keys = Vec::new();
    while let Some((key, _)) = iter.next() {
        keys.push(key.to_owned());
    }
    keys
}

fn matching(trie: &TrieMap<u32>, pattern: &[u8], mode: PatternMode) -> Vec<Vec<u8>> {
    keys(trie.pattern_lending_iter(pattern, mode))
}

fn fruit() -> TrieMap<u32> {
    let mut trie = TrieMap::new();
    for (i, key) in [
        b"apple".as_slice(),
        b"apricot",
        b"banana",
        b"bandana",
        b"grape",
    ]
    .into_iter()
    .enumerate()
    {
        trie.insert(key, i as u32);
    }
    trie
}

fn owned(keys: &[&[u8]]) -> Vec<Vec<u8>> {
    keys.iter().map(|k| k.to_vec()).collect()
}

#[test]
fn prefix_mode_yields_the_keys_starting_with_the_pattern() {
    assert_eq!(
        matching(&fruit(), b"ap", PatternMode::Prefix),
        owned(&[b"apple", b"apricot"])
    );
}

#[test]
fn contains_mode_yields_the_keys_containing_the_pattern() {
    assert_eq!(
        matching(&fruit(), b"an", PatternMode::Contains),
        owned(&[b"banana", b"bandana"])
    );
}

#[test]
fn suffix_mode_yields_the_keys_ending_with_the_pattern() {
    assert_eq!(
        matching(&fruit(), b"ana", PatternMode::Suffix),
        owned(&[b"banana", b"bandana"])
    );
    assert_eq!(
        matching(&fruit(), b"pe", PatternMode::Suffix),
        owned(&[b"grape"])
    );
}

#[test]
fn wildcard_mode_yields_the_keys_matching_the_pattern() {
    assert_eq!(
        matching(&fruit(), b"ba*na", PatternMode::Wildcard),
        owned(&[b"banana", b"bandana"])
    );
    assert_eq!(
        matching(&fruit(), b"?pple", PatternMode::Wildcard),
        owned(&[b"apple"])
    );
}

#[test]
fn an_empty_pattern_matches_every_key_except_under_wildcard() {
    let all = owned(&[b"apple", b"apricot", b"banana", b"bandana", b"grape"]);
    for mode in [
        PatternMode::Prefix,
        PatternMode::Contains,
        PatternMode::Suffix,
    ] {
        assert_eq!(matching(&fruit(), b"", mode), all, "{mode:?}");
    }
    // An empty wildcard pattern matches only the empty key, which is absent.
    assert!(matching(&fruit(), b"", PatternMode::Wildcard).is_empty());
}

#[test]
fn a_converted_lending_iter_yields_every_key() {
    let trie = fruit();
    assert_eq!(
        keys(trie.lending_iter().into()),
        owned(&[b"apple", b"apricot", b"banana", b"bandana", b"grape"])
    );
}

#[test]
fn yields_the_values_stored_under_the_matched_keys() {
    let trie = fruit();
    let mut iter = trie.pattern_lending_iter(b"ana", PatternMode::Suffix);
    let mut values = Vec::new();
    while let Some((_, value)) = iter.next() {
        values.push(*value);
    }
    assert_eq!(values, vec![2, 3]);
}

#[test]
fn an_elapsed_deadline_stops_every_mode_early() {
    // Several times the steps the iterators walk between two clock probes (the
    // crate-private `TIMEOUT_CHECK_GRANULARITY`), so an elapsed deadline must
    // cut every mode's walk short. Kept small because inserting the keys
    // dominates the test's cost under Miri.
    const KEYS: u32 = 300;
    let mut trie = TrieMap::new();
    for i in 0..KEYS {
        trie.insert(format!("a{i:04}").as_bytes(), i);
    }

    for (mode, pattern) in [
        (PatternMode::Prefix, b"a".as_slice()),
        (PatternMode::Contains, b"a"),
        (PatternMode::Suffix, b""),
        (PatternMode::Wildcard, b"a*"),
    ] {
        let mut iter = trie.pattern_lending_iter(pattern, mode);
        iter.set_timeout(Some(Instant::now()));
        let yielded = keys(iter).len();
        assert!(yielded < KEYS as usize, "{mode:?} yielded {yielded} keys");
    }
}

mod property_based {
    #![cfg(not(miri))]

    use proptest::{
        collection::{btree_map, vec},
        prelude::any,
        sample::select,
    };
    use rqe_wildcard::{MatchOutcome, WildcardPattern};

    use super::*;

    /// Whether a key (first) matches a pattern (second).
    type KeyPredicate = fn(&[u8], &[u8]) -> bool;

    proptest::proptest! {
        #[test]
        /// The affix modes yield exactly the entries a naive filter over the
        /// same keys would, in the same order.
        ///
        /// Keys and pattern draw from a three-byte alphabet so that most
        /// patterns match some keys and miss others.
        fn affix_modes_agree_with_a_naive_filter(
            entries in btree_map(vec(0u8..3, 0..6), any::<u32>(), 0..32),
            pattern in vec(0u8..3, 0..3),
        ) {
            let mut trie = TrieMap::new();
            for (key, value) in &entries {
                trie.insert(key, *value);
            }
            let predicates: [(PatternMode, KeyPredicate); 3] = [
                (PatternMode::Prefix, |k, p| k.starts_with(p)),
                (PatternMode::Suffix, |k, p| k.ends_with(p)),
                (PatternMode::Contains, |k, p| {
                    p.is_empty() || k.windows(p.len()).any(|w| w == p)
                }),
            ];
            for (mode, predicate) in predicates {
                let expected: Vec<Vec<u8>> = entries
                    .keys()
                    .filter(|k| predicate(k, &pattern))
                    .cloned()
                    .collect();
                proptest::prop_assert_eq!(matching(&trie, &pattern, mode), expected, "{:?}", mode);
            }
        }

        #[test]
        /// Wildcard mode yields exactly the entries whose key the parsed
        /// pattern matches, in the same order.
        ///
        /// Keys and pattern share a two-letter alphabet, and the pattern may also
        /// hold `*` and `?`, so most patterns match some keys and miss others.
        fn wildcard_mode_agrees_with_a_naive_match(
            entries in btree_map(vec(select(b"ab".as_slice()), 0..6), any::<u32>(), 0..32),
            pattern in vec(select(b"ab*?".as_slice()), 0..5),
        ) {
            let mut trie = TrieMap::new();
            for (key, value) in &entries {
                trie.insert(key, *value);
            }
            let parsed = WildcardPattern::parse(&pattern);
            let expected: Vec<Vec<u8>> = entries
                .keys()
                .filter(|k| parsed.matches(k) == MatchOutcome::Match)
                .cloned()
                .collect();
            proptest::prop_assert_eq!(matching(&trie, &pattern, PatternMode::Wildcard), expected);
        }
    }
}
