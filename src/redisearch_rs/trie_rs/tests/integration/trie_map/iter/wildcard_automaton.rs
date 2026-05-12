/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Cross-check that [`TrieMap::wildcard_iter`] yields the same matches
//! as a naive reference oracle (`trie.iter()` filtered by
//! [`WildcardPattern::matches`]) across the three backends the
//! dispatcher routes to (`u64` NFA, `u128` NFA, and the per-key filter
//! fallback at ≥ 128 atoms).

use rqe_wildcard::{MatchOutcome, WildcardPattern};
use trie_rs::TrieMap;

/// Independent oracle: walk every entry in the trie and keep those that
/// the parsed pattern accepts when applied to the full key. This is the
/// slowest possible implementation and therefore the most trustworthy
/// for cross-checking the optimised iterator.
fn matches_reference<Data>(trie: &TrieMap<Data>, pattern: &str) -> Vec<Vec<u8>> {
    let p = WildcardPattern::parse(pattern.as_bytes());
    trie.iter()
        .filter(|(k, _)| matches!(p.matches(k), MatchOutcome::Match))
        .map(|(k, _)| k)
        .collect()
}

fn matches_specialized<Data>(trie: &TrieMap<Data>, pattern: &str) -> Vec<Vec<u8>> {
    let p = WildcardPattern::parse(pattern.as_bytes());
    trie.wildcard_iter(p).map(|(k, _)| k).collect()
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
        "*a*a*", "c*t", "?", "??", "*ar*", "ban?", "*cot",
    ];
    for p in patterns {
        let f = matches_reference(&trie, p);
        let s = matches_specialized(&trie, p);
        assert_eq!(f, s, "Specialized disagrees on `{p}`");
    }
}

#[test]
fn empty_pattern_matches_empty_key_only() {
    let mut trie = TrieMap::new();
    trie.insert(b"", b"".to_vec());
    trie.insert(b"apple", b"apple".to_vec());
    assert_eq!(matches_specialized(&trie, ""), vec![b""]);
}

#[test]
fn empty_trie_yields_nothing() {
    let trie = TrieMap::<u64>::new();
    assert!(matches_specialized(&trie, "*").is_empty());
}

#[cfg(not(miri))]
mod property_based {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 256,
            ..Default::default()
        })]

        /// Random ASCII keys + random pattern → filter and specialized must
        /// agree. Pattern lengths stay ≤ 8 atoms so the dispatcher routes
        /// through the `u64` NFA branch.
        #[test]
        fn agree_on_random_keys_and_patterns(
            keys in prop::collection::vec("[a-d]{0,6}", 1..30),
            pattern in "[a-d?*]{0,8}",
        ) {
            let mut trie = TrieMap::new();
            for k in &keys {
                trie.insert(k.as_bytes(), ());
            }
            let f = matches_reference(&trie, &pattern);
            let s = matches_specialized(&trie, &pattern);
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
            let f = matches_reference(&trie, &pattern);
            let s = matches_specialized(&trie, &pattern);
            prop_assert_eq!(&f, &s, "filter vs specialized, pattern=`{}`", pattern);
        }

        /// Patterns with no `*` — the dispatcher still routes by atom count,
        /// so these go through the `u64` NFA's `epsilon_table.is_none()`
        /// fixed-length fast path.
        #[test]
        fn fixed_length_patterns_agree(
            keys in prop::collection::vec("[a-d]{0,6}", 1..30),
            pattern in "[a-d?]{1,8}",
        ) {
            let mut trie = TrieMap::new();
            for k in &keys {
                trie.insert(k.as_bytes(), ());
            }
            let f = matches_reference(&trie, &pattern);
            let s = matches_specialized(&trie, &pattern);
            prop_assert_eq!(&f, &s, "filter vs specialized (fixed), pattern=`{}`", pattern);
        }
    }
}

/// Long literal pattern (70 chars) — atom count exceeds the `u64` cap so
/// the dispatcher routes through the `u128` NFA. Cross-check correctness
/// against the filter.
#[test]
fn long_literal_patterns_route_to_u128() {
    let prefix = "a".repeat(70);
    let pattern_long_literal = format!("{prefix}*");
    let pattern_long_fixed = prefix.clone();

    let mut trie = TrieMap::new();
    let matching_key = format!("{prefix}suffix");
    trie.insert(matching_key.as_bytes(), 1u32);
    trie.insert(b"unrelated", 2);
    trie.insert(prefix.as_bytes(), 3);

    let f = matches_reference(&trie, &pattern_long_literal);
    let s = matches_specialized(&trie, &pattern_long_literal);
    assert_eq!(f, s, "filter vs specialized, long literal + `*`");

    let f = matches_reference(&trie, &pattern_long_fixed);
    let s = matches_specialized(&trie, &pattern_long_fixed);
    assert_eq!(f, s, "filter vs specialized, long literal alone");
}

/// Patterns past 127 atoms route to the filter-based fallback in the
/// dispatcher. This exercises the [`WildcardBackend::Filter`] arm of
/// [`WildcardIter`] and confirms the dispatcher wraps
/// [`WildcardFilterIter`] correctly through its lending interface.
#[test]
fn long_patterns_route_to_filter_specialized() {
    // 260-char literal pushes us past the `u128` boundary into the
    // filter fallback (1 leading `*` + 260 + 1 trailing `*` + 1 accept
    // = 263 positions).
    let literal: String = "abcdefghij".repeat(26);
    assert_eq!(literal.len(), 260);
    let star_pattern = format!("*{literal}*");
    let prefix_pattern = format!("ab*{literal}*");

    let mut trie = TrieMap::new();
    let direct_match = format!("xxx{literal}yyy");
    trie.insert(direct_match.as_bytes(), 1u32);
    trie.insert(format!("ab{literal}").as_bytes(), 2);
    trie.insert(format!("abc{literal}post").as_bytes(), 3);
    trie.insert(b"ab", 4);
    trie.insert(b"abc", 5);
    trie.insert(b"unrelated", 6);
    trie.insert(b"", 7);

    for pattern in [&star_pattern, &prefix_pattern] {
        let f = matches_reference(&trie, pattern);
        let s = matches_specialized(&trie, pattern);
        assert_eq!(
            f,
            s,
            "filter vs specialized → Filter, pattern (len={})",
            pattern.len(),
        );
    }
}

/// Patterns with many `Any` atoms — the parser collapses consecutive `*`
/// into one, so 50 `*the` segments produce ~250 atoms, comfortably in
/// the filter-fallback range under the new dispatcher.
#[test]
fn many_any_atoms_route_to_filter() {
    let pattern: String = std::iter::repeat_n("*the", 50).collect::<String>() + "*";

    let mut trie = TrieMap::new();
    trie.insert(b"thethethe", 1u32);
    trie.insert(b"the_in_middle_the", 2);
    trie.insert(b"the", 3);
    trie.insert(b"unrelated", 4);
    trie.insert(b"", 5);

    let f = matches_reference(&trie, &pattern);
    let s = matches_specialized(&trie, &pattern);
    assert_eq!(f, s, "filter vs specialized, many-`Any` pattern");
}
