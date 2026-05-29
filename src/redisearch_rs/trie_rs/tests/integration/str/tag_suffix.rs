/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Pure-Rust property tests for [`TagSuffixIndex`].
//!
//! Same shape as the [`SuffixIndex`] suite: index vs. brute-force scan
//! using [`rqe_wildcard::WildcardPattern::matches`]. ASCII-only corpora
//! here — the tag variant intentionally mirrors C's byte-space rotation,
//! which is buggy on non-ASCII (see `tag_suffix.rs` module docs).
//! Exercising the non-ASCII path is the unit test
//! `non_ascii_byte_rotation_lookups_consistent`, not these properties.

use std::collections::HashSet;

use proptest::prelude::*;
use rqe_wildcard::{MatchOutcome, WildcardPattern};
use trie_rs::str::tag_suffix::TagSuffixIndex;

fn brute_force_wildcard(corpus: &[String], pattern: &str) -> HashSet<String> {
    let parsed = WildcardPattern::parse(pattern.as_bytes());
    corpus
        .iter()
        .filter(|t| parsed.matches(t.as_bytes()) == MatchOutcome::Match)
        .cloned()
        .collect()
}

fn build_index(corpus: &[String]) -> TagSuffixIndex {
    let mut idx = TagSuffixIndex::new();
    for t in corpus {
        idx.add(t);
    }
    idx
}

fn collect_set<I: Iterator<Item = std::rc::Rc<str>>>(it: I) -> HashSet<String> {
    it.map(|r| r.as_ref().to_string()).collect()
}

#[test]
fn focused_iter_suffix() {
    let corpus: Vec<String> = ["cat", "concat", "scat", "catalog"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    let idx = build_index(&corpus);
    let got = collect_set(idx.iter_suffix("cat"));
    let want: HashSet<String> = ["cat", "concat", "scat"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    assert_eq!(got, want);
}

#[test]
fn focused_iter_contains() {
    let corpus: Vec<String> = ["cat", "category", "scatter", "dog"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    let idx = build_index(&corpus);
    let got = collect_set(idx.iter_contains("cat"));
    let want: HashSet<String> = ["cat", "category", "scatter"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    assert_eq!(got, want);
}

#[test]
fn focused_iter_wildcard() {
    let corpus: Vec<String> = ["apple", "happy", "puppet", "banana"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    let idx = build_index(&corpus);
    let got = collect_set(idx.iter_wildcard("*pp*").expect("token chosen"));
    let want = brute_force_wildcard(&corpus, "*pp*");
    assert_eq!(got, want);
}

// --- Proptest wildcard fuzz --------------------------------------------
//
// Gated `#[cfg(not(miri))]` for the same reason as
// `super::suffix::fuzz`: proptest cases over a brute-force oracle blow
// past miri's per-test slow-timeout. The deterministic unit tests in
// `trie_rs::str::tag_suffix::tests` exercise the same code paths under
// miri.

#[cfg(not(miri))]
mod fuzz {
    use super::*;

    fn term_strategy() -> impl Strategy<Value = String> {
        "[a-z]{1,8}"
    }

    fn pattern_strategy() -> impl Strategy<Value = String> {
        (1usize..=4).prop_flat_map(|segs| {
            proptest::collection::vec("[a-z?]{0,4}", segs)
                .prop_map(|parts| parts.join("*"))
                .prop_map(|s| if s.contains('*') { s } else { format!("{s}*") })
        })
    }

    proptest! {
        #![proptest_config(ProptestConfig { cases: 2_000, .. ProptestConfig::default() })]

        #[test]
        fn wildcard_matches_brute_force(
            corpus in proptest::collection::vec(term_strategy(), 1..=20),
            pattern in pattern_strategy(),
        ) {
            let idx = build_index(&corpus);
            let want = brute_force_wildcard(&corpus, &pattern);
            if let Some(it) = idx.iter_wildcard(&pattern) {
                let got = collect_set(it);
                prop_assert_eq!(got, want, "pattern={:?} corpus={:?}", pattern, corpus);
            }
        }

        #[test]
        fn iter_contains_matches_substring_oracle(
            corpus in proptest::collection::vec(term_strategy(), 1..=20),
            needle in "[a-z]{2,4}",
        ) {
            let idx = build_index(&corpus);
            let want: HashSet<String> = corpus.iter().filter(|t| t.contains(&needle)).cloned().collect();
            let got = collect_set(idx.iter_contains(&needle));
            prop_assert_eq!(got, want);
        }

        #[test]
        fn iter_suffix_matches_ends_with_oracle(
            corpus in proptest::collection::vec(term_strategy(), 1..=20),
            needle in "[a-z]{2,4}",
        ) {
            let idx = build_index(&corpus);
            let want: HashSet<String> = corpus.iter().filter(|t| t.ends_with(&needle)).cloned().collect();
            let got = collect_set(idx.iter_suffix(&needle));
            prop_assert_eq!(got, want);
        }

        #[test]
        fn add_then_remove_round_trip(
            corpus in proptest::collection::vec(term_strategy(), 1..=10),
        ) {
            let mut idx = TagSuffixIndex::new();
            let unique: HashSet<String> = corpus.iter().cloned().collect();
            for t in &unique {
                idx.add(t);
            }
            for t in &unique {
                idx.remove(t);
            }
            prop_assert!(idx.is_empty(), "index not empty after full remove");
        }

        #[test]
        fn mixed_add_remove_consistent(
            ops in proptest::collection::vec((any::<bool>(), "[a-z]{1,6}"), 1..=30),
            needle in "[a-z]{2,3}",
        ) {
            let mut idx = TagSuffixIndex::new();
            let mut alive: HashSet<String> = HashSet::new();
            for (is_add, t) in &ops {
                if *is_add {
                    if alive.insert(t.clone()) {
                        idx.add(t);
                    }
                } else if alive.remove(t) {
                    idx.remove(t);
                }
            }
            let want_contains: HashSet<String> =
                alive.iter().filter(|t| t.contains(&needle)).cloned().collect();
            let got_contains = collect_set(idx.iter_contains(&needle));
            prop_assert_eq!(got_contains, want_contains);

            let want_suffix: HashSet<String> =
                alive.iter().filter(|t| t.ends_with(&needle)).cloned().collect();
            let got_suffix = collect_set(idx.iter_suffix(&needle));
            prop_assert_eq!(got_suffix, want_suffix);
        }
    }
} // mod fuzz
