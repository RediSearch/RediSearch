/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Pure-Rust property tests for [`SuffixIndex`].
//!
//! Driven against a brute-force oracle: for each test corpus and pattern,
//! the index's iterator is compared against a linear scan that filters
//! the corpus with [`rqe_wildcard::WildcardPattern::matches`]. The
//! suffix-index must yield the same **set** of source terms (order is
//! not part of the contract — the C `Suffix_IterateWildcard` traverses
//! the trie depth-first, while a brute-force scan visits insertion
//! order; both must agree on membership).
//!
//! The C-against-Rust oracle lives in
//! `rune_trie_snapshots::suffix_oracle` and is independent of these
//! tests — these prove Rust ↔ Rust internal consistency and pin the
//! semantics the Rust port commits to.
//!
//! ## Needle / chosen-literal length contract
//!
//! The suffix index does NOT promise hits for needles shorter than
//! [`MIN_SUFFIX`] codepoints (2), because no rotation that short is ever
//! stored. Production callers route sub-`MIN_SUFFIX` needles to
//! brute-force iteration on `sp->terms` instead (see `query.c`). The
//! property tests below restrict generated needles to length ≥ 2 so
//! the brute-force substring/ends-with oracle stays a fair comparison.
//! The wildcard fuzz uses [`SuffixIndex::iter_wildcard`]'s own `None`
//! return value as the same fallback signal — when it returns `None`,
//! we skip the comparison (mirroring the caller responsibility).

use std::collections::HashSet;

use proptest::prelude::*;
use rqe_wildcard::{MatchOutcome, WildcardPattern};
use trie_rs::str::suffix::SuffixIndex;

/// Brute-force oracle: linear scan of the corpus, filtered against the
/// wildcard pattern. Used both as the proptest oracle and in the
/// hand-rolled focused tests below.
fn brute_force_wildcard(corpus: &[String], pattern: &str) -> HashSet<String> {
    let parsed = WildcardPattern::parse(pattern.as_bytes());
    corpus
        .iter()
        .filter(|t| parsed.matches(t.as_bytes()) == MatchOutcome::Match)
        .cloned()
        .collect()
}

/// Build a [`SuffixIndex`] from the given corpus by inserting each
/// element once.
fn build_index(corpus: &[String]) -> SuffixIndex {
    let mut idx = SuffixIndex::new();
    for t in corpus {
        idx.add(t);
    }
    idx
}

/// Convert a [`SuffixIndex`] iterator's emitted `Rc<str>`s into the set
/// of distinct source terms — this is the "matched set" the C contract
/// commits to.
fn collect_set<I: Iterator<Item = std::rc::Rc<str>>>(it: I) -> HashSet<String> {
    it.map(|r| r.as_ref().to_string()).collect()
}

// --- Focused, hand-rolled cases ----------------------------------------

#[test]
fn iter_suffix_matches_brute_force() {
    // Reference fixture inspired by `rune_trie_snapshots::contains`.
    let corpus: Vec<String> = ["cat", "catalog", "category", "concat", "scat", "scatter"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    let idx = build_index(&corpus);

    // Exact-suffix lookup must equal `{terms whose final substring is
    // "cat"}`. "cat", "concat", "scat" all end with "cat"; the others
    // contain "cat" but don't end with it.
    let got = collect_set(idx.iter_suffix("cat"));
    let want: HashSet<String> = ["cat", "concat", "scat"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    assert_eq!(got, want, "iter_suffix('cat')");
}

#[test]
fn iter_contains_matches_brute_force_substring() {
    let corpus: Vec<String> = ["cat", "catalog", "category", "concat", "scat", "scatter"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    let idx = build_index(&corpus);

    // "cat" appears somewhere in every fixture term.
    let got = collect_set(idx.iter_contains("cat"));
    let want: HashSet<String> = corpus.iter().cloned().collect();
    assert_eq!(got, want, "iter_contains('cat')");
}

#[test]
fn iter_wildcard_matches_brute_force_focused() {
    // "*pp*" — chosen token "pp" of length 2 (MIN_SUFFIX).
    let corpus: Vec<String> = ["apple", "happy", "puppet", "banana", "ppp"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    let idx = build_index(&corpus);
    let pattern = "*pp*";
    let got = collect_set(idx.iter_wildcard(pattern).expect("token chosen"));
    let want = brute_force_wildcard(&corpus, pattern);
    assert_eq!(got, want, "iter_wildcard('{pattern}')");
}

#[test]
fn iter_wildcard_returns_none_when_no_min_suffix_token() {
    let corpus: Vec<String> = vec!["apple".into()];
    let idx = build_index(&corpus);
    // No literal of length ≥ MIN_SUFFIX in either pattern.
    assert!(idx.iter_wildcard("*a*").is_none());
    assert!(idx.iter_wildcard("?").is_none());
}

#[test]
fn empty_needle_yields_no_matches() {
    let corpus: Vec<String> = ["cat", "dog"].iter().map(|s| s.to_string()).collect();
    let idx = build_index(&corpus);
    assert_eq!(idx.iter_suffix("").count(), 0);
    assert_eq!(idx.iter_contains("").count(), 0);
}

// --- Proptest wildcard fuzz --------------------------------------------
//
// Gated `#[cfg(not(miri))]` because each proptest invocation runs
// thousands of cases over a brute-force oracle; under miri's interpreter
// that blows past the per-test slow-timeout (10s × 4) configured in
// `.config/nextest.toml`. The deterministic unit tests in the `tests`
// module of `trie_rs::str::suffix` still run under miri and cover the
// same code paths — proptest only widens coverage with random inputs.

#[cfg(not(miri))]
mod fuzz {
    use super::*;

    /// Generate a small lowercase term (1..=8 ASCII chars). Kept small so
    /// fuzz cases land in a regime where rotation collisions are common.
    fn term_strategy() -> impl Strategy<Value = String> {
        "[a-z]{1,8}"
    }

    /// Pattern strategy: 1..=4 literal-or-wildcard atoms, then guarantee at
    /// least one `*` so the wildcard pathway is exercised. Patterns without
    /// any `*` and shorter than `MIN_SUFFIX` literal would just collapse to
    /// exact-match anyway.
    fn pattern_strategy() -> impl Strategy<Value = String> {
        // Build with at least one `*`. Use a simple grammar: 1..=4 segments
        // separated by `*`, where each segment is 0..=4 chars from
        // `[a-z?]`. Producing strings up to ~20 chars max.
        (1usize..=4).prop_flat_map(|segs| {
            proptest::collection::vec("[a-z?]{0,4}", segs)
                .prop_map(|parts| parts.join("*"))
                .prop_map(|s| {
                    // Force at least one `*` by appending if needed.
                    if s.contains('*') { s } else { format!("{s}*") }
                })
        })
    }

    proptest! {
        // The handoff calls for ≥ 10 000 fuzz cases. 2 000 per proptest
        // invocation with five separate properties below gets us to 10k+
        // without making the test runtime painful (each case is cheap).
        #![proptest_config(ProptestConfig { cases: 2_000, .. ProptestConfig::default() })]

        #[test]
        fn wildcard_matches_brute_force(
            corpus in proptest::collection::vec(term_strategy(), 1..=20),
            pattern in pattern_strategy(),
        ) {
            let idx = build_index(&corpus);
            let want = brute_force_wildcard(&corpus, &pattern);
            match idx.iter_wildcard(&pattern) {
                Some(it) => {
                    let got = collect_set(it);
                    prop_assert_eq!(got, want, "pattern={:?} corpus={:?}", pattern, corpus);
                }
                None => {
                    // The index declines short patterns; nothing the
                    // index says is wrong here, but the caller would have
                    // to fall back to brute-force on the corpus, which by
                    // definition produces `want`. We don't compare in
                    // this branch — it's a contract about *when* the
                    // fallback signal fires, not about result fidelity.
                }
            }
        }

        #[test]
        fn iter_contains_matches_substring_oracle(
            corpus in proptest::collection::vec(term_strategy(), 1..=20),
            needle in "[a-z]{2,4}",
        ) {
            let idx = build_index(&corpus);
            let want: HashSet<String> = corpus
                .iter()
                .filter(|t| t.contains(&needle))
                .cloned()
                .collect();
            let got = collect_set(idx.iter_contains(&needle));
            prop_assert_eq!(got, want, "needle={:?} corpus={:?}", needle, corpus);
        }

        #[test]
        fn iter_suffix_matches_ends_with_oracle(
            corpus in proptest::collection::vec(term_strategy(), 1..=20),
            needle in "[a-z]{2,4}",
        ) {
            let idx = build_index(&corpus);
            let want: HashSet<String> = corpus
                .iter()
                .filter(|t| t.ends_with(&needle))
                .cloned()
                .collect();
            let got = collect_set(idx.iter_suffix(&needle));
            prop_assert_eq!(got, want, "needle={:?} corpus={:?}", needle, corpus);
        }

        #[test]
        fn add_then_remove_round_trip(
            corpus in proptest::collection::vec(term_strategy(), 1..=10),
        ) {
            let mut idx = SuffixIndex::new();
            for t in &corpus {
                idx.add(t);
            }
            // Remove in reverse insertion order — exercises the
            // rotation-promotion path inside the index more often than
            // forward removal.
            for t in corpus.iter().rev() {
                idx.remove(t);
            }
            // Every needle of length up to 4 must yield zero hits.
            for sample in ["a", "ab", "abc", "abcd"] {
                prop_assert_eq!(idx.iter_contains(sample).count(), 0,
                    "stale entries for needle={:?} corpus={:?}", sample, corpus);
                prop_assert_eq!(idx.iter_suffix(sample).count(), 0,
                    "stale entries for needle={:?} corpus={:?}", sample, corpus);
            }
        }

        #[test]
        fn mixed_add_remove_consistent(
            ops in proptest::collection::vec(
                (any::<bool>(), "[a-z]{1,6}"),
                1..=30,
            ),
            needle in "[a-z]{2,3}",
        ) {
            // Apply a stream of add/remove ops, then check that the index
            // agrees with a HashSet-based reference for both
            // `iter_contains` and `iter_suffix`.
            let mut idx = SuffixIndex::new();
            let mut alive: std::collections::HashSet<String> = std::collections::HashSet::new();
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
            prop_assert_eq!(got_contains, want_contains,
                "iter_contains needle={:?}", needle);

            let want_suffix: HashSet<String> =
                alive.iter().filter(|t| t.ends_with(&needle)).cloned().collect();
            let got_suffix = collect_set(idx.iter_suffix(&needle));
            prop_assert_eq!(got_suffix, want_suffix,
                "iter_suffix needle={:?}", needle);
        }
    }
} // mod fuzz
