/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Property tests for [`TermSuffixIndex`].

use std::{collections::HashSet, rc::Rc};

#[cfg(not(miri))]
use proptest::prelude::*;
use trie_rs::term_suffix_index::TermSuffixIndex;

fn build_index(corpus: &[String]) -> TermSuffixIndex {
    let mut sut = TermSuffixIndex::new();
    for t in corpus {
        sut.add(t);
    }
    sut
}

fn collect_set<I: Iterator<Item = Rc<str>>>(it: I) -> HashSet<String> {
    it.map(|r| r.as_ref().to_string()).collect()
}

// --- Focused, hand-rolled cases ----------------------------------------

#[test]
fn iter_suffix_yields_terms_ending_with_needle() {
    let corpus = ["cat", "catalog", "category", "concat", "scat", "scatter"]
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<_>>();
    let sut = build_index(&corpus);
    let expected = ["cat", "concat", "scat"]
        .iter()
        .map(|s| s.to_string())
        .collect::<HashSet<_>>();

    let actual = collect_set(sut.iter_suffix("cat"));

    assert_eq!(actual, expected, "iter_suffix('cat')");
}

#[test]
fn iter_contains_yields_terms_containing_needle() {
    let corpus = ["cat", "catalog", "category", "concat", "scat", "scatter"]
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<_>>();
    let sut = build_index(&corpus);
    let expected = corpus.iter().cloned().collect::<HashSet<_>>();

    let actual = collect_set(sut.iter_contains("cat"));

    assert_eq!(actual, expected, "iter_contains('cat')");
}

#[test]
fn empty_needle_yields_no_matches() {
    let corpus = ["cat", "dog"]
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<_>>();
    let sut = build_index(&corpus);

    let actual_suffix = sut.iter_suffix("").count();
    let actual_contains = sut.iter_contains("").count();

    assert_eq!(actual_suffix, 0);
    assert_eq!(actual_contains, 0);
}

#[test]
fn add_then_remove_clears_the_index() {
    let mut sut = TermSuffixIndex::new();
    sut.add("banana");
    sut.add("anaconda");

    sut.remove("banana");
    sut.remove("anaconda");

    for needle in [
        "banana", "anana", "nana", "ana", "na", "anaconda", "naconda", "aconda", "conda", "onda",
        "nda", "da", "an",
    ] {
        assert_eq!(sut.iter_contains(needle).count(), 0, "needle={needle}");
        assert_eq!(sut.iter_suffix(needle).count(), 0, "needle={needle}");
    }
}

#[test]
fn add_same_term_twice_is_noop() {
    let mut sut = TermSuffixIndex::new();

    sut.add("apple");
    sut.add("apple");
    let actual = sut.iter_contains("apple").collect::<Vec<_>>();

    assert_eq!(actual.len(), 1, "apple inserted twice yields one hit");
    assert_eq!(actual[0].as_ref(), "apple");
}

#[test]
fn add_promotes_existing_suffix_only_node_to_full_term() {
    // "longer" leaves "ger" as a suffix-only node; inserting "ger"
    // must promote it.
    let mut sut = TermSuffixIndex::new();
    sut.add("longer");
    sut.add("ger");

    let actual = collect_set(sut.iter_suffix("ger"));

    let expected = HashSet::from(["longer".to_string(), "ger".to_string()]);
    assert_eq!(actual, expected);
}

#[test]
fn min_suffix_boundary_two_chars_no_proper_suffix() {
    let mut sut = TermSuffixIndex::new();
    sut.add("ab");

    let actual_exact = collect_set(sut.iter_suffix("ab"));
    let actual_single_char = sut.iter_suffix("b").count();

    let expected_exact = HashSet::from(["ab".to_string()]);
    assert_eq!(actual_exact, expected_exact);
    assert_eq!(actual_single_char, 0);
}

#[test]
fn multibyte_utf8_min_suffix_is_codepoint_aware() {
    // Codepoint-aware suffix slicing: "café" has 4 codepoints, so the
    // length-2 suffix "fé" is indexed but length-1 "é" is not. A
    // byte-stride port would either panic (`&term[3..]` splits the
    // first byte of "é") or insert a malformed key — neither should
    // happen here.
    let mut sut = TermSuffixIndex::new();
    for t in ["café", "naïve", "日本語", "🦀rust"] {
        sut.add(t);
    }

    assert!(collect_set(sut.iter_suffix("fé")).contains("café"));
    // "é" is a one-codepoint suffix; below MIN_SUFFIX → no entry.
    assert_eq!(sut.iter_suffix("é").count(), 0);
    // "本語" is a two-codepoint suffix of "日本語" — should be present.
    assert!(collect_set(sut.iter_suffix("本語")).contains("日本語"));

    for t in ["café", "naïve", "日本語", "🦀rust"] {
        sut.remove(t);
    }

    assert_eq!(sut.iter_contains("café").count(), 0);
    assert_eq!(sut.iter_contains("本語").count(), 0);
    assert_eq!(sut.iter_contains("rust").count(), 0);
}

#[test]
fn remove_unknown_term_is_noop() {
    let mut sut = TermSuffixIndex::new();
    sut.add("present");

    sut.remove("absent");

    assert!(collect_set(sut.iter_suffix("present")).contains("present"));
}

#[test]
fn remove_suffix_only_entry_is_noop() {
    // "ger" was never `add`ed, but it exists in the trie as a
    // back-reference key for "longer". Remove must no-op gracefully.
    let mut sut = TermSuffixIndex::new();
    sut.add("longer");

    sut.remove("ger");

    assert!(collect_set(sut.iter_suffix("ger")).contains("longer"));
}

#[test]
fn iter_suffix_yields_one_hit_per_matching_term() {
    let mut sut = TermSuffixIndex::new();
    sut.add("abc");
    sut.add("xabc");

    let actual = sut
        .iter_suffix("abc")
        .map(|r| r.as_ref().to_string())
        .collect::<Vec<_>>();

    assert_eq!(actual.len(), 2, "each match yielded exactly once");
    let actual_set = actual.into_iter().collect::<HashSet<_>>();
    let expected = HashSet::from(["abc".to_string(), "xabc".to_string()]);
    assert_eq!(actual_set, expected);
}

// --- Proptest fuzz

#[cfg(not(miri))] // these proptests are too slow to run under miri
mod fuzz {
    use super::*;

    /// Kept small so fuzz cases land in a regime where suffix collisions are common.
    fn term_strategy() -> impl Strategy<Value = String> {
        "[a-z]{1,8}"
    }

    proptest! {
        #![proptest_config(ProptestConfig { cases: 2_000, .. ProptestConfig::default() })]

        #[test]
        fn iter_contains_matches_substring_oracle(
            corpus in proptest::collection::vec(term_strategy(), 1..=20),
            needle in "[a-z]{2,4}",
        ) {
            let sut = build_index(&corpus);
            let expected = corpus
                .iter()
                .filter(|t| t.contains(&needle))
                .cloned()
                .collect::<HashSet<_>>();

            let actual = collect_set(sut.iter_contains(&needle));

            prop_assert_eq!(actual, expected, "needle={:?} corpus={:?}", needle, corpus);
        }

        #[test]
        fn iter_suffix_matches_ends_with_oracle(
            corpus in proptest::collection::vec(term_strategy(), 1..=20),
            needle in "[a-z]{2,4}",
        ) {
            let sut = build_index(&corpus);
            let expected = corpus
                .iter()
                .filter(|t| t.ends_with(&needle))
                .cloned()
                .collect::<HashSet<_>>();

            let actual = collect_set(sut.iter_suffix(&needle));

            prop_assert_eq!(actual, expected, "needle={:?} corpus={:?}", needle, corpus);
        }

        #[test]
        fn removing_all_added_terms_clears_the_index(
            corpus in proptest::collection::vec(term_strategy(), 1..=10),
        ) {
            let mut sut = TermSuffixIndex::new();
            for t in &corpus {
                sut.add(t);
            }

            // Remove in reverse insertion order — exercises the
            // suffix-promotion path inside the index more often than
            // forward removal.
            for t in corpus.iter().rev() {
                sut.remove(t);
            }

            // Every needle of length up to 4 yields zero hits.
            for sample in ["a", "ab", "abc", "abcd"] {
                prop_assert_eq!(sut.iter_contains(sample).count(), 0,
                    "stale entries for needle={:?} corpus={:?}", sample, corpus);
                prop_assert_eq!(sut.iter_suffix(sample).count(), 0,
                    "stale entries for needle={:?} corpus={:?}", sample, corpus);
            }
        }

        #[test]
        fn mixed_add_remove_matches_hashset_oracle(
            ops in proptest::collection::vec(
                (any::<bool>(), "[a-z]{1,6}"),
                1..=30,
            ),
            needle in "[a-z]{2,3}",
        ) {
            let mut sut = TermSuffixIndex::new();
            let mut alive = HashSet::new();

            // Apply add/remove stream, mirroring into a HashSet oracle.
            for (is_add, t) in &ops {
                if *is_add {
                    if alive.insert(t.clone()) {
                        sut.add(t);
                    }
                } else if alive.remove(t) {
                    sut.remove(t);
                }
            }
            let actual_contains = collect_set(sut.iter_contains(&needle));
            let actual_suffix = collect_set(sut.iter_suffix(&needle));

            let expected_contains =
                alive.iter().filter(|t| t.contains(&needle)).cloned().collect();
            prop_assert_eq!(actual_contains, expected_contains,
                "iter_contains needle={:?}", needle);
            let expected_suffix =
                alive.iter().filter(|t| t.ends_with(&needle)).cloned().collect();
            prop_assert_eq!(actual_suffix, expected_suffix,
                "iter_suffix needle={:?}", needle);
        }
    }
}
