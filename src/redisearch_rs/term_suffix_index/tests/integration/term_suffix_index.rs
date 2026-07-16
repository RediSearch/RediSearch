/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Property tests for [`TermSuffixIndex`].

use std::collections::HashSet;

use term_suffix_index::{TIMEOUT_COUNTER_LIMIT, TermSuffixIndex};

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
#[cfg_attr(miri, ignore = "Takes too long with Miri, causing CI to timeout")]
fn add_term_longer_than_u16_max_bytes_is_noop() {
    // Trie node labels store their length as `u16`; an unrepresentable
    // term must be skipped, not panic.
    let mut sut = TermSuffixIndex::new();

    sut.add(&"a".repeat(u16::MAX as usize + 1));

    assert_eq!(sut.keys().count(), 0);
}

#[test]
#[cfg_attr(miri, ignore = "Takes too long with Miri, causing CI to timeout")]
fn add_term_growing_past_u16_max_bytes_when_lowercased_is_noop() {
    // 'İ' (U+0130, 2 bytes) lowercases to "i\u{307}" (3 bytes), so a
    // term can pass the limit only after folding; the guard must
    // measure the lowercased form.
    let mut sut = TermSuffixIndex::new();
    let term = "İ".repeat(25_000); // 50,000 bytes pre-fold, 75,000 post-fold

    sut.add(&term);

    assert_eq!(sut.keys().count(), 0);
}

#[test]
fn add_same_term_twice_is_noop() {
    let mut sut = TermSuffixIndex::new();

    sut.add("apple");
    sut.add("apple");
    let actual = sut.iter_contains("apple").collect::<Vec<_>>();

    assert_eq!(actual.len(), 1, "apple inserted twice yields one hit");
    assert_eq!(actual[0], "apple");
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
fn iter_contains_yields_terms_containing_needle() {
    let corpus = ["cat", "catalog", "category", "concat", "scat", "scatter"]
        .into_iter()
        .map(String::from)
        .collect::<Vec<_>>();
    let sut = build_index(&corpus);
    let expected = corpus.iter().cloned().collect::<HashSet<_>>();

    let actual = collect_set(sut.iter_contains("cat"));

    assert_eq!(actual, expected, "iter_contains('cat')");
}

#[test]
fn iter_suffix_and_iter_contains_empty_needle_yield_no_matches() {
    let corpus = ["cat", "dog"]
        .into_iter()
        .map(String::from)
        .collect::<Vec<_>>();
    let sut = build_index(&corpus);

    let actual_suffix = sut.iter_suffix("").count();
    let actual_contains = sut.iter_contains("").count();

    assert_eq!(actual_suffix, 0);
    assert_eq!(actual_contains, 0);
}

#[test]
fn iter_suffix_multibyte_needle_is_codepoint_aware() {
    // Codepoint-aware suffix slicing: "café" has 4 codepoints, so every
    // proper suffix down to the length-1 "é" is indexed. A byte-stride
    // port would either panic (`&term[3..]` splits the first byte of
    // "é") or insert a malformed key — neither should happen here.
    let mut sut = TermSuffixIndex::new();
    for t in ["café", "naïve", "日本語", "🦀rust"] {
        sut.add(t);
    }

    assert!(collect_set(sut.iter_suffix("fé")).contains("café"));
    // The length-1 suffix "é" is indexed too.
    assert!(collect_set(sut.iter_suffix("é")).contains("café"));
    // "本語" is a two-codepoint suffix of "日本語" — should be present ...
    assert!(collect_set(sut.iter_suffix("本語")).contains("日本語"));
    // ... as is its length-1 tail "語".
    assert!(collect_set(sut.iter_suffix("語")).contains("日本語"));

    for t in ["café", "naïve", "日本語", "🦀rust"] {
        sut.remove(t);
    }

    assert_eq!(sut.iter_contains("café").count(), 0);
    assert_eq!(sut.iter_contains("本語").count(), 0);
    assert_eq!(sut.iter_contains("rust").count(), 0);
}

#[test]
fn length_one_proper_suffix_is_indexed() {
    // "ab" stores its trailing length-1 suffix "b" as a back-reference,
    // so a 1-char suffix or contains query reaches it.
    let mut sut = TermSuffixIndex::new();
    sut.add("ab");

    assert_eq!(
        collect_set(sut.iter_suffix("ab")),
        HashSet::from(["ab".to_string()])
    );
    assert!(collect_set(sut.iter_suffix("b")).contains("ab"));
    assert!(collect_set(sut.iter_contains("a")).contains("ab"));
}

#[test]
fn iter_suffix_yields_one_hit_per_matching_term() {
    let mut sut = TermSuffixIndex::new();
    sut.add("abc");
    sut.add("xabc");

    let actual = sut
        .iter_suffix("abc")
        .map(str::to_string)
        .collect::<Vec<_>>();

    assert_eq!(actual.len(), 2, "each match yielded exactly once");
    let actual_set = actual.into_iter().collect::<HashSet<_>>();
    let expected = HashSet::from(["abc".to_string(), "xabc".to_string()]);
    assert_eq!(actual_set, expected);
}

#[test]
fn iter_suffix_yields_terms_ending_with_needle() {
    let corpus = ["cat", "catalog", "category", "concat", "scat", "scatter"]
        .into_iter()
        .map(String::from)
        .collect::<Vec<_>>();
    let sut = build_index(&corpus);
    let expected = ["cat", "concat", "scat"]
        .into_iter()
        .map(String::from)
        .collect::<HashSet<_>>();

    let actual = collect_set(sut.iter_suffix("cat"));

    assert_eq!(actual, expected, "iter_suffix('cat')");
}

#[test]
fn iter_wildcard_end_token_uses_suffix_semantics() {
    let corpus = ["concat", "scat", "catalog", "cat"]
        .into_iter()
        .map(String::from)
        .collect::<Vec<_>>();
    let sut = build_index(&corpus);

    let actual = collect_set(
        sut.iter_wildcard("*cat", || false)
            .expect("'cat' is anchorable"),
    );

    let expected = HashSet::from(["concat".to_string(), "scat".to_string(), "cat".to_string()]);
    assert_eq!(actual, expected);
}

#[test]
fn iter_wildcard_middle_tokens_anchor_on_best_literal() {
    let corpus = ["abide", "abcde", "abxxcd", "abandoned", "cdrom"]
        .into_iter()
        .map(String::from)
        .collect::<Vec<_>>();
    let sut = build_index(&corpus);

    let actual = collect_set(
        sut.iter_wildcard("ab*cd", || false)
            .expect("'ab' and 'cd' are anchorable"),
    );

    let expected = HashSet::from(["abxxcd".to_string()]);
    assert_eq!(actual, expected);
}

#[test]
fn iter_wildcard_multibyte_anchor_matches() {
    let corpus = ["日本語", "日本酒", "本語"]
        .into_iter()
        .map(String::from)
        .collect::<Vec<_>>();
    let sut = build_index(&corpus);

    let actual = collect_set(
        sut.iter_wildcard("*本語", || false)
            .expect("two codepoints anchor"),
    );

    let expected = HashSet::from(["日本語".to_string(), "本語".to_string()]);
    assert_eq!(actual, expected);
}

#[test]
fn iter_wildcard_question_mark_is_byte_wise_for_multibyte() {
    // The matcher is byte-wise, so `?` consumes one *byte*, not one
    // codepoint. `é` (U+00E9) is two UTF-8 bytes: `ab*c?` matches only its
    // first byte, the second has nothing to pair with, and the term is
    // dropped. This is the accepted approximation — an ASCII tail matches.
    let corpus = ["abxc\u{e9}", "abxcd"]
        .into_iter()
        .map(String::from)
        .collect::<Vec<_>>();
    let sut = build_index(&corpus);

    let actual = collect_set(
        sut.iter_wildcard("ab*c?", || false)
            .expect("'ab' is anchorable"),
    );

    // The ASCII-tailed term matches; the `é`-tailed one is missed.
    assert_eq!(actual, HashSet::from(["abxcd".to_string()]));
}

#[test]
fn iter_wildcard_question_mark_outside_anchor_is_honored() {
    let corpus = ["abcd", "bcd", "zbxcd"]
        .into_iter()
        .map(String::from)
        .collect::<Vec<_>>();
    let sut = build_index(&corpus);

    // The `?b` token cannot anchor (contains `?`), so `cd` is chosen;
    // the full-pattern filter must still enforce that `?` consumes
    // exactly one character.
    let actual = collect_set(
        sut.iter_wildcard("?b*cd", || false)
            .expect("'cd' is anchorable"),
    );

    let expected = HashSet::from(["abcd".to_string(), "zbxcd".to_string()]);
    assert_eq!(actual, expected);
}

#[test]
fn iter_wildcard_trailing_star_uses_contains_semantics() {
    let corpus = ["scatter", "category", "dog"]
        .into_iter()
        .map(String::from)
        .collect::<Vec<_>>();
    let sut = build_index(&corpus);

    let actual = collect_set(
        sut.iter_wildcard("*cat*", || false)
            .expect("'cat' is anchorable"),
    );

    let expected = HashSet::from(["scatter".to_string(), "category".to_string()]);
    assert_eq!(actual, expected);
}

#[test]
fn iter_wildcard_without_anchorable_token_reports_none() {
    let sut = build_index(&["abc".to_string()]);

    // `?`-bearing tokens and bare stars cannot anchor a literal trie
    // lookup. (Single-char tokens can, now that the floor is gone.)
    for pattern in ["*", "a?c", "??*", ""] {
        assert!(
            sut.iter_wildcard(pattern, || false).is_none(),
            "pattern={pattern:?} must request the fallback scan"
        );
    }
}

#[test]
#[cfg_attr(miri, ignore = "Too slow to run under miri")]
fn iter_wildcard_does_not_poll_within_the_first_window() {
    // Fewer candidates than one polling window: the predicate is never
    // consulted, so even an always-stop request yields every match.
    let small = (0..TIMEOUT_COUNTER_LIMIT / 2)
        .map(|i| format!("cat{i:03}"))
        .collect::<Vec<_>>();
    let sut = build_index(&small);

    let actual = collect_set(sut.iter_wildcard("cat*", || true).expect("'cat' anchors"));

    assert_eq!(
        actual.len(),
        small.len(),
        "no candidate within the first window triggers a poll"
    );
}

#[test]
#[cfg_attr(miri, ignore = "Too slow to run under miri")]
fn iter_wildcard_abandons_scan_once_stop_fires() {
    // More candidates than one window: the first poll fires mid-scan, so
    // an always-stop request abandons it with only a prefix of the matches.
    let large = (0..TIMEOUT_COUNTER_LIMIT + 50)
        .map(|i| format!("cat{i:03}"))
        .collect::<Vec<_>>();
    let sut = build_index(&large);

    let stopped = collect_set(sut.iter_wildcard("cat*", || true).expect("'cat' anchors"));

    assert!(
        !stopped.is_empty() && stopped.len() < large.len(),
        "always stopping abandons the scan partway: {} of {}",
        stopped.len(),
        large.len()
    );
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
fn remove_unknown_term_is_noop() {
    let mut sut = TermSuffixIndex::new();
    sut.add("present");

    sut.remove("absent");

    assert!(collect_set(sut.iter_suffix("present")).contains("present"));
}

#[test]
fn add_lowercases_terms_and_queries_are_case_insensitive() {
    let mut sut = TermSuffixIndex::new();
    sut.add("CataLog");

    // The stored term is lowercased, so it is returned in lowercase
    // regardless of the casing used on insertion.
    let stored = collect_set(sut.iter_contains("cat"));
    assert_eq!(stored, HashSet::from(["catalog".to_string()]));

    // Queries are lowercased too, so any casing of the needle hits.
    for needle in ["CAT", "Cat", "cAt"] {
        assert_eq!(
            collect_set(sut.iter_contains(needle)),
            HashSet::from(["catalog".to_string()]),
            "needle={needle}",
        );
    }
}

#[test]
fn remove_is_case_insensitive() {
    let mut sut = TermSuffixIndex::new();
    sut.add("Banana");

    // Removing with a different casing must clear the lowercased entry.
    sut.remove("baNANa");

    assert_eq!(sut.iter_contains("ana").count(), 0);
    assert_eq!(sut.iter_suffix("nana").count(), 0);
}

#[test]
fn iter_wildcard_is_case_insensitive() {
    let corpus = ["Concat", "SCAT", "cat"]
        .into_iter()
        .map(String::from)
        .collect::<Vec<_>>();
    let sut = build_index(&corpus);

    // An uppercased pattern is lowercased before matching the
    // (lowercased) stored terms.
    let actual = collect_set(
        sut.iter_wildcard("*CAT", || false)
            .expect("'cat' is anchorable"),
    );

    let expected = HashSet::from(["concat".to_string(), "scat".to_string(), "cat".to_string()]);
    assert_eq!(actual, expected);
}

#[test]
fn add_lowercases_per_codepoint_not_with_final_sigma() {
    // Greek "ΟΔΟΣ" (street) ends in a capital sigma. `str::to_lowercase`
    // would apply the word-final rule and yield "οδος" (final sigma ς),
    // but the index uses per-codepoint lowering (matching the C
    // `unicode_tolower`), so the trailing sigma stays σ: "οδοσ".
    let mut sut = TermSuffixIndex::new();
    sut.add("ΟΔΟΣ");

    // Probe the stored member through a suffix query: "οσ" is a registered
    // suffix only if the term was lowered per codepoint.
    let members = collect_set(sut.iter_suffix("οσ"));
    assert_eq!(
        members,
        HashSet::from(["οδοσ".to_string()]),
        "expected per-codepoint lowering (οδοσ), got {members:?}",
    );
    assert!(
        !members.iter().any(|m| m.contains('ς')),
        "final-sigma ς must not appear, got {members:?}",
    );
}

#[cfg(not(miri))]
mod fuzz {
    use proptest::prelude::*;

    use super::*;

    /// Kept small so fuzz cases land in a regime where suffix collisions are common.
    /// Includes `é` (multibyte) to exercise the index against non-ASCII terms.
    fn term_strategy() -> impl Strategy<Value = String> {
        "[a-zé]{1,8}"
    }

    proptest! {
        #![proptest_config(ProptestConfig { cases: 2_000, .. ProptestConfig::default() })]

        #[test]
        fn iter_contains_matches_substring_oracle(
            corpus in proptest::collection::vec(term_strategy(), 1..=20),
            needle in "[a-z]{1,4}",
        ) {
            let sut = build_index(&corpus);
            let expected = corpus
                .iter()
                .filter(|t| t.contains(&needle))
                .cloned()
                .collect::<HashSet<_>>();

            let actual = collect_set(sut.iter_contains(&needle));

            prop_assert_eq!(&actual, &expected, "needle={:?} corpus={:?}", needle, corpus);
        }

        #[test]
        fn iter_suffix_matches_ends_with_oracle(
            corpus in proptest::collection::vec(term_strategy(), 1..=20),
            needle in "[a-z]{1,4}",
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
        fn iter_wildcard_matches_full_scan_oracle(
            corpus in proptest::collection::vec(term_strategy(), 1..=20),
            pattern in "[ab*?]{0,8}",
        ) {
            use rqe_wildcard::{MatchOutcome, WildcardPattern};

            let sut = build_index(&corpus);

            // `None` requests the fallback scan — out of scope here. The
            // oracle uses the same byte-wise matcher iter_wildcard does, so
            // this pins the anchor-selection logic, not the matcher itself.
            if let Some(matches) = sut.iter_wildcard(&pattern, || false) {
                let parsed = WildcardPattern::parse(pattern.as_bytes());
                let expected = corpus
                    .iter()
                    .filter(|t| parsed.matches(t.as_bytes()) == MatchOutcome::Match)
                    .cloned()
                    .collect::<HashSet<_>>();

                let actual = collect_set(matches);

                prop_assert_eq!(actual, expected,
                    "pattern={:?} corpus={:?}", pattern, corpus);
            }
        }

        #[test]
        fn mixed_add_remove_matches_hashset_oracle(
            ops in proptest::collection::vec(
                (any::<bool>(), "[a-z]{1,6}"),
                1..=30,
            ),
            needle in "[a-z]{1,3}",
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
    }
}

fn build_index(corpus: &[String]) -> TermSuffixIndex {
    let mut sut = TermSuffixIndex::new();
    for t in corpus {
        sut.add(t);
    }
    sut
}

fn collect_set<I>(it: I) -> HashSet<String>
where
    I: Iterator,
    I::Item: AsRef<str>,
{
    it.map(|r| r.as_ref().to_string()).collect()
}
