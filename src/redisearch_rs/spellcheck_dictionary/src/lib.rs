/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use trie_rs::str_trie_map::StrTrieMap;

/// Maximum query length, in Unicode codepoints, that the dictionary will match
/// against. C's `Trie_IterateFuzzy` returns no iterator once the lowercased
/// rune length exceeds this, so an over-long term yields no matches rather than
/// scanning every stored entry. Sourced from the C `#define` so the two stay in
/// lockstep.
const TRIE_MAX_PREFIX: usize = ffi::TRIE_MAX_PREFIX as usize;

/// Upper bound on insertable term length, in runes (codepoints), enforced by
/// C's `Trie_Insert`. Sourced from the C `#define` so the two stay in lockstep.
const TRIE_INITIAL_STRING_LEN: usize = ffi::TRIE_INITIAL_STRING_LEN as usize;

#[derive(Debug, Default)]
pub struct SpellCheckDictionary {
    trie: StrTrieMap<()>,
}

impl SpellCheckDictionary {
    pub const fn new() -> Self {
        Self {
            trie: StrTrieMap::new(),
        }
    }

    /// Insert `term`, returning `true` only if it was newly added.
    ///
    /// Mirrors C's `Trie_Insert` accept rule: empty terms, terms whose byte
    /// length exceeds `TRIE_INITIAL_STRING_LEN` runes' worth of bytes, and
    /// terms of `TRIE_INITIAL_STRING_LEN` or more codepoints are silently
    /// rejected (returning `false`) rather than stored.
    pub fn add(&mut self, term: &str) -> bool {
        // C runes are `uint16_t`; the byte gate is `TRIE_INITIAL_STRING_LEN *
        // sizeof(rune)`, matching `Trie_InsertStringBuffer`.
        if term.is_empty()
            || term.len() > TRIE_INITIAL_STRING_LEN * size_of::<u16>()
            || term.chars().count() >= TRIE_INITIAL_STRING_LEN
        {
            return false;
        }
        self.trie.insert(term, ()).is_none()
    }

    pub fn remove(&mut self, term: &str) -> bool {
        self.trie.remove(term).is_some()
    }

    pub const fn len(&self) -> usize {
        self.trie.len()
    }

    pub const fn is_empty(&self) -> bool {
        self.trie.is_empty()
    }

    pub fn dump(&self) -> impl Iterator<Item = String> {
        self.trie.iter().map(|(term, _)| term)
    }

    pub fn contains(&self, term: &str) -> bool {
        let needle = term.to_lowercase();
        if needle.chars().count() > TRIE_MAX_PREFIX {
            return false;
        }
        self.trie
            .iter()
            .any(|(key, _)| key.to_lowercase() == needle)
    }

    /// Yield every stored term whose lowercased form is within Levenshtein
    /// edit distance `max_dist` (in codepoints) of `term`. Matching is
    /// case-insensitive — but the yielded terms keep their original stored case.
    ///
    /// A `term` longer than [`TRIE_MAX_PREFIX`] (after lowercasing) yields
    /// nothing, matching C's `Trie_IterateFuzzy` length cutoff.
    pub fn fuzzy_matches(&self, term: &str, max_dist: u32) -> impl Iterator<Item = String> + '_ {
        let needle = term.to_lowercase();
        let needle = (needle.chars().count() <= TRIE_MAX_PREFIX).then_some(needle);
        needle.into_iter().flat_map(move |needle| {
            self.trie.iter().filter_map(move |(key, _)| {
                (levenshtein(&key.to_lowercase(), &needle) <= max_dist).then_some(key)
            })
        })
    }
}

/// Levenshtein edit distance between `s1` and `s2`, counted in Unicode
/// codepoints. Uses the Wagner–Fischer dynamic-programming algorithm with the
/// single-column space optimization: one buffer holds the running column and a
/// `lastdiag` scalar carries the diagonal cell, giving O(m·n) time and O(n)
/// space. Adapted from
/// <https://en.wikibooks.org/wiki/Algorithm_Implementation/Strings/Levenshtein_distance#Rust>.
fn levenshtein(s1: &str, s2: &str) -> u32 {
    let v1 = s1.chars().collect::<Vec<_>>();
    let v2 = s2.chars().collect::<Vec<_>>();

    let v1len = v1.len();
    let v2len = v2.len();
    if v1len == 0 {
        return v2len as u32;
    }
    if v2len == 0 {
        return v1len as u32;
    }

    fn min3<T: Ord>(v1: T, v2: T, v3: T) -> T {
        std::cmp::min(v1, std::cmp::min(v2, v3))
    }

    const fn delta(x: char, y: char) -> usize {
        if x == y { 0 } else { 1 }
    }

    let mut column = (0..v1len + 1).collect::<Vec<_>>();
    for x in 1..v2len + 1 {
        column[0] = x;
        let mut lastdiag = x - 1;

        for y in 1..v1len + 1 {
            let olddiag = column[y];
            column[y] = min3(
                column[y] + 1,
                column[y - 1] + 1,
                lastdiag + delta(v1[y - 1], v2[x - 1]),
            );
            lastdiag = olddiag;
        }
    }

    column[v1len] as u32
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use rstest::rstest;
    use std::collections::BTreeSet;

    #[rstest]
    #[case(&["Hello"], "Hello", true)]
    #[case(&["Hello"], "hello", true)]
    #[case(&["Hello"], "HELLO", true)]
    #[case(&["Hello"], "world", false)]
    #[case(&["Fußball"], "fußball", true)]
    #[case(&["И"], "и", true)]
    fn contains_lowercases(#[case] stored: &[&str], #[case] query: &str, #[case] expected: bool) {
        let mut sut = SpellCheckDictionary::new();
        for term in stored {
            sut.add(term);
        }

        assert_eq!(sut.contains(query), expected);
    }

    #[test]
    fn remove_is_case_sensitive_but_contains_is_not() {
        let mut sut = SpellCheckDictionary::new();
        sut.add("Foo");

        assert!(!sut.remove("foo"));
        assert!(sut.contains("foo"));

        assert!(sut.remove("Foo"));
        assert!(!sut.contains("foo"));
    }

    #[test]
    fn add_rejects_empty_term() {
        let mut sut = SpellCheckDictionary::new();

        assert!(!sut.add(""));
        assert_eq!(sut.len(), 0);
        assert!(!sut.contains(""));
    }

    #[rstest]
    #[case(TRIE_INITIAL_STRING_LEN - 1, true)] // 255 codepoints: accepted
    #[case(TRIE_INITIAL_STRING_LEN, false)] // 256 codepoints: rejected, like C
    fn add_enforces_codepoint_limit(#[case] codepoints: usize, #[case] accepted: bool) {
        let term: String = "a".repeat(codepoints);
        let mut sut = SpellCheckDictionary::new();

        assert_eq!(sut.add(&term), accepted);
        assert_eq!(sut.len(), usize::from(accepted));
    }

    #[test]
    fn add_enforces_byte_limit_for_multibyte() {
        // 'あ' is 3 UTF-8 bytes / 1 codepoint. 200 of them = 600 bytes (over the
        // 512-byte gate) but only 200 codepoints (under 256): C rejects it via
        // the byte gate, before the codepoint gate is reached.
        let term: String = "あ".repeat(200);
        assert!(term.len() > TRIE_INITIAL_STRING_LEN * size_of::<u16>());
        assert!(term.chars().count() < TRIE_INITIAL_STRING_LEN);

        let mut sut = SpellCheckDictionary::new();

        assert!(!sut.add(&term));
        assert_eq!(sut.len(), 0);
    }

    fn fuzzy(dict: &SpellCheckDictionary, query: &str, max_dist: u32) -> BTreeSet<String> {
        dict.fuzzy_matches(query, max_dist).collect()
    }

    #[test]
    fn fuzzy_matches_within_distance() {
        let mut sut = SpellCheckDictionary::new();
        for term in ["apple", "apply", "ample", "orange"] {
            sut.add(term);
        }

        // dist 0 finds only the exact term.
        assert_eq!(fuzzy(&sut, "apple", 0), BTreeSet::from(["apple".into()]));

        // dist 1: "aple" -> "apple"/"ample" (insert), "apply"/"ample" within 1
        // of "apple"? "apple" vs "aple" = 1, "ample" vs "aple" = 1.
        assert_eq!(
            fuzzy(&sut, "aple", 1),
            BTreeSet::from(["apple".into(), "ample".into()])
        );

        // far term never appears.
        assert!(!fuzzy(&sut, "aple", 2).contains("orange"));
    }

    #[test]
    fn fuzzy_is_case_insensitive_but_preserves_stored_case() {
        let mut sut = SpellCheckDictionary::new();
        sut.add("Apple");

        // Upper-case query still matches; result keeps the stored case.
        assert_eq!(fuzzy(&sut, "APPLE", 0), BTreeSet::from(["Apple".into()]));
        assert_eq!(fuzzy(&sut, "aple", 1), BTreeSet::from(["Apple".into()]));
    }

    #[test]
    fn fuzzy_lowercases_multibyte() {
        let mut sut = SpellCheckDictionary::new();
        sut.add("Fußball");

        assert_eq!(
            fuzzy(&sut, "fußball", 0),
            BTreeSet::from(["Fußball".into()])
        );
    }

    #[rstest]
    #[case(TRIE_MAX_PREFIX, true)] // at the limit: still matches
    #[case(TRIE_MAX_PREFIX + 1, false)] // one over: ignored, like C
    fn query_length_cutoff(#[case] query_len: usize, #[case] expected_match: bool) {
        let term: String = "a".repeat(query_len);
        let mut sut = SpellCheckDictionary::new();
        sut.add(&term);

        assert_eq!(sut.contains(&term), expected_match);
        assert_eq!(
            sut.fuzzy_matches(&term, 0).next().is_some(),
            expected_match
        );
    }

    #[test]
    fn cutoff_measures_lowercased_codepoints() {
        // 'İ' (U+0130) lowercases to two codepoints ("i̇"), so 51 of them
        // exceed the 100-codepoint limit only after lowercasing.
        let term: String = "İ".repeat(51);
        assert_eq!(term.chars().count(), 51);
        assert!(term.to_lowercase().chars().count() > TRIE_MAX_PREFIX);

        let mut sut = SpellCheckDictionary::new();
        sut.add(&term);

        assert!(!sut.contains(&term));
        assert!(sut.fuzzy_matches(&term, 0).next().is_none());
    }

    // Known-answer cases pin `levenshtein` to an independent definition.
    // The `fuzzy_matches_model` proptest below uses `levenshtein` as its
    // own oracle, so it cannot catch a bug in the function itself; these
    // can.
    #[rstest]
    #[case("", "", 0)]
    #[case("abc", "abc", 0)]
    #[case("", "abc", 3)] // pure insertions
    #[case("abc", "", 3)] // pure deletions
    #[case("abc", "abe", 1)] // single substitution
    #[case("kitten", "sitting", 3)] // classic: e>i, +s, +g
    #[case("ab", "ba", 2)] // transposition costs 2 (not Damerau)
    #[case("café", "cafe", 1)] // multibyte codepoint substitution
    fn levenshtein_known_distances(#[case] a: &str, #[case] b: &str, #[case] expected: u32) {
        assert_eq!(levenshtein(a, b), expected);
        assert_eq!(levenshtein(b, a), expected, "distance must be symmetric");
    }

    proptest! {
        #[test]
        fn fuzzy_matches_model(
            stored in prop::collection::vec("[a-zA-Z]{1,5}", 0..20),
            query in "[a-zA-Z]{1,5}",
            max_dist in 0u32..=3,
        ) {
            let mut sut = SpellCheckDictionary::new();
            for term in &stored {
                sut.add(term);
            }

            // Brute-force oracle: a stored term matches iff its lowercased form
            // is within `max_dist` of the lowercased query.
            let lowered_query = query.to_lowercase();
            let expected: BTreeSet<String> = stored
                .iter()
                .filter(|t| levenshtein(&t.to_lowercase(), &lowered_query) <= max_dist)
                .cloned()
                .collect();

            prop_assert_eq!(fuzzy(&sut, &query, max_dist), expected);
        }

        #[test]
        fn add_then_contains_roundtrip(term in "\\PC{1,8}") {
            let mut sut = SpellCheckDictionary::new();
            sut.add(&term);

            prop_assert!(sut.contains(&term));
        }

        #[test]
        fn tracks_set_model(ops in prop::collection::vec((any::<bool>(), "[a-zA-Z]{1,5}"), 0..50)) {
            let mut sut = SpellCheckDictionary::new();
            let mut model = BTreeSet::new();

            for (is_add, term) in ops {
                if is_add {
                    prop_assert_eq!(sut.add(&term), model.insert(term.clone()));
                } else {
                    prop_assert_eq!(sut.remove(&term), model.remove(&term));
                }
            }

            prop_assert_eq!(sut.len(), model.len());
            prop_assert_eq!(sut.dump().collect::<BTreeSet<_>>(), model);
        }
    }
}
