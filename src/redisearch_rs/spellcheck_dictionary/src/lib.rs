/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use trie_rs::str_trie_map::StrTrieMap;

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

    pub fn add(&mut self, term: &str) -> bool {
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
        self.trie.iter().map(|(term, ())| term)
    }

    pub fn contains(&self, term: &str) -> bool {
        let needle = term.to_lowercase();
        self.trie
            .iter()
            .any(|(key, ())| key.to_lowercase() == needle)
    }

    /// Yield every stored term whose case-folded form is within Levenshtein
    /// edit distance `max_dist` (in codepoints) of `term`. Matching is
    /// case-insensitive — but the yielded terms keep their original stored case.
    pub fn fuzzy_matches(&self, term: &str, max_dist: u32) -> impl Iterator<Item = String> + '_ {
        let needle = term.to_lowercase();
        self.trie.iter().filter_map(move |(key, ())| {
            (levenshtein(&key.to_lowercase(), &needle) <= max_dist).then_some(key)
        })
    }
}

fn levenshtein(a: &str, b: &str) -> u32 {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    let (m, n) = (a.len(), b.len());

    let mut prev: Vec<u32> = (0..=n as u32).collect();
    let mut curr = vec![0u32; n + 1];
    for i in 1..=m {
        curr[0] = i as u32;
        for j in 1..=n {
            let cost = u32::from(a[i - 1] != b[j - 1]);
            curr[j] = (curr[j - 1] + 1).min(prev[j] + 1).min(prev[j - 1] + cost);
        }
        std::mem::swap(&mut prev, &mut curr);
    }
    prev[n]
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
    fn contains_folds_case(#[case] stored: &[&str], #[case] query: &str, #[case] expected: bool) {
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
    fn fuzzy_folds_multibyte() {
        let mut sut = SpellCheckDictionary::new();
        sut.add("Fußball");

        assert_eq!(
            fuzzy(&sut, "fußball", 0),
            BTreeSet::from(["Fußball".into()])
        );
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
    #[case("abc", "abd", 1)] // single substitution
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

            // Brute-force oracle: a stored term matches iff its folded form is
            // within `max_dist` of the folded query.
            let folded_query = query.to_lowercase();
            let expected: BTreeSet<String> = stored
                .iter()
                .filter(|t| levenshtein(&t.to_lowercase(), &folded_query) <= max_dist)
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
