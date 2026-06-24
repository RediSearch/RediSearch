/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#[cfg(not(miri))] // proptest calls getcwd() which is not supported on Miri
mod fuzz {
    use proptest::prelude::*;
    use std::collections::BTreeSet;

    use spellcheck_dictionary::SpellCheckDictionary;
    use string_utils::unicode_tolower;

    fn build_dict(stored: &[String]) -> SpellCheckDictionary {
        let mut sut = SpellCheckDictionary::new();
        for term in stored {
            sut.add(term);
        }
        sut
    }

    /// Independent Levenshtein oracle: naive recursion over codepoints. Distinct
    /// from the crate's Wagner–Fischer implementation, so the proptest below is
    /// genuinely cross-checked rather than comparing the function against itself.
    fn edit_distance(a: &[char], b: &[char]) -> u32 {
        match (a.split_first(), b.split_first()) {
            (None, _) => b.len() as u32,
            (_, None) => a.len() as u32,
            (Some((x, a_rest)), Some((y, b_rest))) if x == y => edit_distance(a_rest, b_rest),
            (Some((_, a_rest)), Some((_, b_rest))) => {
                1 + edit_distance(a_rest, b)
                    .min(edit_distance(a, b_rest))
                    .min(edit_distance(a_rest, b_rest))
            }
        }
    }

    fn distance(a: &str, b: &str) -> u32 {
        let a = a.chars().collect::<Vec<_>>();
        let b = b.chars().collect::<Vec<_>>();
        edit_distance(&a, &b)
    }

    proptest! {
        #[test]
        fn contains_model(
            stored in prop::collection::vec("[a-zA-Z]{1,5}", 0..20),
            query in "[a-zA-Z]{1,5}",
        ) {
            let sut = build_dict(&stored);

            // contains(q) iff some stored term lowercases to the same form as q.
            let lowered_query = unicode_tolower(&query);
            let expected = stored.iter().any(|t| unicode_tolower(t) == lowered_query);

            prop_assert_eq!(sut.contains(&query), expected);
        }

        #[test]
        fn fuzzy_matches_model(
            stored in prop::collection::vec("[a-zA-Z]{1,5}", 0..20),
            query in "[a-zA-Z]{1,5}",
            max_dist in 0u32..=3,
        ) {
            let sut = build_dict(&stored);

            // Brute-force oracle: a stored term matches iff its lowercased form
            // is within `max_dist` of the lowercased query, measured by the
            // independent `distance` above.
            let lowered_query = unicode_tolower(&query);
            let expected: BTreeSet<String> = stored
                .iter()
                .filter(|t| distance(&unicode_tolower(t), &lowered_query) <= max_dist)
                .cloned()
                .collect();

            let actual: BTreeSet<String> = sut.fuzzy_matches(&query, max_dist).collect();

            prop_assert_eq!(actual, expected);
        }
    }
}
