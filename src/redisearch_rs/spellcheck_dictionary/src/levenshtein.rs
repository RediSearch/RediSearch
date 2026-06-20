/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

/// Levenshtein edit distance between `s1` and `s2`, counted in Unicode
/// codepoints. Uses the Wagner–Fischer dynamic-programming algorithm with the
/// single-column space optimization: one buffer holds the running column and a
/// `lastdiag` scalar carries the diagonal cell, giving O(m·n) time and O(n)
/// space. Adapted from
/// <https://en.wikibooks.org/wiki/Algorithm_Implementation/Strings/Levenshtein_distance#Rust>.
pub(super) fn levenshtein(s1: &str, s2: &str) -> u32 {
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
    use rstest::rstest;

    // Known-answer cases pin `levenshtein` to an independent definition.
    // The `fuzzy_matches_model` proptest uses `levenshtein` as its own
    // oracle, so it cannot catch a bug in the function itself; these can.
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
}
