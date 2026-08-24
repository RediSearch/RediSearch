/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Wildcard patterns arrive with their escapes already resolved, so a
//! backslash is an ordinary character to match. See the `term_dictionary`
//! module docs.

use term_dictionary::{TermDictionary, TermEntry};

/// A term holding a literal backslash, alongside the term a second round of
/// escape resolution would wrongly steer the walk to.
fn seeded() -> TermDictionary {
    let mut dict = TermDictionary::new();
    for term in [r"a\b", "ab", r"a\bc"] {
        dict.insert(
            term,
            TermEntry {
                score: 1.0,
                num_docs: 1,
            },
        );
    }
    dict
}

fn matches(dict: &TermDictionary, pattern: &str) -> Vec<String> {
    dict.wildcard_iter(pattern, || false)
        .map(|(term, _)| term)
        .collect()
}

/// The query pipeline turns `a\\b` into `a\b` before handing it over, so this
/// is what a query for a literal backslash looks like at this layer.
#[test]
fn backslash_matches_a_backslash() {
    let dict = seeded();

    assert_eq!(matches(&dict, r"a\b"), [r"a\b"]);
}

#[test]
fn backslash_does_not_escape_the_next_character() {
    let dict = seeded();

    // Resolving escapes here would read `\b` as `b` and return `ab` instead.
    assert!(!matches(&dict, r"a\b").contains(&"ab".to_string()));
    assert_eq!(matches(&dict, "ab"), ["ab"]);
}

#[test]
fn wildcards_still_apply_next_to_a_backslash() {
    let dict = seeded();

    assert_eq!(matches(&dict, r"a\b*"), [r"a\b", r"a\bc"]);
    assert_eq!(matches(&dict, r"a\?c"), [r"a\bc"]);
}
