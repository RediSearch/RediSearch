/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Empty-pattern semantics, which deliberately diverge from the underlying
//! `StrTrieMap` to match the C walk this dictionary replaces. See the
//! `term_dictionary` module docs.

use term_dictionary::{TermDictionary, TermEntry};

fn seeded() -> TermDictionary {
    let mut dict = TermDictionary::new();
    for term in ["apple", "banana", "cherry"] {
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

#[test]
fn contains_iter_empty_target_yields_nothing() {
    let dict = seeded();

    assert_eq!(dict.contains_iter("", || false).count(), 0);
}

#[test]
fn suffixed_iter_empty_suffix_yields_nothing() {
    let dict = seeded();

    assert_eq!(dict.suffixed_iter("", || false).count(), 0);
}

/// The guard is specific to the substring and suffix walks: an empty prefix
/// matches every term in C too, so the trie's own semantics stand.
#[test]
fn prefixed_iter_empty_prefix_yields_every_term() {
    let dict = seeded();

    assert_eq!(dict.prefixed_iter("", || false).count(), dict.len());
}

/// A non-empty pattern must still reach the trie — the guard keys off
/// emptiness alone, not off the walk being cheap.
#[test]
fn non_empty_patterns_are_unaffected() {
    let dict = seeded();

    assert_eq!(dict.contains_iter("an", || false).count(), 1);
    assert_eq!(dict.suffixed_iter("y", || false).count(), 1);
}
