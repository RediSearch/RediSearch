/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Every pattern walk carries the caller's stop signal all the way down to
//! the trie, across the case-folding layer that sits between them. The
//! polling contract itself is covered in `trie_rs`; these tests pin the
//! forwarding at this crate's boundary.

use term_dictionary::{TermDictionary, TermEntry};

/// Enough terms that any full walk takes well over the trie's polling
/// granularity, so an always-true predicate fires before the walk ends.
const N_TERMS: usize = 300;

/// Every term shares the `term` prefix, the `z` suffix, and `term` as a
/// substring, so each walk under test matches all of them.
fn seeded() -> TermDictionary {
    let mut dict = TermDictionary::new();
    for i in 0..N_TERMS {
        dict.insert(
            &format!("term{i:04}z"),
            TermEntry {
                score: 1.0,
                num_docs: 1,
            },
        );
    }
    dict
}

#[test]
fn contains_iter_forwards_the_stop_signal() {
    let dict = seeded();

    assert_eq!(dict.contains_iter("term", || false).count(), N_TERMS);
    assert!(dict.contains_iter("term", || true).count() < N_TERMS);
}

#[test]
fn prefixed_iter_forwards_the_stop_signal() {
    let dict = seeded();

    assert_eq!(dict.prefixed_iter("term", || false).count(), N_TERMS);
    assert!(dict.prefixed_iter("term", || true).count() < N_TERMS);
}

#[test]
fn suffixed_iter_forwards_the_stop_signal() {
    let dict = seeded();

    assert_eq!(dict.suffixed_iter("z", || false).count(), N_TERMS);
    assert!(dict.suffixed_iter("z", || true).count() < N_TERMS);
}

#[test]
fn wildcard_iter_forwards_the_stop_signal() {
    let dict = seeded();

    assert_eq!(dict.wildcard_iter("term*", || false).count(), N_TERMS);
    assert!(dict.wildcard_iter("term*", || true).count() < N_TERMS);
}
