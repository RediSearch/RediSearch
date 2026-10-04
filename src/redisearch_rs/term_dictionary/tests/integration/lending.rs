/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Every walk the dictionary exposes can be driven through
//! [`LendingStrIter`], which is how the C entry points erase them behind one
//! trait object.

use term_dictionary::{LendingStrIter, TermDictionary, TermEntry};

fn seeded() -> TermDictionary {
    let mut dict = TermDictionary::new();
    for (i, term) in ["apple", "banana", "grape", "pineapple"]
        .into_iter()
        .enumerate()
    {
        dict.insert(
            term,
            TermEntry {
                score: i as f32,
                num_docs: i,
            },
        );
    }
    dict
}

/// Drain a lending iterator, copying each lent term so the result can be
/// compared with [`owned`].
fn lent<'td, I: LendingStrIter<'td, Data = TermEntry>>(mut iter: I) -> Vec<(String, TermEntry)> {
    let mut out = Vec::new();
    while let Some((term, entry)) = iter.next_borrowed() {
        out.push((term.to_owned(), entry.clone()));
    }
    out
}

fn owned<'td>(iter: impl Iterator<Item = (String, &'td TermEntry)>) -> Vec<(String, TermEntry)> {
    iter.map(|(term, entry)| (term, entry.clone())).collect()
}

#[test]
fn lending_and_owning_views_agree() {
    let dict = seeded();

    assert_eq!(lent(dict.iter()), owned(dict.iter()));
    assert_eq!(
        lent(dict.prefixed_iter("apple")),
        owned(dict.prefixed_iter("apple"))
    );
    assert_eq!(
        lent(dict.suffixed_iter("apple")),
        owned(dict.suffixed_iter("apple"))
    );
    assert_eq!(
        lent(dict.contains_iter("nan")),
        owned(dict.contains_iter("nan"))
    );
    assert_eq!(
        lent(dict.wildcard_iter("*apple")),
        owned(dict.wildcard_iter("*apple"))
    );
    assert_eq!(
        lent(dict.fuzzy_iter("aple", 1)),
        owned(dict.fuzzy_iter("aple", 1))
    );
}

/// The empty-pattern guards return an iterator that never starts a walk; it
/// still has to lend nothing rather than lend an unvisited key.
#[test]
fn empty_patterns_lend_nothing() {
    let dict = seeded();

    assert_eq!(lent(dict.contains_iter("")), Vec::new());
    assert_eq!(lent(dict.suffixed_iter("")), Vec::new());
}
