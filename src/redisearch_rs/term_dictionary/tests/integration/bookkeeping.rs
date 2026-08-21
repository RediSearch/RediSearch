/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Per-term bookkeeping contract for [`TermDictionary`]: the reported
//! outcomes ([`InsertOutcome`], [`DecrResult`], prior-entry returns) and
//! the delete-on-last-doc boundary. Case-folding behaviour is pinned in
//! the sibling `case_folding` module.

use term_dictionary::{DecrResult, InsertOutcome, TermDictionary, TermEntry};

#[test]
fn add_term_reports_new_then_updated() {
    let mut dict = TermDictionary::new();
    assert_eq!(dict.add_term("foo", 1.0, 1), InsertOutcome::New);
    assert_eq!(dict.add_term("foo", 1.0, 1), InsertOutcome::Updated);
}

#[test]
fn replace_term_reports_new_then_updated() {
    let mut dict = TermDictionary::new();
    assert_eq!(dict.replace_term("foo", 1.0, 1), InsertOutcome::New);
    assert_eq!(dict.replace_term("foo", 2.0, 1), InsertOutcome::Updated);
}

#[test]
fn insert_returns_prior_entry() {
    let mut dict = TermDictionary::new();
    assert!(
        dict.insert(
            "foo",
            TermEntry {
                score: 1.0,
                num_docs: 2,
            },
        )
        .is_none(),
        "first insert has no prior entry"
    );
    let prior = dict
        .insert(
            "foo",
            TermEntry {
                score: 5.0,
                num_docs: 7,
            },
        )
        .expect("overwrite must hand back the displaced entry");
    assert_eq!(
        prior,
        TermEntry {
            score: 1.0,
            num_docs: 2,
        }
    );
    // The overwrite did not accumulate — that is what distinguishes
    // `insert` from `replace_term`.
    assert_eq!(dict.get("foo").unwrap().num_docs, 7);
}

#[test]
fn decrement_num_docs_missing_term_is_not_found() {
    let mut dict = TermDictionary::new();
    dict.add_term("foo", 1.0, 1);
    assert_eq!(dict.decrement_num_docs("bar", 1), DecrResult::NotFound);
    assert_eq!(dict.len(), 1, "a miss must not disturb other entries");
}

#[test]
fn decrement_num_docs_exact_delta_deletes() {
    // `delta == num_docs` must take the removal path: a regression to
    // `delta <= num_docs` staying in place would leave a zombie entry
    // with `num_docs == 0` that the over-shoot case cannot catch.
    let mut dict = TermDictionary::new();
    dict.add_term("foo", 1.0, 3);
    assert_eq!(dict.decrement_num_docs("foo", 3), DecrResult::Deleted);
    assert!(dict.get("foo").is_none());
    assert_eq!(dict.len(), 0);
}
