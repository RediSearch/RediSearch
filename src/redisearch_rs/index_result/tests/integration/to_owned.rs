/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Tests for [`RSIndexResult::to_owned`] of a record owning its term: the copy must stay
//! readable after the record it was taken from is dropped.
//!
//! Run these under miri as well: outside miri, a copy still pointing at a freed term
//! usually reads back the old values.

use index_result::{RSIndexResult, RSOffsetSlice, RSOffsetVector, RSQueryTerm, RSTermRecord};

const OFFSETS: &[u8] = &[1, 2, 3];

fn foo_term() -> Box<RSQueryTerm> {
    let mut term = RSQueryTerm::new("foo", 4, 0);
    term.set_idf(1.5);
    term.set_bm25_idf(0.25);
    term
}

fn owned_offsets() -> RSOffsetVector {
    RSOffsetSlice::from_slice(OFFSETS).to_owned()
}

/// A term record that owns both its query term and its offsets.
fn fully_owned_term() -> RSIndexResult<'static> {
    RSIndexResult::build_term()
        .fully_owned_record(Some(foo_term()), owned_offsets())
        .doc_id(7)
        .build()
}

/// Erase `r`'s lifetimes, so that a copy taken through the result can outlive `r`.
///
/// # Safety
///
/// 1. The returned reference must only be used while `r`'s own storage is alive.
/// 2. Once `r` is dropped, only what a copy taken from `r` owns may be read. The tests read
///    exactly that, which is the property miri checks.
unsafe fn detach<'a>(r: &RSIndexResult<'a>) -> &'static RSIndexResult<'static> {
    let r: *const RSIndexResult<'a> = r;
    // SAFETY: `r` comes from a live reference, so it is non-null, aligned and valid for
    // reads; only the lifetimes are extended, and preconditions 1. and 2. bound how long the
    // result is used.
    unsafe { &*r.cast::<RSIndexResult<'static>>() }
}

/// The query term `result` points at, compared by address to tell a shared term from a copy.
fn term_ptr(result: &RSIndexResult<'_>) -> *const RSQueryTerm {
    let term = result
        .as_term()
        .and_then(RSTermRecord::query_term)
        .expect("a term result with a term");
    std::ptr::from_ref(term)
}

/// Buffering result processors deep-copy each record a disk reader yields, then read the copy
/// after the reader has dropped the record on its next read.
#[test]
fn an_owned_copy_of_a_fully_owned_record_outlives_it() {
    let source = fully_owned_term();
    // SAFETY: only the copy's own term and offsets are read once `source` is gone.
    let copy = unsafe { detach(&source) }.to_owned();
    drop(source);

    let record = copy.as_term().expect("a term result");
    assert!(
        matches!(record, RSTermRecord::FullyOwned { .. }),
        "the copy owns its term instead of borrowing it",
    );
    let term = record.query_term().expect("the term is carried along");
    assert_eq!(term.as_bytes(), Some(&b"foo"[..]));
    assert_eq!(term.id(), 4);
    assert_eq!(term.idf(), 1.5);
    assert_eq!(term.bm25_idf(), 0.25);
    assert_eq!(record.offsets(), OFFSETS);
}

#[test]
fn an_owned_copy_shares_the_term_of_a_fully_owned_record() {
    let source = fully_owned_term();
    let copy = source.to_owned();

    assert_eq!(term_ptr(&copy), term_ptr(&source));
}
