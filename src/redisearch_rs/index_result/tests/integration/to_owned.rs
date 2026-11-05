/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Tests for [`RSIndexResult::to_owned`]: a copy must stay readable after the records it was
//! taken from, and the iterators holding their terms, are dropped.
//!
//! Run these under miri as well: outside miri, a copy still pointing at a freed term
//! usually reads back the old values.

use index_result::{
    MetricsVec, RSIndexResult, RSOffsetSlice, RSOffsetVector, RSQueryTerm, RSTermRecord,
};

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

/// A term leaf as an inverted index reader produces it: the record owns its query term
/// and borrows its offsets.
fn term_leaf(offsets: &[u8]) -> RSIndexResult<'_> {
    RSIndexResult::build_term()
        .borrowed_record(Some(foo_term()), RSOffsetSlice::from_slice(offsets))
        .doc_id(7)
        .frequency(2)
        .build()
}

/// A term record that owns both its query term and its offsets.
fn owned_term() -> RSIndexResult<'static> {
    RSIndexResult::build_term()
        .owned_record(Some(foo_term()), owned_offsets())
        .doc_id(7)
        .build()
}

/// Erase `r`'s lifetimes, so that a parent borrowing it can outlive it.
///
/// # Safety
///
/// 1. The returned reference must only be used while `r`'s own storage is alive.
/// 2. Once the data `r` borrows is dropped, only what a copy taken from `r` owns may be read.
///    The tests read exactly that, which is the property miri checks.
unsafe fn detach<'a>(r: &RSIndexResult<'a>) -> &'static RSIndexResult<'static> {
    let r: *const RSIndexResult<'a> = r;
    // SAFETY: `r` comes from a live reference, so it is non-null, aligned and valid for
    // reads; only the lifetimes are extended, and preconditions 1. and 2. bound how long the
    // result is used.
    unsafe { &*r.cast::<RSIndexResult<'static>>() }
}

fn assert_is_detached_foo(record: &RSTermRecord<'_>) {
    assert!(
        matches!(record, RSTermRecord::Owned { .. }),
        "the copy owns its term instead of borrowing it",
    );
    let term = record.query_term().expect("the term is carried along");
    assert_eq!(term.as_bytes(), Some(&b"foo"[..]));
    assert_eq!(term.id(), 4);
    assert_eq!(term.idf(), 1.5);
    assert_eq!(term.bm25_idf(), 0.25);
    assert_eq!(record.offsets(), OFFSETS);
}

/// Walk `result` and assert every term record in it owns its term.
fn assert_all_terms_owned(result: &RSIndexResult<'_>) {
    if let Some(record) = result.as_term() {
        assert!(
            matches!(record, RSTermRecord::Owned { .. }),
            "every term record in the copy owns its term",
        );
    }
    let mut i = 0;
    while let Some(child) = result.get(i) {
        assert_all_terms_owned(child);
        i += 1;
    }
}

/// The query term `result` points at, compared by address to tell a shared term from a copy.
fn term_ptr(result: &RSIndexResult<'_>) -> *const RSQueryTerm {
    let term = result
        .as_term()
        .and_then(RSTermRecord::query_term)
        .expect("a term result with a term");
    std::ptr::from_ref(term)
}

#[test]
fn a_term_copy_outlives_the_record_holding_the_term() {
    let leaf = term_leaf(OFFSETS);
    let copy = leaf.to_owned();
    drop(leaf);

    assert_eq!((copy.doc_id, copy.freq), (7, 2));
    assert_is_detached_foo(copy.as_term().expect("a term result"));
}

/// Buffering result processors deep-copy each record a top-k yields, then read the copy after
/// the top-k has dropped the record on its next read.
#[test]
fn a_term_copy_outlives_an_owned_record() {
    let source = owned_term();
    let copy = source.to_owned();
    drop(source);

    assert_is_detached_foo(copy.as_term().expect("a term result"));
}

#[test]
fn a_term_copy_shares_the_term_of_a_leaf() {
    let leaf = term_leaf(OFFSETS);
    let copy = leaf.to_owned();

    assert_eq!(term_ptr(&copy), term_ptr(&leaf));
}

#[test]
fn a_term_copy_shares_the_term_of_an_owned_record() {
    let source = owned_term();
    let copy = source.to_owned();

    assert_eq!(term_ptr(&copy), term_ptr(&source));
}

#[test]
fn a_term_copy_without_a_term_stays_without_one() {
    let source = RSIndexResult::build_term()
        .borrowed_record(None, RSOffsetSlice::from_slice(OFFSETS))
        .build();
    let copy = source.to_owned();

    let record = copy.as_term().expect("a term result");
    assert!(matches!(record, RSTermRecord::Owned { term: None, .. }));
    assert_eq!(record.offsets(), OFFSETS);
}

#[test]
fn an_aggregate_copy_outlives_the_children_it_borrowed() {
    let leaf = Box::new(term_leaf(OFFSETS));
    let mut union = RSIndexResult::build_union(1).build();
    // SAFETY: `union` is dropped before `leaf`; the copy reads only what it owns once both
    // are gone.
    union.push_borrowed(unsafe { detach(&leaf) }, MetricsVec::new());

    let copy = union.to_owned();
    // The scenario the copy exists for: a composite iterator drops a child, and with it
    // the term that child's record holds, while the copy is still held.
    drop(union);
    drop(leaf);

    assert_eq!(copy.doc_id, 7);
    let child = copy.get(0).expect("the child is copied along");
    assert_is_detached_foo(child.as_term().expect("a term child"));
}

/// Every result kind, at every depth, in both aggregate flavours: an owned hybrid-metric
/// aggregate holding a borrowed intersection of an owned term and a numeric, next to a metric
/// and a virtual result.
#[test]
fn a_nested_copy_equals_its_source_and_owns_every_term() {
    let term_child = Box::new(owned_term());
    let numeric_child = Box::new(RSIndexResult::build_numeric(3.0).doc_id(7).build());
    let mut intersection = RSIndexResult::build_intersect(2).build();
    // SAFETY: `intersection` is dropped before both children; the copy reads only what it
    // owns once all of them are gone.
    intersection.push_borrowed(unsafe { detach(&term_child) }, MetricsVec::new());
    // SAFETY: as above.
    intersection.push_borrowed(unsafe { detach(&numeric_child) }, MetricsVec::new());

    let mut source = RSIndexResult::build_hybrid_metric().build();
    source.push_boxed(Box::new(intersection.to_owned()));
    source.push_boxed(Box::new(RSIndexResult::build_metric(0.5).doc_id(7).build()));
    source.push_boxed(Box::new(RSIndexResult::build_virt().doc_id(7).build()));
    source.metrics.push_without_key(0.75);

    let copy = source.to_owned();
    assert!(copy == source, "the copy holds the same data as its source");
    assert_eq!(
        copy.as_aggregate().map(|agg| agg.kind_mask()),
        source.as_aggregate().map(|agg| agg.kind_mask()),
    );

    drop(source);
    drop(intersection);
    drop(term_child);
    drop(numeric_child);

    assert_all_terms_owned(&copy);
    assert_eq!(copy.metrics.get(0).map(|m| m.value()), Some(0.75));
    let nested = copy.get(0).expect("the intersection is copied along");
    assert_is_detached_foo(
        nested
            .get(0)
            .and_then(RSIndexResult::as_term)
            .expect("the intersection's term child"),
    );
    assert_eq!(nested.get(1).and_then(RSIndexResult::as_numeric), Some(3.0));
}
