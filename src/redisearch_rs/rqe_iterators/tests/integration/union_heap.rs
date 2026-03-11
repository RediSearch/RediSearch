/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Integration tests for the UnionHeap iterator variants.
//!
//! Tests use `UnionFullHeap` by default, which is the "full mode" variant
//! that aggregates results from all matching children using a heap-based
//! minimum finding algorithm.

use rqe_iterators::{
    RQEIterator, SkipToOutcome, UnionFullFlat, UnionFullHeap, UnionQuickHeap,
    id_list::IdListSorted,
};

/// Test that UnionFullHeap produces the same results as UnionFullFlat.
#[test]
#[cfg_attr(miri, ignore)]
fn heap_variant_produces_same_results() {
    let ids1 = vec![10, 20, 30, 40, 50];
    let ids2 = vec![15, 25, 35, 45, 55];

    // Test with flat variant
    let mut flat_iter = UnionFullFlat::new(vec![
        IdListSorted::new(ids1.clone()),
        IdListSorted::new(ids2.clone()),
    ]);
    let mut flat_results = Vec::new();
    while let Some(result) = flat_iter.read().expect("read failed") {
        flat_results.push(result.doc_id);
    }

    // Test with heap variant
    let mut heap_iter = UnionFullHeap::new(vec![IdListSorted::new(ids1), IdListSorted::new(ids2)]);
    let mut heap_results = Vec::new();
    while let Some(result) = heap_iter.read().expect("read failed") {
        heap_results.push(result.doc_id);
    }

    assert_eq!(flat_results, heap_results);
}

/// Test that UnionQuickHeap produces the same doc_ids as other variants.
#[test]
#[cfg_attr(miri, ignore)]
fn quick_heap_variant_produces_same_doc_ids() {
    let ids1 = vec![10, 20, 30, 40, 50];
    let ids2 = vec![15, 25, 35, 45, 55];

    // Test with full flat variant
    let mut full_iter = UnionFullFlat::new(vec![
        IdListSorted::new(ids1.clone()),
        IdListSorted::new(ids2.clone()),
    ]);
    let mut full_results = Vec::new();
    while let Some(result) = full_iter.read().expect("read failed") {
        full_results.push(result.doc_id);
    }

    // Test with quick heap variant
    let mut quick_heap_iter =
        UnionQuickHeap::new(vec![IdListSorted::new(ids1), IdListSorted::new(ids2)]);
    let mut quick_heap_results = Vec::new();
    while let Some(result) = quick_heap_iter.read().expect("read failed") {
        quick_heap_results.push(result.doc_id);
    }

    assert_eq!(full_results, quick_heap_results);
}

/// Test skip_to with heap variant.
#[test]
#[cfg_attr(miri, ignore)]
fn heap_variant_skip_to() {
    let ids1 = vec![10, 20, 30, 40, 50];
    let ids2 = vec![15, 25, 35, 45, 55];

    let mut heap_iter = UnionFullHeap::new(vec![IdListSorted::new(ids1), IdListSorted::new(ids2)]);

    // Skip to 25 (exists in child2)
    let result = heap_iter.skip_to(25).expect("skip_to failed");
    assert!(matches!(result, Some(SkipToOutcome::Found(_))));
    assert_eq!(heap_iter.last_doc_id(), 25);

    // Skip to 33 (between 30 and 35, should land on 35)
    let result = heap_iter.skip_to(33).expect("skip_to failed");
    assert!(matches!(result, Some(SkipToOutcome::NotFound(_))));
    assert_eq!(heap_iter.last_doc_id(), 35);

    // Read remaining
    let mut remaining = Vec::new();
    while let Some(result) = heap_iter.read().expect("read failed") {
        remaining.push(result.doc_id);
    }
    assert_eq!(remaining, vec![40, 45, 50, 55]);
}

/// Test rewind with heap variant.
#[test]
#[cfg_attr(miri, ignore)]
fn heap_variant_rewind() {
    let ids1 = vec![10, 20, 30];
    let ids2 = vec![15, 25, 35];

    let mut heap_iter = UnionFullHeap::new(vec![IdListSorted::new(ids1), IdListSorted::new(ids2)]);

    // Read all
    let mut results1 = Vec::new();
    while let Some(result) = heap_iter.read().expect("read failed") {
        results1.push(result.doc_id);
    }

    // Rewind and read again
    heap_iter.rewind();
    let mut results2 = Vec::new();
    while let Some(result) = heap_iter.read().expect("read failed") {
        results2.push(result.doc_id);
    }

    assert_eq!(results1, results2);
    assert_eq!(results1, vec![10, 15, 20, 25, 30, 35]);
}

/// Test heap variant with no children.
#[test]
fn heap_no_children() {
    let children: Vec<IdListSorted<'static>> = vec![];
    let mut heap_iter = UnionFullHeap::new(children);

    assert!(matches!(heap_iter.read(), Ok(None)));
    assert!(heap_iter.at_eof());
    assert_eq!(heap_iter.num_estimated(), 0);
    assert!(matches!(heap_iter.skip_to(1), Ok(None)));
    assert!(heap_iter.at_eof());
}

/// Test heap variant with a single child.
#[test]
fn heap_single_child() {
    let doc_ids = vec![10, 20, 30, 40, 50];
    let child = IdListSorted::new(doc_ids.clone());
    let mut heap_iter = UnionFullHeap::new(vec![child]);

    for &expected_id in &doc_ids {
        let result = heap_iter.read().expect("read failed");
        assert!(result.is_some());
        assert_eq!(result.unwrap().doc_id, expected_id);
    }

    assert!(matches!(heap_iter.read(), Ok(None)));
    assert!(heap_iter.at_eof());
}

/// Test heap variant with disjoint children.
#[test]
fn heap_disjoint_children() {
    // Children have no overlap - union should return all docs
    let child1 = IdListSorted::new(vec![1, 2, 3]);
    let child2 = IdListSorted::new(vec![10, 20, 30]);
    let child3 = IdListSorted::new(vec![100, 200, 300]);

    let mut heap_iter = UnionFullHeap::new(vec![child1, child2, child3]);
    let expected = vec![1, 2, 3, 10, 20, 30, 100, 200, 300];

    for &expected_id in &expected {
        let result = heap_iter.read().expect("read failed");
        assert!(result.is_some(), "Expected doc {expected_id}");
        assert_eq!(result.unwrap().doc_id, expected_id);
    }

    assert!(matches!(heap_iter.read(), Ok(None)));
    assert!(heap_iter.at_eof());
}

/// Test heap variant with overlapping children.
#[test]
fn heap_overlapping_children() {
    // Children have significant overlap
    let child1 = IdListSorted::new(vec![1, 2, 5, 10, 15, 20]);
    let child2 = IdListSorted::new(vec![2, 5, 8, 10, 18, 20]);
    let child3 = IdListSorted::new(vec![3, 5, 10, 12, 20, 25]);

    let mut heap_iter = UnionFullHeap::new(vec![child1, child2, child3]);
    // Union: 1, 2, 3, 5, 8, 10, 12, 15, 18, 20, 25
    let expected = vec![1, 2, 3, 5, 8, 10, 12, 15, 18, 20, 25];

    for &expected_id in &expected {
        let result = heap_iter.read().expect("read failed");
        assert!(result.is_some(), "Expected doc {expected_id}");
        assert_eq!(result.unwrap().doc_id, expected_id);
    }

    assert!(matches!(heap_iter.read(), Ok(None)));
}

/// Test heap variant interleaved read and skip_to.
#[test]
#[cfg_attr(miri, ignore)]
fn heap_interleaved_read_and_skip_to() {
    let child1 = IdListSorted::new(vec![10, 20, 30, 40, 50, 60, 70, 80]);
    let child2 = IdListSorted::new(vec![15, 25, 35, 45, 55, 65, 75, 85]);

    let mut heap_iter = UnionFullHeap::new(vec![child1, child2]);

    // Read first document
    let result = heap_iter.read().expect("read failed").unwrap();
    assert_eq!(result.doc_id, 10);

    // Skip to 35
    let outcome = heap_iter.skip_to(35).expect("skip_to failed");
    assert!(matches!(outcome, Some(SkipToOutcome::Found(_))));
    assert_eq!(heap_iter.last_doc_id(), 35);

    // Read next
    let result = heap_iter.read().expect("read failed").unwrap();
    assert_eq!(result.doc_id, 40);

    // Skip to 70
    let outcome = heap_iter.skip_to(70).expect("skip_to failed");
    assert!(matches!(outcome, Some(SkipToOutcome::Found(_))));
    assert_eq!(heap_iter.last_doc_id(), 70);
}

/// Test heap variant num_estimated.
#[test]
fn heap_num_estimated_is_sum() {
    let child1 = IdListSorted::new(vec![1, 2, 3, 4, 5]); // 5 elements
    let child2 = IdListSorted::new(vec![10, 20, 30]); // 3 elements
    let child3 = IdListSorted::new(vec![100, 200, 300, 400]); // 4 elements

    let heap_iter = UnionFullHeap::new(vec![child1, child2, child3]);

    // For union, num_estimated is the sum (upper bound)
    assert_eq!(heap_iter.num_estimated(), 12);
}

/// Test skip_to past EOF with heap variant.
#[test]
#[cfg_attr(miri, ignore)]
fn heap_skip_to_past_eof() {
    let child1 = IdListSorted::new(vec![10, 20, 30]);
    let child2 = IdListSorted::new(vec![15, 25]);

    let mut heap_iter = UnionFullHeap::new(vec![child1, child2]);

    assert!(matches!(heap_iter.skip_to(100), Ok(None)));
    assert!(heap_iter.at_eof());

    // Rewind should reset
    heap_iter.rewind();
    assert!(!heap_iter.at_eof());
    let result = heap_iter.read().expect("read failed");
    assert!(result.is_some());
    assert_eq!(result.unwrap().doc_id, 10);
}

