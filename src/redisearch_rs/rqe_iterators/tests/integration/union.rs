/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Integration tests for the Union iterator.
//!
//! Tests use `UnionFullFlat` by default, which is the "full mode" variant
//! that aggregates results from all matching children.

use ffi::t_docId;
use rqe_iterators::{
    RQEIterator, RQEValidateStatus, SkipToOutcome, UnionFullFlat, UnionFullHeap, UnionQuickFlat,
    UnionQuickHeap, id_list::IdListSorted,
};

use crate::utils::{Mock, MockRevalidateResult};

/// Type alias for the union variant used in these tests.
/// Using `UnionFullFlat` which aggregates all matching children (not quick exit)
/// and uses flat array iteration (not heap).
type Union<I> = UnionFullFlat<'static, I>;

/// Helper function to create child iterators for union tests.
///
/// Creates `num_children` child iterators with partial overlap.
/// Returns both the children and the expected union result (sorted, deduplicated).
fn create_union_children(
    num_children: usize,
    base_result_set: &[t_docId],
) -> (Vec<IdListSorted<'static>>, Vec<t_docId>) {
    let mut children = Vec::with_capacity(num_children);
    let mut all_ids = Vec::new();
    let mut next_unique_id: t_docId = 10000;

    for i in 0..num_children {
        // Each child gets a subset of the base result set plus some unique IDs
        let mut child_ids = Vec::new();

        // Add subset of base result set (child i gets every nth element starting at i)
        for (j, &id) in base_result_set.iter().enumerate() {
            if j % num_children == i || j % 2 == 0 {
                // Ensure some overlap
                child_ids.push(id);
            }
        }

        // Add unique IDs to each child
        for _ in 0..50 {
            child_ids.push(next_unique_id);
            next_unique_id += 1;
        }

        child_ids.sort();
        child_ids.dedup();
        all_ids.extend(child_ids.iter().copied());
        children.push(IdListSorted::new(child_ids));
    }

    all_ids.sort();
    all_ids.dedup();
    (children, all_ids)
}

// =============================================================================
// Test parameters
// =============================================================================

const NUM_CHILDREN_CASES: &[usize] = &[2, 5, 10];

const RESULT_SET_CASES: &[&[t_docId]] = &[
    &[1, 2, 3, 40, 50],
    &[5, 6, 7, 24, 25, 46, 47, 48, 49, 50, 51, 234, 2345],
    &[9, 25, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 130],
];

// =============================================================================
// Read tests
// =============================================================================

#[test]
#[cfg_attr(miri, ignore)]
fn read_all_combinations() {
    for &num_children in NUM_CHILDREN_CASES {
        for &result_set in RESULT_SET_CASES {
            read_test_case(num_children, result_set);
        }
    }
}

fn read_test_case(num_children: usize, base_result_set: &[t_docId]) {
    let (children, expected) = create_union_children(num_children, base_result_set);

    let mut union_iter = Union::new(children);

    // Test reading until EOF
    let mut count = 0;
    while let Ok(Some(result)) = union_iter.read() {
        assert_eq!(
            result.doc_id, expected[count],
            "num_children={num_children}, expected doc_id={}, got={}",
            expected[count], result.doc_id
        );
        assert_eq!(
            union_iter.last_doc_id(),
            expected[count],
            "num_children={num_children}, last_doc_id mismatch"
        );
        assert!(
            !union_iter.at_eof(),
            "num_children={num_children}, should not be at EOF yet"
        );
        count += 1;
    }

    assert!(
        union_iter.at_eof(),
        "num_children={num_children}, should be at EOF"
    );
    assert!(
        matches!(union_iter.read(), Ok(None)),
        "num_children={num_children}, reading after EOF should return None"
    );
    assert_eq!(
        count,
        expected.len(),
        "num_children={num_children}, expected {} documents, got {}",
        expected.len(),
        count
    );
}

// =============================================================================
// SkipTo tests
// =============================================================================

#[test]
#[cfg(not(miri))]
fn skip_to_all_combinations() {
    for &num_children in NUM_CHILDREN_CASES {
        for &result_set in RESULT_SET_CASES {
            skip_to_test_case(num_children, result_set);
        }
    }
}

#[cfg(not(miri))]
fn skip_to_test_case(num_children: usize, base_result_set: &[t_docId]) {
    let (children, expected) = create_union_children(num_children, base_result_set);
    let mut union_iter = Union::new(children);

    // Test skipping to each element
    for &id in &expected {
        union_iter.rewind();
        let outcome = union_iter.skip_to(id).expect("skip_to failed");
        match outcome {
            Some(SkipToOutcome::Found(result)) => {
                assert_eq!(
                    result.doc_id, id,
                    "num_children={num_children}, skip_to({id}) should find {id}"
                );
            }
            Some(SkipToOutcome::NotFound(_)) => {
                // This is acceptable if id isn't in the union
            }
            None => panic!("num_children={num_children}, skip_to({id}) should not return None"),
        }
    }

    // Skip beyond last docId should return EOF
    union_iter.rewind();
    let last_id = *expected.last().unwrap();
    assert!(
        matches!(union_iter.skip_to(last_id + 1000), Ok(None)),
        "num_children={num_children}, skip beyond last should be EOF"
    );
    assert!(
        union_iter.at_eof(),
        "num_children={num_children}, should be at EOF after skip beyond"
    );
}

// =============================================================================
// Rewind tests
// =============================================================================

#[test]
#[cfg_attr(miri, ignore)]
fn rewind_all_combinations() {
    for &num_children in NUM_CHILDREN_CASES {
        for &result_set in RESULT_SET_CASES {
            rewind_test_case(num_children, result_set);
        }
    }
}

fn rewind_test_case(num_children: usize, base_result_set: &[t_docId]) {
    let (children, expected) = create_union_children(num_children, base_result_set);
    let mut union_iter = Union::new(children);

    for i in 0..5 {
        for j in 0..=i.min(expected.len() - 1) {
            let result = union_iter.read().expect("read failed");
            assert!(
                result.is_some(),
                "num_children={num_children}, read({j}) in iteration {i} failed"
            );
            let result = result.unwrap();
            assert_eq!(
                result.doc_id, expected[j],
                "num_children={num_children}, wrong doc_id"
            );
        }
        union_iter.rewind();
        assert_eq!(
            union_iter.last_doc_id(),
            0,
            "num_children={num_children}, last_doc_id should be 0 after rewind"
        );
        assert!(
            !union_iter.at_eof(),
            "num_children={num_children}, should not be at EOF after rewind"
        );
    }
}

// =============================================================================
// Edge case tests
// =============================================================================

#[test]
fn no_children() {
    let children: Vec<IdListSorted<'static>> = vec![];
    let mut union_iter = Union::new(children);

    assert!(matches!(union_iter.read(), Ok(None)));
    assert!(union_iter.at_eof());
    assert_eq!(union_iter.num_estimated(), 0);
    assert!(matches!(union_iter.skip_to(1), Ok(None)));
    assert!(union_iter.at_eof());
}

#[test]
fn single_child() {
    let doc_ids = vec![10, 20, 30, 40, 50];
    let child = IdListSorted::new(doc_ids.clone());
    let mut union_iter = Union::new(vec![child]);

    for &expected_id in &doc_ids {
        let result = union_iter.read().expect("read failed");
        assert!(result.is_some());
        assert_eq!(result.unwrap().doc_id, expected_id);
    }

    assert!(matches!(union_iter.read(), Ok(None)));
    assert!(union_iter.at_eof());
}

#[test]
fn disjoint_children() {
    // Children have no overlap - union should return all docs
    let child1 = IdListSorted::new(vec![1, 2, 3]);
    let child2 = IdListSorted::new(vec![10, 20, 30]);
    let child3 = IdListSorted::new(vec![100, 200, 300]);

    let mut union_iter = Union::new(vec![child1, child2, child3]);
    let expected = vec![1, 2, 3, 10, 20, 30, 100, 200, 300];

    for &expected_id in &expected {
        let result = union_iter.read().expect("read failed");
        assert!(result.is_some(), "Expected doc {expected_id}");
        assert_eq!(result.unwrap().doc_id, expected_id);
    }

    assert!(matches!(union_iter.read(), Ok(None)));
    assert!(union_iter.at_eof());
}

#[test]
fn overlapping_children() {
    // Children have significant overlap
    let child1 = IdListSorted::new(vec![1, 2, 5, 10, 15, 20]);
    let child2 = IdListSorted::new(vec![2, 5, 8, 10, 18, 20]);
    let child3 = IdListSorted::new(vec![3, 5, 10, 12, 20, 25]);

    let mut union_iter = Union::new(vec![child1, child2, child3]);
    // Union: 1, 2, 3, 5, 8, 10, 12, 15, 18, 20, 25
    let expected = vec![1, 2, 3, 5, 8, 10, 12, 15, 18, 20, 25];

    for &expected_id in &expected {
        let result = union_iter.read().expect("read failed");
        assert!(result.is_some(), "Expected doc {expected_id}");
        assert_eq!(result.unwrap().doc_id, expected_id);
    }

    assert!(matches!(union_iter.read(), Ok(None)));
}

#[test]
#[cfg_attr(miri, ignore)]
fn skip_to_exact_match() {
    let child1 = IdListSorted::new(vec![10, 20, 30, 40, 50]);
    let child2 = IdListSorted::new(vec![15, 25, 35, 45, 55]);

    let mut union_iter = Union::new(vec![child1, child2]);

    let outcome = union_iter.skip_to(30).expect("skip_to failed");
    match outcome {
        Some(SkipToOutcome::Found(result)) => {
            assert_eq!(result.doc_id, 30);
        }
        _ => panic!("Expected Found(30)"),
    }
    assert_eq!(union_iter.last_doc_id(), 30);
}

#[test]
#[cfg_attr(miri, ignore)]
fn skip_to_not_found() {
    let child1 = IdListSorted::new(vec![10, 20, 30, 40, 50]);
    let child2 = IdListSorted::new(vec![15, 25, 35, 45, 55]);

    let mut union_iter = Union::new(vec![child1, child2]);

    // Skip to 22, should land on 25 (next available in union)
    let outcome = union_iter.skip_to(22).expect("skip_to failed");
    match outcome {
        Some(SkipToOutcome::NotFound(result)) => {
            assert_eq!(result.doc_id, 25);
        }
        _ => panic!("Expected NotFound landing on 25"),
    }
    assert_eq!(union_iter.last_doc_id(), 25);
}

#[test]
#[cfg_attr(miri, ignore)]
fn skip_to_past_eof() {
    let child1 = IdListSorted::new(vec![10, 20, 30]);
    let child2 = IdListSorted::new(vec![15, 25]);

    let mut union_iter = Union::new(vec![child1, child2]);

    assert!(matches!(union_iter.skip_to(100), Ok(None)));
    assert!(union_iter.at_eof());

    // Rewind should reset
    union_iter.rewind();
    assert!(!union_iter.at_eof());
    let result = union_iter.read().expect("read failed");
    assert!(result.is_some());
    assert_eq!(result.unwrap().doc_id, 10);
}

#[test]
#[cfg_attr(miri, ignore)]
fn interleaved_read_and_skip_to() {
    let child1 = IdListSorted::new(vec![10, 20, 30, 40, 50, 60, 70, 80]);
    let child2 = IdListSorted::new(vec![15, 25, 35, 45, 55, 65, 75, 85]);

    let mut union_iter = Union::new(vec![child1, child2]);

    // Read first document
    let result = union_iter.read().expect("read failed").unwrap();
    assert_eq!(result.doc_id, 10);

    // Skip to 35
    let outcome = union_iter.skip_to(35).expect("skip_to failed");
    assert!(matches!(outcome, Some(SkipToOutcome::Found(_))));
    assert_eq!(union_iter.last_doc_id(), 35);

    // Read next
    let result = union_iter.read().expect("read failed").unwrap();
    assert_eq!(result.doc_id, 40);

    // Skip to 70
    let outcome = union_iter.skip_to(70).expect("skip_to failed");
    assert!(matches!(outcome, Some(SkipToOutcome::Found(_))));
    assert_eq!(union_iter.last_doc_id(), 70);
}

#[test]
fn num_estimated_is_sum() {
    let child1 = IdListSorted::new(vec![1, 2, 3, 4, 5]); // 5 elements
    let child2 = IdListSorted::new(vec![10, 20, 30]); // 3 elements
    let child3 = IdListSorted::new(vec![100, 200, 300, 400]); // 4 elements

    let union_iter = Union::new(vec![child1, child2, child3]);

    // For union, num_estimated is the sum (upper bound)
    assert_eq!(union_iter.num_estimated(), 12);
}

// =============================================================================
// Revalidate tests
// =============================================================================

#[test]
#[cfg_attr(miri, ignore)]
fn revalidate_ok() {
    let child0: Mock<'static, 5> = Mock::new([10, 20, 30, 40, 50]);
    let child1: Mock<'static, 5> = Mock::new([15, 25, 35, 45, 55]);

    child0
        .data()
        .set_revalidate_result(MockRevalidateResult::Ok);
    child1
        .data()
        .set_revalidate_result(MockRevalidateResult::Ok);

    let children: Vec<Box<dyn RQEIterator<'static> + 'static>> =
        vec![Box::new(child0), Box::new(child1)];

    let mut union_iter = Union::new(children);

    // Read a few documents
    let result = union_iter.read().expect("read failed").unwrap();
    assert_eq!(result.doc_id, 10);

    let result = union_iter.read().expect("read failed").unwrap();
    assert_eq!(result.doc_id, 15);

    // Revalidate should return Ok
    let status = union_iter.revalidate().expect("revalidate failed");
    assert!(matches!(status, RQEValidateStatus::Ok));

    // Should be able to continue reading
    let result = union_iter.read().expect("read failed").unwrap();
    assert_eq!(result.doc_id, 20);
}

#[test]
#[cfg_attr(miri, ignore)]
fn revalidate_moved() {
    let child0: Mock<'static, 5> = Mock::new([10, 20, 30, 40, 50]);
    let child1: Mock<'static, 5> = Mock::new([15, 25, 35, 45, 55]);

    child0
        .data()
        .set_revalidate_result(MockRevalidateResult::Move);
    child1
        .data()
        .set_revalidate_result(MockRevalidateResult::Move);

    let children: Vec<Box<dyn RQEIterator<'static> + 'static>> =
        vec![Box::new(child0), Box::new(child1)];

    let mut union_iter = Union::new(children);

    // Read first document
    let result = union_iter.read().expect("read failed").unwrap();
    assert_eq!(result.doc_id, 10);

    // Revalidate should return Moved
    let status = union_iter.revalidate().expect("revalidate failed");
    assert!(
        matches!(status, RQEValidateStatus::Moved { current: Some(_) }),
        "Expected Moved with current, got {:?}",
        status
    );
}

#[test]
#[cfg_attr(miri, ignore)]
fn revalidate_after_eof() {
    let child0: Mock<'static, 2> = Mock::new([10, 20]);
    let child1: Mock<'static, 2> = Mock::new([15, 25]);

    child0
        .data()
        .set_revalidate_result(MockRevalidateResult::Move);
    child1
        .data()
        .set_revalidate_result(MockRevalidateResult::Move);

    let children: Vec<Box<dyn RQEIterator<'static> + 'static>> =
        vec![Box::new(child0), Box::new(child1)];

    let mut union_iter = Union::new(children);

    // Advance to EOF
    while union_iter.read().expect("read failed").is_some() {}
    assert!(union_iter.at_eof());

    // Revalidate should return Ok when already at EOF
    let status = union_iter.revalidate().expect("revalidate failed");
    assert!(
        matches!(status, RQEValidateStatus::Ok),
        "Revalidate after EOF should return Ok, got {:?}",
        status
    );

    assert!(union_iter.at_eof());
}

#[test]
#[cfg_attr(miri, ignore)]
fn current_after_operations() {
    let child1 = IdListSorted::new(vec![10, 20, 30, 40, 50]);
    let child2 = IdListSorted::new(vec![15, 25, 35, 45, 55]);

    let mut union_iter = Union::new(vec![child1, child2]);

    // Before any read, current() returns Some (buffer exists)
    assert!(union_iter.current().is_some());
    assert_eq!(union_iter.last_doc_id(), 0);

    // After read
    let _ = union_iter.read().expect("read failed");
    let current = union_iter.current();
    assert!(current.is_some());
    assert_eq!(current.unwrap().doc_id, 10);

    // After skip_to
    let _ = union_iter.skip_to(30).expect("skip_to failed");
    let current = union_iter.current();
    assert!(current.is_some());
    assert_eq!(current.unwrap().doc_id, 30);

    // After rewind
    union_iter.rewind();
    assert!(union_iter.current().is_some());
    assert_eq!(union_iter.last_doc_id(), 0);

    // After EOF
    while union_iter.read().expect("read failed").is_some() {}
    assert!(union_iter.at_eof());
    assert!(union_iter.current().is_none());
}

// ============================================================================
// Tests for other union variants
// ============================================================================

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
    let mut heap_iter = UnionFullHeap::new(vec![
        IdListSorted::new(ids1),
        IdListSorted::new(ids2),
    ]);
    let mut heap_results = Vec::new();
    while let Some(result) = heap_iter.read().expect("read failed") {
        heap_results.push(result.doc_id);
    }

    assert_eq!(flat_results, heap_results);
}

/// Test that UnionQuickFlat produces the same doc_ids as UnionFullFlat.
/// (Quick mode just doesn't aggregate results, but yields same documents)
#[test]
#[cfg_attr(miri, ignore)]
fn quick_variant_produces_same_doc_ids() {
    let ids1 = vec![10, 20, 30, 40, 50];
    let ids2 = vec![15, 25, 35, 45, 55];

    // Test with full variant
    let mut full_iter = UnionFullFlat::new(vec![
        IdListSorted::new(ids1.clone()),
        IdListSorted::new(ids2.clone()),
    ]);
    let mut full_results = Vec::new();
    while let Some(result) = full_iter.read().expect("read failed") {
        full_results.push(result.doc_id);
    }

    // Test with quick variant
    let mut quick_iter = UnionQuickFlat::new(vec![
        IdListSorted::new(ids1),
        IdListSorted::new(ids2),
    ]);
    let mut quick_results = Vec::new();
    while let Some(result) = quick_iter.read().expect("read failed") {
        quick_results.push(result.doc_id);
    }

    assert_eq!(full_results, quick_results);
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
    let mut quick_heap_iter = UnionQuickHeap::new(vec![
        IdListSorted::new(ids1),
        IdListSorted::new(ids2),
    ]);
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

    let mut heap_iter = UnionFullHeap::new(vec![
        IdListSorted::new(ids1),
        IdListSorted::new(ids2),
    ]);

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

    let mut heap_iter = UnionFullHeap::new(vec![
        IdListSorted::new(ids1),
        IdListSorted::new(ids2),
    ]);

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

