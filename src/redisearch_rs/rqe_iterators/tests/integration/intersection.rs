/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Integration tests for the Intersection iterator.
//!
//! These tests are ported from the C++ tests in
//! `tests/cpptests/test_cpp_iterator_intersection.cpp`.
//!
//! Note: Tests for `max_slop` and `in_order` are not included in this first
//! version since those features are not yet implemented in the Rust port.

use ffi::t_docId;
use rqe_iterators::{
    Empty, RQEIterator, RQEValidateStatus, SkipToOutcome, Wildcard,
    id_list::SortedIdList,
    intersection::{Intersection, ReducedIntersection, reduce},
};

use crate::utils::{MockIterator, MockRevalidateResult};

/// Helper function to create child iterators for intersection tests.
///
/// Given a result set (document IDs that should appear in ALL children),
/// creates `num_children` child iterators where each child contains:
/// - All document IDs from the result set
/// - Some unique document IDs specific to that child
///
/// This ensures the intersection of all children equals exactly the result set.
fn create_children(num_children: usize, result_set: &[t_docId]) -> Vec<SortedIdList<'static>> {
    let mut children = Vec::with_capacity(num_children);
    let mut next_unique_id: t_docId = 1;

    for _ in 0..num_children {
        // Start with the result set as base
        let mut child_ids = result_set.to_vec();

        // Add some unique IDs to each child (100 unique IDs per child)
        for _ in 0..100 {
            child_ids.push(next_unique_id);
            next_unique_id += 1;
        }

        // Sort and deduplicate (matching C++ MockIterator behavior)
        child_ids.sort();
        child_ids.dedup();

        children.push(SortedIdList::new(child_ids));
    }

    children
}

// =============================================================================
// Test parameters - matching C++ INSTANTIATE_TEST_SUITE_P
// =============================================================================

/// Number of child iterators to test with
const NUM_CHILDREN_CASES: &[usize] = &[2, 5, 25];

/// Result sets to test with
const RESULT_SET_CASES: &[&[t_docId]] = &[
    &[1, 2, 3, 40, 50],
    &[
        5, 6, 7, 24, 25, 46, 47, 48, 49, 50, 51, 234, 2345, 3456, 4567, 5678, 6789, 7890, 8901,
        9012, 12345, 23456, 34567, 45678, 56789,
    ],
    &[
        9, 25, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150, 160, 170, 180, 190, 200,
        210, 220, 230, 240, 250,
    ],
];

// =============================================================================
// Read tests - equivalent to TEST_P(IntersectionIteratorCommonTest, Read)
// =============================================================================

#[test]
fn read_all_combinations() {
    for &num_children in NUM_CHILDREN_CASES {
        for &result_set in RESULT_SET_CASES {
            read_test_case(num_children, result_set);
        }
    }
}

fn read_test_case(num_children: usize, result_set: &[t_docId]) {
    let children = create_children(num_children, result_set);

    // Compute expected num_estimated (minimum of all children's sizes)
    let expected_num_estimated = children
        .iter()
        .map(|c| c.num_estimated())
        .min()
        .unwrap_or(0);

    let mut ii = Intersection::new(children);

    // Verify children are sorted by estimated count (optimization check)
    // Note: We can't directly access internal children after construction,
    // but we can verify the iterator behavior is correct.

    // Test reading until EOF
    let mut count = 0;
    while let Ok(Some(result)) = ii.read() {
        assert_eq!(
            result.doc_id, result_set[count],
            "num_children={num_children}, expected doc_id={}, got={}",
            result_set[count], result.doc_id
        );
        assert_eq!(
            ii.last_doc_id(),
            result_set[count],
            "num_children={num_children}, last_doc_id mismatch"
        );
        assert!(
            !ii.at_eof(),
            "num_children={num_children}, should not be at EOF yet"
        );
        count += 1;
    }

    assert!(ii.at_eof(), "num_children={num_children}, should be at EOF");

    // Reading after EOF should return None
    assert!(
        matches!(ii.read(), Ok(None)),
        "num_children={num_children}, reading after EOF should return None"
    );

    assert_eq!(
        count,
        result_set.len(),
        "num_children={num_children}, expected {} documents, got {}",
        result_set.len(),
        count
    );

    assert_eq!(
        ii.num_estimated(),
        expected_num_estimated,
        "num_children={num_children}, num_estimated mismatch"
    );
}

// =============================================================================
// SkipTo tests - equivalent to TEST_P(IntersectionIteratorCommonTest, SkipTo)
// =============================================================================

#[test]
#[cfg(not(miri))] // Takes too long with Miri
fn skip_to_all_combinations() {
    for &num_children in NUM_CHILDREN_CASES {
        for &result_set in RESULT_SET_CASES {
            skip_to_test_case(num_children, result_set);
        }
    }
}

fn skip_to_test_case(num_children: usize, result_set: &[t_docId]) {
    let children = create_children(num_children, result_set);
    let mut ii = Intersection::new(children);

    // Test skipping to any id between 1 and the last id
    let mut i: t_docId = 1;
    for &id in result_set {
        // Skip to IDs that don't exist in result set (should return NotFound)
        while i < id {
            ii.rewind();
            let outcome = ii.skip_to(i).expect("skip_to failed");
            match outcome {
                Some(SkipToOutcome::NotFound(result)) => {
                    assert_eq!(
                        result.doc_id, id,
                        "num_children={num_children}, skip_to({i}) should land on {id}"
                    );
                }
                other => panic!(
                    "num_children={num_children}, skip_to({i}) expected NotFound, got {:?}",
                    other.map(|o| match o {
                        SkipToOutcome::Found(_) => "Found",
                        SkipToOutcome::NotFound(_) => "NotFound",
                    })
                ),
            }
            assert_eq!(
                ii.last_doc_id(),
                id,
                "num_children={num_children}, last_doc_id after skip_to({i})"
            );
            i += 1;
        }

        // Skip to ID that exists (should return Found)
        ii.rewind();
        let outcome = ii.skip_to(id).expect("skip_to failed");
        match outcome {
            Some(SkipToOutcome::Found(result)) => {
                assert_eq!(
                    result.doc_id, id,
                    "num_children={num_children}, skip_to({id}) should find {id}"
                );
            }
            other => panic!(
                "num_children={num_children}, skip_to({id}) expected Found, got {:?}",
                other.map(|o| match o {
                    SkipToOutcome::Found(_) => "Found",
                    SkipToOutcome::NotFound(_) => "NotFound",
                })
            ),
        }
        assert_eq!(
            ii.last_doc_id(),
            id,
            "num_children={num_children}, last_doc_id after exact skip_to({id})"
        );
        i += 1;
    }

    // Test reading after skipping to the last id - should return EOF
    assert!(
        matches!(ii.read(), Ok(None)),
        "num_children={num_children}, read after last skip should be EOF"
    );

    // Skip beyond last docId should return EOF
    let last_id = *result_set.last().unwrap();
    assert!(
        matches!(ii.skip_to(last_id + 1), Ok(None)),
        "num_children={num_children}, skip beyond last should be EOF"
    );
    assert!(
        ii.at_eof(),
        "num_children={num_children}, should be at EOF after skip beyond"
    );

    // Rewind and verify state
    ii.rewind();
    assert_eq!(
        ii.last_doc_id(),
        0,
        "num_children={num_children}, last_doc_id should be 0 after rewind"
    );
    assert!(
        !ii.at_eof(),
        "num_children={num_children}, should not be at EOF after rewind"
    );

    // Test skipping to all existing IDs sequentially
    for &id in result_set {
        let outcome = ii.skip_to(id).expect("skip_to failed");
        assert!(
            matches!(outcome, Some(SkipToOutcome::Found(_))),
            "num_children={num_children}, sequential skip_to({id}) expected Found"
        );
        assert_eq!(
            ii.last_doc_id(),
            id,
            "num_children={num_children}, last_doc_id after sequential skip"
        );
    }

    // Test skipping to an ID that exceeds the last ID
    ii.rewind();
    assert_eq!(ii.last_doc_id(), 0);
    assert!(!ii.at_eof());

    let outcome = ii.skip_to(last_id + 1);
    assert!(
        matches!(outcome, Ok(None)),
        "num_children={num_children}, skip beyond last should return None"
    );
    assert!(ii.at_eof());
}

// =============================================================================
// Rewind tests - equivalent to TEST_P(IntersectionIteratorCommonTest, Rewind)
// =============================================================================

#[test]
fn rewind_all_combinations() {
    for &num_children in NUM_CHILDREN_CASES {
        for &result_set in RESULT_SET_CASES {
            rewind_test_case(num_children, result_set);
        }
    }
}

fn rewind_test_case(num_children: usize, result_set: &[t_docId]) {
    let children = create_children(num_children, result_set);
    let mut ii = Intersection::new(children);

    for i in 0..5 {
        for j in 0..=i {
            let result = ii.read().expect("read failed");
            assert!(
                result.is_some(),
                "num_children={num_children}, read({j}) in iteration {i} failed"
            );
            let result = result.unwrap();
            assert_eq!(
                result.doc_id, result_set[j],
                "num_children={num_children}, wrong doc_id"
            );
            assert_eq!(
                ii.last_doc_id(),
                result_set[j],
                "num_children={num_children}, wrong last_doc_id"
            );
        }
        ii.rewind();
        assert_eq!(
            ii.last_doc_id(),
            0,
            "num_children={num_children}, last_doc_id should be 0 after rewind"
        );
        assert!(
            !ii.at_eof(),
            "num_children={num_children}, should not be at EOF after rewind"
        );
    }
}

// =============================================================================
// Edge case tests - from IntersectionIteratorTest and IntersectionIteratorReducerTest
// =============================================================================

// Note: Tests for NULL children, empty children, wildcard removal, and single
// child reduction are constructor-level optimizations that may need to be
// handled differently in Rust (possibly returning Empty iterator or the
// single child directly). These tests will be added once the constructor
// semantics are finalized.

#[test]
fn empty_result_set() {
    // When the intersection has no common documents, should immediately EOF
    let child1 = SortedIdList::new(vec![1, 2, 3]);
    let child2 = SortedIdList::new(vec![4, 5, 6]);

    let mut ii = Intersection::new(vec![child1, child2]);

    // Should immediately return EOF since there's no intersection
    assert!(matches!(ii.read(), Ok(None)));
    assert!(ii.at_eof());
}

#[test]
fn single_element_result_set() {
    let child1 = SortedIdList::new(vec![1, 5, 10]);
    let child2 = SortedIdList::new(vec![5, 15, 20]);
    let child3 = SortedIdList::new(vec![3, 5, 25]);

    let mut ii = Intersection::new(vec![child1, child2, child3]);

    // Only doc 5 is common to all
    let result = ii.read().expect("read failed");
    assert!(result.is_some());
    assert_eq!(result.unwrap().doc_id, 5);

    // No more results
    assert!(matches!(ii.read(), Ok(None)));
    assert!(ii.at_eof());
}

#[test]
fn skip_to_exact_match() {
    let child1 = SortedIdList::new(vec![10, 20, 30, 40, 50]);
    let child2 = SortedIdList::new(vec![10, 20, 30, 40, 50]);

    let mut ii = Intersection::new(vec![child1, child2]);

    // Skip to exact match
    let outcome = ii.skip_to(30).expect("skip_to failed");
    match outcome {
        Some(SkipToOutcome::Found(result)) => {
            assert_eq!(result.doc_id, 30);
        }
        _ => panic!("Expected Found(30)"),
    }
    assert_eq!(ii.last_doc_id(), 30);
}

#[test]
fn skip_to_not_found() {
    let child1 = SortedIdList::new(vec![10, 20, 30, 40, 50]);
    let child2 = SortedIdList::new(vec![10, 20, 30, 40, 50]);

    let mut ii = Intersection::new(vec![child1, child2]);

    // Skip to non-existing ID, should land on next existing
    let outcome = ii.skip_to(25).expect("skip_to failed");
    match outcome {
        Some(SkipToOutcome::NotFound(result)) => {
            assert_eq!(result.doc_id, 30);
        }
        _ => panic!("Expected NotFound landing on 30"),
    }
    assert_eq!(ii.last_doc_id(), 30);
}

// =============================================================================
// Additional edge case tests - from IntersectionIteratorReducerTest
// =============================================================================

/// Test intersection with no children - should behave as empty
/// Equivalent to C++ TestIntersectionWithNoChild
#[test]
fn no_children() {
    let children: Vec<SortedIdList<'static>> = vec![];
    let mut ii = Intersection::new(children);

    // Should immediately return EOF
    assert!(matches!(ii.read(), Ok(None)));
    assert!(ii.at_eof());

    // num_estimated should be 0
    assert_eq!(ii.num_estimated(), 0);

    // skip_to should also return EOF
    assert!(matches!(ii.skip_to(1), Ok(None)));
    assert!(ii.at_eof());
}

/// Test intersection with a single child - should behave like the child itself
/// Equivalent to C++ TestIntersectionWithSingleChild (without wildcard removal)
#[test]
fn single_child() {
    let doc_ids = vec![10, 20, 30, 40, 50];
    let child = SortedIdList::new(doc_ids.clone());
    let mut ii = Intersection::new(vec![child]);

    // Should read all documents from the single child
    for &expected_id in &doc_ids {
        let result = ii.read().expect("read failed");
        assert!(result.is_some());
        assert_eq!(result.unwrap().doc_id, expected_id);
    }

    // Then EOF
    assert!(matches!(ii.read(), Ok(None)));
    assert!(ii.at_eof());

    // Rewind and test skip_to
    ii.rewind();
    let outcome = ii.skip_to(30).expect("skip_to failed");
    assert!(matches!(outcome, Some(SkipToOutcome::Found(_))));
    assert_eq!(ii.last_doc_id(), 30);
}

/// Test that skip_to past EOF stays at EOF
#[test]
fn skip_to_past_eof() {
    let child1 = SortedIdList::new(vec![10, 20, 30]);
    let child2 = SortedIdList::new(vec![10, 20, 30]);

    let mut ii = Intersection::new(vec![child1, child2]);

    // Skip past the last document
    assert!(matches!(ii.skip_to(100), Ok(None)));
    assert!(ii.at_eof());

    // Further operations should return EOF
    assert!(matches!(ii.read(), Ok(None)));
    assert!(matches!(ii.skip_to(10), Ok(None)));

    // Rewind should reset EOF
    ii.rewind();
    assert!(!ii.at_eof());
    let result = ii.read().expect("read failed");
    assert!(result.is_some());
    assert_eq!(result.unwrap().doc_id, 10);
}

/// Test sequential skip_to through all documents
#[test]
fn skip_to_sequential() {
    let doc_ids = vec![10, 20, 30, 40, 50];
    let child1 = SortedIdList::new(doc_ids.clone());
    let child2 = SortedIdList::new(doc_ids.clone());

    let mut ii = Intersection::new(vec![child1, child2]);

    // Skip to each document in sequence
    for &id in &doc_ids {
        let outcome = ii.skip_to(id).expect("skip_to failed");
        match outcome {
            Some(SkipToOutcome::Found(result)) => {
                assert_eq!(result.doc_id, id);
            }
            _ => panic!("Expected Found({id})"),
        }
        assert_eq!(ii.last_doc_id(), id);
    }

    // Next read should be EOF
    assert!(matches!(ii.read(), Ok(None)));
    assert!(ii.at_eof());
}

/// Test interleaved read and skip_to
#[test]
fn interleaved_read_and_skip_to() {
    let doc_ids = vec![10, 20, 30, 40, 50, 60, 70, 80, 90, 100];
    let child1 = SortedIdList::new(doc_ids.clone());
    let child2 = SortedIdList::new(doc_ids.clone());

    let mut ii = Intersection::new(vec![child1, child2]);

    // Read first document
    let result = ii.read().expect("read failed").unwrap();
    assert_eq!(result.doc_id, 10);

    // Skip to 40
    let outcome = ii.skip_to(40).expect("skip_to failed");
    assert!(matches!(outcome, Some(SkipToOutcome::Found(_))));
    assert_eq!(ii.last_doc_id(), 40);

    // Read next (should be 50)
    let result = ii.read().expect("read failed").unwrap();
    assert_eq!(result.doc_id, 50);

    // Skip to 80
    let outcome = ii.skip_to(80).expect("skip_to failed");
    assert!(matches!(outcome, Some(SkipToOutcome::Found(_))));
    assert_eq!(ii.last_doc_id(), 80);

    // Read remaining
    let result = ii.read().expect("read failed").unwrap();
    assert_eq!(result.doc_id, 90);

    let result = ii.read().expect("read failed").unwrap();
    assert_eq!(result.doc_id, 100);

    // EOF
    assert!(matches!(ii.read(), Ok(None)));
}

/// Test many children (stress test)
#[test]
fn many_children() {
    let doc_ids = vec![100, 200, 300, 400, 500];
    let num_children = 50;

    let children: Vec<SortedIdList<'static>> = (0..num_children)
        .map(|i| {
            // Each child has the common docs + some unique ones
            let mut ids = doc_ids.clone();
            ids.push(i as t_docId + 1); // Unique to this child
            ids.sort();
            SortedIdList::new(ids)
        })
        .collect();

    let mut ii = Intersection::new(children);

    // Should find all common documents
    for &expected_id in &doc_ids {
        let result = ii.read().expect("read failed");
        assert!(result.is_some(), "Expected doc {expected_id}");
        assert_eq!(result.unwrap().doc_id, expected_id);
    }

    // Then EOF
    assert!(matches!(ii.read(), Ok(None)));
    assert!(ii.at_eof());
}

// =============================================================================
// Revalidate tests - from IntersectionIteratorRevalidateTest
// =============================================================================

// Note: Mock children for revalidate tests are created inline in each test
// since the new MockIterator uses const generics for array sizes.
// Common docs: [10, 20, 30, 40, 50]
// Child 0: [10, 15, 20, 25, 30, 35, 40, 45, 50, 55] (10 elements)
// Child 1: [5, 10, 18, 20, 28, 30, 38, 40, 48, 50, 60] (11 elements)
// Child 2: [2, 10, 12, 20, 22, 30, 32, 40, 42, 50, 70] (11 elements)

/// Test: All children return VALIDATE_OK
/// Equivalent to C++ TEST_F(IntersectionIteratorRevalidateTest, RevalidateOK)
#[test]
fn revalidate_ok() {
    // Create mock children with const generic arrays
    let child0: MockIterator<'static, 10> =
        MockIterator::new([10, 15, 20, 25, 30, 35, 40, 45, 50, 55]);
    let child1: MockIterator<'static, 11> =
        MockIterator::new([5, 10, 18, 20, 28, 30, 38, 40, 48, 50, 60]);
    let child2: MockIterator<'static, 11> =
        MockIterator::new([2, 10, 12, 20, 22, 30, 32, 40, 42, 50, 70]);

    // Set all children to return OK on revalidate (default, but explicit)
    child0
        .data()
        .set_revalidate_result(MockRevalidateResult::Ok);
    child1
        .data()
        .set_revalidate_result(MockRevalidateResult::Ok);
    child2
        .data()
        .set_revalidate_result(MockRevalidateResult::Ok);

    // Use boxed iterators to allow heterogeneous children
    let children: Vec<Box<dyn RQEIterator<'static> + 'static>> =
        vec![Box::new(child0), Box::new(child1), Box::new(child2)];

    let mut ii = Intersection::new(children);

    // Read a few documents first
    let result = ii.read().expect("read failed").unwrap();
    assert_eq!(result.doc_id, 10);

    let result = ii.read().expect("read failed").unwrap();
    assert_eq!(result.doc_id, 20);

    // Revalidate should return Ok
    let status = ii.revalidate().expect("revalidate failed");
    assert!(matches!(status, RQEValidateStatus::Ok));

    // Should be able to continue reading
    let result = ii.read().expect("read failed").unwrap();
    assert_eq!(result.doc_id, 30);
}

/// Test: One child returns VALIDATE_ABORTED
/// Equivalent to C++ TEST_F(IntersectionIteratorRevalidateTest, RevalidateAborted)
#[test]
fn revalidate_aborted() {
    let child0: MockIterator<'static, 10> =
        MockIterator::new([10, 15, 20, 25, 30, 35, 40, 45, 50, 55]);
    let child1: MockIterator<'static, 11> =
        MockIterator::new([5, 10, 18, 20, 28, 30, 38, 40, 48, 50, 60]);
    let child2: MockIterator<'static, 11> =
        MockIterator::new([2, 10, 12, 20, 22, 30, 32, 40, 42, 50, 70]);

    // Child 1 will abort
    child0
        .data()
        .set_revalidate_result(MockRevalidateResult::Ok);
    child1
        .data()
        .set_revalidate_result(MockRevalidateResult::Abort);
    child2
        .data()
        .set_revalidate_result(MockRevalidateResult::Ok);

    let children: Vec<Box<dyn RQEIterator<'static> + 'static>> =
        vec![Box::new(child0), Box::new(child1), Box::new(child2)];

    let mut ii = Intersection::new(children);

    // Read a document first
    let result = ii.read().expect("read failed").unwrap();
    assert_eq!(result.doc_id, 10);

    // Revalidate should return Aborted since one child aborted
    let status = ii.revalidate().expect("revalidate failed");
    assert!(matches!(status, RQEValidateStatus::Aborted));
}

/// Test: All children return VALIDATE_MOVED
/// Equivalent to C++ TEST_F(IntersectionIteratorRevalidateTest, RevalidateMoved)
#[test]
fn revalidate_moved() {
    let child0: MockIterator<'static, 10> =
        MockIterator::new([10, 15, 20, 25, 30, 35, 40, 45, 50, 55]);
    let child1: MockIterator<'static, 11> =
        MockIterator::new([5, 10, 18, 20, 28, 30, 38, 40, 48, 50, 60]);
    let child2: MockIterator<'static, 11> =
        MockIterator::new([2, 10, 12, 20, 22, 30, 32, 40, 42, 50, 70]);

    // All children will move (advance by one document)
    child0
        .data()
        .set_revalidate_result(MockRevalidateResult::Move);
    child1
        .data()
        .set_revalidate_result(MockRevalidateResult::Move);
    child2
        .data()
        .set_revalidate_result(MockRevalidateResult::Move);

    let children: Vec<Box<dyn RQEIterator<'static> + 'static>> =
        vec![Box::new(child0), Box::new(child1), Box::new(child2)];

    let mut ii = Intersection::new(children);

    // Read first document
    let result = ii.read().expect("read failed").unwrap();
    assert_eq!(result.doc_id, 10);

    // Revalidate should return Moved
    let status = ii.revalidate().expect("revalidate failed");
    assert!(
        matches!(status, RQEValidateStatus::Moved { current: Some(_) }),
        "Expected Moved with current, got {:?}",
        status
    );

    // lastDocId should have advanced to the next common doc
    assert_eq!(
        ii.last_doc_id(),
        20,
        "After revalidation with MOVED, lastDocId should advance to next common doc"
    );
}

/// Test: Mix of OK and MOVED results
/// Equivalent to C++ TEST_F(IntersectionIteratorRevalidateTest, RevalidateMixedResults)
#[test]
fn revalidate_mixed_results() {
    let child0: MockIterator<'static, 10> =
        MockIterator::new([10, 15, 20, 25, 30, 35, 40, 45, 50, 55]);
    let child1: MockIterator<'static, 11> =
        MockIterator::new([5, 10, 18, 20, 28, 30, 38, 40, 48, 50, 60]);
    let child2: MockIterator<'static, 11> =
        MockIterator::new([2, 10, 12, 20, 22, 30, 32, 40, 42, 50, 70]);

    // Mixed: OK, MOVED, OK
    child0
        .data()
        .set_revalidate_result(MockRevalidateResult::Ok);
    child1
        .data()
        .set_revalidate_result(MockRevalidateResult::Move);
    child2
        .data()
        .set_revalidate_result(MockRevalidateResult::Ok);

    let children: Vec<Box<dyn RQEIterator<'static> + 'static>> =
        vec![Box::new(child0), Box::new(child1), Box::new(child2)];

    let mut ii = Intersection::new(children);

    // Read first document
    let result = ii.read().expect("read failed").unwrap();
    assert_eq!(result.doc_id, 10);

    // Revalidate should return Moved (if any child moved)
    let status = ii.revalidate().expect("revalidate failed");
    assert!(matches!(status, RQEValidateStatus::Moved { .. }));
    assert_eq!(ii.last_doc_id(), 20);
}

/// Test: Revalidate after EOF - should return OK even if children moved
/// Equivalent to C++ TEST_F(IntersectionIteratorRevalidateTest, RevalidateAfterEOF)
#[test]
fn revalidate_after_eof() {
    // Pre-set children to return MOVE on revalidate
    let child0: MockIterator<'static, 10> =
        MockIterator::new([10, 15, 20, 25, 30, 35, 40, 45, 50, 55]);
    let child1: MockIterator<'static, 11> =
        MockIterator::new([5, 10, 18, 20, 28, 30, 38, 40, 48, 50, 60]);
    let child2: MockIterator<'static, 11> =
        MockIterator::new([2, 10, 12, 20, 22, 30, 32, 40, 42, 50, 70]);

    child0
        .data()
        .set_revalidate_result(MockRevalidateResult::Move);
    child1
        .data()
        .set_revalidate_result(MockRevalidateResult::Move);
    child2
        .data()
        .set_revalidate_result(MockRevalidateResult::Move);

    let children: Vec<Box<dyn RQEIterator<'static> + 'static>> =
        vec![Box::new(child0), Box::new(child1), Box::new(child2)];

    let mut ii = Intersection::new(children);

    // Advance to EOF
    while ii.read().expect("read failed").is_some() {}
    assert!(ii.at_eof());

    // Revalidate should return OK when already at EOF
    let status = ii.revalidate().expect("revalidate failed");
    assert!(
        matches!(status, RQEValidateStatus::Ok),
        "Revalidate after EOF should return OK, got {:?}",
        status
    );

    // Should still be at EOF
    assert!(ii.at_eof());
    assert!(matches!(ii.read(), Ok(None)));
}

/// Test: Some children move to EOF during revalidate
/// Equivalent to C++ TEST_F(IntersectionIteratorRevalidateTest, RevalidateSomeChildrenMovedToEOF)
///
/// Note: The new MockIterator doesn't have a MovedToEof variant. Instead, we simulate
/// this by using a child that has only 2 elements - after reading doc 10, there's only
/// one element left (20), so when Move is called during revalidate, it reaches EOF.
#[test]
fn revalidate_some_children_moved_to_eof() {
    // Child 0 and 2 have normal data, child 1 is small (only 2 elements: [10, 20])
    // When we read doc 10 and then call Move, child 1 moves to 20 and the next Move
    // would go to EOF
    let child0: MockIterator<'static, 10> =
        MockIterator::new([10, 15, 20, 25, 30, 35, 40, 45, 50, 55]);
    // Child 1 has only doc 10 - after reading it, Move will result in EOF
    let child1: MockIterator<'static, 1> = MockIterator::new([10]);
    let child2: MockIterator<'static, 11> =
        MockIterator::new([2, 10, 12, 20, 22, 30, 32, 40, 42, 50, 70]);

    // Child 0: OK, Child 1: moves (to EOF since only 1 element), Child 2: OK
    child0
        .data()
        .set_revalidate_result(MockRevalidateResult::Ok);
    child1
        .data()
        .set_revalidate_result(MockRevalidateResult::Move);
    child2
        .data()
        .set_revalidate_result(MockRevalidateResult::Ok);

    let children: Vec<Box<dyn RQEIterator<'static> + 'static>> =
        vec![Box::new(child0), Box::new(child1), Box::new(child2)];

    let mut ii = Intersection::new(children);

    // Read first document
    let result = ii.read().expect("read failed").unwrap();
    assert_eq!(result.doc_id, 10);

    // Revalidate should return Moved with current=None (EOF)
    // because child 1 moves to EOF (it only had 1 element which was already read)
    let status = ii.revalidate().expect("revalidate failed");
    assert!(
        matches!(status, RQEValidateStatus::Moved { current: None }),
        "Expected Moved to EOF, got {:?}",
        status
    );

    // Intersection should now be at EOF
    assert!(ii.at_eof());

    // Further reads should return EOF
    assert!(matches!(ii.read(), Ok(None)));
    assert!(matches!(ii.skip_to(100), Ok(None)));
}

// =============================================================================
// Reducer tests - from IntersectionIteratorReducerTest
// =============================================================================

/// Test: Intersection with an empty child returns Empty iterator
/// Equivalent to C++ TEST_F(IntersectionIteratorReducerTest, TestIntersectionWithEmptyChild)
#[test]
fn reduce_with_empty_child() {
    // Create children where one is empty
    let children: Vec<Box<dyn RQEIterator<'static> + 'static>> = vec![
        Box::new(SortedIdList::new(vec![1, 2, 3])),
        Box::new(Empty),
        Box::new(SortedIdList::new(vec![1, 2, 3, 4, 5])),
    ];

    let result = reduce(children);

    // Should return Empty
    assert!(
        matches!(result, ReducedIntersection::Empty(_)),
        "Expected Empty, got {:?}",
        std::mem::discriminant(&result)
    );
}

/// Test: Intersection with no children returns Empty
/// Equivalent to C++ TEST_F(IntersectionIteratorReducerTest, TestIntersectionWithNoChild)
#[test]
fn reduce_with_no_children() {
    let children: Vec<SortedIdList<'static>> = vec![];

    let result = reduce(children);

    assert!(matches!(result, ReducedIntersection::Empty(_)));
}

/// Test: Intersection removes wildcard children
/// Equivalent to C++ TEST_F(IntersectionIteratorReducerTest, TestIntersectionRemovesWildcardChildren)
#[test]
fn reduce_removes_wildcard_children() {
    // 2 regular iterators + 2 wildcards = should keep only 2 regular
    let children: Vec<Box<dyn RQEIterator<'static> + 'static>> = vec![
        Box::new(SortedIdList::new(vec![1, 2, 3])),
        Box::new(Wildcard::new(100, 1.0)),
        Box::new(SortedIdList::new(vec![1, 2, 3])),
        Box::new(Wildcard::new(100, 1.0)),
    ];

    let mut result = reduce(children);

    // Should return Intersection (2 non-wildcard children)
    assert!(
        matches!(result, ReducedIntersection::Intersection(_)),
        "Expected Intersection"
    );

    // Should be able to read documents
    let doc = result.read().expect("read failed").unwrap();
    assert_eq!(doc.doc_id, 1);
}

/// Test: All wildcard children returns the last wildcard
/// Equivalent to C++ TEST_F(IntersectionIteratorReducerTest, TestIntersectionAllWildCardChildren)
#[test]
fn reduce_all_wildcards() {
    let children: Vec<Wildcard<'static>> = vec![
        Wildcard::new(30, 1.0),
        Wildcard::new(40, 1.0),
        Wildcard::new(50, 1.0),
        Wildcard::new(60, 1.0), // Last one should be returned
    ];

    let mut result = reduce(children);

    // Should return Single (the last wildcard)
    assert!(
        matches!(result, ReducedIntersection::Single(_)),
        "Expected Single"
    );

    // The returned wildcard should have top_id = 60
    assert_eq!(result.num_estimated(), 60);

    // Should be able to read documents 1..60
    let doc = result.read().expect("read failed").unwrap();
    assert_eq!(doc.doc_id, 1);
}

/// Test: Single non-wildcard child after removing wildcards
/// Equivalent to C++ TEST_F(IntersectionIteratorReducerTest, TestIntersectionWithSingleChild)
#[test]
fn reduce_single_non_wildcard_after_removing_wildcards() {
    let children: Vec<Box<dyn RQEIterator<'static> + 'static>> = vec![
        Box::new(SortedIdList::new(vec![1, 2, 3])),
        Box::new(Wildcard::new(100, 1.0)),
        Box::new(Wildcard::new(100, 1.0)),
    ];

    let mut result = reduce(children);

    // Should return Single (the non-wildcard iterator)
    assert!(
        matches!(result, ReducedIntersection::Single(_)),
        "Expected Single"
    );

    // Should be able to read documents 1, 2, 3
    assert_eq!(result.read().unwrap().unwrap().doc_id, 1);
    assert_eq!(result.read().unwrap().unwrap().doc_id, 2);
    assert_eq!(result.read().unwrap().unwrap().doc_id, 3);
    assert!(result.read().unwrap().is_none());
}

/// Test: Reduced intersection works correctly end-to-end
#[test]
fn reduce_works_as_iterator() {
    // Mix of regular and wildcard children
    let children: Vec<Box<dyn RQEIterator<'static> + 'static>> = vec![
        Box::new(SortedIdList::new(vec![1, 2, 3, 10, 20])),
        Box::new(Wildcard::new(100, 1.0)), // Will be removed
        Box::new(SortedIdList::new(vec![2, 3, 20, 30])),
    ];

    let mut result = reduce(children);

    // Should be Intersection of the two non-wildcards
    assert!(matches!(result, ReducedIntersection::Intersection(_)));

    // Common docs: 2, 3, 20
    assert_eq!(result.read().unwrap().unwrap().doc_id, 2);
    assert_eq!(result.read().unwrap().unwrap().doc_id, 3);
    assert_eq!(result.read().unwrap().unwrap().doc_id, 20);
    assert!(result.read().unwrap().is_none());
    assert!(result.at_eof());
}

// =============================================================================
// Additional tests for comprehensive coverage
// =============================================================================

/// Test: current() returns correct state after various operations
#[test]
fn current_after_operations() {
    let child1 = SortedIdList::new(vec![10, 20, 30, 40, 50]);
    let child2 = SortedIdList::new(vec![10, 20, 30, 40, 50]);

    let mut ii = Intersection::new(vec![child1, child2]);

    // Before any read, current() returns Some (the result buffer exists),
    // but last_doc_id is 0 since we haven't read anything yet
    // Note: The intersection is not at EOF, so current() returns the buffer
    assert!(
        ii.current().is_some(),
        "current() before first read returns Some (buffer exists)"
    );
    assert_eq!(ii.last_doc_id(), 0, "last_doc_id should be 0 before read");

    // After read, current() should return the current result
    let _ = ii.read().expect("read failed");
    let current = ii.current();
    assert!(current.is_some(), "current() after read should be Some");
    assert_eq!(current.unwrap().doc_id, 10);

    // After another read, current() should reflect the new position
    let _ = ii.read().expect("read failed");
    let current = ii.current();
    assert!(current.is_some());
    assert_eq!(current.unwrap().doc_id, 20);

    // After skip_to, current() should reflect the skipped position
    let _ = ii.skip_to(40).expect("skip_to failed");
    let current = ii.current();
    assert!(current.is_some());
    assert_eq!(current.unwrap().doc_id, 40);

    // After rewind, current() returns Some (not at EOF) and both last_doc_id
    // and current().doc_id should be reset to 0
    ii.rewind();
    assert!(
        ii.current().is_some(),
        "current() after rewind returns Some (not at EOF)"
    );
    assert_eq!(ii.last_doc_id(), 0, "last_doc_id should be 0 after rewind");
    assert_eq!(
        ii.current().unwrap().doc_id,
        0,
        "current().doc_id should be 0 after rewind"
    );

    // After EOF, current() should return None
    while ii.read().expect("read failed").is_some() {}
    assert!(ii.at_eof());
    assert!(ii.current().is_none(), "current() after EOF should be None");
}

/// Test: Large gaps between document IDs
#[test]
fn large_doc_id_gaps() {
    let sparse_ids = vec![1, 1_000_000, 2_000_000, 10_000_000];
    let child1 = SortedIdList::new(sparse_ids.clone());
    let child2 = SortedIdList::new(sparse_ids.clone());

    let mut ii = Intersection::new(vec![child1, child2]);

    // Read all documents
    for &expected_id in &sparse_ids {
        let result = ii.read().expect("read failed");
        assert!(result.is_some(), "Expected doc {expected_id}");
        assert_eq!(result.unwrap().doc_id, expected_id);
    }

    assert!(matches!(ii.read(), Ok(None)));
    assert!(ii.at_eof());

    // Test skip_to with large gaps
    ii.rewind();
    let outcome = ii.skip_to(500_000).expect("skip_to failed");
    match outcome {
        Some(SkipToOutcome::NotFound(result)) => {
            assert_eq!(result.doc_id, 1_000_000);
        }
        _ => panic!("Expected NotFound landing on 1_000_000"),
    }

    // Skip to exact large ID
    ii.rewind();
    let outcome = ii.skip_to(2_000_000).expect("skip_to failed");
    assert!(matches!(outcome, Some(SkipToOutcome::Found(_))));
    assert_eq!(ii.last_doc_id(), 2_000_000);
}

/// Test: Children with overlapping unique IDs don't cause issues
#[test]
fn overlapping_children_ids() {
    // Create children with significant overlap but different unique IDs
    let child1 = SortedIdList::new(vec![1, 2, 3, 5, 10, 15, 20, 25, 30]);
    let child2 = SortedIdList::new(vec![2, 3, 5, 7, 10, 12, 15, 20, 30, 35]);
    let child3 = SortedIdList::new(vec![3, 5, 8, 10, 15, 18, 20, 30, 40]);

    let mut ii = Intersection::new(vec![child1, child2, child3]);

    // Common to all: 3, 5, 10, 15, 20, 30
    let expected = vec![3, 5, 10, 15, 20, 30];

    for &expected_id in &expected {
        let result = ii.read().expect("read failed");
        assert!(result.is_some(), "Expected doc {expected_id}");
        assert_eq!(result.unwrap().doc_id, expected_id);
    }

    assert!(matches!(ii.read(), Ok(None)));
    assert!(ii.at_eof());
}

/// Test: Revalidate immediately after construction (without reading first)
#[test]
fn revalidate_before_read() {
    let child0: MockIterator<'static, 10> =
        MockIterator::new([10, 15, 20, 25, 30, 35, 40, 45, 50, 55]);
    let child1: MockIterator<'static, 11> =
        MockIterator::new([5, 10, 18, 20, 28, 30, 38, 40, 48, 50, 60]);
    let child2: MockIterator<'static, 11> =
        MockIterator::new([2, 10, 12, 20, 22, 30, 32, 40, 42, 50, 70]);

    // All children return OK on revalidate
    child0
        .data()
        .set_revalidate_result(MockRevalidateResult::Ok);
    child1
        .data()
        .set_revalidate_result(MockRevalidateResult::Ok);
    child2
        .data()
        .set_revalidate_result(MockRevalidateResult::Ok);

    let children: Vec<Box<dyn RQEIterator<'static> + 'static>> =
        vec![Box::new(child0), Box::new(child1), Box::new(child2)];

    let mut ii = Intersection::new(children);

    // Revalidate before any read
    let status = ii.revalidate().expect("revalidate failed");
    assert!(
        matches!(status, RQEValidateStatus::Ok),
        "Revalidate before read should return Ok"
    );

    // Should still be able to read normally
    let result = ii.read().expect("read failed").unwrap();
    assert_eq!(result.doc_id, 10);
}

/// Test: Revalidate with Move before first read
#[test]
fn revalidate_move_before_read() {
    let child0: MockIterator<'static, 10> =
        MockIterator::new([10, 15, 20, 25, 30, 35, 40, 45, 50, 55]);
    let child1: MockIterator<'static, 11> =
        MockIterator::new([5, 10, 18, 20, 28, 30, 38, 40, 48, 50, 60]);
    let child2: MockIterator<'static, 11> =
        MockIterator::new([2, 10, 12, 20, 22, 30, 32, 40, 42, 50, 70]);

    // All children will move
    child0
        .data()
        .set_revalidate_result(MockRevalidateResult::Move);
    child1
        .data()
        .set_revalidate_result(MockRevalidateResult::Move);
    child2
        .data()
        .set_revalidate_result(MockRevalidateResult::Move);

    let children: Vec<Box<dyn RQEIterator<'static> + 'static>> =
        vec![Box::new(child0), Box::new(child1), Box::new(child2)];

    let mut ii = Intersection::new(children);

    // Revalidate before any read - children will move
    let status = ii.revalidate().expect("revalidate failed");

    // Since we haven't read anything yet, and children moved,
    // the result depends on implementation. The iterator should
    // either return Ok (no current position to invalidate) or
    // Moved if it tracks that children moved.
    assert!(
        matches!(
            status,
            RQEValidateStatus::Ok | RQEValidateStatus::Moved { .. }
        ),
        "Revalidate before read with Move should return Ok or Moved"
    );
}

/// Test: Verify estimated count is minimum of children
#[test]
fn num_estimated_is_minimum() {
    // Create children with different sizes
    let child1 = SortedIdList::new(vec![1, 2, 3, 4, 5]); // 5 elements
    let child2 = SortedIdList::new(vec![1, 2, 3]); // 3 elements (smallest)
    let child3 = SortedIdList::new(vec![1, 2, 3, 4, 5, 6, 7]); // 7 elements

    let ii = Intersection::new(vec![child1, child2, child3]);

    // num_estimated should be the minimum (3)
    assert_eq!(
        ii.num_estimated(),
        3,
        "num_estimated should be minimum of children"
    );
}

/// Test: Children are processed in order of estimated count (smallest first)
/// We can infer this indirectly by checking behavior with asymmetric children
#[test]
fn children_sorted_by_estimated() {
    // Create children where the smallest (by count) would lead to fastest termination
    // Large child: has docs 1-1000
    let large_child: Vec<t_docId> = (1..=1000).collect();
    // Small child: only has doc 500
    let small_child = vec![500];
    // Medium child: has docs 100, 200, 300, 400, 500, 600, 700
    let medium_child = vec![100, 200, 300, 400, 500, 600, 700];

    // The order we pass them shouldn't matter - they should be sorted internally
    let child1 = SortedIdList::new(large_child);
    let child2 = SortedIdList::new(small_child);
    let child3 = SortedIdList::new(medium_child);

    let mut ii = Intersection::new(vec![child1, child2, child3]);

    // The only common document is 500
    let result = ii.read().expect("read failed");
    assert!(result.is_some());
    assert_eq!(result.unwrap().doc_id, 500);

    // No more results
    assert!(matches!(ii.read(), Ok(None)));
    assert!(ii.at_eof());

    // num_estimated should be 1 (smallest child)
    assert_eq!(ii.num_estimated(), 1);
}

/// Test: Empty children vector via reduce returns Empty
#[test]
fn reduce_empty_vec_returns_empty() {
    let children: Vec<SortedIdList<'static>> = vec![];
    let mut result = reduce(children);

    assert!(matches!(result, ReducedIntersection::Empty(_)));
    assert!(result.at_eof());
    assert!(matches!(result.read(), Ok(None)));
    assert_eq!(result.num_estimated(), 0);
}

/// Test: ReducedIntersection trait methods work correctly for all variants
#[test]
fn reduced_intersection_trait_methods() {
    // Test Empty variant
    let children: Vec<SortedIdList<'static>> = vec![];
    let mut empty_result = reduce(children);
    assert!(empty_result.at_eof());
    assert_eq!(empty_result.last_doc_id(), 0);
    assert_eq!(empty_result.num_estimated(), 0);
    assert!(matches!(empty_result.skip_to(10), Ok(None)));
    // Verify is_empty() delegates correctly to Empty variant
    assert!(
        empty_result.is_empty(),
        "ReducedIntersection::Empty should delegate is_empty() to return true"
    );
    assert!(
        !empty_result.is_wildcard(),
        "ReducedIntersection::Empty should not be a wildcard"
    );

    // Test Single variant (all wildcards returns last)
    let children: Vec<Wildcard<'static>> = vec![Wildcard::new(50, 1.0)];
    let single_result = reduce(children);
    assert!(matches!(single_result, ReducedIntersection::Single(_)));
    assert!(!single_result.at_eof());
    assert_eq!(single_result.num_estimated(), 50);
    // Verify is_wildcard() delegates correctly to Wildcard variant
    assert!(
        single_result.is_wildcard(),
        "ReducedIntersection::Single(Wildcard) should delegate is_wildcard() to return true"
    );
    assert!(
        !single_result.is_empty(),
        "ReducedIntersection::Single(Wildcard) should not be empty"
    );

    // Test Intersection variant
    let children: Vec<SortedIdList<'static>> = vec![
        SortedIdList::new(vec![1, 2, 3]),
        SortedIdList::new(vec![2, 3, 4]),
    ];
    let mut intersect_result = reduce(children);
    assert!(matches!(
        intersect_result,
        ReducedIntersection::Intersection(_)
    ));
    assert!(!intersect_result.at_eof());
    // Intersection should not report as empty or wildcard
    assert!(
        !intersect_result.is_empty(),
        "ReducedIntersection::Intersection should not be empty"
    );
    assert!(
        !intersect_result.is_wildcard(),
        "ReducedIntersection::Intersection should not be a wildcard"
    );

    // Common docs are 2, 3
    assert_eq!(intersect_result.read().unwrap().unwrap().doc_id, 2);
    assert_eq!(intersect_result.last_doc_id(), 2);

    intersect_result.rewind();
    assert_eq!(intersect_result.last_doc_id(), 0);
    assert!(!intersect_result.at_eof());
}
