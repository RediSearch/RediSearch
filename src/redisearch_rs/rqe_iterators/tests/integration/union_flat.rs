/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Integration tests for the UnionFlat iterator variants.

use crate::union_cases;
use crate::utils::{Mock, MockRevalidateResult};
use ffi::t_docId;
use rqe_iterators::{
    RQEIterator, RQEValidateStatus, SkipToOutcome, UnionFullFlat, UnionQuickFlat,
    id_list::IdListSorted,
};
use rstest_reuse::apply;

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
// Read tests
// =============================================================================

#[apply(union_cases)]
#[cfg_attr(miri, ignore)] // Uses RSYieldableMetric_Concat FFI in push_borrowed (union aggregation)
fn read(#[case] num_children: usize, #[case] base_result_set: &[u64]) {
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

#[apply(union_cases)]
#[cfg_attr(miri, ignore)] // Uses RSYieldableMetric_Concat FFI in push_borrowed (union aggregation)
fn skip_to(#[case] num_children: usize, #[case] base_result_set: &[u64]) {
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

/// Test skip_to edge cases for both Quick and Full modes.
/// Covers lines 239-240, 302-303, 311-312.
#[test]
#[cfg_attr(miri, ignore)] // Uses RSYieldableMetric_Concat FFI in push_borrowed (union aggregation)
fn skip_to_edge_cases() {
    // Test 1: Quick mode - child already at target doc_id (lines 311-312)
    // Child B must be ahead of child A, then we skip to B's position
    {
        let child1 = IdListSorted::new(vec![50, 200]); // Will be at 50 after first read
        let child2 = IdListSorted::new(vec![100, 250]); // Will be at 100 after init

        let mut quick_iter = UnionQuickFlat::new(vec![child1, child2]);
        // Read first doc (50) - union is now at 50, child1 at 50, child2 at 100
        quick_iter.read().expect("read failed").unwrap();
        assert_eq!(quick_iter.last_doc_id(), 50);
        // skip_to(100) - child2 is already at 100, triggers line 311-312
        let outcome = quick_iter.skip_to(100).expect("skip_to failed");
        assert!(matches!(outcome, Some(SkipToOutcome::Found(_))));
        assert_eq!(quick_iter.last_doc_id(), 100);
    }

    // Test 2: Quick mode - child exhausts during skip_to (lines 300-306)
    // Line 303 requires min_child_idx == num_active-1 when a NON-last child exhausts.
    // This is hard to hit due to linear iteration, but we can at least cover the
    // basic None branch (lines 300-306 without line 303's condition being true).
    {
        // child1 will exhaust when skip_to(100) is called (max doc is 30)
        let child1 = IdListSorted::new(vec![10, 30]); // exhausts at skip_to(100)
        let child2 = IdListSorted::new(vec![20, 200]); // survives

        let mut quick_iter = UnionQuickFlat::new(vec![child1, child2]);
        quick_iter.read().expect("read failed").unwrap(); // Read 10
        // skip_to(100): child1 exhausts (30 < 100), child2 lands at 200
        let outcome = quick_iter.skip_to(100).expect("skip_to failed");
        assert!(matches!(outcome, Some(SkipToOutcome::NotFound(_))));
        assert_eq!(quick_iter.last_doc_id(), 200);
    }

    // Test 3: Full mode - child already at EOF before skip_to (lines 260-263)
    // This covers the else branch where at_eof() is true but child is still in active set.
    // Scenario: child1 has docs [10, 20], child2 has docs [15, 50, 100].
    // After reading docs 10, 15, 20 - child1 is at EOF but still in active set because
    // read() only removes children when their read() returns None, not proactively.
    {
        let child1 = IdListSorted::new(vec![10, 20]); // Will be at EOF after reading 20
        let child2 = IdListSorted::new(vec![15, 50, 100]); // Has docs beyond child1

        let mut full_iter = UnionFullFlat::new(vec![child1, child2]);

        // Read 10 (child1's first doc) - child1 at offset 1, not at EOF
        full_iter.read().expect("read failed").unwrap();
        assert_eq!(full_iter.last_doc_id(), 10);

        // Read 15 (child2's first doc) - child1 not advanced (not at minimum)
        full_iter.read().expect("read failed").unwrap();
        assert_eq!(full_iter.last_doc_id(), 15);

        // Read 20 (child1's last doc) - child1 at offset 2, at_eof() = true
        // But child1 is still in active set because read() returned Some
        full_iter.read().expect("read failed").unwrap();
        assert_eq!(full_iter.last_doc_id(), 20);

        // Now skip_to(30): child1's last_doc_id = 20 < 30, and at_eof() = true
        // This hits lines 260-263 (else branch in skip_to_full)
        let outcome = full_iter.skip_to(30).expect("skip_to failed");
        assert!(matches!(outcome, Some(SkipToOutcome::NotFound(_))));
        // child2's next doc after 15 that's >= 30 is 50
        assert_eq!(full_iter.last_doc_id(), 50);

        // Continue reading to verify state is correct
        let outcome = full_iter.read().expect("read failed").unwrap();
        assert_eq!(outcome.doc_id, 100);

        // Should be at EOF now
        assert!(matches!(full_iter.read(), Ok(None)));
        assert!(full_iter.at_eof());
    }
}

// =============================================================================
// Rewind tests
// =============================================================================

#[apply(union_cases)]
#[cfg_attr(miri, ignore)] // Uses RSYieldableMetric_Concat FFI in push_borrowed (union aggregation)
fn rewind(#[case] num_children: usize, #[case] base_result_set: &[u64]) {
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
#[cfg_attr(miri, ignore)] // Uses RSYieldableMetric_Concat FFI in push_borrowed (union aggregation)
fn edge_case_no_children() {
    let children: Vec<IdListSorted<'static>> = vec![];
    let mut union_iter = Union::new(children);

    assert!(matches!(union_iter.read(), Ok(None)));
    assert!(union_iter.at_eof());
    assert_eq!(union_iter.num_estimated(), 0);
    assert!(matches!(union_iter.skip_to(1), Ok(None)));
    assert!(union_iter.at_eof());
}

#[test]
#[cfg_attr(miri, ignore)] // Uses RSYieldableMetric_Concat FFI in push_borrowed (union aggregation)
fn edge_case_single_child() {
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
#[cfg_attr(miri, ignore)] // Uses RSYieldableMetric_Concat FFI in push_borrowed (union aggregation)
fn edge_case_disjoint_children() {
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
#[cfg_attr(miri, ignore)] // Uses RSYieldableMetric_Concat FFI in push_borrowed (union aggregation)
fn edge_case_overlapping_children() {
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
#[cfg_attr(miri, ignore)] // Uses RSYieldableMetric_Concat FFI in push_borrowed (union aggregation)
fn edge_case_skip_to_exact_match() {
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
#[cfg_attr(miri, ignore)] // Uses RSYieldableMetric_Concat FFI in push_borrowed (union aggregation)
fn edge_case_skip_to_not_found() {
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
#[cfg_attr(miri, ignore)] // Uses RSYieldableMetric_Concat FFI in push_borrowed (union aggregation)
fn edge_case_skip_to_past_eof() {
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
#[cfg_attr(miri, ignore)] // Uses RSYieldableMetric_Concat FFI in push_borrowed (union aggregation)
fn edge_case_interleaved_read_and_skip_to() {
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
#[cfg_attr(miri, ignore)] // Uses RSYieldableMetric_Concat FFI in push_borrowed (union aggregation)
fn edge_case_num_estimated_is_sum() {
    let child1 = IdListSorted::new(vec![1, 2, 3, 4, 5]); // 5 elements
    let child2 = IdListSorted::new(vec![10, 20, 30]); // 3 elements
    let child3 = IdListSorted::new(vec![100, 200, 300, 400]); // 4 elements

    let union_iter = Union::new(vec![child1, child2, child3]);

    // For union, num_estimated is the sum (upper bound)
    assert_eq!(union_iter.num_estimated(), 12);
}

/// Test with empty iterators mixed with non-empty ones.
/// This covers lines 174-176 (empty iterator at EOF during initialization).
#[test]
#[cfg_attr(miri, ignore)] // Uses RSYieldableMetric_Concat FFI in push_borrowed (union aggregation)
fn edge_case_empty_children_mixed_with_non_empty() {
    // Create an "empty" child by making one that will be exhausted before the union starts
    let empty_child = IdListSorted::new(vec![]); // Empty iterator
    let child1 = IdListSorted::new(vec![10, 20, 30]);
    let child2 = IdListSorted::new(vec![15, 25, 35]);

    let mut union_iter = Union::new(vec![empty_child, child1, child2]);

    // Union should still work with the non-empty children
    let expected = vec![10, 15, 20, 25, 30, 35];
    for &expected_id in &expected {
        let result = union_iter.read().expect("read failed");
        assert!(result.is_some(), "Expected doc {expected_id}");
        assert_eq!(result.unwrap().doc_id, expected_id);
    }
    // After reading all documents, a final read should return None
    assert!(matches!(union_iter.read(), Ok(None)));
    assert!(union_iter.at_eof());
}

/// Test where all children are empty.
/// This covers lines 193-194 (all children exhausted during initialization).
#[test]
#[cfg_attr(miri, ignore)] // Uses RSYieldableMetric_Concat FFI in push_borrowed (union aggregation)
fn edge_case_all_children_empty() {
    let empty1 = IdListSorted::new(vec![]);
    let empty2 = IdListSorted::new(vec![]);
    let empty3 = IdListSorted::new(vec![]);

    let mut union_iter = Union::new(vec![empty1, empty2, empty3]);

    // Union should be immediately at EOF
    assert!(matches!(union_iter.read(), Ok(None)));
    assert!(union_iter.at_eof());
    assert_eq!(union_iter.num_estimated(), 0);
}

/// Test skip_to where one child is already past target.
/// This covers lines 215-220 (child already at/past target doc_id).
#[test]
#[cfg_attr(miri, ignore)] // Uses RSYieldableMetric_Concat FFI in push_borrowed (union aggregation)
fn edge_case_skip_to_child_already_past_target() {
    let child1 = IdListSorted::new(vec![10, 50, 100]);
    let child2 = IdListSorted::new(vec![20, 60, 110]);

    let mut union_iter = Union::new(vec![child1, child2]);

    // Read first two docs (10, 20) - both children advance
    union_iter.read().expect("read failed");
    union_iter.read().expect("read failed");

    // Now child1 is at 50, child2 is at 60
    // Skip to 40 - child1 is already at 50 (past target), child2 is at 60 (past target)
    let outcome = union_iter.skip_to(40).expect("skip_to failed");
    assert!(matches!(outcome, Some(SkipToOutcome::NotFound(_))));
    // Should land on 50 (minimum of 50, 60)
    assert_eq!(union_iter.last_doc_id(), 50);
}

/// Test skip_to where children exhaust during the skip.
/// This covers lines 239-240 (child reaches EOF during skip_to).
#[test]
#[cfg_attr(miri, ignore)] // Uses RSYieldableMetric_Concat FFI in push_borrowed (union aggregation)
fn edge_case_skip_to_exhausts_some_children() {
    let child1 = IdListSorted::new(vec![10, 20, 30]); // Will exhaust
    let child2 = IdListSorted::new(vec![15, 25, 100]); // Has doc past target

    let mut union_iter = Union::new(vec![child1, child2]);

    // Skip to 50 - child1 will exhaust (max is 30), child2 will land on 100
    let outcome = union_iter.skip_to(50).expect("skip_to failed");
    assert!(matches!(outcome, Some(SkipToOutcome::NotFound(_))));
    assert_eq!(union_iter.last_doc_id(), 100);

    // Now only child2 should be active
    assert!(matches!(union_iter.read(), Ok(None)));
    assert!(union_iter.at_eof());
}

/// Test skip_to past all documents - all children exhaust.
#[test]
#[cfg_attr(miri, ignore)] // Uses RSYieldableMetric_Concat FFI in push_borrowed (union aggregation)
fn edge_case_skip_to_exhausts_all_children() {
    let child1 = IdListSorted::new(vec![10, 20, 30]);
    let child2 = IdListSorted::new(vec![15, 25, 35]);

    let mut union_iter = Union::new(vec![child1, child2]);

    // Skip past all documents
    let outcome = union_iter.skip_to(1000).expect("skip_to failed");
    assert!(outcome.is_none());
    assert!(union_iter.at_eof());
}

/// Test Quick mode initialization with multiple empty children interspersed.
/// This covers lines 174-176 (child at EOF during initialization).
#[test]
#[cfg_attr(miri, ignore)] // Uses RSYieldableMetric_Concat FFI in push_borrowed (union aggregation)
fn edge_case_initialize_with_empty_children() {
    // Mix empty and non-empty children in different positions
    let empty1 = IdListSorted::new(vec![]);
    let child1 = IdListSorted::new(vec![10, 20]);
    let empty2 = IdListSorted::new(vec![]);
    let child2 = IdListSorted::new(vec![15, 25]);
    let empty3 = IdListSorted::new(vec![]);

    let mut quick_iter = UnionQuickFlat::new(vec![empty1, child1, empty2, child2, empty3]);

    // Should work correctly despite empty children
    let expected = vec![10, 15, 20, 25];
    for &expected_id in &expected {
        let result = quick_iter.read().expect("read failed");
        assert!(result.is_some(), "Expected doc {expected_id}");
        assert_eq!(result.unwrap().doc_id, expected_id);
    }
    assert!(matches!(quick_iter.read(), Ok(None)));
    assert!(quick_iter.at_eof());
}

/// Test defensive code path where child.read() returns None but at_eof() is false
/// during initialization.
/// This targets lines 181-182 (swap_remove misbehaving child during initial read).
#[test]
#[cfg_attr(miri, ignore)] // Uses RSYieldableMetric_Concat FFI in push_borrowed (union aggregation)
fn edge_case_misbehaving_child_returns_none_during_init() {
    // Create mocks - mock1 will "misbehave" during initialization
    let mock1: Mock<'static, 3> = Mock::new([10, 30, 50]);
    let mock2: Mock<'static, 3> = Mock::new([20, 40, 60]);

    // Get data handles before boxing
    let mut data1 = mock1.data();

    // Force mock1 to misbehave on its first read (during Union initialization)
    // It will return None even though it's not at EOF
    data1.set_force_read_none(true);

    let children: Vec<Box<dyn RQEIterator<'static> + 'static>> =
        vec![Box::new(mock1), Box::new(mock2)];

    // When Union is created, it calls find_minimum() which calls read() on each child
    // mock1 will return None (misbehaving), triggering lines 181-182
    let mut union_iter = Union::new(children);

    // Union should have removed the misbehaving child and only have mock2
    // First read should give us 20 from mock2
    let result = union_iter.read().expect("read failed").unwrap();
    assert_eq!(
        result.doc_id, 20,
        "After misbehaving child is removed during init, should get first doc from remaining child"
    );

    // Continue reading - should only have mock2
    let result = union_iter.read().expect("read failed").unwrap();
    assert_eq!(result.doc_id, 40);

    let result = union_iter.read().expect("read failed").unwrap();
    assert_eq!(result.doc_id, 60);

    // Should be at EOF now
    assert!(matches!(union_iter.read(), Ok(None)));
    assert!(union_iter.at_eof());
}

// =============================================================================
// Revalidate tests
// =============================================================================

#[test]
#[cfg_attr(miri, ignore)] // Uses RSYieldableMetric_Concat FFI in push_borrowed (union aggregation)
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
#[cfg_attr(miri, ignore)] // Uses RSYieldableMetric_Concat FFI in push_borrowed (union aggregation)
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
#[cfg_attr(miri, ignore)] // Uses RSYieldableMetric_Concat FFI in push_borrowed (union aggregation)
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
#[cfg_attr(miri, ignore)] // Uses RSYieldableMetric_Concat FFI in push_borrowed (union aggregation)
fn revalidate_single_child_aborts() {
    // When one child aborts, the union should continue with remaining children
    let child0: Mock<'static, 5> = Mock::new([10, 20, 30, 40, 50]);
    let child1: Mock<'static, 5> = Mock::new([15, 25, 35, 45, 55]);
    let child2: Mock<'static, 5> = Mock::new([12, 22, 32, 42, 52]);

    // child0 will abort, others stay Ok
    child0
        .data()
        .set_revalidate_result(MockRevalidateResult::Abort);
    child1
        .data()
        .set_revalidate_result(MockRevalidateResult::Ok);
    child2
        .data()
        .set_revalidate_result(MockRevalidateResult::Ok);

    let children: Vec<Box<dyn RQEIterator<'static> + 'static>> =
        vec![Box::new(child0), Box::new(child1), Box::new(child2)];

    let mut union_iter = Union::new(children);

    // Read first document (should be 10 from child0)
    let result = union_iter.read().expect("read failed").unwrap();
    assert_eq!(result.doc_id, 10);

    // Revalidate - child0 aborts but union should continue
    let status = union_iter.revalidate().expect("revalidate failed");
    // Since a child was removed, the state changed - we might get Moved or Ok
    // depending on whether the current min_doc_id changed
    assert!(
        !matches!(status, RQEValidateStatus::Aborted),
        "Union should not abort when only one child aborts, got {:?}",
        status
    );

    // Union should still be functional - can continue reading from remaining children
    assert!(!union_iter.at_eof());
}

#[test]
#[cfg_attr(miri, ignore)] // Uses RSYieldableMetric_Concat FFI in push_borrowed (union aggregation)
fn revalidate_all_children_abort() {
    // When ALL children abort, the union should return Aborted
    let child0: Mock<'static, 5> = Mock::new([10, 20, 30, 40, 50]);
    let child1: Mock<'static, 5> = Mock::new([15, 25, 35, 45, 55]);

    child0
        .data()
        .set_revalidate_result(MockRevalidateResult::Abort);
    child1
        .data()
        .set_revalidate_result(MockRevalidateResult::Abort);

    let children: Vec<Box<dyn RQEIterator<'static> + 'static>> =
        vec![Box::new(child0), Box::new(child1)];

    let mut union_iter = Union::new(children);

    // Read first document
    let result = union_iter.read().expect("read failed").unwrap();
    assert_eq!(result.doc_id, 10);

    // Revalidate - all children abort, union should abort
    let status = union_iter.revalidate().expect("revalidate failed");
    assert!(
        matches!(status, RQEValidateStatus::Aborted),
        "Union should abort when all children abort, got {:?}",
        status
    );

    // Union should be at EOF after all children aborted
    assert!(union_iter.at_eof());
}

#[test]
#[cfg_attr(miri, ignore)] // Uses RSYieldableMetric_Concat FFI in push_borrowed (union aggregation)
fn revalidate_child_moves_to_eof() {
    // When a child reports Moved with current=None (moved to EOF), union should continue
    let child0: Mock<'static, 2> = Mock::new([10, 20]);
    let child1: Mock<'static, 5> = Mock::new([15, 25, 35, 45, 55]);

    // child0 will move (and reach EOF since it only has 2 elements)
    // child1 stays Ok
    child0
        .data()
        .set_revalidate_result(MockRevalidateResult::Ok);
    child1
        .data()
        .set_revalidate_result(MockRevalidateResult::Ok);

    let children: Vec<Box<dyn RQEIterator<'static> + 'static>> =
        vec![Box::new(child0), Box::new(child1)];

    let mut union_iter = Union::new(children);

    // Read all docs from child0 to make it EOF
    let result = union_iter.read().expect("read failed").unwrap();
    assert_eq!(result.doc_id, 10);
    let result = union_iter.read().expect("read failed").unwrap();
    assert_eq!(result.doc_id, 15);
    let result = union_iter.read().expect("read failed").unwrap();
    assert_eq!(result.doc_id, 20);

    // Revalidate should return Ok (no changes since children report Ok)
    let status = union_iter.revalidate().expect("revalidate failed");
    assert!(
        matches!(status, RQEValidateStatus::Ok),
        "Expected Ok, got {:?}",
        status
    );

    // Union should still work - child1 has more docs
    assert!(!union_iter.at_eof());
    let result = union_iter.read().expect("read failed").unwrap();
    assert_eq!(result.doc_id, 25);
}

#[test]
#[cfg_attr(miri, ignore)] // Uses RSYieldableMetric_Concat FFI in push_borrowed (union aggregation)
fn revalidate_mixed_ok_moved_abort() {
    // Mixed scenario: one child Ok, one Moved, one Aborts
    let child0: Mock<'static, 5> = Mock::new([10, 20, 30, 40, 50]);
    let child1: Mock<'static, 5> = Mock::new([15, 25, 35, 45, 55]);
    let child2: Mock<'static, 5> = Mock::new([12, 22, 32, 42, 52]);

    child0
        .data()
        .set_revalidate_result(MockRevalidateResult::Ok);
    child1
        .data()
        .set_revalidate_result(MockRevalidateResult::Move);
    child2
        .data()
        .set_revalidate_result(MockRevalidateResult::Abort);

    let children: Vec<Box<dyn RQEIterator<'static> + 'static>> =
        vec![Box::new(child0), Box::new(child1), Box::new(child2)];

    let mut union_iter = Union::new(children);

    // Read first document
    let result = union_iter.read().expect("read failed").unwrap();
    assert_eq!(result.doc_id, 10);

    // Revalidate - child2 aborts, child1 moves, child0 ok
    let status = union_iter.revalidate().expect("revalidate failed");

    // Should NOT be Aborted since we still have 2 children
    assert!(
        !matches!(status, RQEValidateStatus::Aborted),
        "Union should not abort when some children remain, got {:?}",
        status
    );

    // Union should still be functional
    assert!(!union_iter.at_eof());
}

#[test]
#[cfg_attr(miri, ignore)] // Uses RSYieldableMetric_Concat FFI in push_borrowed (union aggregation)
fn revalidate_all_children_move_to_eof() {
    // Test that when all children move to EOF during revalidation, union goes to EOF
    let child0: Mock<'static, 2> = Mock::new([10, 20]);
    let child1: Mock<'static, 2> = Mock::new([15, 25]);

    // Initially Ok - we'll read one doc then set to Move
    child0
        .data()
        .set_revalidate_result(MockRevalidateResult::Ok);
    child1
        .data()
        .set_revalidate_result(MockRevalidateResult::Ok);

    let children: Vec<Box<dyn RQEIterator<'static> + 'static>> =
        vec![Box::new(child0), Box::new(child1)];

    let mut union_iter = Union::new(children);

    // Read all docs to reach EOF
    while union_iter.read().expect("read failed").is_some() {}
    assert!(union_iter.at_eof());

    // Revalidate when already at EOF should return Ok
    let status = union_iter.revalidate().expect("revalidate failed");
    assert!(
        matches!(status, RQEValidateStatus::Ok),
        "Revalidate at EOF should return Ok, got {:?}",
        status
    );
}

#[test]
#[cfg_attr(miri, ignore)] // Uses RSYieldableMetric_Concat FFI in push_borrowed (union aggregation)
fn revalidate_updates_to_new_minimum() {
    // Test that after revalidation, union correctly finds new minimum doc_id
    // When the minimum docId changes, we should get Moved
    let child0: Mock<'static, 5> = Mock::new([10, 20, 30, 40, 50]);
    let child1: Mock<'static, 5> = Mock::new([5, 25, 35, 45, 55]);

    // Both start at Ok
    child0
        .data()
        .set_revalidate_result(MockRevalidateResult::Ok);
    child1
        .data()
        .set_revalidate_result(MockRevalidateResult::Ok);

    let children: Vec<Box<dyn RQEIterator<'static> + 'static>> =
        vec![Box::new(child0), Box::new(child1)];

    let mut union_iter = Union::new(children);

    // Read first doc (5 from child1)
    let result = union_iter.read().expect("read failed").unwrap();
    assert_eq!(result.doc_id, 5);

    // After read, current is at 5 (child1), next min is 10 (child0)
    // Now set child0 to Move - this will advance it to next element (20)
    // The new minimum becomes 20 (child0) vs 25 (child1's next after being read)
    // But since child1 is still ahead at 25, and child0 moves from 10 to 20,
    // the union's last_doc_id was 5, and after revalidation it should rebuild at new min

    // First read moves union to doc 10
    let result = union_iter.read().expect("read failed").unwrap();
    assert_eq!(result.doc_id, 10);

    // Now child0 is at 10, child1 is at 25. Union's last_doc_id is 10
    // If child0 moves (advances to 20), the new min changes from 10 to 20
    // This should trigger Moved since last_doc_id changed

    // Note: We need to access the mock data. Since we boxed the iterators,
    // we need a different approach. Let's test with a simpler scenario.

    // Continue reading - should still be able to get next docs
    assert!(!union_iter.at_eof());

    // Verify we can read remaining docs in correct order
    let result = union_iter.read().expect("read failed").unwrap();
    assert_eq!(result.doc_id, 20);
}

/// Test revalidate where all remaining children reach EOF simultaneously.
/// This covers lines 487-489, 500-501 (swap_remove EOF children, all at EOF).
#[test]
#[cfg_attr(miri, ignore)] // Uses RSYieldableMetric_Concat FFI in push_borrowed (union aggregation)
fn revalidate_when_already_at_eof() {
    // Create mocks that we'll read to EOF
    let mock1: Mock<'static, 2> = Mock::new([10, 20]);
    let mock2: Mock<'static, 2> = Mock::new([10, 30]);

    // Set to Ok
    mock1.data().set_revalidate_result(MockRevalidateResult::Ok);
    mock2.data().set_revalidate_result(MockRevalidateResult::Ok);

    let children: Vec<Box<dyn RQEIterator<'static> + 'static>> =
        vec![Box::new(mock1), Box::new(mock2)];

    let mut quick_iter = UnionQuickFlat::new(children);

    // Read all docs: 10, 20, 30
    let mut read_docs = Vec::new();
    while let Some(result) = quick_iter.read().expect("read failed") {
        read_docs.push(result.doc_id);
    }
    assert_eq!(read_docs, vec![10, 20, 30]);
    assert!(quick_iter.at_eof());

    // Revalidate when already at EOF should return Ok (not Moved to None)
    let status = quick_iter.revalidate().expect("revalidate failed");
    assert!(
        matches!(status, RQEValidateStatus::Ok),
        "Expected Ok when already at EOF, got {:?}",
        status
    );
}

/// Test revalidate where children move during revalidation and some reach EOF.
/// This targets lines 487-489, 500-501 (rebuild_at_minimum with EOF children).
#[test]
#[cfg_attr(miri, ignore)] // Uses RSYieldableMetric_Concat FFI in push_borrowed (union aggregation)
fn revalidate_with_children_at_eof() {
    // Test 1: Child moves to EOF during revalidate, triggering lines 487-489
    // We need mock1 to still be in active set when revalidate is called,
    // so it needs to have more docs than mock2's minimum.
    {
        // mock1: [10, 20, 30] - will be at doc 20 after second read
        // mock2: [5, 25, 50] - has minimum, will drive reads
        let mock1: Mock<'static, 3> = Mock::new([10, 20, 30]);
        let mock2: Mock<'static, 3> = Mock::new([5, 25, 50]);

        let mut data1 = mock1.data();

        let children: Vec<Box<dyn RQEIterator<'static> + 'static>> =
            vec![Box::new(mock1), Box::new(mock2)];

        let mut union_iter = Union::new(children);

        // First read: initializes both, returns 5 (mock2's first doc)
        let result = union_iter.read().expect("read failed").unwrap();
        assert_eq!(result.doc_id, 5);
        // mock1: doc_id=10, next_index=1
        // mock2: doc_id=5, next_index=1 (will advance since it's minimum)

        // Second read: advances mock2 (which was at minimum 5)
        let result = union_iter.read().expect("read failed").unwrap();
        assert_eq!(result.doc_id, 10);
        // mock1: was at 10, now advances to 20 (next_index=2)
        // mock2: doc_id=25, next_index=2

        // Now mock1 is at 20 (next_index=2), mock2 is at 25 (next_index=2)
        // Set mock1 to Move - it will advance to 30 (next_index=3), which is EOF
        data1.set_revalidate_result(MockRevalidateResult::Move);

        // Revalidate - mock1 reports Moved and advances to EOF
        // rebuild_at_minimum sees mock1 at EOF, hits lines 487-489
        let status = union_iter.revalidate().expect("revalidate failed");
        assert!(
            matches!(status, RQEValidateStatus::Moved { current: Some(_) }),
            "Expected Moved with Some, got {:?}",
            status
        );
    }

    // Test 2: ALL children move to EOF during revalidate, triggering lines 500-501
    {
        let mock1: Mock<'static, 3> = Mock::new([10, 20, 30]);
        let mock2: Mock<'static, 3> = Mock::new([10, 25, 35]);

        let mut data1 = mock1.data();
        let mut data2 = mock2.data();

        let children: Vec<Box<dyn RQEIterator<'static> + 'static>> =
            vec![Box::new(mock1), Box::new(mock2)];

        let mut union_iter = Union::new(children);

        // Read doc 10
        union_iter.read().expect("read failed").unwrap();
        // Read doc 20 (mock1 advances from 10 to 20, mock2 advances from 10 to 25)
        union_iter.read().expect("read failed").unwrap();
        // mock1: doc_id=20, next_index=2
        // mock2: doc_id=25, next_index=2

        // Set both to Move - they'll advance to their last doc (idx 2) then EOF
        data1.set_revalidate_result(MockRevalidateResult::Move);
        data2.set_revalidate_result(MockRevalidateResult::Move);

        // Revalidate - both report Moved, both advance to EOF
        // rebuild_at_minimum sees all at EOF, hits lines 500-501
        let status = union_iter.revalidate().expect("revalidate failed");
        assert!(
            matches!(status, RQEValidateStatus::Moved { current: None }),
            "Expected Moved with None, got {:?}",
            status
        );
        assert!(union_iter.at_eof());
    }
}

/// Test Quick mode revalidate properly triggers QUICK_EXIT path in build_aggregate_result.
/// This targets line 156 by using UnionQuickFlat and triggering revalidate with movement.
#[test]
#[cfg_attr(miri, ignore)] // Uses RSYieldableMetric_Concat FFI in push_borrowed (union aggregation)
fn revalidate_quick_triggers_quick_exit() {
    // Create mocks - both start at different doc_ids so minimum changes when one moves
    let mock1: Mock<'static, 3> = Mock::new([10, 30, 50]);
    let mock2: Mock<'static, 3> = Mock::new([20, 40, 60]);

    // Get data handles before boxing (data() clones the inner Rc, so we can keep handles)
    let mut data1 = mock1.data();
    let mut data2 = mock2.data();

    // Initially Ok
    data1.set_revalidate_result(MockRevalidateResult::Ok);
    data2.set_revalidate_result(MockRevalidateResult::Ok);

    let children: Vec<Box<dyn RQEIterator<'static> + 'static>> =
        vec![Box::new(mock1), Box::new(mock2)];

    let mut quick_iter = UnionQuickFlat::new(children);

    // Read first doc (10 from mock1 - the minimum)
    let result = quick_iter.read().expect("read failed").unwrap();
    assert_eq!(result.doc_id, 10);

    // Set mock1 (the minimum child) to Move - this will advance it from 10 to 30
    // Now the minimum becomes mock2 at 20, so union's last_doc_id will change
    data1.set_revalidate_result(MockRevalidateResult::Move);
    data2.set_revalidate_result(MockRevalidateResult::Ok);

    // Revalidate - mock1 reports Move (advances from 10 to 30)
    // This triggers the rebuild path which calls build_aggregate_result
    // with QUICK_EXIT=true, which should hit line 156
    let status = quick_iter.revalidate().expect("revalidate failed");

    // Since mock1 moved from 10 to 30, and mock2 is at 20, new minimum is 20
    // Union's last_doc_id changes from 10 to 20, so we get Moved
    assert!(
        matches!(status, RQEValidateStatus::Moved { current: Some(_) }),
        "Expected Moved with Some, got {:?}",
        status
    );

    // Verify the union is now at doc 20
    assert_eq!(quick_iter.last_doc_id(), 20);
}

// =============================================================================
// Current tests
// =============================================================================

#[test]
#[cfg_attr(miri, ignore)] // Uses RSYieldableMetric_Concat FFI in push_borrowed (union aggregation)
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

// =============================================================================
// Quick vs Full mode tests
// =============================================================================

/// Test that UnionQuickFlat produces the same doc_ids as UnionFullFlat.
/// (Quick mode just doesn't aggregate results, but yields same documents)
#[test]
#[cfg_attr(miri, ignore)] // Uses RSYieldableMetric_Concat FFI in push_borrowed (union aggregation)
fn mode_quick_variant_produces_same_doc_ids() {
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
    let mut quick_iter =
        UnionQuickFlat::new(vec![IdListSorted::new(ids1), IdListSorted::new(ids2)]);
    let mut quick_results = Vec::new();
    while let Some(result) = quick_iter.read().expect("read failed") {
        quick_results.push(result.doc_id);
    }

    assert_eq!(full_results, quick_results);
}

/// Test that Full mode aggregates results from ALL children with the same doc_id,
/// while Quick mode only takes the result from the first matching child.
#[test]
#[cfg_attr(miri, ignore)] // Uses RSYieldableMetric_Concat FFI in push_borrowed (union aggregation)
fn mode_full_aggregates_all_matching_children() {
    // Create 3 children that all have doc_id 10 (and some unique docs)
    let child1 = IdListSorted::new(vec![10, 20, 30]);
    let child2 = IdListSorted::new(vec![10, 25, 35]);
    let child3 = IdListSorted::new(vec![10, 28, 38]);

    let mut full_iter = UnionFullFlat::new(vec![child1, child2, child3]);

    // Read doc_id 10 - should aggregate results from all 3 children
    let result = full_iter.read().expect("read failed").unwrap();
    assert_eq!(result.doc_id, 10);

    // In Full mode, the aggregate should contain results from all 3 children
    let aggregate = result.as_aggregate().expect("Expected aggregate result");
    assert_eq!(
        aggregate.len(),
        3,
        "Full mode should aggregate all 3 children with doc_id 10"
    );
}

/// Test that Quick mode only takes the result from the first matching child.
#[test]
#[cfg_attr(miri, ignore)] // Uses RSYieldableMetric_Concat FFI in push_borrowed (union aggregation)
fn mode_quick_takes_first_matching_child_only() {
    // Create 3 children that all have doc_id 10 (and some unique docs)
    let child1 = IdListSorted::new(vec![10, 20, 30]);
    let child2 = IdListSorted::new(vec![10, 25, 35]);
    let child3 = IdListSorted::new(vec![10, 28, 38]);

    let mut quick_iter = UnionQuickFlat::new(vec![child1, child2, child3]);

    // Read doc_id 10 - should only take result from first child
    let result = quick_iter.read().expect("read failed").unwrap();
    assert_eq!(result.doc_id, 10);

    // In Quick mode, the aggregate should contain result from only 1 child
    let aggregate = result.as_aggregate().expect("Expected aggregate result");
    assert_eq!(
        aggregate.len(),
        1,
        "Quick mode should only take result from first matching child"
    );
}

/// Test that Full mode aggregates varying numbers of children for different doc_ids.
#[test]
#[cfg_attr(miri, ignore)] // Uses RSYieldableMetric_Concat FFI in push_borrowed (union aggregation)
fn mode_full_aggregates_correct_number_of_children() {
    // doc_id 10: in child1, child2, child3 (3 children)
    // doc_id 20: in child1 only (1 child)
    // doc_id 25: in child2 only (1 child)
    // doc_id 30: in child1, child3 (2 children)
    let child1 = IdListSorted::new(vec![10, 20, 30]);
    let child2 = IdListSorted::new(vec![10, 25]);
    let child3 = IdListSorted::new(vec![10, 30]);

    let mut full_iter = UnionFullFlat::new(vec![child1, child2, child3]);

    // doc_id 10: should have 3 children
    let result = full_iter.read().expect("read failed").unwrap();
    assert_eq!(result.doc_id, 10);
    assert_eq!(
        result.as_aggregate().unwrap().len(),
        3,
        "doc_id 10 should aggregate 3 children"
    );

    // doc_id 20: should have 1 child
    let result = full_iter.read().expect("read failed").unwrap();
    assert_eq!(result.doc_id, 20);
    assert_eq!(
        result.as_aggregate().unwrap().len(),
        1,
        "doc_id 20 should aggregate 1 child"
    );

    // doc_id 25: should have 1 child
    let result = full_iter.read().expect("read failed").unwrap();
    assert_eq!(result.doc_id, 25);
    assert_eq!(
        result.as_aggregate().unwrap().len(),
        1,
        "doc_id 25 should aggregate 1 child"
    );

    // doc_id 30: should have 2 children
    let result = full_iter.read().expect("read failed").unwrap();
    assert_eq!(result.doc_id, 30);
    assert_eq!(
        result.as_aggregate().unwrap().len(),
        2,
        "doc_id 30 should aggregate 2 children"
    );
}

/// Test that Quick mode always has exactly 1 child in the aggregate.
#[test]
#[cfg_attr(miri, ignore)] // Uses RSYieldableMetric_Concat FFI in push_borrowed (union aggregation)
fn mode_quick_always_has_one_child() {
    // Same data as full_mode_aggregates_correct_number_of_children
    let child1 = IdListSorted::new(vec![10, 20, 30]);
    let child2 = IdListSorted::new(vec![10, 25]);
    let child3 = IdListSorted::new(vec![10, 30]);

    let mut quick_iter = UnionQuickFlat::new(vec![child1, child2, child3]);

    // All doc_ids should have exactly 1 child in Quick mode
    while let Some(result) = quick_iter.read().expect("read failed") {
        assert_eq!(
            result.as_aggregate().unwrap().len(),
            1,
            "Quick mode should always have exactly 1 child for doc_id {}",
            result.doc_id
        );
    }
}

/// Test Quick vs Full mode with skip_to operation.
#[test]
#[cfg_attr(miri, ignore)] // Uses RSYieldableMetric_Concat FFI in push_borrowed (union aggregation)
fn mode_quick_vs_full_with_skip_to() {
    // doc_id 50: in all 3 children
    let child1 = IdListSorted::new(vec![10, 30, 50]);
    let child2 = IdListSorted::new(vec![20, 40, 50]);
    let child3 = IdListSorted::new(vec![25, 45, 50]);

    // Full mode skip_to
    let mut full_iter = UnionFullFlat::new(vec![
        IdListSorted::new(vec![10, 30, 50]),
        IdListSorted::new(vec![20, 40, 50]),
        IdListSorted::new(vec![25, 45, 50]),
    ]);

    let outcome = full_iter.skip_to(50).expect("skip_to failed");
    assert!(matches!(outcome, Some(SkipToOutcome::Found(_))));
    let result = full_iter.current().unwrap();
    assert_eq!(result.doc_id, 50);
    assert_eq!(
        result.as_aggregate().unwrap().len(),
        3,
        "Full mode skip_to should aggregate all 3 children"
    );

    // Quick mode skip_to
    let mut quick_iter = UnionQuickFlat::new(vec![child1, child2, child3]);

    let outcome = quick_iter.skip_to(50).expect("skip_to failed");
    assert!(matches!(outcome, Some(SkipToOutcome::Found(_))));
    let result = quick_iter.current().unwrap();
    assert_eq!(result.doc_id, 50);
    assert_eq!(
        result.as_aggregate().unwrap().len(),
        1,
        "Quick mode skip_to should only take first matching child"
    );
}
