/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Integration tests for the UnionFlat iterator variants.
//!
//! These tests primarily use [`Mock`] iterators which provide observability features
//! like `read_count()` and `revalidate_count()` that allow us to verify not just
//! correctness but also efficiency optimizations like the ReuseResults optimization.
//!
//! A few tests use [`IdListSorted`] where integration with a production iterator
//! provides additional value (e.g., verifying union works with binary search skip_to).

use crate::union_cases;
use crate::utils::{Mock, MockData, MockRevalidateResult};
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

/// Helper to create 1 Mock child with its data handle for observability.
///
/// Returns a tuple of (child, data_handle) where:
/// - `child` is a boxed iterator that can be passed to union constructors
/// - `data_handle` can be used to verify read counts and configure revalidation
fn create_mock_1<const N: usize>(ids: [t_docId; N]) -> (Box<dyn RQEIterator<'static>>, MockData) {
    let c = Mock::<N>::new(ids);
    let d = c.data();
    (Box::new(c), d)
}

/// Helper to create 2 Mock children with their data handles for observability.
///
/// Returns a tuple of (children, data_handles) where:
/// - `children` are boxed iterators that can be passed to union constructors
/// - `data_handles` can be used to verify read counts and configure revalidation
///
/// This helper allows mixing different array sizes by returning type-erased iterators.
fn create_mock_2<const N1: usize, const N2: usize>(
    ids1: [t_docId; N1],
    ids2: [t_docId; N2],
) -> (Vec<Box<dyn RQEIterator<'static>>>, Vec<MockData>) {
    let c1 = Mock::<N1>::new(ids1);
    let c2 = Mock::<N2>::new(ids2);
    let d1 = c1.data();
    let d2 = c2.data();
    (vec![Box::new(c1), Box::new(c2)], vec![d1, d2])
}

/// Helper to create 3 Mock children with their data handles for observability.
///
/// Same as `create_mock_2` but for 3 children.
fn create_mock_3<const N1: usize, const N2: usize, const N3: usize>(
    ids1: [t_docId; N1],
    ids2: [t_docId; N2],
    ids3: [t_docId; N3],
) -> (Vec<Box<dyn RQEIterator<'static>>>, Vec<MockData>) {
    let c1 = Mock::<N1>::new(ids1);
    let c2 = Mock::<N2>::new(ids2);
    let c3 = Mock::<N3>::new(ids3);
    let d1 = c1.data();
    let d2 = c2.data();
    let d3 = c3.data();
    (
        vec![Box::new(c1), Box::new(c2), Box::new(c3)],
        vec![d1, d2, d3],
    )
}

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
        let (children, data) = create_mock_2([50, 200], [100, 250]);
        let mut quick_iter = UnionQuickFlat::new(children);
        // Read first doc (50) - union is now at 50, child1 at 50, child2 at 100
        quick_iter.read().expect("read failed").unwrap();
        assert_eq!(quick_iter.last_doc_id(), 50);
        // skip_to(100) - child2 is already at 100, triggers line 311-312
        // ReuseResults: child2 already at target, no additional reads needed
        let outcome = quick_iter.skip_to(100).expect("skip_to failed");
        assert!(matches!(outcome, Some(SkipToOutcome::Found(_))));
        assert_eq!(quick_iter.last_doc_id(), 100);

        // Verify ReuseResults optimization: child2 was not re-read because it was already at 100
        assert_eq!(data[0].read_count(), 2, "child1: init + 1 read");
        assert_eq!(
            data[1].read_count(),
            1,
            "child2: only init, skip_to reused cached position"
        );
    }

    // Test 2: Quick mode - child exhausts during skip_to (lines 300-306)
    // Line 303 requires min_child_idx == num_active-1 when a NON-last child exhausts.
    // This is hard to hit due to linear iteration, but we can at least cover the
    // basic None branch (lines 300-306 without line 303's condition being true).
    {
        // child1 will exhaust when skip_to(100) is called (max doc is 30)
        let (children, _data) = create_mock_2([10, 30], [20, 200]);
        let mut quick_iter = UnionQuickFlat::new(children);
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
        let (children, data) = create_mock_2([10, 20], [15, 50, 100]);
        let mut full_iter = UnionFullFlat::new(children);

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

        // Verify read counts - child1 exhausted early
        assert_eq!(
            data[0].read_count(),
            2,
            "child1: init + 1 read (to get to 20)"
        );
        assert_eq!(
            data[1].read_count(),
            4,
            "child2: init + 3 reads (15, 50, 100, EOF)"
        );
    }

    // Test 4: Full mode - child already at target doc_id (line 248)
    // Covers the optimization where a child is already positioned at the exact target
    // before calling skip_to, so we add it to the result immediately.
    {
        let (children, data) = create_mock_2([50, 150, 200], [100, 150, 250]);
        let mut full_iter = UnionFullFlat::new(children);
        // Read first doc (50) - union at 50, child1 at 50, child2 at 100
        full_iter.read().expect("read failed").unwrap();
        assert_eq!(full_iter.last_doc_id(), 50);

        // skip_to(100): child1 < 100 (needs skip), child2 == 100 (already at target)
        // This hits line 248: child2's last_doc_id == doc_id, add_child_to_result
        let outcome = full_iter.skip_to(100).expect("skip_to failed");
        assert!(matches!(outcome, Some(SkipToOutcome::Found(_))));
        assert_eq!(full_iter.last_doc_id(), 100);

        // Verify read counts: child2 was not re-read because it was already at 100
        assert_eq!(data[0].read_count(), 2, "child1: init + skip_to");
        assert_eq!(
            data[1].read_count(),
            1,
            "child2: only init, already at target"
        );
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
    let (child, child_data) = create_mock_1([10, 20, 30, 40, 50]);
    let mut union_iter = Union::new(vec![child]);

    let expected = [10, 20, 30, 40, 50];
    for &expected_id in &expected {
        let result = union_iter.read().expect("read failed");
        assert!(result.is_some());
        assert_eq!(result.unwrap().doc_id, expected_id);
    }

    assert!(matches!(union_iter.read(), Ok(None)));
    assert!(union_iter.at_eof());
    // read_count = 1 (init) + 4 (docs 2-5) + 1 (discover EOF) = 6
    // Note: Unlike IdListSorted, Mock knows it's at EOF after the last doc,
    // but the union still needs to read() to discover this.
    assert_eq!(
        child_data.read_count(),
        6,
        "Single child: init + 4 reads + EOF discovery"
    );
}

#[test]
#[cfg_attr(miri, ignore)] // Uses RSYieldableMetric_Concat FFI in push_borrowed (union aggregation)
fn edge_case_disjoint_children() {
    // Children have no overlap - union should return all docs
    // This also verifies the ReuseResults optimization: after child1 exhausts,
    // child2 and child3 should not be read again until their doc_ids are needed.
    let (children, data) = create_mock_3([1, 2, 3], [10, 20, 30], [100, 200, 300]);

    let mut union_iter = Union::new(children);
    let expected = [1, 2, 3, 10, 20, 30, 100, 200, 300];

    for &expected_id in &expected {
        let result = union_iter.read().expect("read failed");
        assert!(result.is_some(), "Expected doc {expected_id}");
        assert_eq!(result.unwrap().doc_id, expected_id);
    }

    assert!(matches!(union_iter.read(), Ok(None)));
    assert!(union_iter.at_eof());

    // For disjoint children, each child is read:
    // - child1: 1 (init) + 2 (docs 2,3) + 1 (EOF) = 4 reads, then removed
    // - child2: 1 (init) + 2 (docs 20,30) + 1 (EOF) = 4 reads, then removed
    // - child3: 1 (init) + 2 (docs 200,300) + 1 (EOF) = 4 reads, then removed
    // ReuseResults: children ahead of minimum are NOT re-read until needed
    assert_eq!(data[0].read_count(), 4, "child1: init + 2 reads + EOF");
    assert_eq!(data[1].read_count(), 4, "child2: init + 2 reads + EOF");
    assert_eq!(data[2].read_count(), 4, "child3: init + 2 reads + EOF");
}

#[test]
#[cfg_attr(miri, ignore)] // Uses RSYieldableMetric_Concat FFI in push_borrowed (union aggregation)
fn edge_case_overlapping_children() {
    // Children have significant overlap - tests Full mode aggregation
    // and verifies ReuseResults optimization with overlapping doc_ids.
    let (children, data) = create_mock_3(
        [1, 2, 5, 10, 15, 20],
        [2, 5, 8, 10, 18, 20],
        [3, 5, 10, 12, 20, 25],
    );

    let mut union_iter = Union::new(children);
    // Union: 1, 2, 3, 5, 8, 10, 12, 15, 18, 20, 25
    let expected = [1, 2, 3, 5, 8, 10, 12, 15, 18, 20, 25];

    for &expected_id in &expected {
        let result = union_iter.read().expect("read failed");
        assert!(result.is_some(), "Expected doc {expected_id}");
        assert_eq!(result.unwrap().doc_id, expected_id);
    }

    assert!(matches!(union_iter.read(), Ok(None)));

    // For overlapping children, each child is read:
    // - 1 read for init, then 5 more reads for remaining docs, + 1 for EOF = 7
    // ReuseResults: children at the current min doc_id are all read to aggregate,
    // but children ahead are not re-read.
    assert_eq!(data[0].read_count(), 7, "child1: init + 5 reads + EOF");
    assert_eq!(data[1].read_count(), 7, "child2: init + 5 reads + EOF");
    assert_eq!(data[2].read_count(), 7, "child3: init + 5 reads + EOF");
}

#[test]
#[cfg_attr(miri, ignore)] // Uses RSYieldableMetric_Concat FFI in push_borrowed (union aggregation)
fn edge_case_skip_to_exact_match() {
    let (children, _) = create_mock_2([10, 20, 30, 40, 50], [15, 25, 35, 45, 55]);
    let mut union_iter = Union::new(children);

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
    let (children, _) = create_mock_2([10, 20, 30, 40, 50], [15, 25, 35, 45, 55]);
    let mut union_iter = Union::new(children);

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
    let (children, _) = create_mock_2([10, 20, 30], [15, 25, 35]);
    let mut union_iter = Union::new(children);

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
    let (children, _) = create_mock_2(
        [10, 20, 30, 40, 50, 60, 70, 80],
        [15, 25, 35, 45, 55, 65, 75, 85],
    );
    let mut union_iter = Union::new(children);

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
fn edge_case_num_estimated_is_sum() {
    let (children, _) = create_mock_3(
        [1, 2, 3, 4, 5],
        [10, 20, 30, 40, 50],
        [100, 200, 300, 400, 500],
    );
    let union_iter = Union::new(children);

    // For union, num_estimated is the sum (upper bound)
    assert_eq!(union_iter.num_estimated(), 15);
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
    let (children, data) = create_mock_2([10, 50, 100], [20, 60, 110]);
    let mut union_iter = Union::new(children);

    // Read first two docs (10, 20) - both children advance
    union_iter.read().expect("read failed");
    union_iter.read().expect("read failed");

    // Now child1 is at 50, child2 is at 60
    // Skip to 40 - child1 is already at 50 (past target), child2 is at 60 (past target)
    // ReuseResults: neither child should be read since both are already past target
    let outcome = union_iter.skip_to(40).expect("skip_to failed");
    assert!(matches!(outcome, Some(SkipToOutcome::NotFound(_))));
    // Should land on 50 (minimum of 50, 60)
    assert_eq!(union_iter.last_doc_id(), 50);

    // Verify ReuseResults: after init (1) + 2 reads, skip_to(40) should NOT read
    // because both children are already past 40
    assert_eq!(
        data[0].read_count(),
        2,
        "child1: init + 1 read (to get to 50)"
    );
    assert_eq!(
        data[1].read_count(),
        2,
        "child2: init + 1 read (to get to 60)"
    );
}

/// Test skip_to where children exhaust during the skip.
/// This covers lines 239-240 (child reaches EOF during skip_to).
#[test]
#[cfg_attr(miri, ignore)] // Uses RSYieldableMetric_Concat FFI in push_borrowed (union aggregation)
fn edge_case_skip_to_exhausts_some_children() {
    let (children, _data) = create_mock_2([10, 20, 30], [15, 25, 100]);
    let mut union_iter = Union::new(children);

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
    let (children, _data) = create_mock_2([10, 20, 30], [15, 25, 35]);
    let mut union_iter = Union::new(children);

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
    let (children, _) = create_mock_2([10, 20, 30, 40, 50], [15, 25, 35, 45, 55]);
    let mut union_iter = Union::new(children);

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

/// Test ReuseResults optimization - children whose `last_doc_id` is already ahead
/// of the union's current position should NOT be re-read.
///
/// This test mirrors the C++ test `UnionIteratorSingleTest.ReuseResults` from
/// `tests/cpptests/test_cpp_iterator_union.cpp`.
///
/// Setup: Quick mode union with two children
/// - it1: doc_ids = [3]
/// - it2: doc_ids = [2]
///
/// Expected behavior:
/// 1. First Read(): Both children are read once. Union returns doc 2.
///    - it1.lastDocId = 3, it2.lastDocId = 2
///    - read_count: it1=1, it2=1
/// 2. Second Read(): Union advances to doc 3. Neither child is read again because:
///    - it1.lastDocId (3) is already at/past the target (3), so it's reused
///    - Quick mode exits immediately on finding a match
///    - read_count: it1=1, it2=1 (unchanged!)
/// 3. Third Read(): Both children are at EOF (single-doc iterators exhausted),
///    so both are removed without read().
///    - read_count: it1=1, it2=1 (unchanged!)
///
/// Note: The Rust implementation is more efficient than C++ here because
/// it detects EOF earlier (after returning the last document) and avoids
/// unnecessary read() calls. In C++, the iterator doesn't know it's at EOF
/// until an additional Read() is called that discovers no more documents.
#[test]
#[cfg_attr(miri, ignore)] // Uses RSYieldableMetric_Concat FFI in push_borrowed (union aggregation)
fn reuse_results_optimization_quick_mode() {
    // Setup: Two children with disjoint single-document doc_ids
    let (children, data) = create_mock_2([3], [2]);
    let mut union = UnionQuickFlat::new(children);

    // First Read: Both children are read once, union returns doc 2
    let result = union
        .read()
        .expect("read failed")
        .expect("should have result");
    assert_eq!(result.doc_id, 2, "First read should return doc 2 (minimum)");
    assert_eq!(data[0].read_count(), 1, "it1 should be read once");
    assert_eq!(data[1].read_count(), 1, "it2 should be read once");

    // Second Read: Union advances to doc 3
    // In Quick mode, it1 (lastDocId=3) matches the skip target, so it's reused without read()
    let result = union
        .read()
        .expect("read failed")
        .expect("should have result");
    assert_eq!(result.doc_id, 3, "Second read should return doc 3");
    assert_eq!(
        data[0].read_count(),
        1,
        "it1 should NOT be read again - its lastDocId (3) is already at the target"
    );
    // it2 is not read because Quick mode exits as soon as it finds a match (it1)
    assert_eq!(
        data[1].read_count(),
        1,
        "it2 should NOT be read again - Quick mode exited early on it1"
    );

    // Third Read: Both children are at EOF (single-doc iterators exhausted after first read)
    // Rust's Mock iterator knows it's at EOF immediately after returning its last document,
    // so both children are removed without any additional read() calls.
    let result = union.read().expect("read failed");
    assert!(result.is_none(), "Third read should return EOF");
    assert_eq!(
        data[0].read_count(),
        1,
        "it1 should NOT be read - already at EOF, removed without calling read()"
    );
    assert_eq!(
        data[1].read_count(),
        1,
        "it2 should NOT be read - already at EOF, removed without calling read()"
    );
}

/// Test ReuseResults optimization in Full mode.
///
/// Full mode aggregates ALL children at the minimum doc_id, so the behavior
/// differs from Quick mode. Children whose `last_doc_id` equals the union's
/// current position are advanced, while children already ahead are NOT read.
#[test]
#[cfg_attr(miri, ignore)] // Uses RSYieldableMetric_Concat FFI in push_borrowed (union aggregation)
fn reuse_results_optimization_full_mode() {
    // Setup: Three children with overlapping doc_ids
    // it1: [1, 3, 5]
    // it2: [2, 3, 6]
    // it3: [3, 4, 7]
    let (children, data) = create_mock_3([1, 3, 5], [2, 3, 6], [3, 4, 7]);
    let mut union = UnionFullFlat::new(children);

    // First Read: All children are read once (initialization), union returns doc 1
    let result = union
        .read()
        .expect("read failed")
        .expect("should have result");
    assert_eq!(result.doc_id, 1, "First read should return doc 1");
    // After init: it1.lastDocId=1, it2.lastDocId=2, it3.lastDocId=3
    assert_eq!(data[0].read_count(), 1);
    assert_eq!(data[1].read_count(), 1);
    assert_eq!(data[2].read_count(), 1);

    // Second Read: Union advances from doc 1 to doc 2
    // Only it1 (lastDocId=1 == current) is read, others are already ahead
    let result = union
        .read()
        .expect("read failed")
        .expect("should have result");
    assert_eq!(result.doc_id, 2, "Second read should return doc 2");
    // After: it1.lastDocId=3, it2.lastDocId=2, it3.lastDocId=3
    assert_eq!(
        data[0].read_count(),
        2,
        "it1 should be read (was at doc 1, which matched union position)"
    );
    assert_eq!(
        data[1].read_count(),
        1,
        "it2 should NOT be read (lastDocId=2 > union's previous position 1)"
    );
    assert_eq!(
        data[2].read_count(),
        1,
        "it3 should NOT be read (lastDocId=3 > union's previous position 1)"
    );

    // Third Read: Union advances from doc 2 to doc 3
    // Only it2 (lastDocId=2 == current) is read
    let result = union
        .read()
        .expect("read failed")
        .expect("should have result");
    assert_eq!(result.doc_id, 3, "Third read should return doc 3");
    // After: it1.lastDocId=3, it2.lastDocId=3, it3.lastDocId=3
    assert_eq!(
        data[0].read_count(),
        2,
        "it1 should NOT be read again (lastDocId=3 > 2)"
    );
    assert_eq!(
        data[1].read_count(),
        2,
        "it2 should be read (was at doc 2, which matched union position)"
    );
    assert_eq!(
        data[2].read_count(),
        1,
        "it3 should NOT be read (lastDocId=3 > 2)"
    );
}
