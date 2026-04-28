/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Integration tests for the Intersection iterator.

use ffi::t_docId;
use rqe_iterators::{
    IteratorType, RQEIterator, RQEValidateStatus, SkipToOutcome, id_list::IdListSorted,
    intersection::Intersection, profile::Profile,
};

use crate::utils::{Mock, MockRevalidateResult};

/// Helper function to create child iterators for intersection tests.
///
/// Given a result set (document IDs that should appear in ALL children),
/// creates `num_children` child iterators where each child contains:
/// - All document IDs from the result set
/// - Some unique document IDs specific to that child
///
/// This ensures the intersection of all children equals exactly the result set.
fn create_children(num_children: usize, result_set: &[t_docId]) -> Vec<IdListSorted<'static>> {
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

        // Sort and deduplicate (matching C++ Mock behavior)
        child_ids.sort();
        child_ids.dedup();

        children.push(IdListSorted::new(child_ids));
    }

    children
}

#[test]
fn type_() {
    let children = vec![
        IdListSorted::new(vec![1, 2, 3]),
        IdListSorted::new(vec![2, 3, 4]),
    ];
    let it = Intersection::new(children, 1.0, false);
    assert_eq!(it.type_(), IteratorType::Intersect);
}

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

#[test]
#[cfg_attr(miri, ignore = "Too slow under miri")]
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

    let mut ii = Intersection::new(children, 1.0, false);

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

#[test]
#[cfg_attr(miri, ignore = "Takes too long with Miri")]
fn skip_to_all_combinations() {
    for &num_children in NUM_CHILDREN_CASES {
        for &result_set in RESULT_SET_CASES {
            skip_to_test_case(num_children, result_set);
        }
    }
}

fn skip_to_test_case(num_children: usize, result_set: &[t_docId]) {
    let children = create_children(num_children, result_set);
    let mut ii = Intersection::new(children, 1.0, false);

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

#[test]
#[cfg_attr(miri, ignore = "Too slow under miri")]
fn rewind_all_combinations() {
    for &num_children in NUM_CHILDREN_CASES {
        for &result_set in RESULT_SET_CASES {
            rewind_test_case(num_children, result_set);
        }
    }
}

fn rewind_test_case(num_children: usize, result_set: &[t_docId]) {
    let children = create_children(num_children, result_set);
    let mut ii = Intersection::new(children, 1.0, false);

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
// Edge case tests
// =============================================================================

#[test]
fn empty_result_set() {
    // When the intersection has no common documents, should immediately EOF
    let child1 = IdListSorted::new(vec![1, 2, 3]);
    let child2 = IdListSorted::new(vec![4, 5, 6]);

    let mut ii = Intersection::new(vec![child1, child2], 1.0, false);

    // Should immediately return EOF since there's no intersection
    assert!(matches!(ii.read(), Ok(None)));
    assert!(ii.at_eof());
}

#[test]
fn single_element_result_set() {
    let child1 = IdListSorted::new(vec![1, 5, 10]);
    let child2 = IdListSorted::new(vec![5, 15, 20]);
    let child3 = IdListSorted::new(vec![3, 5, 25]);

    let mut ii = Intersection::new(vec![child1, child2, child3], 1.0, false);

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
    let child1 = IdListSorted::new(vec![10, 20, 30, 40, 50]);
    let child2 = IdListSorted::new(vec![10, 20, 30, 40, 50]);

    let mut ii = Intersection::new(vec![child1, child2], 1.0, false);

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
    let child1 = IdListSorted::new(vec![10, 20, 30, 40, 50]);
    let child2 = IdListSorted::new(vec![10, 20, 30, 40, 50]);

    let mut ii = Intersection::new(vec![child1, child2], 1.0, false);

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

/// Test intersection with no children - should behave as empty
#[test]
fn no_children() {
    let children: Vec<IdListSorted<'static>> = vec![];
    let mut ii = Intersection::new(children, 1.0, false);

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
#[test]
fn single_child() {
    let doc_ids = vec![10, 20, 30, 40, 50];
    let child = IdListSorted::new(doc_ids.clone());
    let mut ii = Intersection::new(vec![child], 1.0, false);

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
    let child1 = IdListSorted::new(vec![10, 20, 30]);
    let child2 = IdListSorted::new(vec![10, 20, 30]);

    let mut ii = Intersection::new(vec![child1, child2], 1.0, false);

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
    let child1 = IdListSorted::new(doc_ids.clone());
    let child2 = IdListSorted::new(doc_ids.clone());

    let mut ii = Intersection::new(vec![child1, child2], 1.0, false);

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
    let child1 = IdListSorted::new(doc_ids.clone());
    let child2 = IdListSorted::new(doc_ids.clone());

    let mut ii = Intersection::new(vec![child1, child2], 1.0, false);

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

    let children: Vec<IdListSorted<'static>> = (0..num_children)
        .map(|i| {
            // Each child has the common docs + some unique ones
            let mut ids = doc_ids.clone();
            ids.push(i as t_docId + 1); // Unique to this child
            ids.sort();
            IdListSorted::new(ids)
        })
        .collect();

    let mut ii = Intersection::new(children, 1.0, false);

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

/// Test: All children return VALIDATE_OK
#[test]
fn revalidate_ok() {
    let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
    let ctx = mock_ctx.spec();
    // Create mock children with const generic arrays
    let child0: Mock<'static, 10> = Mock::new([10, 15, 20, 25, 30, 35, 40, 45, 50, 55]);
    let child1: Mock<'static, 11> = Mock::new([5, 10, 18, 20, 28, 30, 38, 40, 48, 50, 60]);
    let child2: Mock<'static, 11> = Mock::new([2, 10, 12, 20, 22, 30, 32, 40, 42, 50, 70]);

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

    let mut ii = Intersection::new(children, 1.0, false);

    // Read a few documents first
    let result = ii.read().expect("read failed").unwrap();
    assert_eq!(result.doc_id, 10);

    let result = ii.read().expect("read failed").unwrap();
    assert_eq!(result.doc_id, 20);

    // Revalidate should return Ok
    // SAFETY: test-only call with valid context
    let status = unsafe { ii.revalidate(ctx) }.expect("revalidate failed");
    assert!(matches!(status, RQEValidateStatus::Ok));

    // Should be able to continue reading
    let result = ii.read().expect("read failed").unwrap();
    assert_eq!(result.doc_id, 30);
}

/// Test: One child returns VALIDATE_ABORTED
#[test]
fn revalidate_aborted() {
    let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
    let ctx = mock_ctx.spec();
    let child0: Mock<'static, 10> = Mock::new([10, 15, 20, 25, 30, 35, 40, 45, 50, 55]);
    let child1: Mock<'static, 11> = Mock::new([5, 10, 18, 20, 28, 30, 38, 40, 48, 50, 60]);
    let child2: Mock<'static, 11> = Mock::new([2, 10, 12, 20, 22, 30, 32, 40, 42, 50, 70]);

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

    let mut ii = Intersection::new(children, 1.0, false);

    // Read a document first
    let result = ii.read().expect("read failed").unwrap();
    assert_eq!(result.doc_id, 10);

    // Revalidate should return Aborted since one child aborted
    // SAFETY: test-only call with valid context
    let status = unsafe { ii.revalidate(ctx) }.expect("revalidate failed");
    assert!(matches!(status, RQEValidateStatus::Aborted));
}

/// Test: All children return VALIDATE_MOVED
#[test]
fn revalidate_moved() {
    let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
    let ctx = mock_ctx.spec();
    let child0: Mock<'static, 10> = Mock::new([10, 15, 20, 25, 30, 35, 40, 45, 50, 55]);
    let child1: Mock<'static, 11> = Mock::new([5, 10, 18, 20, 28, 30, 38, 40, 48, 50, 60]);
    let child2: Mock<'static, 11> = Mock::new([2, 10, 12, 20, 22, 30, 32, 40, 42, 50, 70]);

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

    let mut ii = Intersection::new(children, 1.0, false);

    // Read first document
    let result = ii.read().expect("read failed").unwrap();
    assert_eq!(result.doc_id, 10);

    // Revalidate should return Moved
    // SAFETY: test-only call with valid context
    let status = unsafe { ii.revalidate(ctx) }.expect("revalidate failed");
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
#[test]
fn revalidate_mixed_results() {
    let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
    let ctx = mock_ctx.spec();
    let child0: Mock<'static, 10> = Mock::new([10, 15, 20, 25, 30, 35, 40, 45, 50, 55]);
    let child1: Mock<'static, 11> = Mock::new([5, 10, 18, 20, 28, 30, 38, 40, 48, 50, 60]);
    let child2: Mock<'static, 11> = Mock::new([2, 10, 12, 20, 22, 30, 32, 40, 42, 50, 70]);

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

    let mut ii = Intersection::new(children, 1.0, false);

    // Read first document
    let result = ii.read().expect("read failed").unwrap();
    assert_eq!(result.doc_id, 10);

    // Revalidate should return Moved (if any child moved)
    // SAFETY: test-only call with valid context
    let status = unsafe { ii.revalidate(ctx) }.expect("revalidate failed");
    assert!(matches!(status, RQEValidateStatus::Moved { .. }));
    assert_eq!(ii.last_doc_id(), 20);
}

/// Test: Revalidate after EOF - should return OK even if children moved
#[test]
fn revalidate_after_eof() {
    let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
    let ctx = mock_ctx.spec();
    // Pre-set children to return MOVE on revalidate
    let child0: Mock<'static, 10> = Mock::new([10, 15, 20, 25, 30, 35, 40, 45, 50, 55]);
    let child1: Mock<'static, 11> = Mock::new([5, 10, 18, 20, 28, 30, 38, 40, 48, 50, 60]);
    let child2: Mock<'static, 11> = Mock::new([2, 10, 12, 20, 22, 30, 32, 40, 42, 50, 70]);

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

    let mut ii = Intersection::new(children, 1.0, false);

    // Advance to EOF
    while ii.read().expect("read failed").is_some() {}
    assert!(ii.at_eof());

    // Revalidate should return OK when already at EOF
    // SAFETY: test-only call with valid context
    let status = unsafe { ii.revalidate(ctx) }.expect("revalidate failed");
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
///
/// Note: Mock doesn't have a MovedToEof variant. Instead, we simulate
/// this by using a child that has only 2 elements - after reading doc 10, there's only
/// one element left (20), so when Move is called during revalidate, it reaches EOF.
#[test]
fn revalidate_some_children_moved_to_eof() {
    let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
    let ctx = mock_ctx.spec();
    // Child 0 and 2 have normal data, child 1 is small (only 2 elements: [10, 20])
    // When we read doc 10 and then call Move, child 1 moves to 20 and the next Move
    // would go to EOF
    let child0: Mock<'static, 10> = Mock::new([10, 15, 20, 25, 30, 35, 40, 45, 50, 55]);
    // Child 1 has only doc 10 - after reading it, Move will result in EOF
    let child1: Mock<'static, 1> = Mock::new([10]);
    let child2: Mock<'static, 11> = Mock::new([2, 10, 12, 20, 22, 30, 32, 40, 42, 50, 70]);

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

    let mut ii = Intersection::new(children, 1.0, false);

    // Read first document
    let result = ii.read().expect("read failed").unwrap();
    assert_eq!(result.doc_id, 10);

    // Revalidate should return Moved with current=None (EOF)
    // because child 1 moves to EOF (it only had 1 element which was already read)
    // SAFETY: test-only call with valid context
    let status = unsafe { ii.revalidate(ctx) }.expect("revalidate failed");
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
// Additional tests for comprehensive coverage
// =============================================================================

/// Test: current() returns correct state after various operations
#[test]
fn current_after_operations() {
    let child1 = IdListSorted::new(vec![10, 20, 30, 40, 50]);
    let child2 = IdListSorted::new(vec![10, 20, 30, 40, 50]);

    let mut ii = Intersection::new(vec![child1, child2], 1.0, false);

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
    let child1 = IdListSorted::new(sparse_ids.clone());
    let child2 = IdListSorted::new(sparse_ids.clone());

    let mut ii = Intersection::new(vec![child1, child2], 1.0, false);

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
    let child1 = IdListSorted::new(vec![1, 2, 3, 5, 10, 15, 20, 25, 30]);
    let child2 = IdListSorted::new(vec![2, 3, 5, 7, 10, 12, 15, 20, 30, 35]);
    let child3 = IdListSorted::new(vec![3, 5, 8, 10, 15, 18, 20, 30, 40]);

    let mut ii = Intersection::new(vec![child1, child2, child3], 1.0, false);

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
    let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
    let ctx = mock_ctx.spec();
    let child0: Mock<'static, 10> = Mock::new([10, 15, 20, 25, 30, 35, 40, 45, 50, 55]);
    let child1: Mock<'static, 11> = Mock::new([5, 10, 18, 20, 28, 30, 38, 40, 48, 50, 60]);
    let child2: Mock<'static, 11> = Mock::new([2, 10, 12, 20, 22, 30, 32, 40, 42, 50, 70]);

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

    let mut ii = Intersection::new(children, 1.0, false);

    // Revalidate before any read
    // SAFETY: test-only call with valid context
    let status = unsafe { ii.revalidate(ctx) }.expect("revalidate failed");
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
    let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
    let ctx = mock_ctx.spec();
    let child0: Mock<'static, 10> = Mock::new([10, 15, 20, 25, 30, 35, 40, 45, 50, 55]);
    let child1: Mock<'static, 11> = Mock::new([5, 10, 18, 20, 28, 30, 38, 40, 48, 50, 60]);
    let child2: Mock<'static, 11> = Mock::new([2, 10, 12, 20, 22, 30, 32, 40, 42, 50, 70]);

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

    let mut ii = Intersection::new(children, 1.0, false);

    // Revalidate before any read - children will move
    // SAFETY: test-only call with valid context
    let status = unsafe { ii.revalidate(ctx) }.expect("revalidate failed");

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
    let child1 = IdListSorted::new(vec![1, 2, 3, 4, 5]); // 5 elements
    let child2 = IdListSorted::new(vec![1, 2, 3]); // 3 elements (smallest)
    let child3 = IdListSorted::new(vec![1, 2, 3, 4, 5, 6, 7]); // 7 elements

    let ii = Intersection::new(vec![child1, child2, child3], 1.0, false);

    // num_estimated should be the minimum (3)
    assert_eq!(
        ii.num_estimated(),
        3,
        "num_estimated should be minimum of children"
    );
}

/// Test: `num_estimated` is the minimum of all children even when `in_order=true` prevents sorting.
///
/// When `in_order=true`, children are NOT re-sorted by estimated count (their order is
/// semantically meaningful for positional checks). The minimum must still be computed
/// explicitly rather than relying on sort order as a side effect.
#[test]
fn num_estimated_is_minimum_in_order() {
    // Deliberately pass the LARGEST child first — proves we don't rely on sort order.
    let child1 = IdListSorted::new(vec![1, 2, 3, 4, 5]); // 5 elements — first, but NOT minimum
    let child2 = IdListSorted::new(vec![1, 2, 3]); // 3 elements — minimum
    let child3 = IdListSorted::new(vec![1, 2, 3, 4]); // 4 elements

    let ii =
        Intersection::new_with_slop_order(vec![child1, child2, child3], 1.0, false, None, true);

    assert_eq!(
        ii.num_estimated(),
        3,
        "num_estimated must be the minimum of all children, even when in_order=true prevents sorting"
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
    let child1 = IdListSorted::new(large_child);
    let child2 = IdListSorted::new(small_child);
    let child3 = IdListSorted::new(medium_child);

    let mut ii = Intersection::new(vec![child1, child2, child3], 1.0, false);

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

/// Test: Revalidate where children move but skip_to cannot find consensus (returns None)
///
/// This tests the specific path in `revalidate()` where:
/// - At least one child returns `Moved { current: Some(_) }`
/// - No child moved to EOF directly
/// - But when we call `skip_to(max_child_doc_id)`, no consensus can be found
///   because there's no common document ID >= max_child_doc_id
///
/// The expected result is `RQEValidateStatus::Moved { current: None }`
#[test]
fn revalidate_moved_skip_to_returns_none() {
    let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
    let ctx = mock_ctx.spec();
    // Set up children where:
    // - They share doc 10 (will read this first)
    // - After Move, child0 goes to doc 15, child1 goes to doc 18, child2 goes to doc 22
    // - After that, there are no more common documents, so skip_to will return None
    //
    // child0: [10, 15] - after Move, at 15
    // child1: [10, 18] - after Move, at 18  (max_child_doc_id = 18)
    // child2: [10, 22] - after Move, at 22  (but there's no 18 or higher common doc)
    //
    // When skip_to(18) is called on intersection:
    // - child0 has no doc >= 18, so it goes EOF
    // - Result: None

    let child0: Mock<'static, 2> = Mock::new([10, 15]);
    let child1: Mock<'static, 2> = Mock::new([10, 18]);
    let child2: Mock<'static, 2> = Mock::new([10, 22]);

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

    let mut ii = Intersection::new(children, 1.0, false);

    // Read first document (10 is common to all)
    let result = ii.read().expect("read failed").unwrap();
    assert_eq!(result.doc_id, 10);
    assert!(!ii.at_eof());

    // Revalidate: children will move to 15, 18, 22 respectively
    // max_child_doc_id = 22
    // skip_to(22) will fail because:
    // - child0 has no doc >= 22 (only has [10, 15]), goes EOF
    // - Result: Moved { current: None }
    // SAFETY: test-only call with valid context
    let status = unsafe { ii.revalidate(ctx) }.expect("revalidate failed");
    assert!(
        matches!(status, RQEValidateStatus::Moved { current: None }),
        "Expected Moved {{ current: None }} when skip_to cannot find consensus, got {:?}",
        status
    );

    // Intersection should now be at EOF
    assert!(
        ii.at_eof(),
        "Intersection should be at EOF after revalidate returns None"
    );

    // Further reads should return EOF
    assert!(matches!(ii.read(), Ok(None)));
}

// =============================================================================
// Slop and InOrder tests
// (Slop, InOrder, SlopAndOrder test cases)
// =============================================================================

/// Tests for the intersection iterator's `max_slop` and `in_order` proximity constraints.
///
// Because of `ffi::IndexResult_IsWithinRange`
#[cfg(not(miri))]
mod slop_and_order {
    use crate::utils::Mock;
    use rqe_iterators::{RQEIterator, SkipToOutcome, intersection::Intersection};

    /// Build the shared foo/bar intersection used by slop/order tests.
    ///
    /// | doc | foo pos | bar pos | notes                            |
    /// |-----|---------|---------|----------------------------------|
    /// |  1  |    1    |    2    | adjacent, foo before bar         |
    /// |  2  |    1    |    —    | no bar — excluded by intersection|
    /// |  3  |    2    |    1    | adjacent, bar before foo         |
    /// |  4  |    1    |    3    | slop 1, foo before bar           |
    fn make_intersection(
        max_slop: Option<u32>,
        in_order: bool,
    ) -> Intersection<'static, Box<dyn RQEIterator<'static> + 'static>> {
        let foo: Mock<'static, 4> = Mock::new_with_positions([1, 2, 3, 4], [1, 1, 2, 1]);
        let bar: Mock<'static, 3> = Mock::new_with_positions([1, 3, 4], [2, 1, 3]);
        Intersection::new_with_slop_order(
            vec![Box::new(foo), Box::new(bar)],
            1.0,
            false,
            max_slop,
            in_order,
        )
    }

    /// max_slop=0, in_order=false: only documents where foo and bar appear adjacent
    /// (in any order) are returned.
    ///
    /// Expected results: docs 1 and 3.
    #[test]
    fn slop() {
        let mut ii = make_intersection(Some(0), false);

        // num_estimated = min(foo=4, bar=3) = 3
        assert_eq!(ii.num_estimated(), 3);

        // Read all results: expected docs 1 and 3
        let r = ii.read().expect("read failed").expect("expected doc 1");
        assert_eq!(r.doc_id, 1);
        assert_eq!(ii.last_doc_id(), 1);

        let r = ii.read().expect("read failed").expect("expected doc 3");
        assert_eq!(r.doc_id, 3);
        assert_eq!(ii.last_doc_id(), 3);

        assert!(matches!(ii.read(), Ok(None)));
        assert!(ii.at_eof());
        // last_doc_id must remain at the last *successfully returned* doc (3), not the
        // non-relevant candidate (4) that was scanned internally before hitting EOF.
        assert_eq!(ii.last_doc_id(), 3);
        // Reading after EOF should return EOF again
        assert!(matches!(ii.read(), Ok(None)));

        // Rewind and test SkipTo
        ii.rewind();
        assert_eq!(ii.last_doc_id(), 0);
        assert!(!ii.at_eof());

        // SkipTo(1) → Found
        let outcome = ii.skip_to(1).expect("skip_to failed");
        assert!(matches!(outcome, Some(SkipToOutcome::Found(r)) if r.doc_id == 1));
        assert_eq!(ii.last_doc_id(), 1);

        // SkipTo(2) → NotFound, lands on 3 (doc 2 is not in bar, doc 3 is next valid)
        let outcome = ii.skip_to(2).expect("skip_to failed");
        assert!(matches!(outcome, Some(SkipToOutcome::NotFound(r)) if r.doc_id == 3));
        assert_eq!(ii.last_doc_id(), 3);

        // SkipTo(4) → EOF (doc 4 is in both but fails slop=0)
        assert!(matches!(ii.skip_to(4), Ok(None)));
        assert!(ii.at_eof());
        // last_doc_id must stay at 3, not advance to the non-relevant candidate 4.
        assert_eq!(ii.last_doc_id(), 3);

        // SkipTo beyond EOF → still EOF
        assert!(matches!(ii.skip_to(5), Ok(None)));
        assert!(ii.at_eof());
    }

    /// max_slop=None, in_order=true: only documents where foo appears before bar
    /// (any distance) are returned.
    ///
    /// Expected results: docs 1 and 4.
    #[test]
    fn in_order() {
        let mut ii = make_intersection(None, true);

        assert_eq!(ii.num_estimated(), 3); // min(foo=4, bar=3) = 3

        // Read all results: expected docs 1 and 4
        let r = ii.read().expect("read failed").expect("expected doc 1");
        assert_eq!(r.doc_id, 1);
        assert_eq!(ii.last_doc_id(), 1);

        let r = ii.read().expect("read failed").expect("expected doc 4");
        assert_eq!(r.doc_id, 4);
        assert_eq!(ii.last_doc_id(), 4);

        assert!(matches!(ii.read(), Ok(None)));
        assert!(ii.at_eof());
        // Reading after EOF should return EOF again
        assert!(matches!(ii.read(), Ok(None)));

        // Rewind and test SkipTo
        ii.rewind();
        assert_eq!(ii.last_doc_id(), 0);
        assert!(!ii.at_eof());

        // SkipTo(1) → Found
        let outcome = ii.skip_to(1).expect("skip_to failed");
        assert!(matches!(outcome, Some(SkipToOutcome::Found(r)) if r.doc_id == 1));
        assert_eq!(ii.last_doc_id(), 1);

        // SkipTo(2) → NotFound, lands on 4 (doc 2 not in bar, doc 3 fails in_order)
        let outcome = ii.skip_to(2).expect("skip_to failed");
        assert!(matches!(outcome, Some(SkipToOutcome::NotFound(r)) if r.doc_id == 4));
        assert_eq!(ii.last_doc_id(), 4);

        // SkipTo(5) → EOF
        assert!(matches!(ii.skip_to(5), Ok(None)));
        assert!(ii.at_eof());

        // SkipTo beyond EOF → still EOF
        assert!(matches!(ii.skip_to(6), Ok(None)));
        assert!(ii.at_eof());
    }

    /// max_slop=0, in_order=true: only documents where foo immediately precedes
    /// bar (adjacent and in order) are returned.
    ///
    /// Expected results: doc 1 only.
    #[test]
    fn slop_and_order() {
        let mut ii = make_intersection(Some(0), true);

        // num_estimated = min(foo=4, bar=3) = 3
        assert_eq!(ii.num_estimated(), 3);

        // Read all results: expected doc 1 only
        let r = ii.read().expect("read failed").expect("expected doc 1");
        assert_eq!(r.doc_id, 1);
        assert_eq!(ii.last_doc_id(), 1);

        assert!(matches!(ii.read(), Ok(None)));
        assert!(ii.at_eof());
        // last_doc_id must remain at the last *successfully returned* doc (1), not the
        // non-relevant candidates (3, 4) scanned internally before hitting EOF.
        assert_eq!(ii.last_doc_id(), 1);
        // Reading after EOF should return EOF again
        assert!(matches!(ii.read(), Ok(None)));

        // Rewind and test SkipTo
        ii.rewind();
        assert_eq!(ii.last_doc_id(), 0);
        assert!(!ii.at_eof());

        // SkipTo(1) → Found
        let outcome = ii.skip_to(1).expect("skip_to failed");
        assert!(matches!(outcome, Some(SkipToOutcome::Found(r)) if r.doc_id == 1));
        assert_eq!(ii.last_doc_id(), 1);

        // SkipTo(2) → EOF (no more docs pass slop=0 and in_order)
        assert!(matches!(ii.skip_to(2), Ok(None)));
        assert!(ii.at_eof());
        // last_doc_id must stay at 1, not advance to the non-relevant candidates 3 and 4.
        assert_eq!(ii.last_doc_id(), 1);

        // SkipTo beyond EOF → still EOF
        assert!(matches!(ii.skip_to(3), Ok(None)));
        assert!(ii.at_eof());
    }

    /// When no doc satisfies the relevancy constraint and the second child runs out of
    /// docs before the first child does, the iterator must return EOF cleanly.
    ///
    /// - foo (first child): doc 1 (pos 3), doc 2 (pos 1)
    /// - bar (second child): doc 1 (pos 1) only
    /// - in_order=true (prevents child sorting, so foo stays first): doc 1 fails because
    ///   bar@1 comes before foo@3; foo then tries doc 2, but bar has no doc ≥ 2 → EOF.
    #[test]
    fn relevancy_retry_hits_eof_in_second_consensus() {
        let foo: Mock<'static, 2> = Mock::new_with_positions([1, 2], [3, 1]);
        let bar: Mock<'static, 1> = Mock::new_with_positions([1], [1]);
        let mut ii = Intersection::new_with_slop_order(
            vec![
                Box::new(foo) as Box<dyn RQEIterator<'static> + 'static>,
                Box::new(bar),
            ],
            1.0,
            false,
            None,
            true,
        );

        // No doc satisfies in_order: doc 1 fails (bar@1 < foo@3), doc 2 is only in foo.
        assert!(matches!(ii.read(), Ok(None)));
        assert!(ii.at_eof());
    }
}

/// Same as [`sort_weight_nested_intersection_sorts_first`] but the inner `Intersection` is wrapped
/// in a [`Profile`].
///
/// [`Profile`] forwards [`RQEIterator::intersection_sort_weight`] to its child, so the
/// reduced `1/num_children` weight is preserved even through the wrapper.
#[test]
fn sort_weight_profile_wrapped_nested_intersection_sorts_first() {
    let docs: Vec<t_docId> = (1..=10).collect();

    // Inner intersection: 5 children, num_estimated = 10 → sort key 10 * (1/5) = 2.0.
    // Wrapped in Profile → intersection_sort_weight forwards to child, so sort key is still 2.0.
    let inner_children_count = 5;
    let inner_children: Vec<Box<dyn RQEIterator<'static> + 'static>> = (0..inner_children_count)
        .map(|_| {
            Box::new(IdListSorted::new(docs.clone())) as Box<dyn RQEIterator<'static> + 'static>
        })
        .collect();
    let inner = Profile::new(Intersection::new(inner_children, 1.0, false));

    // Plain child: num_estimated = 10 → sort key 10 * 1.0 = 10.0.
    let plain = IdListSorted::new(docs);

    // Pass plain first — the Profile-wrapped inner intersection sorts to index 0
    // because its sort weight (0.2) is lower than the plain child's (1.0).
    let outer = Intersection::new(
        vec![
            Box::new(plain) as Box<dyn RQEIterator<'static> + 'static>,
            Box::new(inner),
        ],
        1.0,
        false,
    );
    assert!(
        outer.child_at(0).intersection_sort_weight(false) < 1.0,
        "Profile-wrapped Intersection (sort key 2.0) must sort before plain child (sort key 10.0)"
    );
}

/// A nested `Intersection` child (sort key `num_estimated * 1/num_children`) must sort before a
/// plain child with equal `num_estimated` (sort key `num_estimated * 1.0`).
#[test]
fn sort_weight_nested_intersection_sorts_first() {
    let docs: Vec<t_docId> = (1..=10).collect();

    // Inner intersection: 5 children, num_estimated = 10 → sort key 10 * (1/5) = 2.0.
    let inner_children_count = 5;
    let inner_children: Vec<Box<dyn RQEIterator<'static> + 'static>> = (0..inner_children_count)
        .map(|_| {
            Box::new(IdListSorted::new(docs.clone())) as Box<dyn RQEIterator<'static> + 'static>
        })
        .collect();
    let inner = Intersection::new(inner_children, 1.0, false);

    // Plain child: num_estimated = 10 → sort key 10 * 1.0 = 10.0.
    let plain = IdListSorted::new(docs);

    // Pass plain first — after construction the inner must sort to index 0.
    let outer = Intersection::new(
        vec![
            Box::new(plain) as Box<dyn RQEIterator<'static> + 'static>,
            Box::new(inner),
        ],
        1.0,
        false,
    );
    assert!(
        outer.child_at(0).intersection_sort_weight(false) < 1.0,
        "nested Intersection (sort key 2.0) must sort before plain child (sort key 10.0)"
    );
}

// =============================================================================
// Tests for `new_intersection_iterator()` — one test per reduction rule.
// =============================================================================

mod reducer {
    use rqe_iterators::{
        Empty, RQEIterator, Wildcard,
        intersection::{NewIntersectionIterator, new_intersection_iterator},
    };

    use crate::utils::Mock;

    /// Heap-erased iterator, required to mix `Mock<N>`, `Empty`, and `Wildcard`
    /// in a single `Vec` passed to `new_intersection_iterator`.
    type DynIter = Box<dyn RQEIterator<'static> + 'static>;

    // Rule 0: an empty child list is trivially empty.
    #[test]
    fn no_children_yields_empty() {
        let children: Vec<DynIter> = vec![];
        assert!(matches!(
            new_intersection_iterator(children),
            NewIntersectionIterator::Empty
        ));
    }

    // Rule 2: any child that reports `is_empty()` forces the whole intersection
    // to be empty, even when the other children are non-empty.
    #[test]
    fn empty_child_yields_empty() {
        let children: Vec<DynIter> = vec![
            Box::new(Mock::new([1u64, 2, 3])),
            Box::new(Empty),
            Box::new(Mock::new([1u64, 2, 3, 4, 5])),
        ];
        assert!(matches!(
            new_intersection_iterator(children),
            NewIntersectionIterator::Empty
        ));
    }

    // Rule 1 + Rule 4: wildcard children are stripped; the two remaining real
    // children proceed to a full intersection.
    #[test]
    fn wildcard_children_are_removed() {
        let children: Vec<DynIter> = vec![
            Box::new(Mock::new([1u64, 2, 3])),
            Box::new(Wildcard::new(30, 1.0)),
            Box::new(Mock::new([1u64, 2, 3])),
            Box::new(Wildcard::new(1000, 1.0)),
        ];
        let NewIntersectionIterator::Proceed(cs) = new_intersection_iterator(children) else {
            panic!("expected Proceed, got a different variant");
        };
        assert_eq!(cs.len(), 2);
    }

    // Rule 1: when every child is a wildcard the last one is returned as `Single`.
    // Each wildcard is given a distinct `top_id` so that `num_estimated()` can
    // identify which instance survived.
    #[test]
    fn all_wildcard_children_returns_last() {
        let children: Vec<DynIter> = vec![
            Box::new(Wildcard::new(10, 1.0)),
            Box::new(Wildcard::new(20, 1.0)),
            Box::new(Wildcard::new(30, 1.0)),
            Box::new(Wildcard::new(40, 1.0)), // last — expected to survive
        ];
        let NewIntersectionIterator::Single(iter) = new_intersection_iterator(children) else {
            panic!("expected Single, got a different variant");
        };
        // `Wildcard::num_estimated` returns `top_id`, so 40 identifies the last child.
        assert_eq!(iter.num_estimated(), 40);
    }

    // Rule 1 + Rule 3: wildcards are stripped, leaving exactly one real child
    // which is returned directly as `Single`.
    #[test]
    fn single_real_child_yields_single() {
        let children: Vec<DynIter> = vec![
            Box::new(Mock::new([1u64, 2, 3])),
            Box::new(Wildcard::new(30, 1.0)),
            Box::new(Wildcard::new(30, 1.0)),
        ];
        assert!(matches!(
            new_intersection_iterator(children),
            NewIntersectionIterator::Single(_)
        ));
    }
}
