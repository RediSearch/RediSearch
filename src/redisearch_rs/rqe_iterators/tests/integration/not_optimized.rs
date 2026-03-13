/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::time::Duration;

use ffi::t_docId;
use rqe_iterators::{
    RQEIterator, RQEIteratorError, RQEValidateStatus, SkipToOutcome, empty::Empty,
    id_list::IdListSorted, not::NotOptimized,
};

use crate::utils::{Mock, MockIteratorError, MockRevalidateResult};

/// Timeout value that effectively disables timeout checks.
const NO_TIMEOUT: Duration = Duration::ZERO;
const SKIP_TIMEOUT: bool = true;

/// Helper: compute the expected result set for a NOT-optimized iterator.
///
/// Returns all doc IDs in `wc_ids` that are NOT in `child_ids` and are less
/// than `max_doc_id`.
fn compute_result_set(wc_ids: &[t_docId], child_ids: &[t_docId], max_doc_id: t_docId) -> Vec<u64> {
    wc_ids
        .iter()
        .copied()
        .filter(|id| *id < max_doc_id && !child_ids.contains(id))
        .collect()
}

// ---------------------------------------------------------------------------
// Basic read tests
// ---------------------------------------------------------------------------

/// Read all results from the NOT-optimized iterator and compare against
/// expected complement.
fn read_test(wc_ids: Vec<t_docId>, child_ids: Vec<t_docId>, max_doc_id: t_docId) {
    let expected = compute_result_set(&wc_ids, &child_ids, max_doc_id);
    let wcii = IdListSorted::new(wc_ids);
    let child = IdListSorted::new(child_ids);
    let mut it = NotOptimized::new(wcii, child, max_doc_id, 1.0, NO_TIMEOUT, SKIP_TIMEOUT);

    let mut actual = Vec::new();
    while let Ok(Some(doc)) = it.read() {
        let doc_id = doc.doc_id;
        actual.push(doc_id);
        assert_eq!(it.last_doc_id(), doc_id);
        assert!(!it.at_eof());
    }
    assert!(it.at_eof());
    // Reading after EOF should return None.
    assert!(it.read().unwrap().is_none());
    assert_eq!(actual, expected, "Read results mismatch");
}

#[test]
fn read_continuous_child_continuous_wc() {
    read_test(
        vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        15,
    );
}

#[test]
fn read_continuous_child_sparse_wc() {
    read_test(
        vec![500, 600, 700, 800, 900, 1000],
        vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        1005,
    );
}

#[test]
fn read_continuous_child_empty_wc() {
    // Empty wildcard: NOT should produce nothing.
    let wcii = Empty;
    let child = IdListSorted::new(vec![1, 2, 3]);
    let mut it = NotOptimized::new(wcii, child, 10, 1.0, NO_TIMEOUT, SKIP_TIMEOUT);
    assert!(it.read().unwrap().is_none());
    assert!(it.at_eof());
}

#[test]
fn read_sparse_child_continuous_wc() {
    read_test(
        vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        vec![500, 600, 700, 800, 900, 1000],
        1005,
    );
}

#[test]
fn read_sparse_child_sparse_wc() {
    read_test(
        vec![500, 600, 700, 800, 900, 1000],
        vec![500, 600, 700, 800, 900, 1000],
        1005,
    );
}

#[test]
fn read_no_overlap() {
    // Wildcard and child have no common IDs.
    read_test(vec![1, 3, 5, 7, 9], vec![2, 4, 6, 8, 10], 15);
}

#[test]
fn read_partial_overlap() {
    read_test(
        vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        vec![2, 4, 6, 8, 10],
        15,
    );
}

#[test]
fn read_child_beyond_wc() {
    // Child has IDs beyond the wildcard range.
    read_test(vec![1, 2, 3, 4, 5], vec![10, 20, 30], 35);
}

#[test]
fn read_large_random() {
    use std::collections::BTreeSet;

    // Deterministic "random" data.
    let wc_ids: Vec<t_docId> = (1..=10000).step_by(2).collect();
    let child_ids: Vec<t_docId> = (1..=10000).step_by(3).collect();
    let max = 10005;

    let child_set: BTreeSet<_> = child_ids.iter().copied().collect();
    let expected: Vec<t_docId> = wc_ids
        .iter()
        .copied()
        .filter(|id| *id < max && !child_set.contains(id))
        .collect();

    let wcii = IdListSorted::new(wc_ids);
    let child = IdListSorted::new(child_ids);
    let mut it = NotOptimized::new(wcii, child, max, 1.0, NO_TIMEOUT, SKIP_TIMEOUT);

    let mut actual = Vec::new();
    while let Ok(Some(doc)) = it.read() {
        actual.push(doc.doc_id);
    }
    assert_eq!(actual, expected);
}

// ---------------------------------------------------------------------------
// SkipTo tests
// ---------------------------------------------------------------------------

/// SkipTo beyond max_doc_id should return EOF.
#[test]
fn skip_to_beyond_max_returns_eof() {
    let wcii = IdListSorted::new(vec![1, 2, 3, 4, 5]);
    let child = IdListSorted::new(vec![2, 4]);
    let mut it = NotOptimized::new(wcii, child, 10, 1.0, NO_TIMEOUT, SKIP_TIMEOUT);

    assert!(it.skip_to(11).unwrap().is_none());
    assert!(it.at_eof());
    // After EOF, skip_to should still return None.
    assert!(it.skip_to(12).unwrap().is_none());
}

/// SkipTo a doc that exists in wildcard but NOT in child → Found.
#[test]
fn skip_to_found_in_wc_not_in_child() {
    let wcii = IdListSorted::new(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    let child = IdListSorted::new(vec![2, 4, 6, 8, 10]);
    let mut it = NotOptimized::new(wcii, child, 15, 1.0, NO_TIMEOUT, SKIP_TIMEOUT);

    let outcome = it.skip_to(3).unwrap();
    match outcome {
        Some(SkipToOutcome::Found(doc)) => {
            assert_eq!(doc.doc_id, 3);
            assert_eq!(it.last_doc_id(), 3);
        }
        other => panic!("Expected Found(3), got {:?}", other),
    }
}

/// SkipTo a doc that is in both wildcard and child → NotFound (advances to next valid).
#[test]
fn skip_to_in_child_returns_not_found() {
    let wcii = IdListSorted::new(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    let child = IdListSorted::new(vec![2, 4, 6, 8, 10]);
    let mut it = NotOptimized::new(wcii, child, 15, 1.0, NO_TIMEOUT, SKIP_TIMEOUT);

    let outcome = it.skip_to(2).unwrap();
    match outcome {
        Some(SkipToOutcome::NotFound(doc)) => {
            assert!(doc.doc_id > 2);
            assert_eq!(doc.doc_id, 3); // Next valid: 3 is in wc but not in child.
        }
        other => panic!("Expected NotFound, got {:?}", other),
    }
}

/// SkipTo a doc not in wildcard → NotFound with next valid doc from wildcard.
#[test]
fn skip_to_not_in_wc_returns_not_found() {
    let wcii = IdListSorted::new(vec![5, 10, 15, 20]);
    let child = IdListSorted::new(vec![10]);
    let mut it = NotOptimized::new(wcii, child, 25, 1.0, NO_TIMEOUT, SKIP_TIMEOUT);

    // Skip to 7: not in wcii, wcii advances to 10. 10 is in child, so advance
    // further to 15 which is valid.
    let outcome = it.skip_to(7).unwrap();
    match outcome {
        Some(SkipToOutcome::NotFound(doc)) => {
            assert!(doc.doc_id > 7);
            assert_eq!(doc.doc_id, 15);
        }
        other => panic!("Expected NotFound, got {:?}", other),
    }
}

/// SkipTo all doc IDs from 1 to max, checking Found/NotFound/EOF.
fn skip_to_all_test(wc_ids: Vec<t_docId>, child_ids: Vec<t_docId>, max_doc_id: t_docId) {
    let expected = compute_result_set(&wc_ids, &child_ids, max_doc_id);

    for id in 1..max_doc_id {
        let wcii = IdListSorted::new(wc_ids.clone());
        let child = IdListSorted::new(child_ids.clone());
        let mut it = NotOptimized::new(wcii, child, max_doc_id, 1.0, NO_TIMEOUT, SKIP_TIMEOUT);

        let expected_id = expected.iter().find(|&&eid| eid >= id);
        let is_exact = expected.contains(&id);

        let rc = it.skip_to(id).unwrap();
        match rc {
            Some(SkipToOutcome::Found(doc)) => {
                assert!(is_exact, "Expected Found for id {id}");
                assert_eq!(doc.doc_id, id);
            }
            Some(SkipToOutcome::NotFound(doc)) => {
                assert!(!is_exact, "Expected NotFound for id {id}");
                assert_eq!(doc.doc_id, *expected_id.unwrap());
                assert!(doc.doc_id > id);
            }
            None => {
                assert!(
                    expected_id.is_none(),
                    "Expected EOF for id {id} but expected_id = {expected_id:?}"
                );
                assert!(it.at_eof());
            }
        }
    }
}

#[test]
fn skip_to_all_continuous() {
    skip_to_all_test(
        vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        vec![2, 4, 6, 8, 10],
        15,
    );
}

#[test]
fn skip_to_all_sparse() {
    skip_to_all_test(vec![5, 10, 15, 20, 25], vec![10, 20], 30);
}

#[test]
fn skip_to_all_no_overlap() {
    skip_to_all_test(vec![1, 3, 5, 7, 9], vec![2, 4, 6, 8, 10], 15);
}

/// Sequential skip_to calls with increasing IDs (from intermediate results).
#[test]
fn skip_to_sequential() {
    let wc_ids = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    let child_ids = vec![2, 4, 6, 8, 10];
    let expected = compute_result_set(&wc_ids, &child_ids, 15);

    let wcii = IdListSorted::new(wc_ids);
    let child = IdListSorted::new(child_ids);
    let mut it = NotOptimized::new(wcii, child, 15, 1.0, NO_TIMEOUT, SKIP_TIMEOUT);

    // Skip to each expected result sequentially.
    for &eid in &expected {
        if eid <= it.last_doc_id() {
            continue;
        }
        let rc = it.skip_to(eid).unwrap();
        match rc {
            Some(SkipToOutcome::Found(doc)) => {
                assert_eq!(doc.doc_id, eid);
            }
            other => panic!("Expected Found({eid}), got {other:?}"),
        }
    }
}

// ---------------------------------------------------------------------------
// NumEstimated
// ---------------------------------------------------------------------------

#[test]
fn num_estimated_returns_wcii_estimate() {
    let wcii = IdListSorted::new(vec![1, 2, 3, 4, 5]);
    let child = IdListSorted::new(vec![2, 4]);
    let it = NotOptimized::new(wcii, child, 10, 1.0, NO_TIMEOUT, SKIP_TIMEOUT);
    // IdListSorted::num_estimated returns the length of the list.
    assert_eq!(it.num_estimated(), 5);
}

// ---------------------------------------------------------------------------
// Rewind
// ---------------------------------------------------------------------------

#[test]
fn rewind_resets_state() {
    let wc_ids = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    let child_ids = vec![2, 4, 6, 8, 10];
    let expected = compute_result_set(&wc_ids, &child_ids, 15);

    let wcii = IdListSorted::new(wc_ids);
    let child = IdListSorted::new(child_ids);
    let mut it = NotOptimized::new(wcii, child, 15, 1.0, NO_TIMEOUT, SKIP_TIMEOUT);

    for pass in 0..5 {
        for j in 0..=pass.min(expected.len() - 1) {
            let doc = it.read().unwrap().unwrap();
            assert_eq!(doc.doc_id, expected[j]);
        }
        it.rewind();
        assert_eq!(it.last_doc_id(), 0);
        assert!(!it.at_eof());
    }
}

// ---------------------------------------------------------------------------
// Child timeout tests
// ---------------------------------------------------------------------------

/// Child timeout during read: child has IDs overlapping with wildcard,
/// and times out when exhausted. The NOT iterator must propagate the error.
#[test]
fn child_timeout_on_first_read() {
    // Child [1] overlaps with wc [1, 2]. When NOT reads:
    // wcii→1, child behind (last_doc_id=0)→advance child→read()→1.
    // Now wcii=1, child=1→case 2: child.read()→timeout.
    let wc = Mock::new([1, 2, 3]);
    let child = Mock::new([1]);
    let mut child_data = child.data();
    child_data.set_error_at_done(Some(MockIteratorError::TimeoutError(None)));

    let mut it = NotOptimized::new(wc, child, 10, 1.0, NO_TIMEOUT, SKIP_TIMEOUT);
    let rc = it.read();
    assert!(
        matches!(rc, Err(RQEIteratorError::TimedOut)),
        "Expected timeout on first read, got {rc:?}"
    );
}

#[test]
fn child_timeout_on_subsequent_read() {
    // wc=[1,2,3,4,5,6], child=[2,4,6]. First read returns 1 (not in child).
    // Then set child to timeout on exhaustion.
    let wc = Mock::new([1, 2, 3, 4, 5, 6]);
    let child = Mock::new([2, 4, 6]);
    let mut child_data = child.data();
    let mut it = NotOptimized::new(wc, child, 10, 1.0, NO_TIMEOUT, SKIP_TIMEOUT);

    // Read first result: 1 (not in child).
    let doc = it.read().unwrap().unwrap();
    assert_eq!(doc.doc_id, 1);

    // Now make child timeout when exhausted.
    child_data.set_error_at_done(Some(MockIteratorError::TimeoutError(None)));

    // Continue reading until timeout.
    let mut rc = it.read();
    while matches!(rc, Ok(Some(_))) {
        rc = it.read();
    }
    assert!(
        matches!(rc, Err(RQEIteratorError::TimedOut)),
        "Expected timeout, got {rc:?}"
    );
}

#[test]
fn child_timeout_on_skip_to() {
    // wc=[1..10], child=[1..9] with timeout on exhaustion.
    // skip_to(1): wcii→Found(1), child behind→skip to 1→Found.
    // Child has 1. read_inner: wcii→2, child→2→case 2, continue...
    // Eventually child.read()→timeout.
    let wc = Mock::new([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    let child = Mock::new([1, 2, 3, 4, 5, 6, 7, 8, 9]);
    let mut child_data = child.data();
    child_data.set_error_at_done(Some(MockIteratorError::TimeoutError(None)));

    let mut it = NotOptimized::new(wc, child, 15, 1.0, NO_TIMEOUT, SKIP_TIMEOUT);

    let rc = it.skip_to(1);
    assert!(
        matches!(rc, Err(RQEIteratorError::TimedOut)),
        "Expected timeout on skip_to, got {rc:?}"
    );
}

// ---------------------------------------------------------------------------
// Revalidate tests (ported from C++ NotIteratorOptimizedRevalidateTest)
// ---------------------------------------------------------------------------

/// Helper to create a NOT-optimized iterator with Mock wildcard and child.
fn make_revalidate_test_iter() -> (
    NotOptimized<'static, Mock<'static, 20>, Mock<'static, 4>>,
    crate::utils::MockData, // child_data
    crate::utils::MockData, // wc_data
) {
    let child_ids: [t_docId; 4] = [10, 30, 50, 70];
    let wc_ids: [t_docId; 20] = [
        1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80, 85, 90, 95,
    ];

    let child = Mock::new(child_ids);
    let child_data = child.data();
    let wc = Mock::new(wc_ids);
    let wc_data = wc.data();

    let it = NotOptimized::new(wc, child, 100, 1.0, NO_TIMEOUT, SKIP_TIMEOUT);
    (it, child_data, wc_data)
}

// Child OK, Wildcard ABORTED
#[test]
fn revalidate_child_ok_wc_aborted() {
    let (mut it, mut child_data, mut wc_data) = make_revalidate_test_iter();
    child_data.set_revalidate_result(MockRevalidateResult::Ok);
    wc_data.set_revalidate_result(MockRevalidateResult::Abort);

    it.read().unwrap().unwrap();

    let status = it.revalidate().unwrap();
    assert_eq!(status, RQEValidateStatus::Aborted);
    assert_eq!(wc_data.revalidate_count(), 1);
}

// Child ABORTED, Wildcard ABORTED
#[test]
fn revalidate_child_aborted_wc_aborted() {
    let (mut it, mut child_data, mut wc_data) = make_revalidate_test_iter();
    child_data.set_revalidate_result(MockRevalidateResult::Abort);
    wc_data.set_revalidate_result(MockRevalidateResult::Abort);

    it.read().unwrap().unwrap();

    let status = it.revalidate().unwrap();
    assert_eq!(status, RQEValidateStatus::Aborted);
    assert_eq!(wc_data.revalidate_count(), 1);
}

// Child MOVED, Wildcard ABORTED
#[test]
fn revalidate_child_moved_wc_aborted() {
    let (mut it, mut child_data, mut wc_data) = make_revalidate_test_iter();
    child_data.set_revalidate_result(MockRevalidateResult::Move);
    wc_data.set_revalidate_result(MockRevalidateResult::Abort);

    it.read().unwrap().unwrap();

    let status = it.revalidate().unwrap();
    assert_eq!(status, RQEValidateStatus::Aborted);
    assert_eq!(wc_data.revalidate_count(), 1);
}

// Child OK, Wildcard OK
#[test]
fn revalidate_child_ok_wc_ok() {
    let (mut it, mut child_data, mut wc_data) = make_revalidate_test_iter();
    child_data.set_revalidate_result(MockRevalidateResult::Ok);
    wc_data.set_revalidate_result(MockRevalidateResult::Ok);

    it.read().unwrap().unwrap();
    it.read().unwrap().unwrap();
    let original = it.last_doc_id();

    let status = it.revalidate().unwrap();
    assert_eq!(status, RQEValidateStatus::Ok);
    assert_eq!(child_data.revalidate_count(), 1);
    assert_eq!(wc_data.revalidate_count(), 1);
    assert_eq!(it.last_doc_id(), original);
    it.read().unwrap().unwrap();
}

// Child ABORTED, Wildcard OK
#[test]
fn revalidate_child_aborted_wc_ok() {
    let (mut it, mut child_data, mut wc_data) = make_revalidate_test_iter();
    child_data.set_revalidate_result(MockRevalidateResult::Abort);
    wc_data.set_revalidate_result(MockRevalidateResult::Ok);

    it.read().unwrap().unwrap();
    let original = it.last_doc_id();

    let status = it.revalidate().unwrap();
    assert_eq!(status, RQEValidateStatus::Ok);
    // child_data no longer accessible after replacement, but wc was checked.
    assert_eq!(wc_data.revalidate_count(), 1);
    assert_eq!(it.last_doc_id(), original);
    it.read().unwrap().unwrap();
}

// Child MOVED, Wildcard OK
#[test]
fn revalidate_child_moved_wc_ok() {
    let (mut it, mut child_data, mut wc_data) = make_revalidate_test_iter();
    child_data.set_revalidate_result(MockRevalidateResult::Move);
    wc_data.set_revalidate_result(MockRevalidateResult::Ok);

    it.read().unwrap().unwrap();
    let original = it.last_doc_id();

    let status = it.revalidate().unwrap();
    assert_eq!(status, RQEValidateStatus::Ok);
    assert_eq!(child_data.revalidate_count(), 1);
    assert_eq!(wc_data.revalidate_count(), 1);
    assert_eq!(it.last_doc_id(), original);
    it.read().unwrap().unwrap();
}

// Child OK, Wildcard MOVED
#[test]
fn revalidate_child_ok_wc_moved() {
    let (mut it, mut child_data, mut wc_data) = make_revalidate_test_iter();
    child_data.set_revalidate_result(MockRevalidateResult::Ok);
    wc_data.set_revalidate_result(MockRevalidateResult::Move);

    it.read().unwrap().unwrap();
    let original = it.last_doc_id();

    let status = it.revalidate().unwrap();
    assert!(matches!(status, RQEValidateStatus::Moved { .. }));
    assert_eq!(child_data.revalidate_count(), 1);
    assert_eq!(wc_data.revalidate_count(), 1);
    assert!(it.last_doc_id() > original);
}

// Child ABORTED, Wildcard MOVED
#[test]
fn revalidate_child_aborted_wc_moved() {
    let (mut it, mut child_data, mut wc_data) = make_revalidate_test_iter();
    child_data.set_revalidate_result(MockRevalidateResult::Abort);
    wc_data.set_revalidate_result(MockRevalidateResult::Move);

    it.read().unwrap().unwrap();
    let original = it.last_doc_id();

    let status = it.revalidate().unwrap();
    assert!(matches!(status, RQEValidateStatus::Moved { .. }));
    assert_eq!(wc_data.revalidate_count(), 1);
    assert!(it.last_doc_id() > original);
}

// Child MOVED, Wildcard MOVED
#[test]
fn revalidate_child_moved_wc_moved() {
    let (mut it, mut child_data, mut wc_data) = make_revalidate_test_iter();
    child_data.set_revalidate_result(MockRevalidateResult::Move);
    wc_data.set_revalidate_result(MockRevalidateResult::Move);

    it.read().unwrap().unwrap();
    let original = it.last_doc_id();

    let status = it.revalidate().unwrap();
    assert!(matches!(status, RQEValidateStatus::Moved { .. }));
    assert_eq!(child_data.revalidate_count(), 1);
    assert_eq!(wc_data.revalidate_count(), 1);
    assert!(it.last_doc_id() > original);
    it.read().unwrap().unwrap();
}

/// Wildcard moves to the same position as child after revalidation.
/// This exercises the code path where child.last_doc_id == result.doc_id
/// after wildcard moved, triggering read_inner.
#[test]
fn revalidate_wc_moves_to_same_id_as_child() {
    let (mut it, mut child_data, mut wc_data) = make_revalidate_test_iter();
    child_data.set_revalidate_result(MockRevalidateResult::Ok);
    wc_data.set_revalidate_result(MockRevalidateResult::Move);

    // Read two docs to position: wc at 5, child at 10.
    it.read().unwrap().unwrap();
    it.read().unwrap().unwrap();
    assert_eq!(it.last_doc_id(), 5);

    // Revalidate: wc moves from 5 to 10. Child is at 10.
    // Since wc.last_doc_id (10) == child.last_doc_id (10), read_inner is called.
    // Next valid from wc is 15 (not in child).
    let status = it.revalidate().unwrap();
    assert!(matches!(status, RQEValidateStatus::Moved { .. }));
    assert!(!it.at_eof());
    assert_eq!(it.last_doc_id(), 15);
    it.read().unwrap().unwrap();
}

// ---------------------------------------------------------------------------
// Self-timeout tests (via TimeoutContext)
// ---------------------------------------------------------------------------

#[test]
#[cfg_attr(miri, ignore = "Too slow to be run under miri.")]
fn read_timeout_via_timeout_ctx() {
    // Create child and wildcard with the same IDs so the loop runs many times
    // (every doc in wc matches child, so read_inner loops without returning).
    let mut child_ids = [0u64; 5500];
    for i in 0..5500 {
        child_ids[i] = (i + 1) as t_docId;
    }
    let mut wc_ids = [0u64; 5500];
    for i in 0..5500 {
        wc_ids[i] = (i + 1) as t_docId;
    }
    let child = Mock::new(child_ids);
    let mut child_data = child.data();
    child_data.add_delay_since_index(1, Duration::from_micros(100));

    let wc = Mock::new(wc_ids);

    let mut it = NotOptimized::new(wc, child, 10_000, 1.0, Duration::from_micros(50), false);

    let result = it.read();
    assert!(
        matches!(result, Err(RQEIteratorError::TimedOut)),
        "expected timeout, got {result:?}"
    );
}

// ---------------------------------------------------------------------------
// Initial state
// ---------------------------------------------------------------------------

#[test]
fn initial_state() {
    let wcii = IdListSorted::new(vec![1, 2, 3, 4, 5]);
    let child = IdListSorted::new(vec![2, 4]);
    let it = NotOptimized::new(wcii, child, 10, 1.0, NO_TIMEOUT, SKIP_TIMEOUT);

    assert_eq!(it.last_doc_id(), 0);
    assert!(!it.at_eof());
    assert_eq!(it.num_estimated(), 5);
}
