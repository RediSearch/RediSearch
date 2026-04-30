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
use inverted_index::RSIndexResult;
use rqe_iterators::{RQEIterator, RQEIteratorError, SkipToOutcome, not_optimized::NotOptimized};

use crate::utils::{Mock, MockIteratorError, MockVec, WildcardHelper};

/// Helper: compute the expected result set for a NOT-optimized iterator.
///
/// Returns all doc IDs in `wc_ids` that are NOT in `child_ids` and are at
/// most `max_doc_id` (inclusive).
fn compute_result_set(wc_ids: &[t_docId], child_ids: &[t_docId], max_doc_id: t_docId) -> Vec<u64> {
    wc_ids
        .iter()
        .copied()
        .filter(|id| *id <= max_doc_id && !child_ids.contains(id))
        .collect()
}

// ---------------------------------------------------------------------------
// Basic read tests
// ---------------------------------------------------------------------------

/// Read all results from the NOT-optimized iterator and compare against
/// expected complement.
fn read_test(wc_ids: Vec<t_docId>, child_ids: Vec<t_docId>, max_doc_id: t_docId) {
    let expected = compute_result_set(&wc_ids, &child_ids, max_doc_id);
    let wc_helper = WildcardHelper::new(&wc_ids);
    let wcii = wc_helper.create_wildcard();
    let child = MockVec::new(child_ids);
    let mut it = NotOptimized::new(wcii, child, max_doc_id, 1.0, None);

    let mut actual = Vec::new();
    while let Ok(Some(doc)) = it.read() {
        let doc_id = doc.doc_id;
        actual.push(doc_id);
        assert_eq!(it.last_doc_id(), doc_id);
        // at_eof() is computed as `result.doc_id >= max_doc_id`, so it
        // becomes true immediately after yielding a doc at max_doc_id even
        // though the read itself succeeded.
        assert!(doc_id == max_doc_id || !it.at_eof());
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
    let wc_helper = WildcardHelper::new(&[]);
    let wcii = wc_helper.create_wildcard();
    let child = MockVec::new(vec![1, 2, 3]);
    let mut it = NotOptimized::new(wcii, child, 10, 1.0, None);
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
fn read_wc_at_max_doc_id() {
    // Wildcard contains a document at exactly max_doc_id.
    // max_doc_id is inclusive, so this document should be yielded.
    read_test(vec![1, 5, 10], vec![5], 10);
}

// ---------------------------------------------------------------------------
// SkipTo tests
// ---------------------------------------------------------------------------

/// SkipTo beyond max_doc_id should return EOF.
#[test]
fn skip_to_beyond_max_returns_eof() {
    let wc_helper = WildcardHelper::new(&[1, 2, 3, 4, 5]);
    let wcii = wc_helper.create_wildcard();
    let child = MockVec::new(vec![2, 4]);
    let mut it = NotOptimized::new(wcii, child, 10, 1.0, None);

    assert!(it.skip_to(11).unwrap().is_none());
    assert!(it.at_eof());
    // After EOF, skip_to should still return None.
    assert!(it.skip_to(12).unwrap().is_none());
}

/// SkipTo a doc that exists in wildcard but NOT in child → Found.
#[test]
fn skip_to_found_in_wc_not_in_child() {
    let wc_helper = WildcardHelper::new(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    let wcii = wc_helper.create_wildcard();
    let child = MockVec::new(vec![2, 4, 6, 8, 10]);
    let mut it = NotOptimized::new(wcii, child, 15, 1.0, None);

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
    let wc_helper = WildcardHelper::new(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    let wcii = wc_helper.create_wildcard();
    let child = MockVec::new(vec![2, 4, 6, 8, 10]);
    let mut it = NotOptimized::new(wcii, child, 15, 1.0, None);

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
    let wc_helper = WildcardHelper::new(&[5, 10, 15, 20]);
    let wcii = wc_helper.create_wildcard();
    let child = MockVec::new(vec![10]);
    let mut it = NotOptimized::new(wcii, child, 25, 1.0, None);

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
        let wc_helper = WildcardHelper::new(&wc_ids);
        let wcii = wc_helper.create_wildcard();
        let child = MockVec::new(child_ids.clone());
        let mut it = NotOptimized::new(wcii, child, max_doc_id, 1.0, None);

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

    let wc_helper = WildcardHelper::new(&wc_ids);
    let wcii = wc_helper.create_wildcard();
    let child = MockVec::new(child_ids);
    let mut it = NotOptimized::new(wcii, child, 15, 1.0, None);

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

#[test]
fn num_estimated_returns_wcii_estimate() {
    let wc_helper = WildcardHelper::new(&[1, 2, 3, 4, 5]);
    let wcii = wc_helper.create_wildcard();
    let child = MockVec::new(vec![2, 4]);
    let it = NotOptimized::new(wcii, child, 10, 1.0, None);
    assert_eq!(it.num_estimated(), 5);
}

#[test]
fn rewind_resets_state() {
    let wc_ids = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    let child_ids = vec![2, 4, 6, 8, 10];
    let expected = compute_result_set(&wc_ids, &child_ids, 15);

    let wc_helper = WildcardHelper::new(&wc_ids);
    let wcii = wc_helper.create_wildcard();
    let child = MockVec::new(child_ids);
    let mut it = NotOptimized::new(wcii, child, 15, 1.0, None);

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

#[test]
fn initial_state() {
    let wc_helper = WildcardHelper::new(&[1, 2, 3, 4, 5]);
    let wcii = wc_helper.create_wildcard();
    let child = MockVec::new(vec![2, 4]);
    let it = NotOptimized::new(wcii, child, 10, 1.0, None);

    assert_eq!(it.last_doc_id(), 0);
    assert!(!it.at_eof());
    assert_eq!(it.num_estimated(), 5);
}

#[test]
fn current_always_returns_some() {
    let wc_helper = WildcardHelper::new(&[1, 2, 3]);
    let wcii = wc_helper.create_wildcard();
    let child = MockVec::new(vec![2]);
    let mut it = NotOptimized::new(wcii, child, 10, 1.0, None);

    // Before any read.
    assert!(it.current().is_some());
    // After a read.
    it.read().unwrap();
    assert!(it.current().is_some());
    // After EOF.
    while it.read().unwrap().is_some() {}
    assert!(it.at_eof());
    assert!(it.current().is_some());
}

#[test]
fn skip_to_case2_eof() {
    // wc=[1, 2, 3], child=[1, 2, 3]. skip_to(1): wcii lands on 1, child at 1
    // (Case 2). read_inner tries to find a doc not in child but all remaining
    // docs are also in child, so it returns EOF.
    let wc_helper = WildcardHelper::new(&[1, 2, 3]);
    let wcii = wc_helper.create_wildcard();
    let child = MockVec::new(vec![1, 2, 3]);
    let mut it = NotOptimized::new(wcii, child, 10, 1.0, None);

    let outcome = it.skip_to(1).unwrap();
    assert!(outcome.is_none());
    assert!(it.at_eof());
}

/// skip_to hits Case 2 (wcii and child already at same position) and
/// read_inner finds a subsequent valid result → NotFound.
#[test]
fn skip_to_case2_not_found() {
    // wc=[1, 3, 5, 7], child=[3, 5]. After read() → 1, child is at 3.
    // skip_to(3): wcii lands on 3, child already at 3 → Case 2.
    // read_inner advances past 5 (also in child) and finds 7 → NotFound(7).
    let wc_helper = WildcardHelper::new(&[1, 3, 5, 7]);
    let wcii = wc_helper.create_wildcard();
    let child = MockVec::new(vec![3, 5]);
    let mut it = NotOptimized::new(wcii, child, 10, 1.0, None);

    let doc = it.read().unwrap().unwrap();
    assert_eq!(doc.doc_id, 1);

    let outcome = it.skip_to(3).unwrap();
    match outcome {
        Some(SkipToOutcome::NotFound(doc)) => {
            assert_eq!(doc.doc_id, 7);
        }
        other => panic!("Expected NotFound(7), got {other:?}"),
    }
}

/// skip_to hits Case 2 (wcii and child at same position) and read_inner
/// exhausts all remaining docs → EOF (None).
#[test]
fn skip_to_case2_exhausted() {
    // wc=[1, 3, 5], child=[3, 5]. After read() → 1, child is at 3.
    // skip_to(3): wcii lands on 3, child at 3 → Case 2.
    // read_inner advances: wcii→5, child→5 → Case 2 again, both exhausted → EOF.
    let wc_helper = WildcardHelper::new(&[1, 3, 5]);
    let wcii = wc_helper.create_wildcard();
    let child = MockVec::new(vec![3, 5]);
    let mut it = NotOptimized::new(wcii, child, 10, 1.0, None);

    let doc = it.read().unwrap().unwrap();
    assert_eq!(doc.doc_id, 1);

    let outcome = it.skip_to(3).unwrap();
    assert!(outcome.is_none());
    assert!(it.at_eof());
}

/// skip_to hits Case 1 when the child is exhausted (at EOF) and the
/// wildcard lands on a document beyond the child's last position.
#[test]
fn skip_to_case1_child_exhausted() {
    // wc=[1, 3, 5], child=[2, 4]. After two reads, child is exhausted
    // (at_eof=true, last_doc_id=4). skip_to(5): wcii lands on 5,
    // child_does_not_have(5) is true because child is at EOF and 5 > 4.
    let wc_helper = WildcardHelper::new(&[1, 3, 5]);
    let wcii = wc_helper.create_wildcard();
    let child = MockVec::new(vec![2, 4]);
    let mut it = NotOptimized::new(wcii, child, 10, 1.0, None);

    // Consume docs to exhaust the child iterator.
    let doc = it.read().unwrap().unwrap();
    assert_eq!(doc.doc_id, 1);
    let doc = it.read().unwrap().unwrap();
    assert_eq!(doc.doc_id, 3);

    // skip_to(5): child exhausted, wcii finds 5 → Case 1 → Found.
    let outcome = it.skip_to(5).unwrap();
    match outcome {
        Some(SkipToOutcome::Found(doc)) => {
            assert_eq!(doc.doc_id, 5);
        }
        other => panic!("Expected Found(5), got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Child timeout tests
// ---------------------------------------------------------------------------

/// Child timeout during read: child has IDs overlapping with wildcard,
/// and times out when exhausted. The NOT iterator must propagate the error.
#[test]
fn child_timeout_on_first_read() {
    // Child [1] overlaps with wc [1, 2, 3]. When NOT reads:
    // wcii→1, child behind (last_doc_id=0)→advance child→read()→1.
    // Now wcii=1, child=1→case 2: child.read()→timeout.
    let wc_helper = WildcardHelper::new(&[1, 2, 3]);
    let wcii = wc_helper.create_wildcard();
    let child = Mock::new([1]);
    let mut child_data = child.data();
    child_data.set_error_at_done(Some(MockIteratorError::TimeoutError(None)));

    let mut it = NotOptimized::new(wcii, child, 10, 1.0, None);
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
    let wc_helper = WildcardHelper::new(&[1, 2, 3, 4, 5, 6]);
    let wcii = wc_helper.create_wildcard();
    let child = Mock::new([2, 4, 6]);
    let mut child_data = child.data();
    let mut it = NotOptimized::new(wcii, child, 10, 1.0, None);

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
    let wc_helper = WildcardHelper::new(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    let wcii = wc_helper.create_wildcard();
    let child = Mock::new([1, 2, 3, 4, 5, 6, 7, 8, 9]);
    let mut child_data = child.data();
    child_data.set_error_at_done(Some(MockIteratorError::TimeoutError(None)));

    let mut it = NotOptimized::new(wcii, child, 15, 1.0, None);

    let rc = it.skip_to(1);
    assert!(
        matches!(rc, Err(RQEIteratorError::TimedOut)),
        "Expected timeout on skip_to, got {rc:?}"
    );
}

#[test]
#[cfg_attr(miri, ignore = "Too slow to be run under miri.")]
fn read_timeout_via_timeout_ctx() {
    // Create child and wildcard with the same IDs so the loop runs many times
    // (every doc in wc matches child, so read_inner loops without returning).
    let ids: Vec<t_docId> = (1..=5500).collect();
    let wc_helper = WildcardHelper::new(&ids);
    let wcii = wc_helper.create_wildcard();

    let child_ids: [t_docId; 5500] = std::array::from_fn(|i| (i + 1) as t_docId);
    let child = Mock::new(child_ids);
    let mut child_data = child.data();
    child_data.add_delay_since_index(1, Duration::from_micros(100));

    let mut it = NotOptimized::new(wcii, child, 10_000, 1.0, Some(Duration::from_micros(50)));

    let result = it.read();
    assert!(
        matches!(result, Err(RQEIteratorError::TimedOut)),
        "expected timeout, got {result:?}"
    );
}

#[cfg(not(miri))] // TestContext relies on ffi calls
mod revalidate {
    use super::*;
    use crate::utils::MockRevalidateResult;
    use ffi::IndexFlags_Index_DocIdsOnly;
    use inverted_index::{InvertedIndex, doc_ids_only::DocIdsOnly, opaque::OpaqueEncoding};
    use rqe_iterators::RQEValidateStatus;
    use rqe_iterators_test_utils::{GlobalGuard, TestContext};

    /// Wildcard doc IDs used by the revalidate tests.
    const WC_IDS: [t_docId; 20] = [
        1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80, 85, 90, 95,
    ];

    /// Child doc IDs used by the revalidate tests.
    const CHILD_IDS: [t_docId; 4] = [10, 30, 50, 70];

    /// Create the context and guard for revalidate tests.
    fn make_revalidate_context() -> (GlobalGuard, TestContext) {
        (
            GlobalGuard::default(),
            TestContext::wildcard(WC_IDS.iter().copied()),
        )
    }

    /// Create a NOT-optimized iterator backed by a real wildcard from the
    /// given [`TestContext`].
    fn create_not_optimized(
        context: &TestContext,
    ) -> (
        NotOptimized<
            '_,
            rqe_iterators::inverted_index::Wildcard<'_, DocIdsOnly>,
            Mock<'_, { CHILD_IDS.len() }>,
        >,
        crate::utils::MockData,
    ) {
        let ii = DocIdsOnly::from_opaque(context.wildcard_inverted_index());
        let wcii = rqe_iterators::inverted_index::Wildcard::new(ii.reader(), 1.0);
        let child = Mock::new(CHILD_IDS);
        let child_data = child.data();
        let it = NotOptimized::new(wcii, child, 100, 1.0, None);
        (it, child_data)
    }

    /// Helper: GC `doc_id` from the wildcard inverted index.
    fn gc_document(context: &TestContext, doc_id: t_docId) {
        let ii = DocIdsOnly::from_mut_opaque(context.wildcard_inverted_index());
        let scan_delta = ii
            .scan_gc(
                |d| d != doc_id,
                None::<fn(&RSIndexResult, &inverted_index::IndexBlock)>,
            )
            .expect("scan GC failed")
            .expect("no GC scan delta");
        ii.apply_gc(scan_delta);
    }

    // Child OK, Wildcard OK (no index changes)
    #[test]
    fn revalidate_child_ok_wc_ok() {
        let (_guard, context) = make_revalidate_context();
        let (mut it, mut child_data) = create_not_optimized(&context);
        child_data.set_revalidate_result(MockRevalidateResult::Ok);

        it.read().unwrap().unwrap();
        it.read().unwrap().unwrap();
        let original = it.last_doc_id();

        // SAFETY: test-only call with valid context
        let status = unsafe { it.revalidate(context.spec) }.unwrap();
        assert_eq!(status, RQEValidateStatus::Ok);
        assert_eq!(child_data.revalidate_count(), 1);
        assert_eq!(it.last_doc_id(), original);
        it.read().unwrap().unwrap();
    }

    // Child ABORTED, Wildcard OK
    #[test]
    fn revalidate_child_aborted_wc_ok() {
        let (_guard, context) = make_revalidate_context();
        let (mut it, mut child_data) = create_not_optimized(&context);
        child_data.set_revalidate_result(MockRevalidateResult::Abort);

        it.read().unwrap().unwrap();
        let original = it.last_doc_id();

        // SAFETY: test-only call with valid context
        let status = unsafe { it.revalidate(context.spec) }.unwrap();
        assert_eq!(status, RQEValidateStatus::Ok);
        assert_eq!(it.last_doc_id(), original);
        it.read().unwrap().unwrap();
    }

    // Child MOVED, Wildcard OK
    #[test]
    fn revalidate_child_moved_wc_ok() {
        let (_guard, context) = make_revalidate_context();
        let (mut it, mut child_data) = create_not_optimized(&context);
        child_data.set_revalidate_result(MockRevalidateResult::Move);

        it.read().unwrap().unwrap();
        let original = it.last_doc_id();

        // SAFETY: test-only call with valid context
        let status = unsafe { it.revalidate(context.spec) }.unwrap();
        assert_eq!(status, RQEValidateStatus::Ok);
        assert_eq!(child_data.revalidate_count(), 1);
        assert_eq!(it.last_doc_id(), original);
        it.read().unwrap().unwrap();
    }

    // Wildcard ABORTED (existingDocs replaced) — child state is irrelevant
    // because the wildcard abort short-circuits before checking the child.
    #[test]
    fn revalidate_wc_aborted() {
        let (_guard, context) = make_revalidate_context();
        let (mut it, _child_data) = create_not_optimized(&context);

        it.read().unwrap().unwrap();

        // Replace existingDocs with a different inverted index to trigger abort.
        let new_ii = Box::into_raw(Box::new(inverted_index::opaque::InvertedIndex::DocIdsOnly(
            InvertedIndex::<DocIdsOnly>::new(IndexFlags_Index_DocIdsOnly),
        )));
        let old_existing_docs;
        // SAFETY: `context.spec` is a valid, test-owned `IndexSpec` pointer.
        // We temporarily swap `existingDocs` to trigger a wildcard abort.
        unsafe {
            let spec = context.spec.as_ptr();
            old_existing_docs = (*spec).existingDocs;
            (*spec).existingDocs = new_ii.cast();
        }

        // SAFETY: test-only call with valid context
        let status = unsafe { it.revalidate(context.spec) }.unwrap();
        assert_eq!(status, RQEValidateStatus::Aborted);

        // SAFETY: Restoring the original `existingDocs` pointer and dropping
        // `new_ii` which was created via `Box::into_raw` above.
        unsafe {
            let spec = context.spec.as_ptr();
            (*spec).existingDocs = old_existing_docs;
            drop(Box::from_raw(new_ii));
        }
    }

    // Wildcard MOVED (GC a document) + Child OK
    #[test]
    fn revalidate_child_ok_wc_moved() {
        let (_guard, context) = make_revalidate_context();
        let (mut it, mut child_data) = create_not_optimized(&context);
        child_data.set_revalidate_result(MockRevalidateResult::Ok);

        // Read first result (doc_id = 1, which is in wcii but not in child).
        it.read().unwrap().unwrap();
        let original = it.last_doc_id();
        assert_eq!(original, 1);

        // GC doc_id=1 from the wildcard inverted index to trigger Moved.
        gc_document(&context, 1);

        // SAFETY: test-only call with valid context
        let status = unsafe { it.revalidate(context.spec) }.unwrap();
        assert!(matches!(status, RQEValidateStatus::Moved { .. }));
        // Wildcard moved past 1 → iterator advanced.
        assert!(it.last_doc_id() > original);
    }

    // Wildcard MOVED + Child ABORTED
    #[test]
    fn revalidate_child_aborted_wc_moved() {
        let (_guard, context) = make_revalidate_context();
        let (mut it, mut child_data) = create_not_optimized(&context);
        child_data.set_revalidate_result(MockRevalidateResult::Abort);

        it.read().unwrap().unwrap();
        let original = it.last_doc_id();
        assert_eq!(original, 1);

        gc_document(&context, 1);

        // SAFETY: test-only call with valid context
        let status = unsafe { it.revalidate(context.spec) }.unwrap();
        assert!(matches!(status, RQEValidateStatus::Moved { .. }));
        assert!(it.last_doc_id() > original);
    }

    // Wildcard MOVED + Child MOVED
    #[test]
    fn revalidate_child_moved_wc_moved() {
        let (_guard, context) = make_revalidate_context();
        let (mut it, mut child_data) = create_not_optimized(&context);
        child_data.set_revalidate_result(MockRevalidateResult::Move);

        it.read().unwrap().unwrap();
        let original = it.last_doc_id();
        assert_eq!(original, 1);

        gc_document(&context, 1);

        // SAFETY: test-only call with valid context
        let status = unsafe { it.revalidate(context.spec) }.unwrap();
        assert!(matches!(status, RQEValidateStatus::Moved { .. }));
        assert!(it.last_doc_id() > original);
        it.read().unwrap().unwrap();
    }

    /// Wildcard moves to the same position as child after revalidation.
    #[test]
    fn revalidate_wc_moves_to_same_id_as_child() {
        let (_guard, context) = make_revalidate_context();
        let (mut it, mut child_data) = create_not_optimized(&context);
        child_data.set_revalidate_result(MockRevalidateResult::Ok);

        // Read two docs to position: wc at 5, child at 10.
        it.read().unwrap().unwrap(); // doc 1
        it.read().unwrap().unwrap(); // doc 5
        assert_eq!(it.last_doc_id(), 5);

        // GC doc_id=5. After revalidation, wcii should move to 10.
        // Since child is also at 10, read_inner should advance past it.
        gc_document(&context, 5);

        // SAFETY: test-only call with valid context
        let status = unsafe { it.revalidate(context.spec) }.unwrap();
        assert!(matches!(status, RQEValidateStatus::Moved { .. }));
        assert!(!it.at_eof());
        // Wildcard moved to 10 which matches child → read_inner → 15.
        assert_eq!(it.last_doc_id(), 15);
        it.read().unwrap().unwrap();
    }

    /// Wildcard moves to EOF after GC removes all remaining documents.
    #[test]
    fn revalidate_wc_moved_to_eof() {
        // Wildcard has a single document.
        let (_guard, context) = (
            GlobalGuard::default(),
            TestContext::wildcard([1].iter().copied()),
        );
        let ii = DocIdsOnly::from_opaque(context.wildcard_inverted_index());
        let wcii = rqe_iterators::inverted_index::Wildcard::new(ii.reader(), 1.0);
        let child = Mock::<1>::new([100]); // child won't match
        let mut child_data = child.data();
        child_data.set_revalidate_result(MockRevalidateResult::Ok);
        let mut it = NotOptimized::new(wcii, child, 200, 1.0, None);

        // Read the single document.
        let doc = it.read().unwrap().unwrap();
        assert_eq!(doc.doc_id, 1);

        // GC the only document so wildcard becomes empty on revalidation.
        gc_document(&context, 1);

        // SAFETY: test-only call with valid context
        let status = unsafe { it.revalidate(context.spec) }.unwrap();
        assert!(
            matches!(status, RQEValidateStatus::Moved { current: None }),
            "Expected Moved {{ current: None }}, got {status:?}"
        );
        assert!(it.at_eof());
    }
}
