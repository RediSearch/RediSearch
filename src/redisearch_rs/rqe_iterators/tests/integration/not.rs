/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::time::Duration;

use rqe_core::DocId;
use rqe_iterators::{
    IteratorType, RQEIterator, RQEIteratorError, RQEValidateStatus, SkipToOutcome,
    id_list::IdListSorted,
    not::Not,
    utils::{DeadlineTimeoutChecker, NoTimeoutChecker},
};

/// Granularity used by the production reducer; tests reuse it for parity.
const CLOCK_CHECK_GRANULARITY: u32 = 5_000;

use rqe_iterators_test_utils::ContractChecker;

use crate::utils::{Mock, MockIteratorError, MockRevalidateResult};

#[test]
fn type_() {
    let child = IdListSorted::new(vec![2, 4, 6]);
    let it = ContractChecker::new(Not::new(child, 10, 1.0, NoTimeoutChecker));
    assert_eq!(it.type_(), IteratorType::Not);
}

// Basic iterator invariants before any read.
#[test]
fn initial_state() {
    let child = IdListSorted::new(vec![2, 4, 6]);
    let it = ContractChecker::new(Not::new(child, 10, 1.0, NoTimeoutChecker));

    // Before first read, cursor is at 0 and we are not at EOF.
    assert_eq!(it.last_doc_id(), 0);
    assert!(!it.at_eof());
    // max_doc_id=10, so NOT can yield at most 10 docs.
    assert_eq!(it.num_estimated(), 10);
}

// Read path with sparse child: NOT must skip exactly the child doc IDs.
#[test]
fn read_skips_child_docs() {
    let child_ids = vec![2, 4, 7];
    let mut it = ContractChecker::new(Not::new(
        IdListSorted::new(child_ids),
        10,
        1.0,
        NoTimeoutChecker,
    ));

    // Child has [2, 4, 7]; complement in [1..=10] is [1, 3, 5, 6, 8, 9, 10].
    let expected = vec![1, 3, 5, 6, 8, 9, 10];

    for &expected_id in &expected {
        let result = it.read();
        let result = result.expect("read() must not error");
        let doc = result.expect("iterator should yield more docs");

        assert_eq!(doc.doc_id, expected_id);
        assert_eq!(it.last_doc_id(), expected_id);
        assert_eq!(it.current().unwrap().doc_id, expected_id);
    }

    // After consuming all expected docs, we must be at EOF
    let result = it.read().unwrap();
    assert!(result.is_none());
    assert!(it.at_eof());
}

// Empty child: NOT behaves like a wildcard over [1, max_doc_id].
#[test]
fn read_with_empty_child_behaves_like_wildcard() {
    // When the child is empty, NOT should yield all doc IDs in [1, max_doc_id]
    let mut it = ContractChecker::new(Not::new(
        IdListSorted::new(vec![]),
        5,
        1.0,
        NoTimeoutChecker,
    ));

    for expected_id in 1u64..=5 {
        let result = it.read();
        let result = result.unwrap();
        let doc = result.unwrap();

        assert_eq!(doc.doc_id, expected_id);
        assert_eq!(it.last_doc_id(), expected_id);
    }

    // Next read should be EOF
    let result = it.read().unwrap();
    assert!(result.is_none());
    assert!(it.at_eof());
}

// Child covers full range: NOT should be empty and report EOF.
#[test]
fn read_with_child_covering_full_range_yields_no_docs() {
    let mut it = ContractChecker::new(Not::new(
        IdListSorted::new(vec![1, 2, 3, 4, 5]),
        5,
        1.0,
        NoTimeoutChecker,
    ));

    // Child already produces 1..=5, so there is no doc left for NOT to return.
    let res = it.read().expect("read() must not error");
    assert!(res.is_none(), "NOT of full-range child should be empty");
    // Iterator still walks up to max_doc_id=5 internally and then reports EOF.
    assert!(it.at_eof());
    assert_eq!(it.last_doc_id(), 5);

    assert!(matches!(it.read(), Ok(None)));
}

// skip_to on ids below, between and inside child: Found vs NotFound semantics.
#[test]
fn skip_to_honours_child_membership() {
    let mut it = ContractChecker::new(Not::new(
        IdListSorted::new(vec![2, 4, 7]),
        10,
        1.0,
        NoTimeoutChecker,
    ));

    // 5 is not in child {2, 4, 7}, so NOT must return Found(5).
    let outcome = it.skip_to(5).expect("skip_to(5) must not error");
    if let Some(SkipToOutcome::Found(doc)) = outcome {
        assert_eq!(doc.doc_id, 5);
        assert_eq!(it.last_doc_id(), 5);
        assert!(!it.at_eof());
    } else {
        panic!("Expected Found outcome for skip_to(5), got {:?}", outcome);
    }

    // 1 is below first child doc (2) and not in child, so Found(1).
    it.rewind();
    let outcome = it.skip_to(1).expect("skip_to(1) must not error");
    if let Some(SkipToOutcome::Found(doc)) = outcome {
        assert_eq!(doc.doc_id, 1);
        assert_eq!(it.last_doc_id(), 1);
    } else {
        panic!("Expected Found outcome for skip_to(1), got {:?}", outcome);
    }

    // 4 is in child, so NOT should skip it and return NotFound(next allowed = 5).
    it.rewind();
    let outcome = it.skip_to(4).expect("skip_to(4) must not error");
    match outcome {
        Some(SkipToOutcome::NotFound(doc)) => {
            assert_eq!(doc.doc_id, 5);
            assert_eq!(it.last_doc_id(), 5);
        }
        other => panic!("Expected NotFound outcome for skip_to(4), got {:?}", other),
    }
}

// skip_to to a child doc at max_doc_id: should return None (EOF) since the doc
// is in child and there's no next doc to return.
#[test]
fn skip_to_child_doc_at_max_docid_returns_none() {
    // Child has doc 10, which is also max_doc_id
    let mut it = ContractChecker::new(Not::new(
        IdListSorted::new(vec![2, 5, 10]),
        10,
        1.0,
        NoTimeoutChecker,
    ));

    // Read first to position before the skip
    let doc = it.read().unwrap().unwrap();
    assert_eq!(doc.doc_id, 1);

    // skip_to(10) - 10 is in child AND is max_doc_id, so there's no next doc
    let outcome = it.skip_to(10).expect("skip_to(10) must not error");
    assert!(
        outcome.is_none(),
        "Expected None when skipping to child doc at max_doc_id"
    );
    assert!(it.at_eof());
}

// skip_to when child is ahead of docId: Case 1 - child.last_doc_id() > doc_id
#[test]
fn skip_to_child_ahead_returns_found() {
    let mut it = ContractChecker::new(Not::new(
        IdListSorted::new(vec![5, 10]),
        15,
        1.0,
        NoTimeoutChecker,
    ));

    // Read once to advance child to doc_id=5
    let doc = it.read().unwrap().unwrap();
    assert_eq!(doc.doc_id, 1);

    // Now child.last_doc_id()=5, skip_to(3) should hit Case 1: child is ahead
    let outcome = it.skip_to(3).expect("skip_to(3) must not error");
    if let Some(SkipToOutcome::Found(doc)) = outcome {
        assert_eq!(doc.doc_id, 3);
    } else {
        panic!(
            "Expected Found outcome for skip_to(3) when child is ahead, got {:?}",
            outcome
        );
    }
}

// skip_to when child is at EOF: Case 1 - child.at_eof()
#[test]
fn skip_to_child_at_eof_returns_found() {
    let mut it = ContractChecker::new(Not::new(
        IdListSorted::new(vec![1, 2]),
        10,
        1.0,
        NoTimeoutChecker,
    ));

    // Exhaust the child by reading past its docs
    while let Some(doc) = it.read().unwrap() {
        if doc.doc_id >= 3 {
            break; // Now child should be at EOF (exhausted [1, 2])
        }
    }

    // Child is now at EOF, skip_to(8) should hit Case 1
    let outcome = it.skip_to(8).expect("skip_to(8) must not error");
    if let Some(SkipToOutcome::Found(doc)) = outcome {
        assert_eq!(doc.doc_id, 8);
    } else {
        panic!(
            "Expected Found outcome for skip_to(8) when child at EOF, got {:?}",
            outcome
        );
    }
}

// skip_to to child's last doc when child is at EOF: should exclude it
#[test]
fn skip_to_child_last_doc_when_at_eof_excludes_it() {
    let mut it = ContractChecker::new(Not::new(
        IdListSorted::new(vec![5, 10]),
        15,
        1.0,
        NoTimeoutChecker,
    ));

    // Read up to doc 9 to exhaust the child
    while let Some(doc) = it.read().unwrap() {
        if doc.doc_id >= 9 {
            break; // Now child is at EOF with last_doc_id=10, NOT is at 9
        }
    }

    // Child is at EOF with last_doc_id=10, NOT is at 9
    // skip_to(10) should NOT return Found(10) because 10 is in the child
    // It should skip to the next valid doc (11) and return NotFound(11)
    let outcome = it.skip_to(10).expect("skip_to(10) must not error");
    match outcome {
        Some(SkipToOutcome::NotFound(doc)) => {
            assert_eq!(doc.doc_id, 11, "Should skip to next valid doc after 10");
        }
        other => panic!(
            "Expected NotFound(11) when skipping to child's last doc at EOF, got {:?}",
            other
        ),
    }
}

// A skip_to whose target is `max_doc_id` *and* held by the child runs the scan
// off the end. The probe target must not be left behind as this iterator's
// position: a parent pairing `last_doc_id()` with `current()` would then read a
// position it has no result for.
#[test]
fn skip_to_onto_child_doc_at_max_docid_leaves_the_position_alone() {
    let mut it = ContractChecker::new(Not::new(
        IdListSorted::new(vec![4, 10]),
        10,
        1.0,
        NoTimeoutChecker,
    ));

    let doc = it.read().expect("read() must not error").unwrap();
    assert_eq!(doc.doc_id, 1);

    // 10 is both `max_doc_id` and a child doc, so there is no result at or after
    // it: the scan for the next valid doc immediately runs past the end.
    let res = it.skip_to(10).expect("skip_to(10) must not error");
    assert!(res.is_none(), "10 is excluded and nothing follows it");

    assert!(it.at_eof());
    assert!(
        it.current().is_none(),
        "no result was produced, so there is no current one",
    );
    assert_eq!(
        it.last_doc_id(),
        1,
        "the probe target must not become the position of a skip that \
         produced no result",
    );
}

// skip_to past max_doc_id: should return None and move to EOF.
#[test]
fn skip_to_past_max_docid_returns_none_and_sets_eof() {
    let mut it = ContractChecker::new(Not::new(
        IdListSorted::new(vec![2, 4, 7]),
        10,
        1.0,
        NoTimeoutChecker,
    ));

    let doc = it.read().expect("read() must not error").unwrap();
    assert_eq!(doc.doc_id, 1);

    // 11 > max_doc_id=10, so there is no valid target and we end at EOF.
    let res = it.skip_to(11).expect("skip_to(11) must not error");
    assert!(res.is_none());
    assert!(it.at_eof());
    assert_eq!(
        it.last_doc_id(),
        1,
        "a skip_to that returns None produced no result, so it must leave the \
         position where it was",
    );

    assert!(matches!(it.read(), Ok(None)));
    assert!(it.current().is_none());
}

// rewind should restore the initial state and read sequence.
#[test]
fn rewind_resets_state() {
    let mut it = ContractChecker::new(Not::new(
        IdListSorted::new(vec![2, 4, 7]),
        10,
        1.0,
        NoTimeoutChecker,
    ));

    // For child [2, 4, 7] and max_doc_id=10, the first two NOT results are 1 and 3.
    for expected in [1u64, 3] {
        let doc = it.read().unwrap().unwrap();
        assert_eq!(doc.doc_id, expected);
    }
    assert_eq!(it.last_doc_id(), 3);
    assert!(!it.at_eof());

    it.rewind();

    assert_eq!(it.last_doc_id(), 0);
    assert!(!it.at_eof());

    let doc = it.read().unwrap().unwrap();
    assert_eq!(doc.doc_id, 1);
    assert_eq!(it.last_doc_id(), 1);
}

// Child revalidate Ok: NOT still excludes the child's doc IDs.
#[test]
fn revalidate_child_ok_preserves_exclusions() {
    let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
    let child = Mock::new([2, 4]);
    let mut it = ContractChecker::new(Not::new(child, 5, 1.0, NoTimeoutChecker));

    let status = it
        .revalidate(&*mock_ctx.spec_read())
        .expect("revalidate() failed");
    assert_eq!(status, RQEValidateStatus::Ok);

    let mut seen = Vec::new();
    while let Some(doc) = it.read().unwrap() {
        seen.push(doc.doc_id);
    }

    // Child has [2, 4] in [1..=5], so NOT must yield the complement [1, 3, 5].
    assert_eq!(seen, vec![1, 3, 5]);
}

// Child revalidate Aborted: NOT degenerates to wildcard (empty child).
#[test]
fn revalidate_child_aborted_replaces_child_with_empty() {
    let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
    let child = Mock::new([2, 4]);
    let mut data = child.data();
    data.set_revalidate_result(MockRevalidateResult::Abort);
    let mut it = ContractChecker::new(Not::new(child, 5, 1.0, NoTimeoutChecker));

    let status = it
        .revalidate(&*mock_ctx.spec_read())
        .expect("revalidate() failed");
    assert_eq!(status, RQEValidateStatus::Ok);

    let mut seen = Vec::new();
    while let Some(doc) = it.read().unwrap() {
        seen.push(doc.doc_id);
    }

    // After child aborts, NOT behaves like having an empty child: [1..=5] is returned.
    assert_eq!(seen, vec![1, 2, 3, 4, 5]);
}

// Child revalidate Moved on fresh iterator: should not panic.
#[test]
fn revalidate_child_moved_on_fresh_iterator() {
    let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
    let child = Mock::new([2, 4]);
    let mut data = child.data();
    data.set_revalidate_result(MockRevalidateResult::Move);
    let mut it = ContractChecker::new(Not::new(child, 5, 1.0, NoTimeoutChecker));

    // Revalidate before any read/skip_to - both iterators at doc_id = 0
    let status = it
        .revalidate(&*mock_ctx.spec_read())
        .expect("revalidate() failed");
    assert_eq!(status, RQEValidateStatus::Ok);

    // Iterator should still work correctly after revalidate
    let mut seen = Vec::new();
    while let Some(doc) = it.read().unwrap() {
        seen.push(doc.doc_id);
    }

    // Child has [2, 4] in [1..=5], so NOT must yield the complement [1, 3, 5].
    assert_eq!(seen, vec![1, 3, 5]);
}

// Child revalidate Moved after read: child ahead, should not panic.
#[test]
fn revalidate_child_moved_after_read_with_child_ahead() {
    let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
    let child = Mock::new([5, 10]);
    let mut data = child.data();
    let mut it = ContractChecker::new(Not::new(child, 15, 1.0, NoTimeoutChecker));

    // Read first doc (1) - child will be at 5, NOT at 1
    let doc = it.read().expect("read() failed").expect("expected doc");
    assert_eq!(doc.doc_id, 1);
    assert_eq!(it.last_doc_id(), 1);

    // Now child is ahead (at 5) and NOT is at 1
    // Simulate child moving forward during revalidate (child advances from 5 to 10)
    data.set_revalidate_result(MockRevalidateResult::Move);

    // This should not panic - child is ahead of NOT's position
    let status = it
        .revalidate(&*mock_ctx.spec_read())
        .expect("revalidate() failed");
    assert_eq!(status, RQEValidateStatus::Ok);

    // Continue reading - should still work correctly
    let mut seen = vec![1]; // Already read 1
    while let Some(doc) = it.read().unwrap() {
        seen.push(doc.doc_id);
    }

    // After revalidate, child moved from 5 to 10, so only 10 is excluded now
    // NOT yields [1,2,3,4,5,6,7,8,9,11,12,13,14,15] (5 is now included!)
    assert_eq!(seen, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 11, 12, 13, 14, 15]);
}

// Child revalidate Moved after skip_to: child ahead, should not panic.
#[test]
fn revalidate_child_moved_after_skip_to_with_child_ahead() {
    let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
    let child = Mock::new([8, 15]);
    let mut data = child.data();
    let mut it = ContractChecker::new(Not::new(child, 20, 1.0, NoTimeoutChecker));

    // Skip to 3 - child will be at 8, NOT at 3
    let outcome = it
        .skip_to(3)
        .expect("skip_to() failed")
        .expect("expected outcome");
    match outcome {
        SkipToOutcome::Found(doc) => assert_eq!(doc.doc_id, 3),
        _ => panic!("Expected Found outcome"),
    }
    assert_eq!(it.last_doc_id(), 3);

    // Now child is ahead (at 8) and NOT is at 3
    // Simulate child moving forward during revalidate (child advances from 8 to 15)
    data.set_revalidate_result(MockRevalidateResult::Move);

    // This should not panic - child is ahead of NOT's position
    let status = it
        .revalidate(&*mock_ctx.spec_read())
        .expect("revalidate() failed");
    assert_eq!(status, RQEValidateStatus::Ok);

    // Continue reading - should still work correctly
    let mut seen = vec![3]; // Already at 3
    while let Some(doc) = it.read().unwrap() {
        seen.push(doc.doc_id);
    }

    // After revalidate, child moved from 8 to 15, so only 15 is excluded now
    // NOT at 3 should yield [4,5,6,7,8,9,10,11,12,13,14,16,17,18,19,20] (8 is now included!)
    assert_eq!(
        seen,
        vec![3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 16, 17, 18, 19, 20]
    );
}

// Timeout propagation: child timeout during read() should propagate to NOT iterator.
#[test]
fn read_propagates_child_timeout() {
    let child = Mock::new([3, 5]);
    let mut data = child.data();
    // Set child to return timeout error when it reaches EOF
    data.set_error_at_done(Some(MockIteratorError::TimeoutError(None)));
    let mut it = ContractChecker::new(Not::new(child, 6, 1.0, NoTimeoutChecker));

    // Read docs that are NOT in child: [1, 2, 4, 6]
    // Child has [3, 5]. When NOT reads doc 6, child.read() is called to check
    // if 6 is in child. Child advances to EOF and returns timeout error.
    let doc = it.read().unwrap().unwrap();
    assert_eq!(doc.doc_id, 1);

    let doc = it.read().unwrap().unwrap();
    assert_eq!(doc.doc_id, 2);

    // At doc_id=3, NOT needs to check child which has 3, so it skips
    // At doc_id=4, child is at 5, so NOT returns 4
    let doc = it.read().unwrap().unwrap();
    assert_eq!(doc.doc_id, 4);

    // At doc_id=5, NOT skips (in child)
    // At doc_id=6, NOT calls child.read() which goes past EOF and returns timeout
    let result = it.read();
    assert!(
        matches!(result, Err(RQEIteratorError::TimedOut)),
        "Expected timeout error to propagate from child during read, got {:?}",
        result
    );
}

// Timeout propagation: child timeout during skip_to() should propagate to NOT iterator.
#[test]
fn skip_to_propagates_child_timeout() {
    let child = Mock::new([2, 4, 6]);
    let mut data = child.data();
    // Set child to return timeout error when it reaches EOF
    data.set_error_at_done(Some(MockIteratorError::TimeoutError(None)));
    let mut it = ContractChecker::new(Not::new(child, 10, 1.0, NoTimeoutChecker));

    // skip_to(7) - child has [2,4,6], child.last_doc_id()=0 < 7, so we call
    // child.skip_to(7) which will go past child's last doc (6) and hit EOF,
    // triggering the timeout error.
    let result = it.skip_to(7);
    assert!(
        matches!(result, Err(RQEIteratorError::TimedOut)),
        "Expected timeout error to propagate from child during skip_to, got {:?}",
        result
    );
}

// skip_to when already at EOF should return None immediately.
#[test]
fn skip_to_at_eof_returns_none() {
    let mut it = ContractChecker::new(Not::new(
        IdListSorted::new(vec![1, 2, 3, 4, 5]),
        5,
        1.0,
        NoTimeoutChecker,
    ));

    // Exhaust the iterator - child covers full range so NOT produces nothing
    assert!(it.read().unwrap().is_none());
    assert!(it.at_eof());

    // Now call skip_to on an already-EOF iterator
    let result = it.skip_to(6).unwrap();
    assert!(
        result.is_none(),
        "skip_to on EOF iterator should return None"
    );
    assert!(it.at_eof());
}

// skip_to when child is behind and child.skip_to returns None (child at EOF).
// This exercises Case 2 where child.skip_to returns None.
#[test]
fn skip_to_child_behind_child_skip_returns_eof() {
    // Child has [2], max_doc_id=10
    let mut it = ContractChecker::new(Not::new(
        IdListSorted::new(vec![2]),
        10,
        1.0,
        NoTimeoutChecker,
    ));

    // Read first doc (1) to advance child to position 2
    let doc = it.read().unwrap().unwrap();
    assert_eq!(doc.doc_id, 1);

    // Now child.last_doc_id()=2, NOT is at 1.
    // skip_to(5): child.last_doc_id()=2 < 5, so we enter Case 2.
    // child.skip_to(5) will return None (child only has [2], past end).
    // So NOT returns Found(5).
    let outcome = it.skip_to(5).expect("skip_to(5) must not error");
    if let Some(SkipToOutcome::Found(doc)) = outcome {
        assert_eq!(doc.doc_id, 5);
        assert_eq!(it.last_doc_id(), 5);
    } else {
        panic!(
            "Expected Found(5) when child.skip_to returns EOF, got {:?}",
            outcome
        );
    }
}

#[test]
#[cfg_attr(miri, ignore = "Too slow to be run under miri.")]
fn read_timeout_via_timeout_ctx() {
    let mut child_doc_ids = [0; 5_000];
    for i in 0..5_000 {
        child_doc_ids[i] = (i + 1) as DocId;
    }
    let child = Mock::new(child_doc_ids);
    let mut data = child.data();
    // Set child to return timeout error when it reaches EOF
    data.add_delay_since_index(1, Duration::from_micros(100));

    let mut it = ContractChecker::new(Not::new(
        child,
        10_000,
        1.0,
        DeadlineTimeoutChecker::new(Duration::from_micros(50), CLOCK_CHECK_GRANULARITY),
    ));

    let result = it.read();
    assert!(
        matches!(result, Err(RQEIteratorError::TimedOut)),
        "expected timeout due to timeout context in Not iterator triggered: result = {result:?}",
    );
}

#[test]
#[cfg_attr(miri, ignore = "Too slow to be run under miri.")]
fn skip_to_timeout_via_timeout_ctx() {
    let child = Mock::new([5_001]);
    let mut data = child.data();
    // Set child to return timeout error when it reaches EOF
    data.add_delay_since_index(1, Duration::from_micros(100));

    let mut it = ContractChecker::new(Not::new(
        child,
        10_000,
        1.0,
        DeadlineTimeoutChecker::new(Duration::from_micros(50), CLOCK_CHECK_GRANULARITY),
    ));

    for idx in 1..=4_999 {
        let outcome = it
            .skip_to(idx as u64)
            .expect(&format!("iteration #{idx} not to timeout yet"));
        if let Some(SkipToOutcome::Found(doc)) = outcome {
            assert_eq!(doc.doc_id, idx as u64);
            assert_eq!(it.last_doc_id(), idx as u64);
            assert!(!it.at_eof());
        } else {
            panic!("Expected Found outcome for skip_to(5), got {:?}", outcome);
        }
    }

    assert!(!it.at_eof(), "did not yet expect to EOF");

    assert!(
        matches!(it.skip_to(6_000), Err(RQEIteratorError::TimedOut)),
        "expected timeout due to timeout context in Not iterator triggered"
    );

    // The timeout latched `forced_eof`, so nothing more will be produced — but
    // the iterator is still positioned on its last result, and `at_eof()` is the
    // negation of `current()`. The read that runs past the end is what flips it.
    assert!(!it.at_eof(), "still positioned on its last result");
    assert!(it.current().is_some());

    assert!(
        it.read()
            .expect("a latched forced_eof yields None rather than erroring")
            .is_none()
    );
    assert!(
        it.at_eof(),
        "iterator is expected to EOF once timed out via timeout context"
    );
    assert!(it.current().is_none());

    it.rewind();
    assert!(
        !it.at_eof(),
        "rewind should have also cleared the force EOF"
    );
    assert_eq!(
        1,
        it.read()
            .expect("rewind should have allowed reading once again")
            .expect("as such we expect a result here")
            .doc_id,
        "rewind should have allowed us to start reading again from start, despites earlier timeout"
    )
    // that said... internal timeout context is _not_ reset,
    // so it is bound to timeout once you make the required amount of read/skip_to calls...
}

#[test]
fn not_upholds_current_contract() {
    use rqe_iterators_test_utils::{assert_current_contract, assert_current_contract_via_skip_to};
    // Not over [2, 4] within 1..=5 yields the complement.
    let mut it = ContractChecker::new(rqe_iterators::not::Not::new(
        crate::utils::Mock::new([2u64, 4]),
        5,
        1.0,
        timeout::NoTimeoutChecker,
    ));
    assert_eq!(assert_current_contract(&mut it), [1, 3, 5]);
    assert_current_contract_via_skip_to(&mut it, 6);
}

// ---------------------------------------------------------------------------
// A skip_to that carries no result must not claim the probed id as its position
// ---------------------------------------------------------------------------

/// A timeout during `skip_to` returns no result, so the probe target must not
/// become the iterator's position — a parent pairs `last_doc_id()` with
/// `current()` and reads a match as a promise of a result there.
///
/// `Empty` as the child puts this on the branch that accepts `doc_id` outright,
/// which is where the position used to be published before the timeout check.
#[test]
fn skip_to_timing_out_leaves_the_position_alone() {
    use rqe_iterators::empty::Empty;

    let mut it = ContractChecker::new(Not::new(
        Empty,
        10,
        1.0,
        // Already elapsed, and checked on every call.
        DeadlineTimeoutChecker::new(Duration::ZERO, 1),
    ));

    let rc = it.skip_to(5);
    assert!(
        matches!(rc, Err(RQEIteratorError::TimedOut)),
        "expected a timeout, got {rc:?}",
    );
    assert_eq!(
        it.last_doc_id(),
        0,
        "no result was produced, so the probe target must not become the position",
    );
}

/// The same for the tail path, where the scan for the next valid document fails:
/// the position is published so the scan can start from it, and has to go back.
#[test]
fn skip_to_whose_scan_fails_leaves_the_position_alone() {
    // 4 is in the child, so skipping to it takes the tail path that scans for the
    // next valid document, and that scan reads the child past its end.
    let child = Mock::new([4u64]);
    let mut child_data = child.data();
    let mut it = ContractChecker::new(Not::new(child, 10, 1.0, NoTimeoutChecker));

    let doc = it
        .read()
        .expect("read must not fail")
        .expect("1 is not in the child");
    assert_eq!(doc.doc_id, 1);

    child_data.set_error_at_done(Some(MockIteratorError::TimeoutError(None)));

    let rc = it.skip_to(4);
    assert!(
        matches!(rc, Err(RQEIteratorError::TimedOut)),
        "expected the child's error to propagate, got {rc:?}",
    );
    assert_eq!(
        it.last_doc_id(),
        1,
        "the failed scan produced no result, so the position stays where it was",
    );
}

mod via_resume {
    use super::*;
    use rqe_iterators::TypeErasedRQEIterator;
    use rqe_iterators_test_utils::{ResumeOutcomeExt, revalidate_via_resume};

    #[test]
    fn revalidate_child_ok_preserves_exclusions() {
        let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
        let child = Mock::new([2, 4]);
        let it = Not::new(child, 5, 1.0, NoTimeoutChecker);

        let guard = mock_ctx.spec_read();
        let mut it = revalidate_via_resume(TypeErasedRQEIterator::new(Box::new(it)), &guard)
            .expect("resume failed")
            .expect_ok();

        let mut seen = Vec::new();
        while let Some(doc) = it.read().unwrap() {
            seen.push(doc.doc_id);
        }
        assert_eq!(seen, vec![1, 3, 5]);
    }

    #[test]
    fn revalidate_child_aborted_replaces_child_with_empty() {
        let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
        let child = Mock::new([2, 4]);
        let mut data = child.data();
        data.set_revalidate_result(MockRevalidateResult::Abort);
        let it = Not::new(child, 5, 1.0, NoTimeoutChecker);

        let guard = mock_ctx.spec_read();
        let mut it = revalidate_via_resume(TypeErasedRQEIterator::new(Box::new(it)), &guard)
            .expect("resume failed")
            .expect_ok();

        let mut seen = Vec::new();
        while let Some(doc) = it.read().unwrap() {
            seen.push(doc.doc_id);
        }
        assert_eq!(seen, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn revalidate_child_moved_on_fresh_iterator() {
        let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
        let child = Mock::new([2, 4]);
        let mut data = child.data();
        data.set_revalidate_result(MockRevalidateResult::Move);
        let it = Not::new(child, 5, 1.0, NoTimeoutChecker);

        let guard = mock_ctx.spec_read();
        let mut it = revalidate_via_resume(TypeErasedRQEIterator::new(Box::new(it)), &guard)
            .expect("resume failed")
            .expect_ok();

        let mut seen = Vec::new();
        while let Some(doc) = it.read().unwrap() {
            seen.push(doc.doc_id);
        }
        assert_eq!(seen, vec![1, 3, 5]);
    }

    #[test]
    fn revalidate_child_moved_after_read_with_child_ahead() {
        let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
        let child = Mock::new([5, 10]);
        let mut data = child.data();
        let mut it = Not::new(child, 15, 1.0, NoTimeoutChecker);

        let doc = it.read().expect("read() failed").expect("expected doc");
        assert_eq!(doc.doc_id, 1);
        assert_eq!(it.last_doc_id(), 1);

        data.set_revalidate_result(MockRevalidateResult::Move);
        let guard = mock_ctx.spec_read();
        let mut it = revalidate_via_resume(TypeErasedRQEIterator::new(Box::new(it)), &guard)
            .expect("resume failed")
            .expect_ok();

        let mut seen = vec![1];
        while let Some(doc) = it.read().unwrap() {
            seen.push(doc.doc_id);
        }
        assert_eq!(seen, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 11, 12, 13, 14, 15]);
    }

    #[test]
    fn revalidate_child_moved_after_skip_to_with_child_ahead() {
        let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
        let child = Mock::new([8, 15]);
        let mut data = child.data();
        let mut it = Not::new(child, 20, 1.0, NoTimeoutChecker);

        let outcome = it
            .skip_to(3)
            .expect("skip_to() failed")
            .expect("expected outcome");
        match outcome {
            SkipToOutcome::Found(doc) => assert_eq!(doc.doc_id, 3),
            _ => panic!("Expected Found outcome"),
        }
        assert_eq!(it.last_doc_id(), 3);

        data.set_revalidate_result(MockRevalidateResult::Move);
        let guard = mock_ctx.spec_read();
        let mut it = revalidate_via_resume(TypeErasedRQEIterator::new(Box::new(it)), &guard)
            .expect("resume failed")
            .expect_ok();

        let mut seen = vec![3];
        while let Some(doc) = it.read().unwrap() {
            seen.push(doc.doc_id);
        }
        assert_eq!(
            seen,
            vec![3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 16, 17, 18, 19, 20]
        );
    }

    /// The suspend/resume cycle must reuse the allocation: the FFI wrapper and
    /// delegating parents cache raw pointers into the iterator's storage, and a
    /// rebuilt box would dangle them.
    #[test]
    fn resume_preserves_box_address() {
        use rqe_iterators::{RQEIteratorBoxed, RQESuspendedIterator};

        let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
        let guard = mock_ctx.spec_read();
        let child = Mock::new([2, 4]);
        let mut it = Box::new(Not::new(child, 5, 1.0, NoTimeoutChecker));
        let doc = it.read().expect("read failed").expect("expected doc");
        assert_eq!(doc.doc_id, 1);
        let addr_before = &*it as *const _ as usize;

        let suspended = it.suspend();
        assert_eq!(
            &*suspended as *const _ as usize, addr_before,
            "suspend must reuse the allocation",
        );
        let mut active = match suspended.resume(&guard).expect("resume failed") {
            rqe_iterators::ResumeOutcome::Ok(a) => a,
            rqe_iterators::ResumeOutcome::Moved(_) => panic!("expected Ok, got Moved"),
            rqe_iterators::ResumeOutcome::Aborted => panic!("expected Ok, got Aborted"),
        };
        assert_eq!(
            &*active as *const _ as usize, addr_before,
            "resume must reuse the allocation",
        );

        let doc = active.read().expect("read failed").expect("expected doc");
        assert_eq!(doc.doc_id, 3, "the exclusion set survives the cycle");
    }

    /// A type-erased child crosses the suspend boundary through a vtable swap,
    /// not a byte cast — the active and suspended erased forms are different
    /// `dyn` types. A concrete-child round trip cannot catch a wrong-vtable bug;
    /// this one (also run under miri) can.
    #[test]
    fn suspend_resume_with_type_erased_child_survives() {
        use rqe_iterators::{RQEIteratorBoxed, RQESuspendedIterator};

        let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
        let guard = mock_ctx.spec_read();
        let child = TypeErasedRQEIterator::new(Box::new(Mock::new([2u64, 4])));
        let mut it = Box::new(Not::new(child, 5, 1.0, NoTimeoutChecker));
        let doc = it.read().expect("read failed").expect("expected doc");
        assert_eq!(doc.doc_id, 1);

        let mut active = match it.suspend().resume(&guard).expect("resume failed") {
            rqe_iterators::ResumeOutcome::Ok(a) => a,
            rqe_iterators::ResumeOutcome::Moved(_) => panic!("expected Ok, got Moved"),
            rqe_iterators::ResumeOutcome::Aborted => panic!("expected Ok, got Aborted"),
        };

        let mut seen = vec![1];
        while let Some(doc) = active.read().expect("read failed") {
            seen.push(doc.doc_id);
        }
        assert_eq!(seen, vec![1, 3, 5]);
    }

    /// If the virtual sentinel is no longer virtual — a consumer replaced it via
    /// the mutable `current()`/`read()`/`skip_to` handout — resume cannot
    /// re-validate it, so it aborts the whole iterator (returns `Aborted`, not an
    /// error), mirroring `Optional::resume`.
    #[test]
    fn resume_aborts_when_result_no_longer_virtual() {
        use rqe_iterators::{RQEIteratorBoxed, RQESuspendedIterator};

        let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
        let guard = mock_ctx.spec_read();
        let child = Mock::new([2, 4]);
        let mut it = Box::new(Not::new(child, 5, 1.0, NoTimeoutChecker));
        let doc = it.read().expect("read failed").expect("expected doc");
        assert_eq!(doc.doc_id, 1);
        // Simulate a consumer swapping the sentinel for a real, index-backed result.
        *doc = index_result::RSIndexResult::build_numeric(1.0).build();

        let outcome = it
            .suspend()
            .resume(&guard)
            .expect("resume must not surface an error");
        assert!(
            matches!(outcome, rqe_iterators::ResumeOutcome::Aborted),
            "a non-virtual sentinel cannot be re-validated, so resume aborts",
        );
    }

    /// A failing child resume leaves `Not`'s child slot consumed, and the
    /// bespoke teardown that frees the reused allocation from there is reachable
    /// no other way: every other path either keeps a live child or hands the box
    /// back. A teardown that dropped the moved-from child, or reclaimed the
    /// allocation at a type whose drop glue disagrees with what the slot holds,
    /// only shows up here — and only under miri.
    #[test]
    fn resume_propagates_child_error() {
        use rqe_iterators::{RQEIteratorBoxed, RQESuspendedIterator};

        let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
        let guard = mock_ctx.spec_read();
        let child = Mock::new([2, 4]);
        child
            .data()
            .set_error_on_resume(Some(MockIteratorError::TimeoutError(None)));
        let mut it = Box::new(Not::new(child, 5, 1.0, NoTimeoutChecker));

        let doc = it.read().expect("read failed").expect("expected doc");
        assert_eq!(doc.doc_id, 1);

        assert!(
            matches!(it.suspend().resume(&guard), Err(RQEIteratorError::TimedOut)),
            "a child resume error must propagate out of Not::resume",
        );
    }

    /// The same, with a type-erased child: the failing resume is dispatched
    /// through the erased vtable, so the slot the teardown reinitialises held a
    /// `Box<dyn …>` rather than an inline child.
    #[test]
    fn resume_propagates_type_erased_child_error() {
        use rqe_iterators::{RQEIteratorBoxed, RQESuspendedIterator};

        let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
        let guard = mock_ctx.spec_read();
        let child = Mock::new([2u64, 4]);
        child
            .data()
            .set_error_on_resume(Some(MockIteratorError::TimeoutError(None)));
        let mut it = Box::new(Not::new(
            TypeErasedRQEIterator::new(Box::new(child)),
            5,
            1.0,
            NoTimeoutChecker,
        ));

        let doc = it.read().expect("read failed").expect("expected doc");
        assert_eq!(doc.doc_id, 1);

        assert!(
            matches!(it.suspend().resume(&guard), Err(RQEIteratorError::TimedOut)),
            "a child resume error must propagate out of Not::resume",
        );
    }

    /// FFI profile printing reads the suspended form's position and estimate
    /// without resuming it, so those accessors must report what the active form
    /// held rather than a default or the child's own view.
    #[test]
    fn suspended_accessors_report_the_pre_suspend_position_and_estimate() {
        use rqe_iterators::{RQEIteratorBoxed, RQESuspendedIterator};

        let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
        let guard = mock_ctx.spec_read();
        let mut it = Box::new(Not::new(Mock::new([2, 4]), 5, 1.0, NoTimeoutChecker));

        let doc = it.read().expect("read failed").expect("expected doc");
        assert_eq!(doc.doc_id, 1);
        assert_eq!(it.last_doc_id(), 1);
        assert_eq!(it.num_estimated(), 5);

        let suspended = it.suspend();
        assert_eq!(
            RQESuspendedIterator::last_doc_id(&*suspended),
            1,
            "the suspended form keeps NOT's own cursor, not the child's",
        );
        assert_eq!(RQESuspendedIterator::num_estimated(&*suspended), 5);

        let mut active = match suspended.resume(&guard).expect("resume failed") {
            rqe_iterators::ResumeOutcome::Ok(a) => a,
            rqe_iterators::ResumeOutcome::Moved(_) => panic!("expected Ok, got Moved"),
            rqe_iterators::ResumeOutcome::Aborted => panic!("expected Ok, got Aborted"),
        };
        assert_eq!(active.last_doc_id(), 1);
        let doc = active.read().expect("read failed").expect("expected doc");
        assert_eq!(doc.doc_id, 3);
    }

    /// An aborting *type-erased* child is consumed inside the erased vtable, and
    /// the empty arm written over that slot is typed as the resumed child. A
    /// teardown that dropped the moved-from erased child, or that rebuilt the
    /// wrapper instead of reusing its allocation, is invisible to the
    /// concrete-child abort test.
    #[test]
    fn abort_of_type_erased_child_reuses_the_allocation() {
        use rqe_iterators::{RQEIteratorBoxed, RQESuspendedIterator};

        let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
        let guard = mock_ctx.spec_read();
        let child = Mock::new([2u64, 4]);
        child
            .data()
            .set_revalidate_result(MockRevalidateResult::Abort);
        let mut it = Box::new(Not::new(
            TypeErasedRQEIterator::new(Box::new(child)),
            5,
            1.0,
            NoTimeoutChecker,
        ));

        let doc = it.read().expect("read failed").expect("expected doc");
        assert_eq!(doc.doc_id, 1);
        let addr_before = &*it as *const _ as usize;

        let suspended = it.suspend();
        assert_eq!(
            &*suspended as *const _ as usize, addr_before,
            "suspend must reuse the allocation",
        );
        let mut active = match suspended.resume(&guard).expect("resume failed") {
            rqe_iterators::ResumeOutcome::Ok(a) => a,
            rqe_iterators::ResumeOutcome::Moved(_) => panic!("expected Ok, got Moved"),
            rqe_iterators::ResumeOutcome::Aborted => {
                panic!("NOT absorbs a child abort rather than aborting itself")
            }
        };
        assert_eq!(
            &*active as *const _ as usize, addr_before,
            "the abort teardown must reuse the allocation too",
        );

        let mut seen = vec![1];
        while let Some(doc) = active.read().expect("read failed") {
            seen.push(doc.doc_id);
        }
        assert_eq!(
            seen,
            vec![1, 2, 3, 4, 5],
            "the aborted child excludes nothing any more",
        );
    }

    /// Once a child has aborted, the slot holds the empty arm — and a further
    /// resume has to walk that arm, which has no child to dispatch through.
    /// Nothing else reaches it for `Not`, so an empty slot mistaken for a live
    /// child would surface only on a second cycle.
    #[test]
    fn resume_over_an_already_empty_child_slot() {
        use rqe_iterators::{RQEIteratorBoxed, RQESuspendedIterator};

        let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
        let guard = mock_ctx.spec_read();
        let child = Mock::new([2, 4]);
        child
            .data()
            .set_revalidate_result(MockRevalidateResult::Abort);
        let mut it = Box::new(Not::new(child, 5, 1.0, NoTimeoutChecker));

        let doc = it.read().expect("read failed").expect("expected doc");
        assert_eq!(doc.doc_id, 1);

        // First cycle: the child aborts and is replaced by the empty arm.
        let active = match it.suspend().resume(&guard).expect("resume failed") {
            rqe_iterators::ResumeOutcome::Ok(a) => a,
            rqe_iterators::ResumeOutcome::Moved(_) => panic!("expected Ok, got Moved"),
            rqe_iterators::ResumeOutcome::Aborted => {
                panic!("NOT absorbs a child abort rather than aborting itself")
            }
        };
        assert!(active.child().is_none(), "the aborted child is dropped");

        // Second cycle: the empty arm resumes into itself.
        let mut active = match active.suspend().resume(&guard).expect("resume failed") {
            rqe_iterators::ResumeOutcome::Ok(a) => a,
            rqe_iterators::ResumeOutcome::Moved(_) => panic!("an empty child cannot move"),
            rqe_iterators::ResumeOutcome::Aborted => panic!("an empty child cannot abort"),
        };
        assert!(active.child().is_none(), "the child stays empty");

        let mut seen = vec![1];
        while let Some(doc) = active.read().expect("read failed") {
            seen.push(doc.doc_id);
        }
        assert_eq!(seen, vec![1, 2, 3, 4, 5]);
    }

    /// While the legacy `revalidate` and the new `resume` both exist they are two
    /// spellings of one transition, and sibling iterators in this stack have
    /// already drifted between them — a child outcome absorbed on one path and
    /// forwarded on the other, or a re-read done only once. Drive both over
    /// identical setups and require the outcome *and* everything read afterwards
    /// to agree.
    #[test]
    fn revalidate_and_resume_agree_on_every_child_outcome() {
        use rqe_iterators::{RQEIteratorBoxed, RQESuspendedIterator};

        /// The outcome shape the two paths share.
        #[derive(Debug, PartialEq, Eq)]
        enum Outcome {
            Ok,
            Moved,
            Aborted,
            Failed,
        }

        fn drain<'index>(it: &mut impl RQEIterator<'index>) -> Vec<DocId> {
            let mut seen = Vec::new();
            while let Some(doc) = it.read().expect("read failed") {
                seen.push(doc.doc_id);
            }
            seen
        }

        for child_outcome in [
            MockRevalidateResult::Ok,
            MockRevalidateResult::Move,
            MockRevalidateResult::Abort,
            MockRevalidateResult::TimedOut,
        ] {
            // NOT sits on doc 1 with the child ahead on 5 — the shape the
            // child-ahead invariant is stated over.
            let build = || {
                let child = Mock::new([5u64, 10]);
                child.data().set_revalidate_result(child_outcome);
                let mut it = Box::new(Not::new(child, 15, 1.0, NoTimeoutChecker));
                let doc = it.read().expect("read failed").expect("expected doc");
                assert_eq!(doc.doc_id, 1);
                it
            };

            let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
            let guard = mock_ctx.spec_read();

            let mut legacy = build();
            let legacy_outcome = match legacy.revalidate(&guard) {
                Ok(RQEValidateStatus::Ok) => Outcome::Ok,
                Ok(RQEValidateStatus::Moved { .. }) => Outcome::Moved,
                Ok(RQEValidateStatus::Aborted) => Outcome::Aborted,
                Err(_) => Outcome::Failed,
            };
            let legacy_tail = match legacy_outcome {
                Outcome::Ok | Outcome::Moved => drain(&mut *legacy),
                Outcome::Aborted | Outcome::Failed => Vec::new(),
            };

            let (resumed_outcome, resumed_tail) = match build().suspend().resume(&guard) {
                Ok(rqe_iterators::ResumeOutcome::Ok(mut it)) => (Outcome::Ok, drain(&mut *it)),
                Ok(rqe_iterators::ResumeOutcome::Moved(mut it)) => {
                    (Outcome::Moved, drain(&mut *it))
                }
                Ok(rqe_iterators::ResumeOutcome::Aborted) => (Outcome::Aborted, Vec::new()),
                Err(_) => (Outcome::Failed, Vec::new()),
            };

            assert_eq!(
                legacy_outcome, resumed_outcome,
                "child {child_outcome:?}: the two paths disagree on the outcome",
            );
            assert_eq!(
                legacy_tail, resumed_tail,
                "child {child_outcome:?}: the two paths disagree on what is read afterwards",
            );
        }
    }
}
