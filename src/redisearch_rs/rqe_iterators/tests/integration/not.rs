/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::time::Duration;

use rqe_iterators::{
    RQEIterator, RQEIteratorError, RQEValidateStatus, SkipToOutcome, id_list::IdListSorted,
    not::Not,
};

use crate::utils::{Mock, MockIteratorError, MockRevalidateResult};

/// Duration chosen to be big enough such that it will not be reached.
const NOT_ITERATOR_LARGE_TIMEOUT: Duration = Duration::from_secs(300);

// Basic iterator invariants before any read.
#[test]
fn initial_state() {
    let child = IdListSorted::new(vec![2, 4, 6]);
    let it = Not::new(child, 10, 1.0, NOT_ITERATOR_LARGE_TIMEOUT, false);

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
    let mut it = Not::new(
        IdListSorted::new(child_ids),
        10,
        1.0,
        NOT_ITERATOR_LARGE_TIMEOUT,
        false,
    );

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
    let mut it = Not::new(
        IdListSorted::new(vec![]),
        5,
        1.0,
        NOT_ITERATOR_LARGE_TIMEOUT,
        false,
    );

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
    let mut it = Not::new(
        IdListSorted::new(vec![1, 2, 3, 4, 5]),
        5,
        1.0,
        NOT_ITERATOR_LARGE_TIMEOUT,
        false,
    );

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
    let mut it = Not::new(
        IdListSorted::new(vec![2, 4, 7]),
        10,
        1.0,
        NOT_ITERATOR_LARGE_TIMEOUT,
        false,
    );

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
    let mut it = Not::new(
        IdListSorted::new(vec![2, 5, 10]),
        10,
        1.0,
        NOT_ITERATOR_LARGE_TIMEOUT,
        false,
    );

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
    let mut it = Not::new(
        IdListSorted::new(vec![5, 10]),
        15,
        1.0,
        NOT_ITERATOR_LARGE_TIMEOUT,
        false,
    );

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
    let mut it = Not::new(
        IdListSorted::new(vec![1, 2]),
        10,
        1.0,
        NOT_ITERATOR_LARGE_TIMEOUT,
        false,
    );

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
    let mut it = Not::new(
        IdListSorted::new(vec![5, 10]),
        15,
        1.0,
        NOT_ITERATOR_LARGE_TIMEOUT,
        false,
    );

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

// skip_to past max_doc_id: should return None and move to EOF.
#[test]
fn skip_to_past_max_docid_returns_none_and_sets_eof() {
    let mut it = Not::new(
        IdListSorted::new(vec![2, 4, 7]),
        10,
        1.0,
        NOT_ITERATOR_LARGE_TIMEOUT,
        false,
    );

    // 11 > max_doc_id=10, so there is no valid target and we end at EOF.
    let res = it.skip_to(11).expect("skip_to(11) must not error");
    assert!(res.is_none());
    assert!(it.at_eof());
    assert_eq!(it.last_doc_id(), 10);

    assert!(matches!(it.read(), Ok(None)));
}

// rewind should restore the initial state and read sequence.
#[test]
fn rewind_resets_state() {
    let mut it = Not::new(
        IdListSorted::new(vec![2, 4, 7]),
        10,
        1.0,
        NOT_ITERATOR_LARGE_TIMEOUT,
        false,
    );

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
    let child = Mock::new([2, 4]);
    let mut it = Not::new(child, 5, 1.0, NOT_ITERATOR_LARGE_TIMEOUT, false);

    let status = it.revalidate().expect("revalidate() failed");
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
    let child = Mock::new([2, 4]);
    let mut data = child.data();
    data.set_revalidate_result(MockRevalidateResult::Abort);
    let mut it = Not::new(child, 5, 1.0, NOT_ITERATOR_LARGE_TIMEOUT, false);

    let status = it.revalidate().expect("revalidate() failed");
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
    let child = Mock::new([2, 4]);
    let mut data = child.data();
    data.set_revalidate_result(MockRevalidateResult::Move);
    let mut it = Not::new(child, 5, 1.0, NOT_ITERATOR_LARGE_TIMEOUT, false);

    // Revalidate before any read/skip_to - both iterators at doc_id = 0
    let status = it.revalidate().expect("revalidate() failed");
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
    let child = Mock::new([5, 10]);
    let mut data = child.data();
    let mut it = Not::new(child, 15, 1.0, NOT_ITERATOR_LARGE_TIMEOUT, false);

    // Read first doc (1) - child will be at 5, NOT at 1
    let doc = it.read().expect("read() failed").expect("expected doc");
    assert_eq!(doc.doc_id, 1);
    assert_eq!(it.last_doc_id(), 1);

    // Now child is ahead (at 5) and NOT is at 1
    // Simulate child moving forward during revalidate (child advances from 5 to 10)
    data.set_revalidate_result(MockRevalidateResult::Move);

    // This should not panic - child is ahead of NOT's position
    let status = it.revalidate().expect("revalidate() failed");
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
    let child = Mock::new([8, 15]);
    let mut data = child.data();
    let mut it = Not::new(child, 20, 1.0, NOT_ITERATOR_LARGE_TIMEOUT, false);

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
    let status = it.revalidate().expect("revalidate() failed");
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
    let mut it = Not::new(child, 6, 1.0, NOT_ITERATOR_LARGE_TIMEOUT, false);

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
    let mut it = Not::new(child, 10, 1.0, NOT_ITERATOR_LARGE_TIMEOUT, false);

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
    let mut it = Not::new(
        IdListSorted::new(vec![1, 2, 3, 4, 5]),
        5,
        1.0,
        NOT_ITERATOR_LARGE_TIMEOUT,
    );

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
    let mut it = Not::new(
        IdListSorted::new(vec![2]),
        10,
        1.0,
        NOT_ITERATOR_LARGE_TIMEOUT,
    );

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
    let child = Mock::new([5_001]);
    let mut data = child.data();
    // Set child to return timeout error when it reaches EOF
    data.add_delay_since_index(1, Duration::from_micros(100));

    let mut it = Not::new(child, 10_000, 1.0, Duration::from_micros(50), false);

    for idx in 1..=4_999 {
        assert_eq!(
            idx as u64,
            it.read()
                .expect(&format!("iteration #{idx} not to timeout yet"))
                .expect(&format!("iteration #{idx} to be some"))
                .doc_id,
            "iteration #{idx} to have expected Some(doc)",
        )
    }

    assert!(!it.at_eof(), "did not yet expect to EOF");

    assert!(
        matches!(it.read(), Err(RQEIteratorError::TimedOut)),
        "expected timeout due to timeout context in Not iterator triggered"
    );

    assert!(
        it.at_eof(),
        "iterator is expected to EOF once timed out via timeout context"
    );

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
#[cfg_attr(miri, ignore = "Too slow to be run under miri.")]
fn skip_to_timeout_via_timeout_ctx() {
    let child = Mock::new([5_001]);
    let mut data = child.data();
    // Set child to return timeout error when it reaches EOF
    data.add_delay_since_index(1, Duration::from_micros(100));

    let mut it = Not::new(child, 10_000, 1.0, Duration::from_micros(50), false);

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

    assert!(
        it.at_eof(),
        "iterator is expected to EOF once timed out via timeout context"
    );

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
