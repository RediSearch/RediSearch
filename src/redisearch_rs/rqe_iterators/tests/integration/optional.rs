/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use rqe_core::{DocId, RS_FIELDMASK_ALL};
use rqe_iterators::{
    IteratorType, RQEIterator, RQEValidateStatus, SkipToOutcome, empty::Empty, optional::Optional,
    wildcard::Wildcard,
};

use crate::utils;

#[test]
fn type_() {
    let it = Optional::new(10, 1.0, Empty::default());
    assert_eq!(it.type_(), IteratorType::Optional);
}

mod optional_iterator_skip_backward_panics {
    use super::*;

    #[test]
    #[should_panic]
    fn skip_to_pure_virtual_backwards() {
        let mut it = Optional::new(3, 5., Empty::default());

        let _ = it.skip_to(2);

        // Try to skip backwards to position 1, should panic
        let _ = it.skip_to(1);
    }

    #[test]
    #[should_panic]
    fn skip_to_pure_wildcard_backwards() {
        let mut it = Optional::new(3, 5., Wildcard::new(8, 1.));

        let _ = it.skip_to(2);

        // Try to skip backwards to position 1, should panic
        let _ = it.skip_to(1);
    }

    #[test]
    #[should_panic]
    fn skip_to_hybrid_virtual_backwards() {
        let mut it = Optional::new(6, 5., Wildcard::new(3, 1.));

        let _ = it.skip_to(4);

        // Try to skip backwards to position 1, should panic
        let _ = it.skip_to(2);
    }
}

mod optional_iterator_tests {
    use super::*;

    const MAX_DOC_ID: DocId = 100;
    const WEIGHT: f64 = 2.;

    const NUM_DOCS: usize = 5;
    const CHILD_DOCS: [DocId; NUM_DOCS] = [10, 20, 30, 50, 80];

    fn setup_optional_iterator_with_mock_child<'index>()
    -> Optional<'index, utils::Mock<'index, NUM_DOCS>> {
        // Create child iterator with specific docIds
        let child = utils::Mock::new(CHILD_DOCS);

        Optional::new(MAX_DOC_ID, WEIGHT, child)
    }

    #[test]
    fn test_read_mixed_results() {
        let mut it = setup_optional_iterator_with_mock_child();

        assert_eq!(MAX_DOC_ID as usize, it.num_estimated());

        for expected_id in 1..=MAX_DOC_ID {
            let outcome = it.read().expect("read without error").expect("some result");
            assert_eq!(outcome.doc_id, expected_id);

            // Check if this is a real hit from child or virtual
            let is_real_hit = CHILD_DOCS.contains(&outcome.doc_id);

            if is_real_hit {
                // Real hit should have the weight applied
                assert_eq!(outcome.weight, WEIGHT);

                // weight should be seen as applied to current == child :)
                assert_eq!(
                    it.current()
                        .expect("current to equal the returned result")
                        .weight,
                    WEIGHT,
                );
            } else {
                // Virtual hit
                assert_eq!(outcome.weight, 0.);
                assert_eq!(outcome.freq, 1);
                assert_eq!(outcome.field_mask, RS_FIELDMASK_ALL);
            }

            // verify also that current has the expected doc_id etc

            assert_eq!(it.last_doc_id(), expected_id);
            assert_eq!(
                it.current()
                    .expect("current to equal the returned result")
                    .doc_id,
                expected_id,
            );
        }

        // After reading all docs, should return EOF
        assert!(it.read().expect("no error to be returned").is_none());
        assert!(it.at_eof());
    }

    #[test]
    fn test_skip_to_real_hit() {
        let mut it = setup_optional_iterator_with_mock_child();

        const SKIP_TO_DOC_ID: DocId = 20;

        // Skip to a docId that exists in child
        match it
            .skip_to(SKIP_TO_DOC_ID)
            .expect("no error to be returned while skipping")
        {
            Some(SkipToOutcome::Found(result)) => {
                assert_eq!(result.doc_id, SKIP_TO_DOC_ID);
                assert_eq!(result.weight, WEIGHT);
            }
            outcome @ (None | Some(SkipToOutcome::NotFound(_))) => {
                panic!("unexpected outcome: {outcome:?}");
            }
        }

        // (current) should be real hit from child
        let current = it
            .current()
            .expect("to have a current result which is from child");
        assert_eq!(current.doc_id, SKIP_TO_DOC_ID);
        assert_eq!(current.weight, WEIGHT);
        assert_eq!(it.last_doc_id(), SKIP_TO_DOC_ID);
    }

    #[test]
    fn test_skip_to_virtual_hit() {
        let mut it = setup_optional_iterator_with_mock_child();

        const SKIP_TO_DOC_ID: DocId = 25;

        // Skip to a docId that doesn't exist in child
        match it
            .skip_to(SKIP_TO_DOC_ID)
            .expect("no error to be returned while skipping")
        {
            Some(SkipToOutcome::Found(result)) => {
                assert_eq!(result.doc_id, SKIP_TO_DOC_ID);
                assert_eq!(result.weight, 0.);
            }
            outcome @ (None | Some(SkipToOutcome::NotFound(_))) => {
                panic!("unexpected outcome: {outcome:?}");
            }
        }

        // (current) should be virtual hit
        let current = it
            .current()
            .expect("to have a current result which is NOT from child");
        assert_eq!(current.doc_id, SKIP_TO_DOC_ID);
        assert_eq!(current.weight, 0.);
        assert_eq!(it.last_doc_id(), SKIP_TO_DOC_ID);
    }

    #[test]
    fn test_skip_to_sequence() {
        let mut it = setup_optional_iterator_with_mock_child();

        // Test skipping to various docIds in sequence
        const TARGETS: [DocId; 10] = [5, 15, 25, 35, 45, 55, 65, 75, 85, 95];

        for target in TARGETS {
            // Skip to the target docId
            match it
                .skip_to(target)
                .expect("no error to be returned while skipping")
            {
                Some(SkipToOutcome::Found(result)) => {
                    assert_eq!(result.doc_id, target);
                }
                outcome @ (None | Some(SkipToOutcome::NotFound(_))) => {
                    panic!("unexpected outcome: {outcome:?}");
                }
            }

            assert_eq!(it.current().unwrap().doc_id, target);
            assert_eq!(it.last_doc_id(), target);

            // Check if it's a real or virtual hit
            let is_real_hit = CHILD_DOCS.contains(&target);

            if is_real_hit {
                // Real hit
                assert_eq!(it.current().unwrap().weight, WEIGHT);
            } else {
                // Virtual hit
                assert_eq!(it.current().unwrap().weight, 0.);
            }
        }
    }

    #[test]
    fn test_rewind_behavior() {
        let mut it = setup_optional_iterator_with_mock_child();

        // Read some documents first
        for _ in 0..10 {
            let _ = it
                .read()
                .expect("read without error")
                .expect("read some result, be it virtual or real");
        }
        assert_eq!(it.last_doc_id(), 10);

        // Test that Rewind resets the iterator
        it.rewind();
        assert_eq!(it.last_doc_id(), 0);
        assert!(!it.at_eof());

        // In the original C++ test this is `oi->virt->docId == 0`
        // which we approximate by checking the current doc_id.
        assert_eq!(
            it.current()
                .expect("iterator to have a current result after rewind")
                .doc_id,
            0,
        );

        // After Rewind, should be able to read from the beginning
        let result = it.read().expect("read without error").expect("some result");
        assert_eq!(result.doc_id, 1);
    }

    #[test]
    fn test_eof_behavior() {
        let mut it = setup_optional_iterator_with_mock_child();

        // Test EOF when reaching maxDocId
        match it
            .skip_to(MAX_DOC_ID)
            .expect("no error to be returned while skipping")
        {
            Some(SkipToOutcome::Found(result)) => {
                assert_eq!(result.doc_id, MAX_DOC_ID);
            }
            outcome @ (None | Some(SkipToOutcome::NotFound(_))) => {
                panic!("unexpected outcome: {outcome:?}");
            }
        }

        assert_eq!(it.current().unwrap().doc_id, MAX_DOC_ID);
        assert_eq!(it.last_doc_id(), MAX_DOC_ID);

        // Next read should return EOF
        assert!(it.read().expect("no error to be returned").is_none());
        assert!(it.at_eof());

        // Further operations should still return EOF
        assert!(it.read().expect("no error to be returned").is_none());
        assert!(
            it.skip_to(MAX_DOC_ID + 1)
                .expect("no error to be returned while skipping beyond max")
                .is_none()
        );
    }

    #[test]
    fn test_weight_application() {
        let mut it = setup_optional_iterator_with_mock_child();

        // Test that weight is correctly applied to real hits
        for doc_id in CHILD_DOCS {
            it.rewind();
            match it
                .skip_to(doc_id)
                .expect("no error to be returned while skipping")
            {
                Some(SkipToOutcome::Found(result)) => {
                    assert_eq!(result.doc_id, doc_id);
                    assert_eq!(result.weight, WEIGHT);
                }
                outcome @ (None | Some(SkipToOutcome::NotFound(_))) => {
                    panic!("unexpected outcome: {outcome:?}");
                }
            }

            // Verify it's a real hit from child
            let current = it
                .current()
                .expect("to have a current result which should be from child");
            assert_eq!(current.doc_id, doc_id);
            assert_eq!(current.weight, WEIGHT);
        }
    }

    #[test]
    fn test_virtual_result_weight() {
        let mut it = setup_optional_iterator_with_mock_child();

        // Test that virtual results have the correct weight
        // Skip to a virtual hit (not in childDocIds)
        match it
            .skip_to(15)
            .expect("no error to be returned while skipping")
        {
            Some(SkipToOutcome::Found(result)) => {
                assert_eq!(result.doc_id, 15);
                assert_eq!(result.weight, 0.);
            }
            outcome @ (None | Some(SkipToOutcome::NotFound(_))) => {
                panic!("unexpected outcome: {outcome:?}");
            }
        }

        let current = it
            .current()
            .expect("to have a current result which should be virtual");
        assert_eq!(current.doc_id, 15);
        assert_eq!(current.weight, 0.);
        assert_eq!(it.last_doc_id(), 15);
    }
}

mod optional_iterator_timeout_tests {
    use super::*;

    const MAX_DOC_ID: DocId = 100;
    const WEIGHT: f64 = 2.;

    const NUM_DOCS: usize = 3;
    const CHILD_DOCS: [DocId; NUM_DOCS] = [10, 20, 30];

    fn setup_optional_iterator_with_mock_child<'index>()
    -> Optional<'index, utils::Mock<'index, NUM_DOCS>> {
        // Create child iterator with specific docIds
        let child = utils::Mock::new(CHILD_DOCS);
        child
            .data()
            .set_error_at_done(Some(utils::MockIteratorError::TimeoutError(None)));

        Optional::new(MAX_DOC_ID, WEIGHT, child)
    }

    #[test]
    fn test_read_timeout_from_child() {
        let mut it = setup_optional_iterator_with_mock_child();

        // Should get virtua/real results
        for expected_id in 1..=30 {
            let outcome = it.read().expect("read without error").expect("some result");
            assert_eq!(outcome.doc_id, expected_id);

            // Check if this is a real hit from child or virtual
            let is_real_hit = CHILD_DOCS.contains(&outcome.doc_id);

            if is_real_hit {
                // Real hit should have the weight applied
                assert_eq!(outcome.weight, WEIGHT);

                // weight should be seen as applied to current == child :)
                assert_eq!(
                    it.current()
                        .expect("current to equal the returned result")
                        .weight,
                    WEIGHT,
                );
            } else {
                // Virtual hit
                assert_eq!(outcome.weight, 0.);
                assert_eq!(outcome.freq, 1);
                assert_eq!(outcome.field_mask, RS_FIELDMASK_ALL);
            }

            // verify also that current has the expected doc_id etc

            assert_eq!(it.last_doc_id(), expected_id);
            assert_eq!(
                it.current()
                    .expect("current to equal the returned result")
                    .doc_id,
                expected_id,
            );
        }

        // Now the child iterator is exhausted, next read should trigger timeout
        // when the optional iterator tries to advance the child beyond its documents

        assert!(matches!(
            it.read(),
            Err(rqe_iterators::RQEIteratorError::TimedOut)
        ));
    }

    #[test]
    fn test_skip_to_timeout_from_child() {
        let mut it = setup_optional_iterator_with_mock_child();

        // Skip to a document that exists in child (should work)
        match it
            .skip_to(20)
            .expect("no error to be returned while skipping")
        {
            Some(SkipToOutcome::Found(result)) => {
                assert_eq!(result.doc_id, 20);
            }
            outcome @ (None | Some(SkipToOutcome::NotFound(_))) => {
                panic!("unexpected outcome: {outcome:?}");
            }
        }
        assert_eq!(it.current().unwrap().doc_id, 20);

        // Skip to a document beyond child's range
        // This should trigger timeout when trying to advance the child

        assert!(matches!(
            it.skip_to(50),
            Err(rqe_iterators::RQEIteratorError::TimedOut)
        ));
    }

    #[test]
    fn test_rewind_after_timeout() {
        let mut it = setup_optional_iterator_with_mock_child();

        // Read past the child's documents to trigger timeout handling
        for _ in 0..35 {
            let _ = it.read();
        }
        assert_eq!(30, it.last_doc_id());

        // Rewind should reset everything
        it.rewind();
        assert_eq!(0, it.last_doc_id());
        assert!(!it.at_eof());

        // Should be able to read from beginning again
        let outcome = it.read().expect("read without error").expect("some result");
        assert_eq!(outcome.doc_id, 1);

        assert_eq!(it.current().unwrap().doc_id, 1);
    }
}

mod optional_iterator_with_empty_child_test {
    use super::*;

    const MAX_DOC_ID: DocId = 50;
    const WEIGHT: f64 = 3.;

    fn setup_optional_iterator_with_empty_child<'index>() -> Optional<'index, Empty> {
        // Create empty child iterator
        let child = Empty::default();

        Optional::new(MAX_DOC_ID, WEIGHT, child)
    }

    /// A skip past `max_doc_id` carries no result, so it must not adopt one as
    /// the position — `max_doc_id` least of all, which a parent would read as
    /// "this iterator is sitting on the last document".
    #[test]
    fn skip_to_beyond_max_doc_id_keeps_the_last_yielded_position() {
        let mut it = setup_optional_iterator_with_empty_child();

        for expected_id in 1..=3 {
            assert_eq!(it.read().unwrap().unwrap().doc_id, expected_id);
        }

        assert!(matches!(it.skip_to(MAX_DOC_ID + 1), Ok(None)));
        assert!(it.at_eof());
        assert_eq!(it.last_doc_id(), 3);
        assert!(it.current().is_none());

        // Exhaustion is recorded on its own, so it holds even though the position
        // (3) is far below `max_doc_id` and a forward probe is well-formed.
        assert!(matches!(it.skip_to(4), Ok(None)));
        assert!(matches!(it.read(), Ok(None)));
        assert!(it.at_eof());
        assert_eq!(it.last_doc_id(), 3);

        // Only a rewind revives it.
        it.rewind();
        assert!(!it.at_eof());
        assert_eq!(it.read().unwrap().unwrap().doc_id, 1);
    }

    #[test]
    fn test_read_all_virtual_results() {
        let mut it = setup_optional_iterator_with_empty_child();

        // Test reading - should return all virtual results
        for expected_id in 1..=MAX_DOC_ID {
            let result = it
                .read()
                .expect("read without error")
                .expect("read some result, be it virtual or real");
            assert_eq!(result.doc_id, expected_id);

            // All hits should be virtual
            assert_eq!(result.weight, 0.);
            assert_eq!(result.freq, 1);
            assert_eq!(result.field_mask, RS_FIELDMASK_ALL);

            // last doc id should e equal to expected id as well
            assert_eq!(it.last_doc_id(), expected_id);

            // and same for current
            let current = it
                .current()
                .expect("to have a current result which should be virtual");
            assert_eq!(current.doc_id, expected_id);
            assert_eq!(current.weight, 0.);
            assert_eq!(current.freq, 1);
            assert_eq!(current.field_mask, RS_FIELDMASK_ALL);
        }

        // After reading all docs, should return EOF
        assert!(it.read().expect("no error to be returned").is_none());
        assert!(it.at_eof());
    }

    #[test]
    fn test_skip_to_virtual_hits() {
        let mut it = setup_optional_iterator_with_empty_child();

        // Skip to various docIds - all should be virtual hits
        const TARGETS: [DocId; 5] = [5, 15, 25, 35, 45];

        for target in TARGETS {
            match it
                .skip_to(target)
                .expect("no error to be returned while skipping")
            {
                Some(SkipToOutcome::Found(result)) => {
                    assert_eq!(result.doc_id, target);
                    assert_eq!(it.last_doc_id(), target);
                }
                outcome @ (None | Some(SkipToOutcome::NotFound(_))) => {
                    panic!("unexpected outcome: {outcome:?}");
                }
            }

            let current = it.current().expect("to have a current result");
            assert_eq!(current.doc_id, target);
            assert_eq!(current.weight, 0.);

            // last doc id should also equal this
            assert_eq!(it.last_doc_id(), target);
        }
    }

    #[test]
    fn test_rewind_behavior() {
        let mut it = setup_optional_iterator_with_empty_child();

        // Read some documents first
        for _ in 0..10 {
            let _ = it
                .read()
                .expect("read without error")
                .expect("read some result, be it virtual or real");
        }
        assert_eq!(it.last_doc_id(), 10);

        // Test that Rewind resets the iterator
        it.rewind();
        assert_eq!(it.last_doc_id(), 0);
        assert!(!it.at_eof());

        // After Rewind, should be able to read from the beginning
        let result = it
            .read()
            .expect("read without error")
            .expect("read some result, be it virtual or real");
        assert_eq!(result.doc_id, 1);
        assert_eq!(result.weight, 0.);
        assert_eq!(result.freq, 1);
        assert_eq!(result.field_mask, RS_FIELDMASK_ALL);

        let current = it
            .current()
            .expect("to have a current result which should be virtual");
        assert_eq!(current.doc_id, 1);
        assert_eq!(current.weight, 0.);
        assert_eq!(current.freq, 1);
        assert_eq!(current.field_mask, RS_FIELDMASK_ALL);
        assert_eq!(it.last_doc_id(), 1);
    }

    #[test]
    fn test_eof_behavior() {
        let mut it = setup_optional_iterator_with_empty_child();

        // Test EOF when reaching maxDocId
        match it
            .skip_to(MAX_DOC_ID)
            .expect("no error to be returned while skipping")
        {
            Some(SkipToOutcome::Found(result)) => {
                assert_eq!(result.doc_id, MAX_DOC_ID);
                assert_eq!(result.weight, 0.);
                assert_eq!(result.freq, 1);
                assert_eq!(result.field_mask, RS_FIELDMASK_ALL);
            }
            outcome @ (None | Some(SkipToOutcome::NotFound(_))) => {
                panic!("unexpected outcome: {outcome:?}");
            }
        }

        let current = it
            .current()
            .expect("to have a current result which should be virtual");
        assert_eq!(current.doc_id, MAX_DOC_ID);
        assert_eq!(current.weight, 0.);
        assert_eq!(current.freq, 1);
        assert_eq!(current.field_mask, RS_FIELDMASK_ALL);
        assert_eq!(it.last_doc_id(), MAX_DOC_ID);

        // Next read should return EOF
        assert!(it.read().expect("no error to be returned").is_none());
        assert!(it.at_eof());

        // Further operations should still return EOF
        assert!(it.read().expect("no error to be returned").is_none());
        assert!(
            it.skip_to(MAX_DOC_ID + 1)
                .expect("no error to be returned while skipping beyond max")
                .is_none()
        );
    }

    #[test]
    fn test_virtual_result_properties() {
        let mut it = setup_optional_iterator_with_empty_child();

        // Test that virtual results have correct properties
        let result = it
            .read()
            .expect("read without error")
            .expect("read some result, be it virtual or real");

        assert_eq!(result.doc_id, 1);
        assert_eq!(result.weight, 0.);
        assert_eq!(result.freq, 1);
        assert_eq!(result.field_mask, RS_FIELDMASK_ALL);

        let current = it
            .current()
            .expect("to have a current result which should be virtual");
        assert_eq!(current.doc_id, 1);
        assert_eq!(current.weight, 0.);
        assert_eq!(current.freq, 1);
        assert_eq!(current.field_mask, RS_FIELDMASK_ALL);
        assert_eq!(it.last_doc_id(), 1);
    }
}

mod optional_iterator_revalidate_test {
    use super::*;

    const MAX_DOC_ID: DocId = 100;
    const WEIGHT: f64 = 2.;

    const NUM_DOCS: usize = 5;
    const CHILD_DOCS: [DocId; NUM_DOCS] = [10, 20, 30, 50, 80];

    fn setup_optional_iterator_with_mock_child_and_data<'index>() -> (
        Optional<'index, utils::Mock<'index, NUM_DOCS>>,
        utils::MockData,
    ) {
        // Create child iterator with specific docIds
        let child = utils::Mock::new(CHILD_DOCS);
        let data = child.data();

        let it = Optional::new(MAX_DOC_ID, WEIGHT, child);

        (it, data)
    }

    #[test]
    fn test_revalidate_ok() {
        let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
        let (mut it, mut data) = setup_optional_iterator_with_mock_child_and_data();

        // Child returns VALIDATE_OK
        data.set_revalidate_result(utils::MockRevalidateResult::Ok);

        // Read a few documents first to establish position
        let _ = it
            .read()
            .expect("read without error")
            .expect("read some result, be it virtual or real");
        let _ = it
            .read()
            .expect("read without error")
            .expect("read some result, be it virtual or real");

        // Revalidate should return VALIDATE_OK
        let status = it
            .revalidate(&*mock_ctx.spec_read())
            .expect("revalidate without error");
        assert!(matches!(status, RQEValidateStatus::Ok));

        // Verify child was revalidated
        assert_eq!(data.revalidate_count(), 1);

        // Should be able to continue reading
        let _ = it
            .read()
            .expect("read without error after revalidate")
            .expect("read some result after revalidate");
    }

    #[test]
    fn test_revalidate_aborted() {
        let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
        let (mut it, mut data) = setup_optional_iterator_with_mock_child_and_data();

        // Child returns VALIDATE_ABORTED
        data.set_revalidate_result(utils::MockRevalidateResult::Abort);

        // Read a document first
        let _ = it
            .read()
            .expect("read without error")
            .expect("read some result, be it virtual or real");

        // Optional iterator handles child abort gracefully by replacing with empty iterator
        let status = it
            .revalidate(&*mock_ctx.spec_read())
            .expect("revalidate without error");
        assert!(matches!(status, RQEValidateStatus::Ok)); // Optional iterator continues even when child is aborted

        // Should be able to continue reading (now all virtual hits)
        let result = it
            .read()
            .expect("read without error after revalidate")
            .expect("read some result after revalidate");
        assert_eq!(result.weight, 0.);
    }

    #[test]
    fn test_revalidate_moved() {
        let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
        let (mut it, mut data) = setup_optional_iterator_with_mock_child_and_data();

        // Child returns VALIDATE_MOVED
        data.set_revalidate_result(utils::MockRevalidateResult::Move);

        // Read to a real hit (document from child)
        const DOC_ID: DocId = 10;
        match it
            .skip_to(DOC_ID)
            .expect("no error to be returned while skipping")
        {
            Some(SkipToOutcome::Found(result)) => {
                assert_eq!(result.doc_id, DOC_ID);
            }
            outcome @ (None | Some(SkipToOutcome::NotFound(_))) => {
                panic!("unexpected outcome: {outcome:?}");
            }
        }
        assert_eq!(it.last_doc_id(), DOC_ID);

        // Revalidate should handle child movement
        let status = it
            .revalidate(&*mock_ctx.spec_read())
            .expect("revalidate without error");
        // Should be MOVED (as real result was affected)
        assert!(matches!(status, RQEValidateStatus::Moved { .. }));

        // Should be able to continue reading after revalidation
        let result = it
            .read()
            .expect("read returns either some result or EOF after revalidate")
            .expect("should return an actual result here");
        assert_eq!(12, result.doc_id);
    }

    #[test]
    fn test_revalidate_moved_virtual_result() {
        let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
        let (mut it, mut data) = setup_optional_iterator_with_mock_child_and_data();

        // Child returns VALIDATE_MOVED
        data.set_revalidate_result(utils::MockRevalidateResult::Move);

        // Read to a virtual hit (document not in child)
        const DOC_ID: DocId = 15;
        match it
            .skip_to(DOC_ID)
            .expect("no error to be returned while skipping")
        {
            Some(SkipToOutcome::Found(result)) => {
                assert_eq!(result.doc_id, DOC_ID);
            }
            outcome @ (None | Some(SkipToOutcome::NotFound(_))) => {
                panic!("unexpected outcome: {outcome:?}");
            }
        }
        assert_eq!(it.last_doc_id(), DOC_ID);

        // Since current result is virtual, revalidate should return OK
        let status = it
            .revalidate(&*mock_ctx.spec_read())
            .expect("revalidate without error");
        assert!(matches!(status, RQEValidateStatus::Ok));

        // Should be able to continue reading
        let result = it
            .read()
            .expect("read without error after revalidate")
            .expect("read some result after revalidate");
        assert_eq!(16, result.doc_id);
    }
}

mod optional_iterator_revalidate_after_abort {
    use super::*;

    const MAX_DOC_ID: DocId = 20;
    const WEIGHT: f64 = 2.;

    /// After child abort + a second revalidate, the child is `None` and
    /// `revalidate` should return `Ok` immediately.
    #[test]
    fn test_revalidate_twice_after_abort() {
        let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
        let child = utils::Mock::new([5, 10, 15]);
        let mut data = child.data();
        let mut it = Optional::new(MAX_DOC_ID, WEIGHT, child);

        // Position on a virtual result (doc 1)
        let doc = it.read().unwrap().unwrap();
        assert_eq!(doc.doc_id, 1);

        // First revalidate with abort: child is dropped
        data.set_revalidate_result(utils::MockRevalidateResult::Abort);
        let status = it.revalidate(&*mock_ctx.spec_read()).unwrap();
        assert!(matches!(status, RQEValidateStatus::Ok));

        // Second revalidate: child is None, should return Ok immediately
        let status = it.revalidate(&*mock_ctx.spec_read()).unwrap();
        assert!(matches!(status, RQEValidateStatus::Ok));

        // Should still be able to read (all virtual)
        let doc = it.read().unwrap().unwrap();
        assert_eq!(doc.doc_id, 2);
        assert_eq!(doc.weight, 0.);
    }

    /// After child abort, skip_to should still work (all virtual results).
    /// When child is `None`, the skip_to falls through to the virtual result path.
    #[test]
    fn test_skip_to_after_abort() {
        let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
        let child = utils::Mock::new([5, 10, 15]);
        let mut data = child.data();
        let mut it = Optional::new(MAX_DOC_ID, WEIGHT, child);

        // Position on a virtual result
        let doc = it.read().unwrap().unwrap();
        assert_eq!(doc.doc_id, 1);

        // Abort the child
        data.set_revalidate_result(utils::MockRevalidateResult::Abort);
        let _ = it.revalidate(&*mock_ctx.spec_read()).unwrap();

        // skip_to with child=None should yield a virtual Found result
        match it.skip_to(8).unwrap().unwrap() {
            SkipToOutcome::Found(result) => {
                assert_eq!(result.doc_id, 8);
                assert_eq!(result.weight, 0.);
            }
            SkipToOutcome::NotFound(r) => panic!("unexpected NotFound: {r:?}"),
        }

        // Continue reading - all virtual
        let doc = it.read().unwrap().unwrap();
        assert_eq!(doc.doc_id, 9);
        assert_eq!(doc.weight, 0.);
    }

    /// After child abort, rewind should work correctly with child=None.
    #[test]
    fn test_rewind_after_abort() {
        let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
        let child = utils::Mock::new([5, 10, 15]);
        let mut data = child.data();
        let mut it = Optional::new(MAX_DOC_ID, WEIGHT, child);

        // Read several docs
        for _ in 0..5 {
            let _ = it.read().unwrap().unwrap();
        }
        assert_eq!(it.last_doc_id(), 5);

        // Abort the child
        data.set_revalidate_result(utils::MockRevalidateResult::Abort);
        let _ = it.revalidate(&*mock_ctx.spec_read()).unwrap();

        // Rewind with child=None
        it.rewind();
        assert_eq!(it.last_doc_id(), 0);
        assert!(!it.at_eof());

        // Should produce all virtual results after rewind
        let result = it.read().unwrap().unwrap();
        assert_eq!(result.doc_id, 1);
        assert_eq!(result.weight, 0.);
    }
}

mod optional_iterator_non_sequential_reads {
    use super::*;

    struct ReadStepIterator<'index, const N: usize> {
        read_steps: [DocId; N],
        /// Index of the next step to serve, [`utils::past_end_cursor`] once a
        /// `read`/`skip_to` ran past the last one.
        read_step: usize,
        result: index_result::RSIndexResult<'index>,
    }

    impl<'index, const N: usize> ReadStepIterator<'index, N> {
        fn new(read_steps: [DocId; N]) -> Self {
            Self {
                read_steps,
                read_step: 0,
                result: index_result::RSIndexResult::build_numeric(42.).build(),
            }
        }

        /// Whether a `read`/`skip_to` has already run past the last step — the
        /// state behind both `current()` and `at_eof()`.
        fn past_end(&self) -> bool {
            self.read_step == utils::past_end_cursor(N)
        }

        /// Whether the next `read` would find nothing, true one step before
        /// [`Self::past_end`].
        fn no_more_steps(&self) -> bool {
            self.read_step >= N
        }
    }

    impl<'index, const N: usize> RQEIterator<'index> for ReadStepIterator<'index, N> {
        fn current(&mut self) -> Option<&mut index_result::RSIndexResult<'index>> {
            if self.past_end() {
                return None;
            }
            Some(&mut self.result)
        }

        fn read(
            &mut self,
        ) -> Result<Option<&mut index_result::RSIndexResult<'index>>, rqe_iterators::RQEIteratorError>
        {
            if self.no_more_steps() {
                self.read_step = utils::past_end_cursor(N);
                return Ok(None);
            }

            self.result.doc_id = self.read_steps[self.read_step];
            self.read_step += 1;
            Ok(Some(&mut self.result))
        }

        fn skip_to(
            &mut self,
            doc_id: DocId,
        ) -> Result<Option<SkipToOutcome<'_, 'index>>, rqe_iterators::RQEIteratorError> {
            while !self.no_more_steps() && self.result.doc_id < doc_id {
                self.result.doc_id = self.read_steps[self.read_step];
                self.read_step += 1;
            }

            match self.result.doc_id.cmp(&doc_id) {
                std::cmp::Ordering::Less => {
                    self.read_step = utils::past_end_cursor(N);
                    Ok(None)
                }
                std::cmp::Ordering::Equal => Ok(Some(SkipToOutcome::Found(&mut self.result))),
                std::cmp::Ordering::Greater => Ok(Some(SkipToOutcome::NotFound(&mut self.result))),
            }
        }

        fn rewind(&mut self) {
            self.result.doc_id = 0;
            self.read_step = 0;
        }

        fn num_estimated(&self) -> usize {
            unimplemented!()
        }

        fn last_doc_id(&self) -> DocId {
            self.result.doc_id
        }

        fn at_eof(&self) -> bool {
            self.past_end()
        }

        fn revalidate(
            &mut self,
            _spec: &index_spec::IndexSpecReadGuard,
        ) -> Result<RQEValidateStatus<'_, 'index>, rqe_iterators::RQEIteratorError> {
            Ok(RQEValidateStatus::Ok)
        }

        #[inline(always)]
        fn type_(&self) -> IteratorType {
            IteratorType::Mock
        }

        fn intersection_sort_weight(&self, _prioritize_union_children: bool) -> f64 {
            1.0
        }
    }

    fn assert_numeric_read<'index>(
        it: &mut impl RQEIterator<'index>,
        expected_id: DocId,
        expected_weight: f64,
    ) {
        assert!(!it.at_eof());
        let outcome = it
            .read()
            .expect("read == Ok(..)")
            .expect("read == Ok(Some(..))");
        assert!(
            outcome.as_numeric().is_some(),
            "expected numeric at id: {expected_id}"
        );
        assert_eq!(
            outcome.weight, expected_weight,
            "expected id: {expected_id}"
        );
        assert_eq!(outcome.doc_id, expected_id);
        assert_eq!(it.last_doc_id(), expected_id);
    }

    fn assert_virtual_read<'index>(it: &mut impl RQEIterator<'index>, expected_id: DocId) {
        assert!(!it.at_eof());
        let outcome = it
            .read()
            .expect("read == Ok(..)")
            .expect("read == Ok(Some(..))");
        assert!(
            outcome.as_numeric().is_none(),
            "expected virtual at id: {expected_id}"
        );
        assert_eq!(outcome.weight, 0., "expected id: {expected_id}");
        assert_eq!(outcome.doc_id, expected_id);
        assert_eq!(it.last_doc_id(), expected_id);
    }

    #[test]
    fn test_non_sequential_reads() {
        let mut it = Optional::new(9, 1., ReadStepIterator::new([1, 2, 4, 8]));

        // do twice, rewinding at end...
        for _ in 1..=2 {
            // real reads
            for expected_id in 1..=2 {
                assert_numeric_read(&mut it, expected_id, 1.);
            }

            // virtual because read-step-iterator jumped to 4!
            assert_virtual_read(&mut it, 3);

            // real for one, and only one, the one that read-step-iterator jumped to last time
            assert_numeric_read(&mut it, 4, 1.);

            // virtual for a while... until we get to the one it jumped to this time (8)
            for expected_id in 5..=7 {
                assert_virtual_read(&mut it, expected_id);
            }

            assert_numeric_read(&mut it, 8, 1.);
            assert_virtual_read(&mut it, 9);

            // EOF now :)

            assert!(matches!(it.read(), Ok(None)));
            assert!(it.at_eof());

            it.rewind();
        }
    }

    #[test]
    fn test_non_sequential_reads_mixed_with_skip_to() {
        let mut it = Optional::new(9, 1., ReadStepIterator::new([1, 2, 4, 8]));

        // real read
        // + skip just after real
        match it
            .skip_to(3)
            .expect("skip_to == Ok(..)")
            .expect("skip_to == Ok(Some(..))")
        {
            SkipToOutcome::Found(outcome) => {
                assert_eq!(outcome.weight, 0.);
                assert_eq!(outcome.doc_id, 3);
            }
            SkipToOutcome::NotFound(outcome) => panic!("unexpected not-found outcome: {outcome:?}"),
        }

        // real for one, and only one, the one that read-step-iterator jumped to last time
        assert_numeric_read(&mut it, 4, 1.);
        // + skip to just before real
        match it
            .skip_to(7)
            .expect("skip_to == Ok(..)")
            .expect("skip_to == Ok(Some(..))")
        {
            SkipToOutcome::Found(outcome) => {
                assert_eq!(outcome.weight, 0.);
                assert_eq!(outcome.doc_id, 7);
            }
            SkipToOutcome::NotFound(outcome) => panic!("unexpected not-found outcome: {outcome:?}"),
        }

        assert_numeric_read(&mut it, 8, 1.);
        assert_virtual_read(&mut it, 9);

        // EOF now :)

        assert!(matches!(it.read(), Ok(None)));
        assert!(it.at_eof());
    }

    #[test]
    fn test_non_sequential_skip_to_pre_read_child_result() {
        let mut it = Optional::new(9, 1., ReadStepIterator::new([1, 4]));

        assert_numeric_read(&mut it, 1, 1.);
        assert_virtual_read(&mut it, 2);

        // skip to pre-read child result
        match it
            .skip_to(4)
            .expect("skip_to == Ok(..)")
            .expect("skip_to == Ok(Some(..))")
        {
            SkipToOutcome::Found(outcome) => {
                assert_eq!(outcome.weight, 1.);
                assert_eq!(outcome.doc_id, 4);
            }
            SkipToOutcome::NotFound(outcome) => panic!("unexpected not-found outcome: {outcome:?}"),
        }

        // remaining ones are virtual
        for expected_id in 5..=9 {
            assert_virtual_read(&mut it, expected_id);
        }

        // EOF now :)

        assert!(matches!(it.read(), Ok(None)));
        assert!(it.at_eof());
    }

    #[test]
    #[should_panic]
    fn test_reads_backwards_panic() {
        let mut it = Optional::new(5, 1., ReadStepIterator::new([1, 2, 1]));

        for _ in 1..=2 {
            let _ = it.read();
        }

        // this will panic (debug_assert) as we read backwards from 2 -> 1
        let _ = it.read();
    }
}

mod via_resume {
    use super::*;
    use crate::utils::{Mock, MockIteratorError, MockRevalidateResult};
    use index_result::{RSIndexResult, RSResultKind};
    use rqe_iterators::{
        RQEIteratorBoxed, RQESuspendedIterator, ResumeOutcome, TypeErasedRQEIterator,
    };

    /// Regression: `Optional` wrapping a *type-erased* child must dispatch the
    /// child's own `suspend`/`resume` rather than byte-casting the child slot
    /// (a byte cast keeps the erased child's active `dyn` vtable under a
    /// suspended type, so `resume` would dispatch through the wrong vtable), and
    /// must **reuse its allocation** across the cycle because it hands out
    /// `&mut self.result` (a pointer into the box) from
    /// `current()`/`read()`/`skip_to`.
    #[test]
    fn optional_over_type_erased_child_round_trips() {
        let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
        // Type-erased child — the case a whole-box cast gets wrong.
        let child = TypeErasedRQEIterator::new(Box::new(Wildcard::new(10, 1.0)));
        let mut optional = Box::new(Optional::new(MAX_DOC_ID, WEIGHT, child));

        // Advance into the real-child range (docs 1..=10 come from the child).
        let _ = optional.read().unwrap(); // doc 1
        let _ = optional.read().unwrap(); // doc 2
        let last_before = optional.last_doc_id();
        let addr_before = &*optional as *const _ as usize;

        let suspended = optional.suspend();
        let mut active = match suspended
            .resume(&mock_ctx.spec_read())
            .expect("resume failed")
        {
            ResumeOutcome::Ok(it) | ResumeOutcome::Moved(it) => it,
            ResumeOutcome::Aborted => panic!("Optional never aborts as a whole"),
        };

        // Allocation reused → the box (and the `result` it hands out) keeps its
        // address across the cycle.
        assert_eq!(
            &*active as *const _ as usize, addr_before,
            "resume must reuse the allocation (Optional hands out &mut self.result)"
        );
        // The erased child must have survived the round-trip. If `suspend`
        // byte-casts instead of dispatching, the retained active vtable makes the
        // child resume dispatch through the wrong vtable; Optional then reads it
        // as a spurious abort and drops the child to `Gone` (fully virtual). So a
        // surviving `Present` child is the discriminating signal.
        assert!(
            active.child().is_some(),
            "type-erased child must survive suspend/resume (not be dropped to Gone)"
        );
        assert_eq!(active.last_doc_id(), last_before);
        // Doc `last_before + 1` (3) is within the child's range (1..=10), so it is
        // a *real* child hit with the Optional's weight applied — not a virtual
        // sentinel (which is what a dropped child would yield).
        let next = active.read().unwrap().unwrap();
        assert_eq!(next.doc_id, last_before + 1);
        assert_eq!(next.weight, WEIGHT, "must be a real (weighted) child hit");
    }

    // The tests below drive concrete `Mock` children (rather than the
    // type-erased case above) so `MockData` can steer the child's
    // resume outcome, covering every branch of `Optional`'s resume:
    // the non-virtual-sentinel guard, a `Gone` child, and a `Present` child
    // that resumes Unchanged / Moved / Aborted / Err — each crossed with
    // whether the last emitted result was a child hit (forcing a re-read) or a
    // virtual sentinel.

    /// Non-virtual-sentinel guard: if a consumer has replaced the virtual
    /// sentinel handed out by `read`/`current`/`skip_to` with index-backed
    /// data, `resume` cannot prove the new `'index` borrows are still valid, so
    /// it aborts the whole `Optional` rather than reinterpret `Suspended →
    /// Active`.
    #[test]
    fn resume_aborts_when_result_no_longer_virtual() {
        let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
        // `[5]` has no hit at doc 1, so the first read yields the virtual sentinel.
        let child = Mock::new([5]);
        let mut optional = Box::new(Optional::new(MAX_DOC_ID, WEIGHT, child));

        let sentinel = optional.read().unwrap().unwrap();
        assert_eq!(sentinel.kind(), RSResultKind::Virtual);
        // Simulate a consumer swapping the sentinel for a real, index-backed
        // result (e.g. a numeric record).
        *sentinel = RSIndexResult::build_numeric(1.0).build();

        let outcome = optional
            .suspend()
            .resume(&mock_ctx.spec_read())
            .expect("resume must not surface an error for a non-virtual sentinel");
        assert!(
            matches!(outcome, ResumeOutcome::Aborted),
            "a non-virtual sentinel must abort the resume",
        );
    }

    /// `Gone` child: an `Optional` whose child has already been dropped stays
    /// fully virtual and resumes cleanly. The first cycle aborts the child
    /// (covering the abort-then-`Ok`, no-re-read branch); the second cycle
    /// covers the `Gone`/Absent branch.
    #[test]
    fn resume_with_gone_child_stays_virtual() {
        let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
        // `[5]` yields a virtual sentinel at doc 1, so the aborting child's last
        // doc id (5) does not match the current doc id (1): no re-read.
        let child = Mock::new([5]);
        child
            .data()
            .set_revalidate_result(MockRevalidateResult::Abort);
        let mut optional = Box::new(Optional::new(MAX_DOC_ID, WEIGHT, child));

        let _ = optional.read().unwrap(); // virtual sentinel (doc 1)

        // First cycle: the child aborts and is dropped to `Gone`. The last
        // result was virtual, so no re-read happens and the outcome is `Ok`.
        let active = match optional
            .suspend()
            .resume(&mock_ctx.spec_read())
            .expect("resume failed")
        {
            ResumeOutcome::Ok(it) => it,
            ResumeOutcome::Moved(_) => {
                panic!("aborted child after a virtual result must not re-read (Ok)")
            }
            ResumeOutcome::Aborted => panic!("Optional never aborts as a whole for a child abort"),
        };
        assert!(
            active.child().is_none(),
            "aborted child must be dropped to Gone"
        );

        // Second cycle: the child is already `Gone` — the Absent branch.
        let active = match active
            .suspend()
            .resume(&mock_ctx.spec_read())
            .expect("resume failed")
        {
            ResumeOutcome::Ok(it) => it,
            ResumeOutcome::Moved(_) => panic!("a Gone child must resume as Ok, not Moved"),
            ResumeOutcome::Aborted => panic!("a Gone child must resume as Ok, not Aborted"),
        };
        assert!(active.child().is_none(), "the child stays Gone");
    }

    /// `Present` child that reports `Moved`, with the last result coming from
    /// the child: `Optional` must re-read to avoid stale data and report
    /// `Moved`.
    #[test]
    fn resume_moved_child_with_child_hit_rereads() {
        let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
        // `[1]` hits at doc 1, so the first read is a real (weighted) child hit.
        let child = Mock::new([1]);
        child
            .data()
            .set_revalidate_result(MockRevalidateResult::Move);
        let mut optional = Box::new(Optional::new(MAX_DOC_ID, WEIGHT, child));

        let hit = optional.read().unwrap().unwrap();
        assert_eq!(hit.doc_id, 1);
        assert_eq!(hit.weight, WEIGHT, "doc 1 is a real child hit");

        let suspended = optional.suspend();
        // The suspended form exposes the aggregate's position/estimate to a
        // composite parent that inspects a suspended child mid-resume.
        assert_eq!(suspended.last_doc_id(), 1);
        assert_eq!(suspended.num_estimated(), MAX_DOC_ID as usize);

        let outcome = suspended
            .resume(&mock_ctx.spec_read())
            .expect("resume failed");
        assert!(
            matches!(outcome, ResumeOutcome::Moved(_)),
            "a moved child whose hit was the last result must re-read (Moved)",
        );
    }

    /// A resume whose re-read runs off the end must report `Moved` with **no
    /// current** — not the stale pre-suspend result.
    ///
    /// `ResumeOutcome::Moved` carries no `Option`, so the caller recovers the
    /// new position from `current()`. The legacy `revalidate` forwarded
    /// `read()` straight into `Moved { current }` and so surfaced EOF as
    /// `current: None`; the resume path discarded the re-read's outcome, which
    /// left `current()` handing back the very position the resume was repairing.
    #[test]
    fn resume_reread_to_eof_reports_no_current() {
        let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
        // `max_doc_id` of 1 with a child hit at doc 1: after reading it the
        // iterator sits on its final result, so the resume's re-read finds
        // nothing.
        let child = Mock::new([1]);
        child
            .data()
            .set_revalidate_result(MockRevalidateResult::Move);
        let mut optional = Box::new(Optional::new(1, WEIGHT, child));

        let hit = optional.read().unwrap().unwrap();
        assert_eq!(hit.doc_id, 1);
        assert_eq!(hit.weight, WEIGHT, "doc 1 is a real child hit");
        assert!(
            !optional.at_eof(),
            "doc 1 is the bound, but it is still the current result",
        );
        assert!(optional.current().is_some(), "doc 1 is the current result");

        let mut active = match optional
            .suspend()
            .resume(&mock_ctx.spec_read())
            .expect("resume failed")
        {
            ResumeOutcome::Moved(it) => it,
            ResumeOutcome::Ok(_) => panic!("expected Moved, got Ok"),
            ResumeOutcome::Aborted => panic!("expected Moved, got Aborted"),
        };
        assert!(
            active.current().is_none(),
            "the re-read ran off the end, so the moved iterator has no current",
        );
    }

    /// A child whose resume *re-seeks* past the end — the inverted-index shape
    /// where the cached offset was invalidated, so the seek runs off the end and
    /// leaves `last_doc_id()` reset to 0 rather than advanced.
    ///
    /// `Optional` cannot notice that by comparing the child's `last_doc_id`
    /// before and after: it went backwards, not forwards. Only the child's
    /// `current()` reporting `None` identifies it. What must not happen is
    /// `Optional` handing back the real hit it was sitting on, which no longer
    /// exists.
    #[test]
    fn resume_child_reseeking_past_end_does_not_yield_stale_hit() {
        let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
        // Child hits at doc 2 only; `max_doc_id` of 4 leaves room to move on.
        let child = Mock::new([2]);
        child.data().set_resume_reseeks_past_end(true);
        let mut optional = Box::new(Optional::new(4, WEIGHT, child));

        assert_eq!(optional.read().unwrap().unwrap().doc_id, 1, "virtual");
        let hit = optional.read().unwrap().unwrap();
        assert_eq!(hit.doc_id, 2);
        assert_eq!(hit.weight, WEIGHT, "doc 2 is a real child hit");

        let mut active = match optional
            .suspend()
            .resume(&mock_ctx.spec_read())
            .expect("resume failed")
        {
            ResumeOutcome::Moved(it) => it,
            ResumeOutcome::Ok(_) => panic!("expected Moved, got Ok"),
            ResumeOutcome::Aborted => panic!("expected Moved, got Aborted"),
        };

        let current = active
            .current()
            .expect("doc 3 is still within max_doc_id, so there is a current");
        assert_eq!(
            current.doc_id, 3,
            "the re-read moved past the hit that vanished",
        );
        assert_eq!(
            current.weight, 0.,
            "doc 3 is virtual: the child is left unpositioned",
        );
        assert!(!active.at_eof());
    }

    /// `Present` child that moves during resume, forcing a re-read whose own
    /// `read` then fails: the error propagates out of `Optional::resume`.
    #[test]
    fn resume_reread_error_propagates() {
        let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
        // `[1]` hits at doc 1 (a real hit, so the resume re-reads), and the child
        // errors once it reaches its end — which the re-read triggers.
        let child = Mock::new([1]);
        {
            let mut data = child.data();
            data.set_revalidate_result(MockRevalidateResult::Move);
            data.set_error_at_done(Some(MockIteratorError::TimeoutError(None)));
        }
        let mut optional = Box::new(Optional::new(MAX_DOC_ID, WEIGHT, child));

        let _ = optional.read().unwrap(); // doc 1 (child hit)

        let result = optional.suspend().resume(&mock_ctx.spec_read());
        assert!(
            matches!(result, Err(rqe_iterators::RQEIteratorError::TimedOut)),
            "an error from the re-read must propagate out of Optional::resume",
        );
    }

    /// `Present` child that reports `Moved`, with the last result being a
    /// virtual sentinel: no re-read, reported as `Ok`.
    #[test]
    fn resume_moved_child_after_virtual_does_not_reread() {
        let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
        // `[5]` has no hit at doc 1, so the first read is the virtual sentinel.
        let child = Mock::new([5]);
        child
            .data()
            .set_revalidate_result(MockRevalidateResult::Move);
        let mut optional = Box::new(Optional::new(MAX_DOC_ID, WEIGHT, child));

        let sentinel = optional.read().unwrap().unwrap();
        assert_eq!(sentinel.doc_id, 1, "doc 1 is a virtual sentinel");

        let outcome = optional
            .suspend()
            .resume(&mock_ctx.spec_read())
            .expect("resume failed");
        assert!(
            matches!(outcome, ResumeOutcome::Ok(_)),
            "a moved child after a virtual result must not re-read (Ok)",
        );
    }

    /// `Present` child that reports `Aborted`, with the last result coming from
    /// the child: the child is dropped to `Gone` and `Optional` re-reads,
    /// reporting `Moved`.
    #[test]
    fn resume_aborted_child_with_hit_rereads_and_drops_child() {
        let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
        let child = Mock::new([1]);
        child
            .data()
            .set_revalidate_result(MockRevalidateResult::Abort);
        let mut optional = Box::new(Optional::new(MAX_DOC_ID, WEIGHT, child));

        let hit = optional.read().unwrap().unwrap();
        assert_eq!(hit.doc_id, 1);

        let active = match optional
            .suspend()
            .resume(&mock_ctx.spec_read())
            .expect("resume failed")
        {
            ResumeOutcome::Moved(it) => it,
            ResumeOutcome::Ok(_) => panic!("an aborted child hit must re-read and report Moved"),
            ResumeOutcome::Aborted => panic!("Optional never aborts as a whole for a child abort"),
        };
        assert!(
            active.child().is_none(),
            "aborted child must be dropped to Gone"
        );
    }

    /// `Present` child whose own resume fails: `Optional::resume` propagates the
    /// error (after restoring a well-formed `Gone`-child box so it drops
    /// soundly).
    #[test]
    fn resume_propagates_child_error() {
        let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
        let child = Mock::new([1]);
        child
            .data()
            .set_error_on_resume(Some(MockIteratorError::TimeoutError(None)));
        let mut optional = Box::new(Optional::new(MAX_DOC_ID, WEIGHT, child));

        let _ = optional.read().unwrap(); // doc 1 (child hit)

        let result = optional.suspend().resume(&mock_ctx.spec_read());
        assert!(
            matches!(result, Err(rqe_iterators::RQEIteratorError::TimedOut)),
            "a child resume error must propagate out of Optional::resume",
        );
    }

    const MAX_DOC_ID: DocId = 100;
    const WEIGHT: f64 = 2.0;
}

#[test]
fn optional_upholds_current_contract() {
    use rqe_iterators_test_utils::{assert_current_contract, assert_current_contract_via_skip_to};
    // Yields every id in 1..=5, real at 2 and 4, virtual elsewhere.
    let mut it = Optional::new(5, 2.0, utils::Mock::new([2u64, 4]));
    assert_eq!(assert_current_contract(&mut it), [1, 2, 3, 4, 5]);
    assert_current_contract_via_skip_to(&mut it, 6);
}
