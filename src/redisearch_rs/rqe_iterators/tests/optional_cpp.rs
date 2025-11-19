/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use ffi::{RS_FIELDMASK_ALL, t_docId};
use rqe_iterators::{
    RQEIterator as _, RQEValidateStatus, SkipToOutcome, empty::Empty, optional::Optional,
};

mod c_mocks;
mod utils;

// port of OptionalIteratorTest + its `TEST_F` usage
mod optional_iterator_tests {
    use super::*;

    const MAX_DOC_ID: t_docId = 100;
    const WEIGHT: f64 = 2.;

    const NUM_DOCS: usize = 5;
    const CHILD_DOCS: [t_docId; NUM_DOCS] = [10, 20, 30, 50, 80];

    // port of OptionalIteratorTest::SetUp
    // as found in tests/cpptests/test_cpp_iterator_optional.cpp
    fn setup_optional_iterator_with_mock_child<'index>()
    -> Optional<'index, utils::MockIterator<'index, NUM_DOCS>> {
        // Create child iterator with specific docIds
        let child = utils::MockIterator::new(CHILD_DOCS);

        Optional::new(MAX_DOC_ID, WEIGHT, child)
    }

    #[test]
    fn test_cpp_read_mixed_results() {
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
    fn test_cpp_skip_to_real_hit() {
        let mut it = setup_optional_iterator_with_mock_child();

        const SKIP_TO_DOC_ID: t_docId = 20;

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
    fn test_cpp_skip_to_virtual_hit() {
        let mut it = setup_optional_iterator_with_mock_child();

        const SKIP_TO_DOC_ID: t_docId = 25;

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
    fn test_cpp_skip_to_sequence() {
        let mut it = setup_optional_iterator_with_mock_child();

        // Test skipping to various docIds in sequence
        const TARGETS: [t_docId; 10] = [5, 15, 25, 35, 45, 55, 65, 75, 85, 95];

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
    fn test_cpp_rewind_behavior() {
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
    fn test_cpp_eof_behavior() {
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
    fn test_cpp_weight_application() {
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
    fn test_cpp_virtual_result_weight() {
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

// Port of OptionalIteratorWithEmptyChildTest and `TEST_F` usage
mod optional_iterator_with_empty_child_test {
    use super::*;

    const MAX_DOC_ID: t_docId = 50;
    const WEIGHT: f64 = 3.;

    // port of OptionalIteratorWithEmptyChildTest::SetUp
    // as found in tests/cpptests/test_cpp_iterator_optional.cpp
    fn setup_optional_iterator_with_empty_child<'index>() -> Optional<'index, Empty> {
        // Create empty child iterator
        let child = Empty::default();

        Optional::new(MAX_DOC_ID, WEIGHT, child)
    }

    #[test]
    fn test_cpp_read_all_virtual_results() {
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
    fn test_cpp_skip_to_virtual_hits() {
        let mut it = setup_optional_iterator_with_empty_child();

        // Skip to various docIds - all should be virtual hits
        const TARGETS: [t_docId; 5] = [5, 15, 25, 35, 45];

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
    fn test_cpp_rewind_behavior() {
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
    fn test_cpp_eof_behavior() {
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
    fn test_cpp_virtual_result_properties() {
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

// port of OptionalIteratorRevalidateTest and `TEST_F` usage
mod optional_iterator_revalidate_test {
    use super::*;

    const MAX_DOC_ID: t_docId = 100;
    const WEIGHT: f64 = 2.;

    const NUM_DOCS: usize = 5;
    const CHILD_DOCS: [t_docId; NUM_DOCS] = [10, 20, 30, 50, 80];

    // port of OptionalIteratorRevalidateTest::SetUp
    // as found in tests/cpptests/test_cpp_iterator_optional.cpp
    fn setup_optional_iterator_with_mock_child_and_data<'index>() -> (
        Optional<'index, utils::MockIterator<'index, NUM_DOCS>>,
        utils::MockData,
    ) {
        // Create child iterator with specific docIds
        let child = utils::MockIterator::new(CHILD_DOCS);
        let data = child.data();

        let it = Optional::new(MAX_DOC_ID, WEIGHT, child);

        (it, data)
    }

    #[test]
    fn test_cpp_revalidate_ok() {
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
        let status = it.revalidate().expect("revalidate without error");
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
    fn test_cpp_revalidate_aborted() {
        let (mut it, mut data) = setup_optional_iterator_with_mock_child_and_data();

        // Child returns VALIDATE_ABORTED
        data.set_revalidate_result(utils::MockRevalidateResult::Abort);

        // Read a document first
        let _ = it
            .read()
            .expect("read without error")
            .expect("read some result, be it virtual or real");

        // Optional iterator handles child abort gracefully by replacing with empty iterator
        let status = it.revalidate().expect("revalidate without error");
        assert!(matches!(status, RQEValidateStatus::Ok)); // Optional iterator continues even when child is aborted

        // Should be able to continue reading (now all virtual hits)
        let result = it
            .read()
            .expect("read without error after revalidate")
            .expect("read some result after revalidate");
        assert_eq!(result.weight, 0.);
    }

    #[test]
    fn test_cpp_revalidate_moved() {
        let (mut it, mut data) = setup_optional_iterator_with_mock_child_and_data();

        // Child returns VALIDATE_MOVED
        data.set_revalidate_result(utils::MockRevalidateResult::Move);

        // Read to a real hit (document from child)
        const DOC_ID: t_docId = 10;
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
        let status = it.revalidate().expect("revalidate without error");
        // Should either be OK (if virtual result) or MOVED (if real result was affected)
        assert!(matches!(
            status,
            RQEValidateStatus::Ok | RQEValidateStatus::Moved { .. }
        ));

        // Should be able to continue reading after revalidation
        let _ = it
            .read()
            .expect("read returns either some result or EOF after revalidate");
    }

    #[test]
    fn test_cpp_revalidate_moved_virtual_result() {
        let (mut it, mut data) = setup_optional_iterator_with_mock_child_and_data();

        // Child returns VALIDATE_MOVED
        data.set_revalidate_result(utils::MockRevalidateResult::Move);

        // Read to a virtual hit (document not in child)
        const DOC_ID: t_docId = 15;
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
        let status = it.revalidate().expect("revalidate without error");
        assert!(matches!(status, RQEValidateStatus::Ok));

        // Should be able to continue reading
        let _ = it
            .read()
            .expect("read without error after revalidate")
            .expect("read some result after revalidate");
    }
}
