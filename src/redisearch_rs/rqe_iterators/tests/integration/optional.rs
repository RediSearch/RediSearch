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
    IteratorType, RQEIterator, SkipToOutcome, empty::Empty, optional::Optional, wildcard::Wildcard,
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

mod optional_iterator_non_sequential_reads {
    use super::*;

    #[repr(C)]
    struct ReadStepIterator<'index, const N: usize> {
        read_steps: [DocId; N],
        read_step: usize,
        result: index_result::RSIndexResult<'index>,
    }

    /// Suspended counterpart of [`ReadStepIterator`].
    ///
    /// The iterator holds only owned data (its `result` is a freshly-built
    /// numeric [`RSIndexResult`] that borrows nothing), so the suspended form
    /// is byte-identical to the active form at any lifetime.
    ///
    /// `#[repr(C)]` so the layout matches [`ReadStepIterator`] for the
    /// `suspend`/`resume` pointer relabel.
    #[repr(C)]
    struct ReadStepIteratorSuspended<const N: usize> {
        read_steps: [DocId; N],
        read_step: usize,
        result: index_result::RSIndexResult<'static>,
    }

    impl<'query, const N: usize> rqe_iterators::RQESuspendedIterator<'query>
        for ReadStepIteratorSuspended<N>
    {
        type Resumed<'a>
            = ReadStepIterator<'a, N>
        where
            'query: 'a;

        fn resume<'a>(
            self: Box<Self>,
            _guard: &index_spec::IndexSpecReadGuard<'a>,
        ) -> Result<
            rqe_iterators::ResumeOutcome<Box<Self::Resumed<'a>>>,
            rqe_iterators::RQEIteratorError,
        >
        where
            'query: 'a,
        {
            let raw = Box::into_raw(self);
            // SAFETY: `ReadStepIterator<'a, N>` and `ReadStepIteratorSuspended<N>`
            // have identical layout (both `#[repr(C)]`, same fields; the lifetime
            // parameter is phantom as `result` borrows nothing).
            let active = unsafe { Box::from_raw(raw as *mut ReadStepIterator<'a, N>) };
            Ok(rqe_iterators::ResumeOutcome::Ok(active))
        }

        fn last_doc_id(&self) -> DocId {
            self.result.doc_id
        }

        fn num_estimated(&self) -> usize {
            N
        }
    }

    impl<'index, const N: usize> rqe_iterators::profile_print::ProfilePrint
        for ReadStepIterator<'index, N>
    {
        fn print_profile(
            &self,
            map: &mut redis_reply::MapBuilder<'_>,
            _ctx: &mut rqe_iterators::profile_print::ProfilePrintCtx<'_>,
        ) {
            map.kv_simple_string(c"Type", c"READ STEP");
        }
    }

    impl<const N: usize> rqe_iterators::profile_print::ProfilePrint for ReadStepIteratorSuspended<N> {
        fn print_profile(
            &self,
            map: &mut redis_reply::MapBuilder<'_>,
            _ctx: &mut rqe_iterators::profile_print::ProfilePrintCtx<'_>,
        ) {
            map.kv_simple_string(c"Type", c"READ STEP");
        }
    }

    impl<'index, const N: usize> ReadStepIterator<'index, N> {
        fn new(read_steps: [DocId; N]) -> Self {
            Self {
                read_steps,
                read_step: 0,
                result: index_result::RSIndexResult::build_numeric(42.).build(),
            }
        }
    }

    impl<'index, const N: usize> RQEIterator<'index> for ReadStepIterator<'index, N> {
        type Suspended = ReadStepIteratorSuspended<N>;

        fn suspend(self: Box<Self>) -> Box<Self::Suspended> {
            let raw = Box::into_raw(self);
            // SAFETY: layout-identical (see [`ReadStepIteratorSuspended`]).
            unsafe { Box::from_raw(raw as *mut ReadStepIteratorSuspended<N>) }
        }

        fn current(&mut self) -> Option<&mut index_result::RSIndexResult<'index>> {
            Some(&mut self.result)
        }

        fn read(
            &mut self,
        ) -> Result<Option<&mut index_result::RSIndexResult<'index>>, rqe_iterators::RQEIteratorError>
        {
            if self.at_eof() {
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
            while !self.at_eof() && self.result.doc_id < doc_id {
                self.result.doc_id = self.read_steps[self.read_step];
                self.read_step += 1;
            }

            match self.result.doc_id.cmp(&doc_id) {
                std::cmp::Ordering::Less => Ok(None),
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
            self.read_step >= N
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
