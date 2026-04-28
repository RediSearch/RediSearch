/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use ffi::{IndexFlags_Index_DocIdsOnly, RS_FIELDMASK_ALL, t_docId};
use inverted_index::{InvertedIndex, RSIndexResult, RSResultKind, doc_ids_only::DocIdsOnly};
use rqe_iterators::{
    RQEIterator, RQEValidateStatus, SkipToOutcome, empty::Empty, inverted_index::Wildcard,
    optional_optimized::OptionalOptimized,
};

use crate::utils;

/// An inverted index populated with all consecutive doc IDs 1..=`max_doc_id`,
/// simulating `existingDocs` for use with [`Wildcard`]
/// in read/skip tests.
///
/// Uses [`MockContext`], which leaves `spec.existingDocs` null.
/// This means [`Wildcard::revalidate`] will
/// return `Aborted` — so this helper must not be used in revalidation tests.
/// Use [`TestContext::wildcard`](rqe_iterators_test_utils::TestContext::wildcard)
/// for those instead.
struct WildcardIndex {
    ii: InvertedIndex<DocIdsOnly>,
}

impl WildcardIndex {
    fn new(max_doc_id: t_docId) -> Self {
        let mut ii = InvertedIndex::<DocIdsOnly>::new(IndexFlags_Index_DocIdsOnly);
        for doc_id in 1..=max_doc_id {
            let record = RSIndexResult::build_virt()
                .doc_id(doc_id)
                .field_mask(RS_FIELDMASK_ALL)
                .frequency(1)
                .build();
            ii.add_record(&record).unwrap();
        }
        Self { ii }
    }

    fn create_iterator(&self) -> Wildcard<'_, DocIdsOnly> {
        Wildcard::new(self.ii.reader(), 0.)
    }
}

mod optional_optimized_iterator_tests {
    use rqe_iterators::inverted_index::Wildcard;

    use super::*;

    const MAX_DOC_ID: t_docId = 100;
    const WEIGHT: f64 = 2.;
    const NUM_DOCS: usize = 5;
    const CHILD_DOCS: [t_docId; NUM_DOCS] = [10, 20, 30, 50, 80];

    fn setup() -> WildcardIndex {
        WildcardIndex::new(MAX_DOC_ID)
    }

    fn create_optional_optimized<'index>(
        wcii_index: &'index WildcardIndex,
    ) -> OptionalOptimized<'index, Wildcard<'index, DocIdsOnly>, utils::Mock<'index, NUM_DOCS>>
    {
        let wcii = wcii_index.create_iterator();
        let child = utils::Mock::new(CHILD_DOCS);
        OptionalOptimized::new(wcii, child, MAX_DOC_ID, WEIGHT)
    }

    #[test]
    fn test_read_mixed_results() {
        let wcii_index = setup();
        let mut it = create_optional_optimized(&wcii_index);

        assert_eq!(MAX_DOC_ID as usize, it.num_estimated());

        for expected_id in 1..=MAX_DOC_ID {
            let outcome = it.read().expect("read without error").expect("some result");
            assert_eq!(outcome.doc_id, expected_id);

            let is_real_hit = CHILD_DOCS.contains(&expected_id);
            if is_real_hit {
                assert_eq!(outcome.weight, WEIGHT);
                assert_eq!(it.current().unwrap().weight, WEIGHT);
            } else {
                assert_eq!(outcome.weight, 0.);
                assert_eq!(outcome.freq, 1);
                assert_eq!(outcome.field_mask, RS_FIELDMASK_ALL);
            }

            assert_eq!(it.last_doc_id(), expected_id);
            assert_eq!(it.current().unwrap().doc_id, expected_id);
        }

        assert!(it.read().expect("no error").is_none());
        assert!(it.at_eof());
    }

    #[test]
    fn test_skip_to_real_hit() {
        let wcii_index = setup();
        let mut it = create_optional_optimized(&wcii_index);

        const TARGET: t_docId = 20;
        match it.skip_to(TARGET).expect("no error") {
            Some(SkipToOutcome::Found(r)) => {
                assert_eq!(r.doc_id, TARGET);
                assert_eq!(r.weight, WEIGHT);
            }
            other => panic!("unexpected outcome: {other:?}"),
        }

        let cur = it.current().unwrap();
        assert_eq!(cur.doc_id, TARGET);
        assert_eq!(cur.weight, WEIGHT);
        assert_eq!(it.last_doc_id(), TARGET);
    }

    #[test]
    fn test_skip_to_virtual_hit() {
        let wcii_index = setup();
        let mut it = create_optional_optimized(&wcii_index);

        // 25 is not in CHILD_DOCS but is present in wcii (covers 1..=100)
        const TARGET: t_docId = 25;
        match it.skip_to(TARGET).expect("no error") {
            Some(SkipToOutcome::Found(r)) => {
                assert_eq!(r.doc_id, TARGET);
                assert_eq!(r.weight, 0.);
            }
            other => panic!("unexpected outcome: {other:?}"),
        }

        let cur = it.current().unwrap();
        assert_eq!(cur.doc_id, TARGET);
        assert_eq!(cur.weight, 0.);
        assert_eq!(it.last_doc_id(), TARGET);
    }

    #[test]
    fn test_skip_to_gap() {
        let wcii_index = setup();
        let mut it = create_optional_optimized(&wcii_index);

        // Skip to doc 15; wcii lands exactly on 15 (Found), child has no match.
        match it.skip_to(15).expect("no error") {
            Some(SkipToOutcome::Found(r)) => {
                assert_eq!(r.doc_id, 15);
                assert_eq!(r.weight, 0.);
            }
            other => panic!("unexpected outcome: {other:?}"),
        }
        assert_eq!(it.last_doc_id(), 15);
        let cur = it.current().unwrap();
        assert_eq!(cur.doc_id, 15);
        assert_eq!(cur.weight, 0.);

        // Skip further to 35; still virtual.
        match it.skip_to(35).expect("no error") {
            Some(SkipToOutcome::Found(r)) => {
                assert_eq!(r.doc_id, 35);
                assert_eq!(r.weight, 0.);
            }
            other => panic!("unexpected outcome: {other:?}"),
        }
        assert_eq!(it.last_doc_id(), 35);
    }

    #[test]
    fn test_rewind_behavior() {
        let wcii_index = setup();
        let mut it = create_optional_optimized(&wcii_index);

        for _ in 0..10 {
            let _ = it.read().expect("read without error").expect("some result");
        }
        assert_eq!(it.last_doc_id(), 10);

        it.rewind();
        assert_eq!(it.last_doc_id(), 0);
        assert!(!it.at_eof());
        assert_eq!(it.current().unwrap().doc_id, 0);

        let r = it.read().expect("read after rewind").expect("some result");
        assert_eq!(r.doc_id, 1);
    }

    #[test]
    fn test_eof_behavior() {
        let wcii_index = setup();
        let mut it = create_optional_optimized(&wcii_index);

        match it.skip_to(MAX_DOC_ID).expect("no error") {
            Some(SkipToOutcome::Found(r)) => assert_eq!(r.doc_id, MAX_DOC_ID),
            other => panic!("unexpected outcome: {other:?}"),
        }

        assert_eq!(it.last_doc_id(), MAX_DOC_ID);
        assert!(it.read().expect("no error").is_none());
        assert!(it.at_eof());
        assert!(it.read().expect("no error").is_none());
        assert!(
            it.skip_to(MAX_DOC_ID + 1)
                .expect("no error beyond max")
                .is_none()
        );
    }

    /// `read` stops at `max_doc_id` even when `wcii` jumps past it in a single step.
    ///
    /// A sparse index may have no document between some value and a doc ID well
    /// beyond `max_doc_id`, so `wcii` can skip over the boundary in one advance.
    #[test]
    fn test_read_stops_at_max_doc_id() {
        // wcii has docs [5, 150] and max_doc_id is 100.
        // Doc 150 must never be returned; after doc 5 the next read must be EOF.
        const WCII_DOCS: [t_docId; 2] = [5, 150];
        let wcii = utils::Mock::new(WCII_DOCS);
        let child: Empty = Empty;
        let mut it = OptionalOptimized::new(wcii, child, 100, WEIGHT);

        let r = it.read().expect("no error").expect("doc 5");
        assert_eq!(r.doc_id, 5);

        // wcii returns 150 > max_doc_id (100) → EOF.
        assert!(it.read().expect("no error").is_none());
        assert!(it.at_eof());
    }

    /// `skip_to` stops at `max_doc_id` even when `wcii` lands beyond it.
    #[test]
    fn test_skip_to_stops_at_max_doc_id() {
        // wcii has docs [5, 150] and max_doc_id is 100.
        // Skipping to 10 causes wcii to land on 150 > max_doc_id → EOF.
        const WCII_DOCS: [t_docId; 2] = [5, 150];
        let wcii = utils::Mock::new(WCII_DOCS);
        let child: Empty = Empty;
        let mut it = OptionalOptimized::new(wcii, child, 100, WEIGHT);

        let r = it.read().expect("no error").expect("doc 5");
        assert_eq!(r.doc_id, 5);

        // wcii's next doc is 150 > max_doc_id (100) → EOF.
        assert!(it.skip_to(10).expect("no error").is_none());
        assert!(it.at_eof());
    }

    /// For every ordered pair `(from_id, skip_to_id)` drawn from the wildcard document
    /// range, rewinds the iterator, positions it at `from_id`, then calls `skip_to`
    /// targeting `skip_to_id`. Verifies that:
    /// - The iterator lands on the correct next wildcard doc ≥ `skip_to_id`.
    /// - `Found`/`NotFound` outcome matches whether `skip_to_id` is an exact wildcard hit.
    /// - Real vs. virtual result distinction (weight) is correct at the landing position.
    #[test]
    fn test_skip_to_exhaustive() {
        // Mirror the C++ fixture: wildcard = multiples of 5 in [5..=95],
        // child = even multiples of 10 in [20..=90].
        const WILDCARD_DOCS: [t_docId; 19] = [
            5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80, 85, 90, 95,
        ];
        const CHILD_DOCS_EXH: [t_docId; 8] = [20, 30, 40, 50, 60, 70, 80, 90];
        const WEIGHT_EXH: f64 = 4.6;
        const MAX_EXH: t_docId = 95;

        let wcii = utils::Mock::new(WILDCARD_DOCS);
        let child = utils::Mock::new(CHILD_DOCS_EXH);
        let mut it = OptionalOptimized::new(wcii, child, MAX_EXH, WEIGHT_EXH);

        for &from_id in &WILDCARD_DOCS {
            for skip_to_id in (from_id + 1)..=*WILDCARD_DOCS.last().unwrap() {
                it.rewind();

                // Position at from_id.
                match it.skip_to(from_id).expect("no error") {
                    Some(SkipToOutcome::Found(r)) => assert_eq!(r.doc_id, from_id),
                    other => panic!("unexpected when positioning at {from_id}: {other:?}"),
                }
                assert_eq!(it.last_doc_id(), from_id);

                // Expected landing position: first wildcard doc ≥ skip_to_id.
                let &expected_id = WILDCARD_DOCS.iter().find(|&&id| id >= skip_to_id).unwrap();

                let is_real = CHILD_DOCS_EXH.contains(&expected_id);
                match it.skip_to(skip_to_id).expect("no error") {
                    Some(SkipToOutcome::Found(r)) => {
                        assert_eq!(
                            skip_to_id, expected_id,
                            "Found outcome only valid on exact wildcard hit"
                        );
                        assert_eq!(r.doc_id, expected_id);
                        assert_eq!(r.weight, if is_real { WEIGHT_EXH } else { 0. });
                    }
                    Some(SkipToOutcome::NotFound(r)) => {
                        assert_ne!(skip_to_id, expected_id);
                        assert_eq!(r.doc_id, expected_id);
                        assert_eq!(r.weight, if is_real { WEIGHT_EXH } else { 0. });
                    }
                    None => panic!("unexpected EOF skipping to {skip_to_id}"),
                }
                assert_eq!(it.last_doc_id(), expected_id);
            }
        }
    }

    #[test]
    fn test_weight_application() {
        let wcii_index = setup();
        let mut it = create_optional_optimized(&wcii_index);

        for &doc_id in &CHILD_DOCS {
            it.rewind();
            match it.skip_to(doc_id).expect("no error") {
                Some(SkipToOutcome::Found(r)) => {
                    assert_eq!(r.doc_id, doc_id);
                    assert_eq!(r.weight, WEIGHT);
                }
                other => panic!("unexpected outcome: {other:?}"),
            }
            let cur = it.current().unwrap();
            assert_eq!(cur.doc_id, doc_id);
            assert_eq!(cur.weight, WEIGHT);
        }
    }
}

mod optional_optimized_iterator_with_empty_child_tests {
    use rqe_iterators::inverted_index::Wildcard;

    use super::*;

    const MAX_DOC_ID: t_docId = 50;
    const WEIGHT: f64 = 3.;

    fn setup() -> WildcardIndex {
        WildcardIndex::new(MAX_DOC_ID)
    }

    fn create<'index>(
        wcii_index: &'index WildcardIndex,
    ) -> OptionalOptimized<'index, Wildcard<'index, DocIdsOnly>, Empty> {
        let wcii = wcii_index.create_iterator();
        OptionalOptimized::new(wcii, Empty, MAX_DOC_ID, WEIGHT)
    }

    #[test]
    fn test_read_all_virtual_results() {
        let wcii_index = setup();
        let mut it = create(&wcii_index);

        for expected_id in 1..=MAX_DOC_ID {
            let r = it.read().expect("no error").expect("some result");
            assert_eq!(r.doc_id, expected_id);
            assert_eq!(r.weight, 0.);
            assert_eq!(r.freq, 1);
            assert_eq!(r.field_mask, RS_FIELDMASK_ALL);
            assert_eq!(r.kind(), RSResultKind::Virtual);
            assert_eq!(it.last_doc_id(), expected_id);
            let cur = it.current().unwrap();
            assert_eq!(cur.doc_id, expected_id);
            assert_eq!(cur.weight, 0.);
        }

        assert!(it.read().expect("no error").is_none());
        assert!(it.at_eof());
    }

    #[test]
    fn test_skip_to_virtual_hits() {
        let wcii_index = setup();
        let mut it = create(&wcii_index);

        for target in [5u64, 15, 25, 35, 45] {
            match it.skip_to(target).expect("no error") {
                Some(SkipToOutcome::Found(r)) => {
                    assert_eq!(r.doc_id, target);
                    assert_eq!(it.last_doc_id(), target);
                }
                other => panic!("unexpected outcome: {other:?}"),
            }
            let cur = it.current().unwrap();
            assert_eq!(cur.doc_id, target);
            assert_eq!(cur.weight, 0.);
        }
    }

    #[test]
    fn test_eof_behavior() {
        let wcii_index = setup();
        let mut it = create(&wcii_index);

        match it.skip_to(MAX_DOC_ID).expect("no error") {
            Some(SkipToOutcome::Found(r)) => {
                assert_eq!(r.doc_id, MAX_DOC_ID);
                assert_eq!(r.weight, 0.);
            }
            other => panic!("unexpected outcome: {other:?}"),
        }

        assert!(it.read().expect("no error").is_none());
        assert!(it.at_eof());
    }
}

/// Tests that use a `Mock` wildcard iterator (instead of `Wildcard`) to exercise
/// code paths that are only reachable when `wcii` is not a dense counter.
mod optional_optimized_iterator_sparse_wcii_tests {
    use super::*;

    const MAX_DOC_ID: t_docId = 100;
    const WEIGHT: f64 = 1.5;

    /// `read()` returns `None` and sets `at_eof` when `wcii` runs out of documents
    /// before `max_doc_id` is reached.
    #[test]
    fn test_read_wcii_exhausted_before_max_doc_id() {
        // wcii only has docs [5, 15]; max_doc_id is 100.
        // After consuming both, the next read() must return None.
        let wcii = utils::Mock::new([5u64, 15]);
        let child: Empty = Empty;
        let mut it = OptionalOptimized::new(wcii, child, MAX_DOC_ID, WEIGHT);

        let r = it.read().expect("no error").expect("doc 5");
        assert_eq!(r.doc_id, 5);
        assert!(!it.at_eof());

        let r = it.read().expect("no error").expect("doc 15");
        assert_eq!(r.doc_id, 15);
        assert!(!it.at_eof());

        // wcii is now exhausted; read() must hit the None arm.
        assert!(it.read().expect("no error").is_none());
        assert!(it.at_eof());

        // Subsequent reads must also return None.
        assert!(it.read().expect("no error").is_none());
    }

    /// The child-catch-up loop in `read()` executes multiple iterations when
    /// `wcii` lands on a doc that is well ahead of the child's current position.
    #[test]
    fn test_read_child_catches_up_multiple_steps() {
        // wcii has a single doc at 20. child has docs [5, 10, 15, 25].
        // When read() is called, child must advance through 5→10→15 before
        // landing on 25 (which is past wcii_doc_id=20), so the loop body runs
        // three times.
        let wcii = utils::Mock::new([20u64]);
        let child = utils::Mock::new([5u64, 10, 15, 25]);
        let mut it = OptionalOptimized::new(wcii, child, MAX_DOC_ID, WEIGHT);

        // wcii_doc_id=20, child advances 5→10→15→25; 25≠20 → virtual hit.
        let r = it.read().expect("no error").expect("doc 20");
        assert_eq!(r.doc_id, 20);
        assert_eq!(r.weight, 0.); // virtual
    }

    /// `skip_to()` returns `None` and sets `at_eof` when `wcii.skip_to()` itself
    /// returns `None` (i.e. `wcii` is exhausted before it can reach the target).
    #[test]
    fn test_skip_to_wcii_returns_none() {
        // wcii has only doc 10; after reading it, wcii is at_eof.
        let wcii = utils::Mock::new([10u64]);
        let child: Empty = Empty;
        let mut it = OptionalOptimized::new(wcii, child, MAX_DOC_ID, WEIGHT);

        // Consume the only wcii doc so wcii is at_eof.
        let r = it.read().expect("no error").expect("doc 10");
        assert_eq!(r.doc_id, 10);
        assert!(!it.at_eof()); // 10 < 100

        // skip_to(20): wcii is exhausted → returns None → at_eof = true.
        assert!(it.skip_to(20).expect("no error").is_none());
        assert!(it.at_eof());
    }

    /// `skip_to()` returns `SkipToOutcome::NotFound` carrying a **real** result when
    /// `wcii` lands on a document that differs from the requested id but `child`
    /// has a hit at that effective position.
    #[test]
    fn test_skip_to_not_found_real_hit() {
        // wcii = [15], child = [15]. Requesting skip_to(10):
        // wcii returns NotFound(15) (landed past the requested id).
        // child also has doc 15 → real hit at 15, but outcome is NotFound.
        let wcii = utils::Mock::new([15u64]);
        let child = utils::Mock::new([15u64]);
        let mut it = OptionalOptimized::new(wcii, child, MAX_DOC_ID, WEIGHT);

        match it.skip_to(10).expect("no error") {
            Some(SkipToOutcome::NotFound(r)) => {
                assert_eq!(r.doc_id, 15);
                assert_eq!(r.weight, WEIGHT); // real hit
            }
            other => panic!("expected NotFound, got {other:?}"),
        }
        assert_eq!(it.last_doc_id(), 15);
    }

    /// `skip_to()` returns `SkipToOutcome::NotFound` carrying a **virtual** result
    /// when `wcii` lands on a document that differs from the requested id and
    /// `child` has no hit at that effective position.
    #[test]
    fn test_skip_to_not_found_virtual_hit() {
        // wcii = [15], child = Empty. Requesting skip_to(10):
        // wcii returns NotFound(15). No child match → virtual hit at 15, NotFound.
        let wcii = utils::Mock::new([15u64]);
        let child: Empty = Empty;
        let mut it = OptionalOptimized::new(wcii, child, MAX_DOC_ID, WEIGHT);

        match it.skip_to(10).expect("no error") {
            Some(SkipToOutcome::NotFound(r)) => {
                assert_eq!(r.doc_id, 15);
                assert_eq!(r.weight, 0.); // virtual
            }
            other => panic!("expected NotFound, got {other:?}"),
        }
        assert_eq!(it.last_doc_id(), 15);
    }

    /// An error from `wcii.read()` is propagated by `read()`.
    #[test]
    fn test_read_propagates_wcii_error() {
        let wcii = utils::Mock::new([5u64]);
        let mut wcii_data = wcii.data();
        let child: Empty = Empty;
        let mut it = OptionalOptimized::new(wcii, child, MAX_DOC_ID, WEIGHT);

        // Consume the only wcii doc.
        let r = it.read().expect("no error").expect("doc 5");
        assert_eq!(r.doc_id, 5);

        // Configure wcii to error when exhausted (next read() call).
        wcii_data.set_error_at_done(Some(utils::MockIteratorError::TimeoutError(None)));

        let err = it.read().expect_err("expected timeout error");
        assert!(matches!(err, rqe_iterators::RQEIteratorError::TimedOut));
    }

    /// An error from `wcii.skip_to()` is propagated by `skip_to()`.
    #[test]
    fn test_skip_to_propagates_wcii_error() {
        let wcii = utils::Mock::new([5u64]);
        let mut wcii_data = wcii.data();
        let child: Empty = Empty;
        let mut it = OptionalOptimized::new(wcii, child, MAX_DOC_ID, WEIGHT);

        let r = it.read().expect("no error").expect("doc 5");
        assert_eq!(r.doc_id, 5);

        wcii_data.set_error_at_done(Some(utils::MockIteratorError::TimeoutError(None)));

        let err = it.skip_to(10).expect_err("expected timeout error");
        assert!(matches!(err, rqe_iterators::RQEIteratorError::TimedOut));
    }

    /// An error from `child.skip_to()` is propagated by `skip_to()`.
    #[test]
    fn test_skip_to_propagates_child_error() {
        // wcii lands on 30 (Found). child has [10, 20] with an error after exhaustion.
        // child.skip_to(30) advances through 10 and 20, then hits at_eof → error.
        let wcii = utils::Mock::new([30u64]);
        let child = utils::Mock::new([10u64, 20]);
        let mut child_data = child.data();
        child_data.set_error_at_done(Some(utils::MockIteratorError::TimeoutError(None)));
        let mut it = OptionalOptimized::new(wcii, child, MAX_DOC_ID, WEIGHT);

        let err = it.skip_to(30).expect_err("expected timeout error");
        assert!(matches!(err, rqe_iterators::RQEIteratorError::TimedOut));
    }

    /// `skip_to()` calls `child.skip_to()` to advance the child to `effective_id`
    /// when the child's current position is behind it.
    #[test]
    fn test_skip_to_advances_child_to_effective_id() {
        // wcii = [30], child = [20, 30]. skip_to(30):
        // wcii lands Found(30). child.last_doc_id()=0 < 30 → child.skip_to(30) called.
        // child finds doc 30 → real Found hit.
        let wcii = utils::Mock::new([30u64]);
        let child = utils::Mock::new([20u64, 30]);
        let mut it = OptionalOptimized::new(wcii, child, MAX_DOC_ID, WEIGHT);

        match it.skip_to(30).expect("no error") {
            Some(SkipToOutcome::Found(r)) => {
                assert_eq!(r.doc_id, 30);
                assert_eq!(r.weight, WEIGHT);
            }
            other => panic!("expected Found, got {other:?}"),
        }
    }
}

mod optional_optimized_iterator_revalidate_tests {
    use super::*;

    const MAX_DOC_ID: t_docId = 100;
    const WEIGHT: f64 = 2.;
    const NUM_DOCS: usize = 5;
    const CHILD_DOCS: [t_docId; NUM_DOCS] = [10, 20, 30, 50, 80];

    /// Tests using [`Wildcard`] as the wildcard iterator,
    /// requiring [`TestContext::wildcard`] which touches global C state and is not
    /// compatible with miri.
    #[cfg(not(miri))]
    mod with_inverted_wildcard {
        use inverted_index::opaque::OpaqueEncoding;
        use rqe_iterators::inverted_index::Wildcard;
        use rqe_iterators_test_utils::{GlobalGuard, TestContext};

        use super::*;

        fn setup<'index>(
            test_ctx: &'index TestContext,
        ) -> (
            OptionalOptimized<'index, Wildcard<'index, DocIdsOnly>, utils::Mock<'index, NUM_DOCS>>,
            utils::MockData,
        ) {
            let ii = DocIdsOnly::from_opaque(test_ctx.wildcard_inverted_index());
            let wcii = Wildcard::new(ii.reader(), 0.);
            let child = utils::Mock::new(CHILD_DOCS);
            let data = child.data();
            let it = OptionalOptimized::new(wcii, child, MAX_DOC_ID, WEIGHT);
            (it, data)
        }

        #[test]
        fn test_revalidate_ok() {
            let _guard = GlobalGuard::default();
            let test_ctx = TestContext::wildcard(1..=MAX_DOC_ID);
            let (mut it, mut data) = setup(&test_ctx);

            data.set_revalidate_result(utils::MockRevalidateResult::Ok);

            let _ = it.read().expect("read").expect("result");
            let _ = it.read().expect("read").expect("result");

            // SAFETY: test-only call with valid context
            let status = unsafe { it.revalidate(test_ctx.spec) }.expect("revalidate");
            assert!(matches!(status, RQEValidateStatus::Ok));
            assert_eq!(data.revalidate_count(), 1);

            // Can continue reading
            let _ = it.read().expect("read after revalidate").expect("result");
        }

        #[test]
        fn test_revalidate_child_aborted() {
            let _guard = GlobalGuard::default();
            let test_ctx = TestContext::wildcard(1..=MAX_DOC_ID);
            let (mut it, mut data) = setup(&test_ctx);

            data.set_revalidate_result(utils::MockRevalidateResult::Abort);

            // Position on a virtual result (doc 1)
            let r = it.read().expect("read").expect("result");
            assert_eq!(r.doc_id, 1);

            // SAFETY: test-only call with valid context
            let status = unsafe { it.revalidate(test_ctx.spec) }.expect("revalidate");
            // Child aborted while on a virtual result → Ok (no state change needed)
            assert!(matches!(status, RQEValidateStatus::Ok));
            assert!(
                it.child().is_none(),
                "child must be replaced by Empty after abort"
            );
            assert_eq!(data.revalidate_count(), 1);

            // All subsequent reads are virtual
            let r = it.read().expect("read").expect("result");
            assert_eq!(r.weight, 0.);
        }

        #[test]
        fn test_revalidate_child_moved_on_real() {
            let _guard = GlobalGuard::default();
            let test_ctx = TestContext::wildcard(1..=MAX_DOC_ID);
            let (mut it, mut data) = setup(&test_ctx);

            // Position on a real result (doc 10)
            match it.skip_to(10).expect("no error") {
                Some(SkipToOutcome::Found(r)) => assert_eq!(r.doc_id, 10),
                other => panic!("unexpected: {other:?}"),
            }

            data.set_revalidate_result(utils::MockRevalidateResult::Move);
            // SAFETY: test-only call with valid context
            let status = unsafe { it.revalidate(test_ctx.spec) }.expect("revalidate");
            // Child moved while on a real result → Moved
            assert!(matches!(status, RQEValidateStatus::Moved { .. }));
            assert_eq!(data.revalidate_count(), 1);
        }

        #[test]
        fn test_revalidate_child_moved_on_virtual() {
            let _guard = GlobalGuard::default();
            let test_ctx = TestContext::wildcard(1..=MAX_DOC_ID);
            let (mut it, mut data) = setup(&test_ctx);

            // Position on a virtual result (doc 15, not in CHILD_DOCS)
            match it.skip_to(15).expect("no error") {
                Some(SkipToOutcome::Found(r)) => assert_eq!(r.doc_id, 15),
                other => panic!("unexpected: {other:?}"),
            }

            data.set_revalidate_result(utils::MockRevalidateResult::Move);
            // SAFETY: test-only call with valid context
            let status = unsafe { it.revalidate(test_ctx.spec) }.expect("revalidate");
            // Child moved while on a virtual result → Ok
            assert!(matches!(status, RQEValidateStatus::Ok));
            assert_eq!(data.revalidate_count(), 1);
        }
    }

    #[test]
    fn test_revalidate_wcii_aborted() {
        // Use Mock as wcii so we can configure it to abort.
        const WCII_DOCS: usize = 10;
        let wcii_docs: [t_docId; WCII_DOCS] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let wcii = utils::Mock::new(wcii_docs);
        let mut wcii_data = wcii.data();
        let child = utils::Mock::new(CHILD_DOCS);
        let mut it = OptionalOptimized::new(wcii, child, MAX_DOC_ID, WEIGHT);

        // Read one result first
        let _ = it.read().expect("read").expect("result");

        wcii_data.set_revalidate_result(utils::MockRevalidateResult::Abort);
        let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
        let ctx = mock_ctx.spec();
        // SAFETY: test-only call with valid context
        let status = unsafe { it.revalidate(ctx) }.expect("revalidate");
        assert!(matches!(status, RQEValidateStatus::Aborted));
    }

    /// When `wcii` moves to a position where `child` also has a match, `revalidate`
    /// must return `Moved` with a real result carrying the configured weight.
    #[test]
    fn test_revalidate_wcii_moved_real_hit() {
        // wcii: [5, 20], child: [5, 20]
        // After reading doc 5, wcii moves to doc 20 on revalidation.
        // Child has doc 20 as well → real hit.
        let wcii = utils::Mock::new([5u64, 20]);
        let mut wcii_data = wcii.data();
        let child = utils::Mock::new([5u64, 20]);
        let mut it = OptionalOptimized::new(wcii, child, MAX_DOC_ID, WEIGHT);

        let r = it.read().expect("read").expect("result");
        assert_eq!(r.doc_id, 5);

        wcii_data.set_revalidate_result(utils::MockRevalidateResult::Move);
        let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
        let ctx = mock_ctx.spec();
        // SAFETY: test-only call with valid context
        match unsafe { it.revalidate(ctx) }.expect("revalidate") {
            RQEValidateStatus::Moved { current: Some(r) } => {
                assert_eq!(r.doc_id, 20);
                assert_eq!(r.weight, WEIGHT);
            }
            _ => panic!("expected Moved with a real result"),
        }
        assert_eq!(it.last_doc_id(), 20);
    }

    /// When `wcii` moves to a position where `child` has no match, `revalidate`
    /// must return `Moved` with a virtual result (zero weight) at the new doc ID.
    #[test]
    fn test_revalidate_wcii_moved_virtual_hit() {
        // wcii: [5, 20], child: [5, 25]
        // After reading doc 5, wcii moves to doc 20 on revalidation.
        // Child's next doc after 5 is 25, so there is no match at 20 → virtual hit.
        let wcii = utils::Mock::new([5u64, 20]);
        let mut wcii_data = wcii.data();
        let child = utils::Mock::new([5u64, 25]);
        let mut it = OptionalOptimized::new(wcii, child, MAX_DOC_ID, WEIGHT);

        let r = it.read().expect("read").expect("result");
        assert_eq!(r.doc_id, 5);

        wcii_data.set_revalidate_result(utils::MockRevalidateResult::Move);
        let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
        let ctx = mock_ctx.spec();
        // SAFETY: test-only call with valid context
        match unsafe { it.revalidate(ctx) }.expect("revalidate") {
            RQEValidateStatus::Moved { current: Some(r) } => {
                assert_eq!(r.doc_id, 20);
                assert_eq!(r.weight, 0.); // virtual result carries zero weight
            }
            _ => panic!("expected Moved with a virtual result"),
        }
        assert_eq!(it.last_doc_id(), 20);
    }

    /// When `wcii` moves during `revalidate` to a doc ID that exceeds `max_doc_id`,
    /// `revalidate` must return `Moved { current: None }` and set `at_eof`.
    #[test]
    fn test_revalidate_wcii_moved_past_max_doc_id() {
        // wcii: [5, 150], max_doc_id: 100.
        // After reading doc 5, wcii moves to doc 150 on revalidation.
        // 150 > max_doc_id → iterator is at EOF.
        let wcii = utils::Mock::new([5u64, 150]);
        let mut wcii_data = wcii.data();
        let child = utils::Mock::new(CHILD_DOCS);
        let mut it = OptionalOptimized::new(wcii, child, MAX_DOC_ID, WEIGHT);

        let r = it.read().expect("read").expect("result");
        assert_eq!(r.doc_id, 5);

        wcii_data.set_revalidate_result(utils::MockRevalidateResult::Move);
        let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
        let ctx = mock_ctx.spec();
        // SAFETY: test-only call with valid context
        match unsafe { it.revalidate(ctx) }.expect("revalidate") {
            RQEValidateStatus::Moved { current: None } => {}
            other => panic!("expected Moved{{None}}, got {other:?}"),
        }
        assert!(it.at_eof());
    }

    /// Regression test: when `wcii` moves to its own EOF during `revalidate`
    /// (i.e. `wcii.revalidate()` returns `Moved { current: None }`), the
    /// optional iterator must propagate `Moved { current: None }` immediately,
    /// without reading the stale `last_doc_id` from `wcii`.
    ///
    /// Before the fix, the `Moved` branch called `wcii.last_doc_id()` — which
    /// still held the previous position — and resolved a result there instead
    /// of propagating the EOF signal.
    #[test]
    fn test_revalidate_wcii_moved_to_eof() {
        // wcii has a single document (5). After reading it, wcii is at its own EOF.
        // Mock::revalidate with Move returns Moved { current: None } when at EOF.
        let wcii = utils::Mock::new([5u64]);
        let mut wcii_data = wcii.data();
        let child = utils::Mock::new([5u64]);
        let mut it = OptionalOptimized::new(wcii, child, MAX_DOC_ID, WEIGHT);

        // Consume the only document; wcii's last_doc_id is now 5 (stale after EOF).
        let r = it.read().expect("read").expect("result");
        assert_eq!(r.doc_id, 5);

        // wcii is at EOF; Move revalidation returns Moved { current: None }.
        wcii_data.set_revalidate_result(utils::MockRevalidateResult::Move);
        let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
        let ctx = mock_ctx.spec();
        // SAFETY: test-only call with valid context
        match unsafe { it.revalidate(ctx) }.expect("revalidate") {
            RQEValidateStatus::Moved { current: None } => {}
            other => panic!("expected Moved{{None}}, got {other:?}"),
        }
        assert!(it.at_eof(), "iterator must be at EOF");
        assert_eq!(wcii_data.revalidate_count(), 1);
    }

    /// When `child` aborts and `wcii` moves simultaneously, the iterator must:
    /// - Replace `child` with `Empty`.
    /// - Return `Moved` at the new `wcii` position (virtual hit, since child is gone).
    #[test]
    fn test_revalidate_child_aborted_wcii_moved() {
        let wcii = utils::Mock::new([5u64, 20]);
        let mut wcii_data = wcii.data();
        let child = utils::Mock::new([5u64, 35]);
        let mut child_data = child.data();
        let mut it = OptionalOptimized::new(wcii, child, MAX_DOC_ID, WEIGHT);

        // Position on doc 5 (real hit: both wcii and child land there).
        let r = it.read().expect("read").expect("result");
        assert_eq!(r.doc_id, 5);

        wcii_data.set_revalidate_result(utils::MockRevalidateResult::Move);
        child_data.set_revalidate_result(utils::MockRevalidateResult::Abort);

        // wcii moves to 20; child aborts → replaced by Empty → virtual hit at 20.
        let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
        let ctx = mock_ctx.spec();
        // SAFETY: test-only call with valid context
        match unsafe { it.revalidate(ctx) }.expect("revalidate") {
            RQEValidateStatus::Moved { current: Some(r) } => {
                assert_eq!(r.doc_id, 20);
                assert_eq!(r.weight, 0.); // virtual: child is gone
            }
            other => panic!("expected Moved with virtual result, got {other:?}"),
        }
        assert!(
            it.child().is_none(),
            "child must be replaced by Empty after abort"
        );
        assert_eq!(wcii_data.revalidate_count(), 1);
        assert_eq!(child_data.revalidate_count(), 1);
    }

    /// When `wcii` aborts the entire optional iterator must abort immediately,
    /// without even revalidating `child`.
    #[test]
    fn test_revalidate_child_moved_wcii_aborted() {
        let wcii = utils::Mock::new([5u64, 20]);
        let mut wcii_data = wcii.data();
        let child = utils::Mock::new([5u64, 35]);
        let mut child_data = child.data();
        let mut it = OptionalOptimized::new(wcii, child, MAX_DOC_ID, WEIGHT);

        let r = it.read().expect("read").expect("result");
        assert_eq!(r.doc_id, 5);

        wcii_data.set_revalidate_result(utils::MockRevalidateResult::Abort);
        child_data.set_revalidate_result(utils::MockRevalidateResult::Move);

        let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
        let ctx = mock_ctx.spec();
        // SAFETY: test-only call with valid context
        let status = unsafe { it.revalidate(ctx) }.expect("revalidate");
        assert!(matches!(status, RQEValidateStatus::Aborted));
        // wcii was checked; child must NOT have been revalidated (short-circuit).
        assert_eq!(wcii_data.revalidate_count(), 1);
        assert_eq!(child_data.revalidate_count(), 0);
    }

    /// When both `wcii` and `child` move, the iterator must return `Moved` at
    /// `wcii`'s new position, with the appropriate real-vs-virtual result.
    #[test]
    fn test_revalidate_child_moved_wcii_moved() {
        // wcii: [5, 20, 35] — after reading doc 5 it will move to 20 on revalidation.
        // child: [5, 25, 35] — child has no hit at 20, so landing is virtual.
        let wcii = utils::Mock::new([5u64, 20, 35]);
        let mut wcii_data = wcii.data();
        let child = utils::Mock::new([5u64, 25, 35]);
        let mut child_data = child.data();
        let mut it = OptionalOptimized::new(wcii, child, MAX_DOC_ID, WEIGHT);

        let r = it.read().expect("read").expect("result");
        assert_eq!(r.doc_id, 5);

        wcii_data.set_revalidate_result(utils::MockRevalidateResult::Move);
        child_data.set_revalidate_result(utils::MockRevalidateResult::Move);

        // wcii moves to 20; child moves to 25 — no child hit at 20 → virtual.
        let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
        let ctx = mock_ctx.spec();
        // SAFETY: test-only call with valid context
        match unsafe { it.revalidate(ctx) }.expect("revalidate") {
            RQEValidateStatus::Moved { current: Some(r) } => {
                assert_eq!(r.doc_id, 20);
                assert_eq!(r.weight, 0.); // virtual
            }
            other => panic!("expected Moved, got {other:?}"),
        }
        assert_eq!(it.last_doc_id(), 20);
        assert_eq!(wcii_data.revalidate_count(), 1);
        assert_eq!(child_data.revalidate_count(), 1);

        // Can still read after revalidation.
        let r = it.read().expect("read after revalidate").expect("result");
        assert!(r.doc_id > 20);
    }
}
