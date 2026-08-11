/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use ffi::IndexFlags_Index_DocIdsOnly;
use index_result::{RSIndexResult, RSResultKind};
use inverted_index::{InvertedIndex, doc_ids_only::DocIdsOnly};
use rqe_core::{DocId, RS_FIELDMASK_ALL};
use rqe_iterators::{
    RQEIterator, RQEValidateStatus, ResumeOutcome, SkipToOutcome, TypeErasedRQEIterator,
    empty::Empty, id_list::IdListSorted, inverted_index::Wildcard, not::Not,
    optional_optimized::OptionalOptimized,
};
use rqe_iterators_test_utils::{ResumeOutcomeExt, revalidate_via_resume};

use rqe_iterators_test_utils::ContractChecker;

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
    fn new(max_doc_id: DocId) -> Self {
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

    const MAX_DOC_ID: DocId = 100;
    const WEIGHT: f64 = 2.;
    const NUM_DOCS: usize = 5;
    const CHILD_DOCS: [DocId; NUM_DOCS] = [10, 20, 30, 50, 80];

    fn setup() -> WildcardIndex {
        WildcardIndex::new(MAX_DOC_ID)
    }

    fn create_optional_optimized<'index>(
        wcii_index: &'index WildcardIndex,
    ) -> ContractChecker<
        OptionalOptimized<'index, Wildcard<'index, DocIdsOnly>, utils::Mock<'index, NUM_DOCS>>,
    > {
        let wcii = wcii_index.create_iterator();
        let child = utils::Mock::new(CHILD_DOCS);
        ContractChecker::new(OptionalOptimized::new(wcii, child, MAX_DOC_ID, WEIGHT))
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

        const TARGET: DocId = 20;
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
        const TARGET: DocId = 25;
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
        const WCII_DOCS: [DocId; 2] = [5, 150];
        let wcii = utils::Mock::new(WCII_DOCS);
        let child: Empty = Empty;
        let mut it = ContractChecker::new(OptionalOptimized::new(wcii, child, 100, WEIGHT));

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
        const WCII_DOCS: [DocId; 2] = [5, 150];
        let wcii = utils::Mock::new(WCII_DOCS);
        let child: Empty = Empty;
        let mut it = ContractChecker::new(OptionalOptimized::new(wcii, child, 100, WEIGHT));

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
        const WILDCARD_DOCS: [DocId; 19] = [
            5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80, 85, 90, 95,
        ];
        const CHILD_DOCS_EXH: [DocId; 8] = [20, 30, 40, 50, 60, 70, 80, 90];
        const WEIGHT_EXH: f64 = 4.6;
        const MAX_EXH: DocId = 95;

        let wcii = utils::Mock::new(WILDCARD_DOCS);
        let child = utils::Mock::new(CHILD_DOCS_EXH);
        let mut it = ContractChecker::new(OptionalOptimized::new(wcii, child, MAX_EXH, WEIGHT_EXH));

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

    const MAX_DOC_ID: DocId = 50;
    const WEIGHT: f64 = 3.;

    fn setup() -> WildcardIndex {
        WildcardIndex::new(MAX_DOC_ID)
    }

    fn create<'index>(
        wcii_index: &'index WildcardIndex,
    ) -> ContractChecker<OptionalOptimized<'index, Wildcard<'index, DocIdsOnly>, Empty>> {
        let wcii = wcii_index.create_iterator();
        ContractChecker::new(OptionalOptimized::new(wcii, Empty, MAX_DOC_ID, WEIGHT))
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

    const MAX_DOC_ID: DocId = 100;
    const WEIGHT: f64 = 1.5;

    /// `read()` returns `None` and sets `at_eof` when `wcii` runs out of documents
    /// before `max_doc_id` is reached.
    #[test]
    fn test_read_wcii_exhausted_before_max_doc_id() {
        // wcii only has docs [5, 15]; max_doc_id is 100.
        // After consuming both, the next read() must return None.
        let wcii = utils::Mock::new([5u64, 15]);
        let child: Empty = Empty;
        let mut it = ContractChecker::new(OptionalOptimized::new(wcii, child, MAX_DOC_ID, WEIGHT));

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
        let mut it = ContractChecker::new(OptionalOptimized::new(wcii, child, MAX_DOC_ID, WEIGHT));

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
        let mut it = ContractChecker::new(OptionalOptimized::new(wcii, child, MAX_DOC_ID, WEIGHT));

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
        let mut it = ContractChecker::new(OptionalOptimized::new(wcii, child, MAX_DOC_ID, WEIGHT));

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
        let mut it = ContractChecker::new(OptionalOptimized::new(wcii, child, MAX_DOC_ID, WEIGHT));

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
        let mut it = ContractChecker::new(OptionalOptimized::new(wcii, child, MAX_DOC_ID, WEIGHT));

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
        let mut it = ContractChecker::new(OptionalOptimized::new(wcii, child, MAX_DOC_ID, WEIGHT));

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
        let mut it = ContractChecker::new(OptionalOptimized::new(wcii, child, MAX_DOC_ID, WEIGHT));

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
        let mut it = ContractChecker::new(OptionalOptimized::new(wcii, child, MAX_DOC_ID, WEIGHT));

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

    const MAX_DOC_ID: DocId = 100;
    const WEIGHT: f64 = 2.;
    const NUM_DOCS: usize = 5;
    const CHILD_DOCS: [DocId; NUM_DOCS] = [10, 20, 30, 50, 80];

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
            ContractChecker<
                OptionalOptimized<
                    'index,
                    Wildcard<'index, DocIdsOnly>,
                    utils::Mock<'index, NUM_DOCS>,
                >,
            >,
            utils::MockData,
        ) {
            let ii = DocIdsOnly::from_opaque(test_ctx.wildcard_inverted_index());
            let wcii = Wildcard::new(ii.reader(), 0.);
            let child = utils::Mock::new(CHILD_DOCS);
            let data = child.data();
            let it = ContractChecker::new(OptionalOptimized::new(wcii, child, MAX_DOC_ID, WEIGHT));
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

            let status = it.revalidate(&*test_ctx.spec_read()).expect("revalidate");
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

            let status = it.revalidate(&*test_ctx.spec_read()).expect("revalidate");
            // Child aborted while on a virtual result → Ok (no state change needed)
            assert!(matches!(status, RQEValidateStatus::Ok));
            assert!(
                it.inner().child().is_none(),
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
            let status = it.revalidate(&*test_ctx.spec_read()).expect("revalidate");
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
            let status = it.revalidate(&*test_ctx.spec_read()).expect("revalidate");
            // Child moved while on a virtual result → Ok
            assert!(matches!(status, RQEValidateStatus::Ok));
            assert_eq!(data.revalidate_count(), 1);
        }
    }

    #[test]
    fn test_revalidate_wcii_aborted() {
        // Use Mock as wcii so we can configure it to abort.
        const WCII_DOCS: usize = 10;
        let wcii_docs: [DocId; WCII_DOCS] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let wcii = utils::Mock::new(wcii_docs);
        let mut wcii_data = wcii.data();
        let child = utils::Mock::new(CHILD_DOCS);
        let mut it = ContractChecker::new(OptionalOptimized::new(wcii, child, MAX_DOC_ID, WEIGHT));

        // Read one result first
        let _ = it.read().expect("read").expect("result");

        wcii_data.set_revalidate_result(utils::MockRevalidateResult::Abort);
        let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
        let status = it.revalidate(&*mock_ctx.spec_read()).expect("revalidate");
        assert!(matches!(status, RQEValidateStatus::Aborted));
    }

    /// An iterator that has already run past its own end is not revived by a
    /// wildcard that moves onto a live document: the status must agree with what
    /// `current()` and `at_eof()` report, or a parent is handed a result the
    /// iterator itself denies having.
    #[test]
    fn test_revalidate_after_latched_eof_reports_no_current() {
        // `wcii` still holds 5 and 7 after doc 1, so its move lands well within
        // `MAX_DOC_ID` — the iterator's own end is what has been passed.
        let wcii = utils::Mock::new([1u64, 5, 7]);
        let mut wcii_data = wcii.data();
        let child = utils::Mock::new([9u64]);
        let mut it = ContractChecker::new(OptionalOptimized::new(wcii, child, MAX_DOC_ID, WEIGHT));

        let r = it.read().expect("read").expect("result");
        assert_eq!(r.doc_id, 1);

        // Skipping beyond `MAX_DOC_ID` runs the iterator past its end.
        assert!(it.skip_to(MAX_DOC_ID + 1).expect("skip_to").is_none());
        assert!(it.at_eof());
        assert!(it.current().is_none());

        wcii_data.set_revalidate_result(utils::MockRevalidateResult::Move);
        let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
        let status = it.revalidate(&*mock_ctx.spec_read()).expect("revalidate");
        assert!(
            matches!(status, RQEValidateStatus::Moved { current: None }),
            "expected Moved with no current, got {status:?}",
        );
        assert!(it.at_eof());
        assert!(it.current().is_none());
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
        let mut it = ContractChecker::new(OptionalOptimized::new(wcii, child, MAX_DOC_ID, WEIGHT));

        let r = it.read().expect("read").expect("result");
        assert_eq!(r.doc_id, 5);

        wcii_data.set_revalidate_result(utils::MockRevalidateResult::Move);
        let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
        let status = it.revalidate(&*mock_ctx.spec_read()).expect("revalidate");
        match status {
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
        let mut it = ContractChecker::new(OptionalOptimized::new(wcii, child, MAX_DOC_ID, WEIGHT));

        let r = it.read().expect("read").expect("result");
        assert_eq!(r.doc_id, 5);

        wcii_data.set_revalidate_result(utils::MockRevalidateResult::Move);
        let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
        let status = it.revalidate(&*mock_ctx.spec_read()).expect("revalidate");
        match status {
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
        let mut it = ContractChecker::new(OptionalOptimized::new(wcii, child, MAX_DOC_ID, WEIGHT));

        let r = it.read().expect("read").expect("result");
        assert_eq!(r.doc_id, 5);

        wcii_data.set_revalidate_result(utils::MockRevalidateResult::Move);
        let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
        let status = it.revalidate(&*mock_ctx.spec_read()).expect("revalidate");
        match status {
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
        let mut it = ContractChecker::new(OptionalOptimized::new(wcii, child, MAX_DOC_ID, WEIGHT));

        // Consume the only document; wcii's last_doc_id is now 5 (stale after EOF).
        let r = it.read().expect("read").expect("result");
        assert_eq!(r.doc_id, 5);

        // wcii is at EOF; Move revalidation returns Moved { current: None }.
        wcii_data.set_revalidate_result(utils::MockRevalidateResult::Move);
        let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
        let status = it.revalidate(&*mock_ctx.spec_read()).expect("revalidate");
        match status {
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
        let mut it = ContractChecker::new(OptionalOptimized::new(wcii, child, MAX_DOC_ID, WEIGHT));

        // Position on doc 5 (real hit: both wcii and child land there).
        let r = it.read().expect("read").expect("result");
        assert_eq!(r.doc_id, 5);

        wcii_data.set_revalidate_result(utils::MockRevalidateResult::Move);
        child_data.set_revalidate_result(utils::MockRevalidateResult::Abort);

        // wcii moves to 20; child aborts → replaced by Empty → virtual hit at 20.
        let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
        let status = it.revalidate(&*mock_ctx.spec_read()).expect("revalidate");
        match status {
            RQEValidateStatus::Moved { current: Some(r) } => {
                assert_eq!(r.doc_id, 20);
                assert_eq!(r.weight, 0.); // virtual: child is gone
            }
            other => panic!("expected Moved with virtual result, got {other:?}"),
        }
        assert!(
            it.inner().child().is_none(),
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
        let mut it = ContractChecker::new(OptionalOptimized::new(wcii, child, MAX_DOC_ID, WEIGHT));

        let r = it.read().expect("read").expect("result");
        assert_eq!(r.doc_id, 5);

        wcii_data.set_revalidate_result(utils::MockRevalidateResult::Abort);
        child_data.set_revalidate_result(utils::MockRevalidateResult::Move);

        let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
        let status = it.revalidate(&*mock_ctx.spec_read()).expect("revalidate");
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
        let mut it = ContractChecker::new(OptionalOptimized::new(wcii, child, MAX_DOC_ID, WEIGHT));

        let r = it.read().expect("read").expect("result");
        assert_eq!(r.doc_id, 5);

        wcii_data.set_revalidate_result(utils::MockRevalidateResult::Move);
        child_data.set_revalidate_result(utils::MockRevalidateResult::Move);

        // wcii moves to 20; child moves to 25 — no child hit at 20 → virtual.
        let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
        let status = it.revalidate(&*mock_ctx.spec_read()).expect("revalidate");
        match status {
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

/// Revalidation driven through [`RQEIteratorBoxed::suspend`] and
/// [`RQESuspendedIterator::resume`] rather than [`RQEIterator::revalidate`]: the
/// scenarios of
/// [`optional_optimized_iterator_revalidate_tests`](super::optional_optimized_iterator_revalidate_tests),
/// plus the allocation reuse and the boundary behaviour that only this path can
/// exhibit.
mod via_resume {
    use super::*;
    use rqe_iterators::{RQEIteratorBoxed, RQESuspendedIterator};

    const MAX_DOC_ID: DocId = 100;
    const WEIGHT: f64 = 2.;
    const NUM_DOCS: usize = 5;
    const CHILD_DOCS: [DocId; NUM_DOCS] = [10, 20, 30, 50, 80];

    /// Resume tests whose wildcard is a real [`Wildcard`] over an inverted index
    /// rather than a [`utils::Mock`]. The fixture goes through
    /// [`TestContext::wildcard`](rqe_iterators_test_utils::TestContext::wildcard),
    /// which touches global C state.
    #[cfg(not(miri))]
    mod with_inverted_wildcard {
        use super::*;
        use inverted_index::opaque::OpaqueEncoding;
        use rqe_iterators::inverted_index::Wildcard;
        use rqe_iterators_test_utils::{GlobalGuard, TestContext};

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

            let guard = test_ctx.spec_read();
            let mut it = revalidate_via_resume(TypeErasedRQEIterator::new(Box::new(it)), &guard)
                .expect("resume failed")
                .expect_ok();
            assert_eq!(data.revalidate_count(), 1);

            let _ = it.read().expect("read after revalidate").expect("result");
        }

        #[test]
        fn test_revalidate_child_aborted() {
            let _guard = GlobalGuard::default();
            let test_ctx = TestContext::wildcard(1..=MAX_DOC_ID);
            let (mut it, mut data) = setup(&test_ctx);

            data.set_revalidate_result(utils::MockRevalidateResult::Abort);

            let r = it.read().expect("read").expect("result");
            assert_eq!(r.doc_id, 1);

            let guard = test_ctx.spec_read();
            // Child aborted while on a virtual result → Ok (no state change needed).
            let mut it = revalidate_via_resume(TypeErasedRQEIterator::new(Box::new(it)), &guard)
                .expect("resume failed")
                .expect_ok();
            // The resumed iterator is type-erased and no longer exposes the
            // concrete `child()` accessor; the virtual read below (weight 0)
            // confirms the child was dropped and we fell back to virtual.
            assert_eq!(data.revalidate_count(), 1);

            let r = it.read().expect("read").expect("result");
            assert_eq!(r.weight, 0.);
        }

        #[test]
        fn test_revalidate_child_moved_on_real() {
            let _guard = GlobalGuard::default();
            let test_ctx = TestContext::wildcard(1..=MAX_DOC_ID);
            let (mut it, mut data) = setup(&test_ctx);

            match it.skip_to(10).expect("no error") {
                Some(SkipToOutcome::Found(r)) => assert_eq!(r.doc_id, 10),
                other => panic!("unexpected: {other:?}"),
            }

            data.set_revalidate_result(utils::MockRevalidateResult::Move);
            let guard = test_ctx.spec_read();
            revalidate_via_resume(TypeErasedRQEIterator::new(Box::new(it)), &guard)
                .expect("resume failed")
                .expect_moved();
            assert_eq!(data.revalidate_count(), 1);
        }

        #[test]
        fn test_revalidate_child_moved_on_virtual() {
            let _guard = GlobalGuard::default();
            let test_ctx = TestContext::wildcard(1..=MAX_DOC_ID);
            let (mut it, mut data) = setup(&test_ctx);

            match it.skip_to(15).expect("no error") {
                Some(SkipToOutcome::Found(r)) => assert_eq!(r.doc_id, 15),
                other => panic!("unexpected: {other:?}"),
            }

            data.set_revalidate_result(utils::MockRevalidateResult::Move);
            let guard = test_ctx.spec_read();
            // Child moved while on a virtual result → Ok
            revalidate_via_resume(TypeErasedRQEIterator::new(Box::new(it)), &guard)
                .expect("resume failed")
                .expect_ok();
            assert_eq!(data.revalidate_count(), 1);
        }
    }

    #[test]
    fn test_revalidate_wcii_aborted() {
        const WCII_DOCS: usize = 10;
        let wcii_docs: [DocId; WCII_DOCS] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let wcii = utils::Mock::new(wcii_docs);
        let mut wcii_data = wcii.data();
        let child = utils::Mock::new(CHILD_DOCS);
        let mut it = OptionalOptimized::new(wcii, child, MAX_DOC_ID, WEIGHT);

        let _ = it.read().expect("read").expect("result");

        wcii_data.set_revalidate_result(utils::MockRevalidateResult::Abort);
        let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
        let guard = mock_ctx.spec_read();
        let outcome = revalidate_via_resume(TypeErasedRQEIterator::new(Box::new(it)), &guard)
            .expect("resume failed");
        assert!(matches!(outcome, ResumeOutcome::Aborted));
    }

    #[test]
    fn test_revalidate_wcii_moved_real_hit() {
        let wcii = utils::Mock::new([5u64, 20]);
        let mut wcii_data = wcii.data();
        let child = utils::Mock::new([5u64, 20]);
        let mut it = OptionalOptimized::new(wcii, child, MAX_DOC_ID, WEIGHT);

        let r = it.read().expect("read").expect("result");
        assert_eq!(r.doc_id, 5);

        wcii_data.set_revalidate_result(utils::MockRevalidateResult::Move);
        let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
        let guard = mock_ctx.spec_read();
        let mut it = revalidate_via_resume(TypeErasedRQEIterator::new(Box::new(it)), &guard)
            .expect("resume failed")
            .expect_moved();
        let r = it.current().expect("current");
        assert_eq!(r.doc_id, 20);
        assert_eq!(r.weight, WEIGHT);
        assert_eq!(it.last_doc_id(), 20);
    }

    #[test]
    fn test_revalidate_wcii_moved_virtual_hit() {
        let wcii = utils::Mock::new([5u64, 20]);
        let mut wcii_data = wcii.data();
        let child = utils::Mock::new([5u64, 25]);
        let mut it = OptionalOptimized::new(wcii, child, MAX_DOC_ID, WEIGHT);

        let r = it.read().expect("read").expect("result");
        assert_eq!(r.doc_id, 5);

        wcii_data.set_revalidate_result(utils::MockRevalidateResult::Move);
        let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
        let guard = mock_ctx.spec_read();
        let mut it = revalidate_via_resume(TypeErasedRQEIterator::new(Box::new(it)), &guard)
            .expect("resume failed")
            .expect_moved();
        let r = it.current().expect("current");
        assert_eq!(r.doc_id, 20);
        assert_eq!(r.weight, 0.);
        assert_eq!(it.last_doc_id(), 20);
    }

    #[test]
    fn test_revalidate_wcii_moved_past_max_doc_id() {
        let wcii = utils::Mock::new([5u64, 150]);
        let mut wcii_data = wcii.data();
        let child = utils::Mock::new(CHILD_DOCS);
        let mut it = OptionalOptimized::new(wcii, child, MAX_DOC_ID, WEIGHT);

        let r = it.read().expect("read").expect("result");
        assert_eq!(r.doc_id, 5);

        wcii_data.set_revalidate_result(utils::MockRevalidateResult::Move);
        let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
        let guard = mock_ctx.spec_read();
        let it = revalidate_via_resume(TypeErasedRQEIterator::new(Box::new(it)), &guard)
            .expect("resume failed")
            .expect_moved();
        assert!(it.at_eof());
    }

    /// Regression: wcii moves to its own EOF (single-doc Mock at EOF returns
    /// "Moved without new doc"). Optional must propagate that as Moved + EOF.
    #[test]
    fn test_revalidate_wcii_moved_to_eof() {
        let wcii = utils::Mock::new([5u64]);
        let mut wcii_data = wcii.data();
        let child = utils::Mock::new([5u64]);
        let mut it = OptionalOptimized::new(wcii, child, MAX_DOC_ID, WEIGHT);

        let r = it.read().expect("read").expect("result");
        assert_eq!(r.doc_id, 5);

        wcii_data.set_revalidate_result(utils::MockRevalidateResult::Move);
        let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
        let guard = mock_ctx.spec_read();
        let it = revalidate_via_resume(TypeErasedRQEIterator::new(Box::new(it)), &guard)
            .expect("resume failed")
            .expect_moved();
        assert!(it.at_eof(), "iterator must be at EOF");
        assert_eq!(wcii_data.revalidate_count(), 1);
    }

    #[test]
    fn test_revalidate_child_aborted_wcii_moved() {
        let wcii = utils::Mock::new([5u64, 20]);
        let mut wcii_data = wcii.data();
        let child = utils::Mock::new([5u64, 35]);
        let mut child_data = child.data();
        let mut it = OptionalOptimized::new(wcii, child, MAX_DOC_ID, WEIGHT);

        let r = it.read().expect("read").expect("result");
        assert_eq!(r.doc_id, 5);

        wcii_data.set_revalidate_result(utils::MockRevalidateResult::Move);
        child_data.set_revalidate_result(utils::MockRevalidateResult::Abort);

        let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
        let guard = mock_ctx.spec_read();
        let mut it = revalidate_via_resume(TypeErasedRQEIterator::new(Box::new(it)), &guard)
            .expect("resume failed")
            .expect_moved();
        let r = it.current().expect("current");
        assert_eq!(r.doc_id, 20);
        assert_eq!(r.weight, 0.);
        // The resumed iterator is type-erased and no longer exposes the
        // concrete `child()` accessor; the virtual result above (weight 0)
        // confirms the aborted child was replaced by `Empty`.
        assert_eq!(wcii_data.revalidate_count(), 1);
        assert_eq!(child_data.revalidate_count(), 1);
    }

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
        let guard = mock_ctx.spec_read();
        let outcome = revalidate_via_resume(TypeErasedRQEIterator::new(Box::new(it)), &guard)
            .expect("resume failed");
        assert!(matches!(outcome, ResumeOutcome::Aborted));
        assert_eq!(wcii_data.revalidate_count(), 1);
        assert_eq!(child_data.revalidate_count(), 0);
    }

    #[test]
    fn test_revalidate_child_moved_wcii_moved() {
        let wcii = utils::Mock::new([5u64, 20, 35]);
        let mut wcii_data = wcii.data();
        let child = utils::Mock::new([5u64, 25, 35]);
        let mut child_data = child.data();
        let mut it = OptionalOptimized::new(wcii, child, MAX_DOC_ID, WEIGHT);

        let r = it.read().expect("read").expect("result");
        assert_eq!(r.doc_id, 5);

        wcii_data.set_revalidate_result(utils::MockRevalidateResult::Move);
        child_data.set_revalidate_result(utils::MockRevalidateResult::Move);

        let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
        let guard = mock_ctx.spec_read();
        let mut it = revalidate_via_resume(TypeErasedRQEIterator::new(Box::new(it)), &guard)
            .expect("resume failed")
            .expect_moved();
        let r = it.current().expect("current");
        assert_eq!(r.doc_id, 20);
        assert_eq!(r.weight, 0.);
        assert_eq!(it.last_doc_id(), 20);
        assert_eq!(wcii_data.revalidate_count(), 1);
        assert_eq!(child_data.revalidate_count(), 1);

        let r = it.read().expect("read after revalidate").expect("result");
        assert!(r.doc_id > 20);
    }

    /// Regression: both `suspend` and `resume` must **reuse the allocation**.
    /// `OptionalOptimized` hands out `&mut self.virt` from
    /// `current()`/`read()`/`skip_to`, so the FFI's cached `header.current` points
    /// into the box; the previous `resume` rebuilt via `Box::new`, which moved
    /// `virt` (and the two inline sub-iterators) and dangled it.
    #[test]
    fn resume_preserves_box_address() {
        let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
        let guard = mock_ctx.spec_read();
        let wcii = utils::Mock::new([1u64, 2, 3]);
        let child = utils::Mock::new([2u64]);
        let mut it = Box::new(OptionalOptimized::new(wcii, child, 100, 2.0));
        let _ = it.read().expect("read").expect("result");
        let addr_before = &*it as *const _ as usize;

        let suspended = it.suspend();
        assert_eq!(
            &*suspended as *const _ as usize, addr_before,
            "suspend must reuse the allocation"
        );

        let active = match suspended.resume(&guard).expect("resume failed") {
            ResumeOutcome::Ok(a) | ResumeOutcome::Moved(a) => a,
            ResumeOutcome::Aborted => panic!("default mocks should not abort"),
        };
        assert_eq!(
            &*active as *const _ as usize, addr_before,
            "resume must reuse the allocation (OptionalOptimized hands out &mut self.virt)"
        );
    }

    /// Regression: `suspend` must dispatch the child's own `suspend` (not
    /// byte-cast it). With a *type-erased* child, a byte cast would keep the
    /// child's active `dyn` vtable under a suspended type, so `resume` would
    /// mis-dispatch and the child would be lost (dropped to `Empty`). A surviving
    /// `Present` child is the discriminating signal.
    #[test]
    fn resume_with_type_erased_child_survives() {
        let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
        let guard = mock_ctx.spec_read();
        let wcii = utils::Mock::new([1u64, 2, 3]);
        let child = TypeErasedRQEIterator::new(Box::new(utils::Mock::new([2u64])));
        let mut it = Box::new(OptionalOptimized::new(wcii, child, 100, 2.0));
        let _ = it.read().expect("read").expect("result");

        let suspended = it.suspend();
        let active = match suspended.resume(&guard).expect("resume failed") {
            ResumeOutcome::Ok(a) | ResumeOutcome::Moved(a) => a,
            ResumeOutcome::Aborted => panic!("default mocks should not abort"),
        };
        assert!(
            active.child().is_some(),
            "type-erased child must survive suspend/resume (not be dropped to Empty)"
        );
    }

    /// Regression: a wildcard that resumes by re-seeking *past* the end must be
    /// recognised as having no current, not settled onto doc 0.
    ///
    /// `resume` used to recover "moved to a new doc" vs "moved off the end" by
    /// comparing the wildcard's pre- and post-resume `last_doc_id`, expecting
    /// EOF to leave it unchanged. The inverted-index wildcards rewind before
    /// re-seeking (`RawInvIndIterator::resume_in_place`), so a GC that removes
    /// the current doc and everything after it returns EOF with `last_doc_id()`
    /// reset to 0 — never equal to the pre-resume value. The comparison missed
    /// it, fell through to the moved-to-a-valid-doc path with `wcii_doc_id == 0`
    /// and produced a virtual result for the non-existent doc 0, reported as
    /// `Moved` with `at_eof()` false.
    #[test]
    fn resume_wcii_reseeking_past_end_reports_no_current() {
        let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
        let guard = mock_ctx.spec_read();
        // The wildcard still has ids after 5, so nothing about its *contents*
        // signals EOF — only the rewind-then-miss resume does.
        let wcii = utils::Mock::new([5u64, 20, 50]);
        let mut wcii_data = wcii.data();
        let child = utils::Mock::new([5u64]);
        let mut it = Box::new(OptionalOptimized::new(wcii, child, 100, WEIGHT));

        let r = it.read().expect("read").expect("result");
        assert_eq!(r.doc_id, 5);

        wcii_data.set_resume_reseeks_past_end(true);
        let mut active = match it.suspend().resume(&guard).expect("resume failed") {
            ResumeOutcome::Moved(a) => a,
            ResumeOutcome::Ok(_) => panic!("expected Moved, got Ok"),
            ResumeOutcome::Aborted => panic!("expected Moved, got Aborted"),
        };

        assert!(
            active.current().is_none(),
            "the wildcard re-seeked past its end, so there is no current result",
        );
        assert!(active.at_eof(), "and the iterator is exhausted");
        assert!(
            active.read().expect("read after resume").is_none(),
            "nothing left to read",
        );
    }

    /// A moved wcii that lands on a doc the child also matches must return that
    /// hit with the optional weight applied (mirrors `read`/`skip_to`), and must
    /// NOT report EOF for a valid in-bound doc even though the wildcard itself is
    /// now `at_eof()`. (Codex T3 + T13/T16.)
    #[test]
    fn resume_moved_real_hit_applies_weight_and_is_not_eof() {
        let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
        let guard = mock_ctx.spec_read();
        // wcii `[5, 20]`, child `[5, 20]`, max_doc_id 100: after reading doc 5,
        // a moved resume advances wcii to its last doc 20 (`wcii.at_eof()` now
        // true) which the child also matches.
        let wcii = utils::Mock::new([5u64, 20]);
        let mut wcii_data = wcii.data();
        let child = utils::Mock::new([5u64, 20]);
        let mut it = Box::new(OptionalOptimized::new(wcii, child, 100, WEIGHT));

        let r = it.read().expect("read").expect("result");
        assert_eq!(r.doc_id, 5);

        wcii_data.set_revalidate_result(utils::MockRevalidateResult::Move);
        let mut active = match it.suspend().resume(&guard).expect("resume failed") {
            ResumeOutcome::Moved(a) => a,
            ResumeOutcome::Ok(_) => panic!("expected Moved, got Ok"),
            ResumeOutcome::Aborted => panic!("expected Moved, got Aborted"),
        };
        assert!(
            !active.at_eof(),
            "doc 20 < max_doc_id is a valid current, not EOF, even though wcii.at_eof()"
        );
        let cur = active.current().expect("current after moved resume");
        assert_eq!(cur.doc_id, 20);
        assert_eq!(
            cur.weight, WEIGHT,
            "moved real hit must carry the optional weight"
        );
    }

    /// A moved wcii that lands on a valid in-bound doc the child does NOT match
    /// returns a virtual hit at that doc (zero weight) and is not EOF, again even
    /// though the wildcard is exhausted. (Codex T13/T16, virtual case.)
    #[test]
    fn resume_moved_virtual_hit_at_last_doc_is_not_eof() {
        let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
        let guard = mock_ctx.spec_read();
        // wcii `[5, 20]`, child `[5, 25]`: after doc 5, wcii moves to 20; the
        // child has no match at 20 (its next is 25) → virtual hit.
        let wcii = utils::Mock::new([5u64, 20]);
        let mut wcii_data = wcii.data();
        let child = utils::Mock::new([5u64, 25]);
        let mut it = Box::new(OptionalOptimized::new(wcii, child, 100, WEIGHT));

        let r = it.read().expect("read").expect("result");
        assert_eq!(r.doc_id, 5);

        wcii_data.set_revalidate_result(utils::MockRevalidateResult::Move);
        let mut active = match it.suspend().resume(&guard).expect("resume failed") {
            ResumeOutcome::Moved(a) => a,
            ResumeOutcome::Ok(_) => panic!("expected Moved, got Ok"),
            ResumeOutcome::Aborted => panic!("expected Moved, got Aborted"),
        };
        assert!(
            !active.at_eof(),
            "doc 20 < max_doc_id is a valid current, not EOF"
        );
        let cur = active.current().expect("current after moved resume");
        assert_eq!(cur.doc_id, 20);
        assert_eq!(cur.weight, 0., "virtual hit carries zero weight");
    }

    /// An `OptionalOptimized` already exhausted by its own bound (via
    /// `skip_to(max_doc_id + 1)`, which sets `at_eof` without advancing wcii) must
    /// keep that EOF across a resume where the wildcard is unchanged — the resume
    /// must not reset `at_eof` from the still-live `wcii.at_eof()`. (Codex T12.)
    #[test]
    fn resume_preserves_bound_eof_when_wcii_unchanged() {
        let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
        let guard = mock_ctx.spec_read();
        // wcii still has docs (so `wcii.at_eof()` is false), but the iterator is
        // driven past its own bound.
        let wcii = utils::Mock::new([5u64, 20, 50]);
        let child = utils::Mock::new([5u64]);
        let mut it = Box::new(OptionalOptimized::new(wcii, child, 100, WEIGHT));

        // Exhaust by bound without advancing wcii.
        assert!(it.skip_to(101).expect("skip_to").is_none());
        assert!(it.at_eof());

        // wcii resumes unchanged (default Ok); EOF must survive.
        let active = match it.suspend().resume(&guard).expect("resume failed") {
            ResumeOutcome::Ok(a) => a,
            ResumeOutcome::Moved(_) => panic!("expected Ok (unchanged wcii), got Moved"),
            ResumeOutcome::Aborted => panic!("expected Ok, got Aborted"),
        };
        assert!(
            active.at_eof(),
            "bound-EOF must survive resume; must not be reset from wcii.at_eof()"
        );
    }

    /// The moved-wcii sibling of [`resume_preserves_bound_eof_when_wcii_unchanged`]:
    /// a wildcard that resumes onto a live in-bound document does not revive an
    /// iterator already past its own end — and must not *settle* on it either.
    /// `revalidate`'s `past_end` guard reports the move and leaves the position and
    /// the child untouched; resume has to make the same call rather than dragging
    /// `last_doc_id` and the child forward on a finished iterator.
    #[test]
    fn resume_moved_wcii_does_not_settle_a_finished_iterator() {
        let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
        let guard = mock_ctx.spec_read();
        // wcii holds live in-bound docs beyond 1; the iterator is finished by its
        // own bound instead (`skip_to` past `max_doc_id` sets `past_end` without
        // moving wcii).
        let wcii = utils::Mock::new([1u64, 5, 8]);
        let mut wcii_data = wcii.data();
        // The child sits at 3 after the first read's catch-up; a settle at wcii's
        // moved-to doc 5 would have to seek it again, which the counter exposes.
        let child = utils::Mock::new([3u64, 7]);
        let child_data = child.data();
        let mut it = Box::new(OptionalOptimized::new(wcii, child, 10, WEIGHT));

        let r = it.read().expect("read").expect("result");
        assert_eq!(r.doc_id, 1);
        assert!(it.skip_to(11).expect("skip_to").is_none());
        assert!(it.at_eof());
        let child_reads_before = child_data.read_count();

        // GC moves the wildcard onto doc 5 — live and within the bound.
        wcii_data.set_revalidate_result(utils::MockRevalidateResult::Move);
        let mut active = match it.suspend().resume(&guard).expect("resume failed") {
            ResumeOutcome::Moved(a) => a,
            ResumeOutcome::Ok(_) => panic!("expected Moved (wcii moved), got Ok"),
            ResumeOutcome::Aborted => panic!("expected Moved, got Aborted"),
        };

        assert!(active.at_eof(), "past the end stays past the end");
        assert!(active.current().is_none());
        assert_eq!(
            active.last_doc_id(),
            1,
            "a finished iterator's position must not be dragged forward",
        );
        assert_eq!(
            child_data.read_count(),
            child_reads_before,
            "the child of a finished iterator is not seeked",
        );
    }

    /// If the virtual sentinel is no longer virtual — a consumer replaced it with
    /// index-backed data via the mutable `current()`/`read()`/`skip_to` handout —
    /// resume cannot re-validate it, so it aborts the whole iterator (returns
    /// `Aborted`, not an error), mirroring `Optional::resume`.
    #[test]
    fn resume_aborts_when_virt_no_longer_virtual() {
        let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
        let guard = mock_ctx.spec_read();
        // child has no match at doc 5 → the first read yields the virtual sentinel.
        let wcii = utils::Mock::new([5u64]);
        let child = utils::Mock::new([10u64]);
        let mut it = Box::new(OptionalOptimized::new(wcii, child, 100, WEIGHT));

        let r = it.read().expect("read").expect("result");
        assert_eq!(r.doc_id, 5);
        assert_eq!(r.kind(), RSResultKind::Virtual);
        // Simulate a consumer swapping the sentinel for a real, index-backed result.
        *r = RSIndexResult::build_numeric(1.0).build();

        let outcome = it
            .suspend()
            .resume(&guard)
            .expect("resume must not surface an error");
        assert!(
            matches!(outcome, ResumeOutcome::Aborted),
            "a non-virtual sentinel must abort the resume",
        );
    }

    /// A wcii that moves to exactly `max_doc_id` still has a valid current result
    /// there, and is *not* yet at EOF.
    ///
    /// This used to assert the opposite — `at_eof()` true alongside a live
    /// `current()` — because `at_eof` was a look-ahead that `settle_at` raised the
    /// moment the position reached the bound. `at_eof()` is now the negation of
    /// `current()`, so landing on the last document of the range is an ordinary live
    /// position and settling records nothing; the next `read` sees `!has_next()` and
    /// sets `past_end` there.
    ///
    /// The hazard the test guards is unchanged: a caller must not lose the last
    /// document of the range on a moved resume. No other moved-resume test lands
    /// exactly on `max_doc_id`, so this is the only place the boundary is
    /// distinguishable from an ordinary in-range hit.
    #[test]
    fn resume_moved_to_exactly_max_doc_id_keeps_the_hit() {
        let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
        let guard = mock_ctx.spec_read();
        // max_doc_id == 20, and the move lands exactly on it, with a child hit.
        let wcii = utils::Mock::new([5u64, 20]);
        let mut wcii_data = wcii.data();
        let child = utils::Mock::new([5u64, 20]);
        let mut it = Box::new(OptionalOptimized::new(wcii, child, 20, WEIGHT));

        let r = it.read().expect("read").expect("result");
        assert_eq!(r.doc_id, 5);

        wcii_data.set_revalidate_result(utils::MockRevalidateResult::Move);
        let mut active = match it.suspend().resume(&guard).expect("resume failed") {
            ResumeOutcome::Moved(a) => a,
            ResumeOutcome::Ok(_) => panic!("expected Moved, got Ok"),
            ResumeOutcome::Aborted => panic!("expected Moved, got Aborted"),
        };

        assert!(
            !active.at_eof(),
            "doc 20 is the bound but still a live result, so nothing has run past the end",
        );
        let cur = active.current().expect("doc 20 is a valid current result");
        assert_eq!(cur.doc_id, 20);
        assert_eq!(cur.weight, WEIGHT, "a real hit carries the optional weight");

        // Only the read *after* the bound runs past the end.
        assert!(active.read().expect("read must not fail").is_none());
        assert!(active.at_eof());
        assert!(active.current().is_none());
    }

    /// A *type-erased wildcard* must survive the cycle.
    ///
    /// `suspend` dispatches both slots through `suspend_child_slot_in_place`
    /// precisely because an erased slot's active and suspended forms carry
    /// different vtables. Only the child slot was covered; a byte cast of the
    /// wildcard slot would mis-dispatch on resume.
    #[test]
    fn resume_with_type_erased_wcii_survives() {
        let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
        let guard = mock_ctx.spec_read();
        let wcii = TypeErasedRQEIterator::new(Box::new(utils::Mock::new([1u64, 2, 3])));
        let child = utils::Mock::new([2u64]);
        let mut it = Box::new(OptionalOptimized::new(wcii, child, 100, WEIGHT));

        let r = it.read().expect("read").expect("result");
        assert_eq!(r.doc_id, 1);

        let mut active = match it.suspend().resume(&guard).expect("resume failed") {
            ResumeOutcome::Ok(a) | ResumeOutcome::Moved(a) => a,
            ResumeOutcome::Aborted => panic!("default mocks should not abort"),
        };
        // The erased wildcard still drives the iteration after the round trip.
        let r = active.read().expect("read after resume").expect("result");
        assert_eq!(r.doc_id, 2);
        assert_eq!(r.weight, WEIGHT, "doc 2 is a real child hit");
    }

    /// Resuming a second time, with the child already replaced by `Empty`.
    ///
    /// The first resume aborts the child and installs `MaybeEmpty`'s `None`
    /// arm; the second then has to suspend and resume *that* arm. Every other
    /// test stops after one cycle, so the empty-child arm was never carried
    /// through one.
    #[test]
    fn resume_twice_after_child_abort_stays_virtual() {
        let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
        let wcii = utils::Mock::new([5u64, 20, 50]);
        let child = utils::Mock::new([5u64, 20]);
        let mut child_data = child.data();
        let mut it = Box::new(OptionalOptimized::new(wcii, child, 100, WEIGHT));

        let r = it.read().expect("read").expect("result");
        assert_eq!(r.doc_id, 5);
        assert_eq!(r.weight, WEIGHT, "doc 5 is a real child hit");

        // First cycle: the child aborts and is replaced by `Empty`.
        child_data.set_revalidate_result(utils::MockRevalidateResult::Abort);
        let guard = mock_ctx.spec_read();
        let active = match it.suspend().resume(&guard).expect("first resume failed") {
            ResumeOutcome::Ok(a) | ResumeOutcome::Moved(a) => a,
            ResumeOutcome::Aborted => panic!("an aborted child must not abort the whole iterator"),
        };
        assert!(active.child().is_none(), "child replaced by Empty");

        // Second cycle: suspend/resume the `None` arm.
        let mut active = match active
            .suspend()
            .resume(&guard)
            .expect("second resume failed")
        {
            ResumeOutcome::Ok(a) | ResumeOutcome::Moved(a) => a,
            ResumeOutcome::Aborted => panic!("an empty child must not abort the resume"),
        };
        assert!(active.child().is_none(), "still Empty after a second cycle");

        // Still usable, and now purely virtual.
        let r = active.read().expect("read after resume").expect("result");
        assert_eq!(r.weight, 0., "the child is gone, so every hit is virtual");
    }

    /// An error from the wildcard's own `resume` propagates, exercising the
    /// teardown guard.
    ///
    /// This is the early-return path where `FreeSuspendedShell` has to drop the
    /// still-suspended `virt` and free the shell by hand, without touching the
    /// moved-out child slots. Nothing else in the suite reaches it, so the
    /// hand-rolled `dealloc` went unverified — including under miri.
    #[test]
    fn resume_wcii_error_propagates_and_frees_the_shell() {
        let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
        let guard = mock_ctx.spec_read();
        let wcii = utils::Mock::new([5u64, 20]);
        let mut wcii_data = wcii.data();
        let child = utils::Mock::new([5u64]);
        let mut it = Box::new(OptionalOptimized::new(wcii, child, 100, WEIGHT));

        let r = it.read().expect("read").expect("result");
        assert_eq!(r.doc_id, 5);

        wcii_data.set_error_on_resume(Some(utils::MockIteratorError::TimeoutError(None)));
        let result = it.suspend().resume(&guard);
        assert!(
            matches!(result, Err(rqe_iterators::RQEIteratorError::TimedOut)),
            "a wildcard resume failure must propagate out of OptionalOptimized::resume",
        );
    }

    /// The suspended form's accessors exist so callers can interrogate a
    /// suspended iterator *without* resuming it, which is only useful if they
    /// answer what the active iterator answered a moment earlier. Suspending
    /// changes no position, so both must survive the transition unchanged.
    #[test]
    fn suspended_accessors_agree_with_the_active_iterator() {
        let wcii = utils::Mock::new([5u64, 20, 50]);
        let child = utils::Mock::new([5u64, 20]);
        let mut it = Box::new(OptionalOptimized::new(wcii, child, MAX_DOC_ID, WEIGHT));

        // Advance past the initial state so `last_doc_id` is not the `0` an
        // unread iterator would report either way.
        assert_eq!(it.read().expect("read").expect("result").doc_id, 5);
        assert_eq!(it.read().expect("read").expect("result").doc_id, 20);

        let active_last_doc_id = RQEIterator::last_doc_id(&*it);
        let active_num_estimated = RQEIterator::num_estimated(&*it);
        assert_eq!(active_last_doc_id, 20);
        assert_eq!(
            active_num_estimated, 3,
            "delegated to the wildcard base, which has 3 docs"
        );

        let suspended = it.suspend();
        assert_eq!(
            RQESuspendedIterator::last_doc_id(&*suspended),
            active_last_doc_id,
        );
        assert_eq!(
            RQESuspendedIterator::num_estimated(&*suspended),
            active_num_estimated,
        );
    }

    /// The type-erased sibling of
    /// [`suspended_accessors_agree_with_the_active_iterator`].
    ///
    /// The suspended `num_estimated` delegates to the suspended wildcard, so
    /// with an erased base it dispatches through the *suspended* vtable — a
    /// different code path from the concrete case, and the one that only holds
    /// because `suspend` transitions the slot through the base's own `suspend`
    /// rather than byte-casting it.
    #[test]
    fn suspended_accessors_agree_with_a_type_erased_wildcard() {
        let wcii = TypeErasedRQEIterator::new(Box::new(utils::Mock::new([5u64, 20, 50])));
        let child = utils::Mock::new([5u64, 20]);
        let mut it = Box::new(OptionalOptimized::new(wcii, child, MAX_DOC_ID, WEIGHT));

        assert_eq!(it.read().expect("read").expect("result").doc_id, 5);
        assert_eq!(it.read().expect("read").expect("result").doc_id, 20);

        let active_last_doc_id = RQEIterator::last_doc_id(&*it);
        let active_num_estimated = RQEIterator::num_estimated(&*it);
        assert_eq!(active_last_doc_id, 20);
        assert_eq!(active_num_estimated, 3);

        let suspended = it.suspend();
        assert_eq!(
            RQESuspendedIterator::last_doc_id(&*suspended),
            active_last_doc_id,
        );
        assert_eq!(
            RQESuspendedIterator::num_estimated(&*suspended),
            active_num_estimated,
        );
    }
}

/// A `Not` child whose `max_doc_id` coincides with this iterator's, over a
/// document the negation excludes, is the configuration that used to panic: the
/// child's `skip_to` ran off the end while publishing the probed id as its
/// position, and the real-hit branch unwrapped a `current()` that was `None`.
///
/// The document is virtual — the negation excludes it — so the wildcard's own
/// result is what must come back.
#[test]
fn not_child_running_off_the_end_at_max_doc_id_yields_a_virtual_hit() {
    const MAX: DocId = 10;

    let wcii_index = WildcardIndex::new(MAX);
    // `Not` over {MAX} yields 1..MAX and nothing at MAX itself, so a probe at MAX
    // finds no result at or after it.
    let child = Not::new(
        IdListSorted::new(vec![MAX]),
        MAX,
        1.0,
        timeout::NoTimeoutChecker,
    );
    let mut it = ContractChecker::new(OptionalOptimized::new(
        wcii_index.create_iterator(),
        child,
        MAX,
        2.0,
    ));

    let mut doc_ids = Vec::new();
    while let Some(result) = it.read().expect("read must not fail") {
        let doc_id = result.doc_id;
        // Every document up to MAX - 1 is a real hit through `Not`; MAX itself is
        // excluded by the negation, so it comes back virtual and unweighted.
        let expected_weight = if doc_id == MAX { 0.0 } else { 2.0 };
        assert_eq!(
            result.weight,
            expected_weight,
            "doc {doc_id} should be a {} hit",
            if doc_id == MAX { "virtual" } else { "real" },
        );
        doc_ids.push(doc_id);
    }

    assert_eq!(doc_ids, (1..=MAX).collect::<Vec<_>>());
    assert!(it.at_eof());
    assert!(it.current().is_none());
}

#[test]
fn optional_optimized_upholds_current_contract() {
    use rqe_iterators_test_utils::{assert_current_contract, assert_current_contract_via_skip_to};
    let mut it = ContractChecker::new(OptionalOptimized::new(
        utils::Mock::new([1u64, 2, 3, 4, 5]),
        utils::Mock::new([2u64, 4]),
        5,
        2.0,
    ));
    assert_eq!(assert_current_contract(&mut it), [1, 2, 3, 4, 5]);
    assert_current_contract_via_skip_to(&mut it, 6);
}
