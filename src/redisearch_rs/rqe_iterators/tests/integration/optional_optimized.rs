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
    RQEIterator, RQEValidateStatus, SkipToOutcome,
    empty::Empty,
    optional_optimized::OptionalOptimized,
    wildcard::Wildcard,
};

use crate::utils;

mod optional_optimized_iterator_tests {
    use super::*;

    const MAX_DOC_ID: t_docId = 100;
    const WEIGHT: f64 = 2.;
    const NUM_DOCS: usize = 5;
    const CHILD_DOCS: [t_docId; NUM_DOCS] = [10, 20, 30, 50, 80];

    fn setup<'index>() -> OptionalOptimized<'index, Wildcard<'index>, utils::Mock<'index, NUM_DOCS>>
    {
        let wcii = Wildcard::new(MAX_DOC_ID, 0.);
        let child = utils::Mock::new(CHILD_DOCS);
        OptionalOptimized::new(wcii, child, MAX_DOC_ID, WEIGHT)
    }

    #[test]
    fn test_read_mixed_results() {
        let mut it = setup();

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
        let mut it = setup();

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
        let mut it = setup();

        // 25 is not in CHILD_DOCS but is present in wcii (Wildcard covers 1..=100)
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
        let mut it = setup();

        // Skip to doc 15; wcii (Wildcard) lands exactly on 15 (Found), child has no match.
        match it.skip_to(15).expect("no error") {
            Some(SkipToOutcome::Found(r)) => {
                assert_eq!(r.doc_id, 15);
                assert_eq!(r.weight, 0.);
            }
            other => panic!("unexpected outcome: {other:?}"),
        }
        assert_eq!(it.last_doc_id(), 15);

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
        let mut it = setup();

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
        let mut it = setup();

        match it.skip_to(MAX_DOC_ID).expect("no error") {
            Some(SkipToOutcome::Found(r)) => assert_eq!(r.doc_id, MAX_DOC_ID),
            other => panic!("unexpected outcome: {other:?}"),
        }

        assert_eq!(it.last_doc_id(), MAX_DOC_ID);
        assert!(it.read().expect("no error").is_none());
        assert!(it.at_eof());
        assert!(it.read().expect("no error").is_none());
        assert!(it.skip_to(MAX_DOC_ID + 1).expect("no error beyond max").is_none());
    }

    /// Regression test: `read` must stop at `max_doc_id` even when `wcii` would
    /// yield documents beyond that bound.
    ///
    /// Before the fix, the `last_doc_id >= max_doc_id` guard was absent from
    /// `read`, so a `wcii` that produced doc IDs beyond `max_doc_id` would cause
    /// those extra documents to be emitted instead of returning EOF.
    #[test]
    fn test_read_stops_at_max_doc_id() {
        // wcii has docs [5, 10, 150] but max_doc_id is 100.
        // Doc 150 must never be returned.
        const WCII_DOCS: [t_docId; 3] = [5, 10, 150];
        let wcii = utils::Mock::new(WCII_DOCS);
        let child: Empty = Empty;
        let mut it = OptionalOptimized::new(wcii, child, 10, WEIGHT);

        let r = it.read().expect("no error").expect("doc 5");
        assert_eq!(r.doc_id, 5);

        let r = it.read().expect("no error").expect("doc 10");
        assert_eq!(r.doc_id, 10);

        // last_doc_id (10) >= max_doc_id (10) → must stop here.
        assert!(it.read().expect("no error").is_none());
        assert!(it.at_eof());
    }

    #[test]
    fn test_weight_application() {
        let mut it = setup();

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
    use super::*;

    const MAX_DOC_ID: t_docId = 50;
    const WEIGHT: f64 = 3.;

    fn setup<'index>() -> OptionalOptimized<'index, Wildcard<'index>, Empty> {
        let wcii = Wildcard::new(MAX_DOC_ID, 0.);
        OptionalOptimized::new(wcii, Empty, MAX_DOC_ID, WEIGHT)
    }

    #[test]
    fn test_read_all_virtual_results() {
        let mut it = setup();

        for expected_id in 1..=MAX_DOC_ID {
            let r = it.read().expect("no error").expect("some result");
            assert_eq!(r.doc_id, expected_id);
            assert_eq!(r.weight, 0.);
            assert_eq!(r.freq, 1);
            assert_eq!(r.field_mask, RS_FIELDMASK_ALL);
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
        let mut it = setup();

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
        let mut it = setup();

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

mod optional_optimized_iterator_revalidate_tests {
    use super::*;

    const MAX_DOC_ID: t_docId = 100;
    const WEIGHT: f64 = 2.;
    const NUM_DOCS: usize = 5;
    const CHILD_DOCS: [t_docId; NUM_DOCS] = [10, 20, 30, 50, 80];

    fn setup<'index>() -> (
        OptionalOptimized<'index, Wildcard<'index>, utils::Mock<'index, NUM_DOCS>>,
        utils::MockData,
    ) {
        let wcii = Wildcard::new(MAX_DOC_ID, 0.);
        let child = utils::Mock::new(CHILD_DOCS);
        let data = child.data();
        let it = OptionalOptimized::new(wcii, child, MAX_DOC_ID, WEIGHT);
        (it, data)
    }

    #[test]
    fn test_revalidate_ok() {
        let (mut it, mut data) = setup();
        data.set_revalidate_result(utils::MockRevalidateResult::Ok);

        let _ = it.read().expect("read").expect("result");
        let _ = it.read().expect("read").expect("result");

        let status = it.revalidate().expect("revalidate");
        assert!(matches!(status, RQEValidateStatus::Ok));
        assert_eq!(data.revalidate_count(), 1);

        // Can continue reading
        let _ = it.read().expect("read after revalidate").expect("result");
    }

    #[test]
    fn test_revalidate_child_aborted() {
        let (mut it, mut data) = setup();
        data.set_revalidate_result(utils::MockRevalidateResult::Abort);

        // Position on a virtual result (doc 1)
        let r = it.read().expect("read").expect("result");
        assert_eq!(r.doc_id, 1);

        let status = it.revalidate().expect("revalidate");
        // Child aborted while on a virtual result → Ok (no state change needed)
        assert!(matches!(status, RQEValidateStatus::Ok));

        // All subsequent reads are virtual
        let r = it.read().expect("read").expect("result");
        assert_eq!(r.weight, 0.);
    }

    #[test]
    fn test_revalidate_child_moved_on_real() {
        let (mut it, mut data) = setup();

        // Position on a real result (doc 10)
        match it.skip_to(10).expect("no error") {
            Some(SkipToOutcome::Found(r)) => assert_eq!(r.doc_id, 10),
            other => panic!("unexpected: {other:?}"),
        }

        data.set_revalidate_result(utils::MockRevalidateResult::Move);
        let status = it.revalidate().expect("revalidate");
        // Child moved while on a real result → Moved
        assert!(matches!(status, RQEValidateStatus::Moved { .. }));
    }

    #[test]
    fn test_revalidate_child_moved_on_virtual() {
        let (mut it, mut data) = setup();

        // Position on a virtual result (doc 15, not in CHILD_DOCS)
        match it.skip_to(15).expect("no error") {
            Some(SkipToOutcome::Found(r)) => assert_eq!(r.doc_id, 15),
            other => panic!("unexpected: {other:?}"),
        }

        data.set_revalidate_result(utils::MockRevalidateResult::Move);
        let status = it.revalidate().expect("revalidate");
        // Child moved while on a virtual result → Ok
        assert!(matches!(status, RQEValidateStatus::Ok));
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
        let status = it.revalidate().expect("revalidate");
        assert!(matches!(status, RQEValidateStatus::Aborted));
    }
}
