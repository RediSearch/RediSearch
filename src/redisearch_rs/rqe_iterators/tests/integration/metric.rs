/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use rqe_iterators::{
    IteratorType, RQEIterator, RQEValidateStatus,
    metric::{MetricSortedById, MetricSortedByScore},
};
use rqe_iterators_test_utils::ContractChecker;

#[test]
fn type_sorted_by_id() {
    let it = ContractChecker::new(MetricSortedById::new(vec![1, 3, 5], vec![0.1, 0.3, 0.5]));
    assert_eq!(it.type_(), IteratorType::MetricSortedById);
}

#[test]
fn type_sorted_by_score() {
    let it = ContractChecker::new_unordered(MetricSortedByScore::new(
        vec![1, 3, 5],
        vec![0.1, 0.3, 0.5],
    ));
    assert_eq!(it.type_(), IteratorType::MetricSortedByScore);
}

#[test]
#[should_panic(expected = "assertion failed: ids.len() == metric_data.len()")]
fn test_metric_creation_panic() {
    let ids = vec![1, 3, 5, 7, 9];
    let metric_data = vec![0.1, 0.3, 0.5, 0.7];
    let _ = MetricSortedById::new(ids, metric_data);
}

#[test]
fn test_metric_creation() {
    let ids = vec![1, 3, 5, 7, 9];
    let metric_data = vec![0.1, 0.3, 0.5, 0.7, 0.9];
    let mut metric = ContractChecker::new(MetricSortedById::new(ids.clone(), metric_data.clone()));

    // Test that the metric was created with correct data
    assert_eq!(metric.num_estimated(), ids.len());

    // test current is correctly init based on child (idList)
    assert_eq!(metric.current().unwrap().doc_id, 0);
}

#[test]
fn score_variant_can_handle_unsorted_ids() {
    let ids = vec![5, 3, 1, 4, 2];
    assert!(!ids.is_sorted());
    let metric_data = vec![0.1, 0.3, 0.5, 0.7, 0.9];
    let _ = MetricSortedByScore::new(ids, metric_data);
}

#[test]
#[should_panic(expected = "Can't skip when working with unsorted document ids")]
fn score_variant_cannot_skip() {
    let ids = vec![5, 3, 1, 4, 2];
    let metric_data = vec![0.1, 0.3, 0.5, 0.7, 0.9];
    let mut i = ContractChecker::new_unordered(MetricSortedByScore::new(ids, metric_data));
    let _ = i.skip_to(3);
}

mod metrics_tests {
    use crate::id_cases;
    use index_result::RSResultKind;
    use rqe_iterators::{RQEIterator, SkipToOutcome, metric::MetricSortedById};
    use rqe_iterators_test_utils::ContractChecker;
    use rstest_reuse::apply;

    #[apply(id_cases)]
    fn read(#[case] case: &[u64]) {
        let metric_data: Vec<f64> = case.iter().map(|&id| id as f64 * 0.1).collect();
        let mut it =
            ContractChecker::new(MetricSortedById::new(case.to_vec(), metric_data.clone()));

        assert_eq!(it.num_estimated(), case.len());
        assert!(!it.at_eof());

        for (j, &expected_id) in case.iter().enumerate() {
            assert!(!it.at_eof());
            let res = it.read().unwrap().unwrap();
            assert_eq!(res.doc_id, expected_id);
            assert_eq!(res.kind(), RSResultKind::Metric);
            assert_eq!(res.as_numeric(), Some(metric_data[j]));

            let metrics = res.metrics_ref();
            let entry = metrics.get(0).expect("should have one entry");
            assert!(entry.key().is_none());
            assert_eq!(entry.value(), metric_data[j]);
            assert_eq!(it.last_doc_id(), expected_id);
        }

        // Sitting on the last id is not EOF; the read that runs past it is.
        assert!(!it.at_eof());
        assert!(matches!(it.read(), Ok(None)));
        assert!(it.at_eof());

        assert!(matches!(it.read(), Ok(None)));
        assert!(it.at_eof());
    }

    #[apply(id_cases)]
    #[cfg_attr(miri, ignore = "Too slow under miri")]
    fn skip_to(#[case] case: &[u64]) {
        let metric_data: Vec<f64> = case.iter().map(|&id| id as f64 * 0.1).collect();
        let mut it =
            ContractChecker::new(MetricSortedById::new(case.to_vec(), metric_data.clone()));

        // Read first element
        let first_doc = it.read().unwrap().unwrap();
        let first_id = case[0];
        assert_eq!(first_doc.doc_id, first_id);
        assert_eq!(first_doc.kind(), RSResultKind::Metric);
        assert_eq!(first_doc.as_numeric().unwrap(), metric_data[0]);

        let metrics = first_doc.metrics_ref();
        let entry = metrics.get(0).expect("should have one entry");
        assert!(entry.key().is_none());
        assert_eq!(entry.value(), metric_data[0]);
        assert_eq!(it.last_doc_id(), first_id);
        assert_eq!(it.current().unwrap().doc_id, first_id);
        // Positioned on an id, so not past the end, even when it is the last one.
        assert!(!it.at_eof(), "still positioned on {first_id}");

        // Skip to higher than last doc id: expect EOF, last_doc_id unchanged
        let last = *case.last().unwrap();
        let res = it.skip_to(last + 1); // Expect some EOF status; we only assert observable effects
        assert!(matches!(res, Ok(None)));
        drop(res);
        assert!(it.at_eof());
        assert_eq!(Some(&it.last_doc_id()), case.last());

        // Rewind
        it.rewind();
        assert!(!it.at_eof());

        // probe walks all ids from 1 up to last, probing missing and existing ids
        let mut probe = 1u64;
        for (j, &id) in case.iter().enumerate() {
            // Probe all gaps before this id
            while probe < id {
                it.rewind();
                let Ok(Some(SkipToOutcome::NotFound(res))) = it.skip_to(probe) else {
                    panic!("probe {probe} -> Expected `Some`");
                };
                assert_eq!(res.doc_id, id);
                assert_eq!(res.kind(), RSResultKind::Metric);
                assert_eq!(res.as_numeric().unwrap(), metric_data[j]);

                let metrics = res.metrics_ref();
                let entry = metrics.get(0).expect("should have one entry");
                assert!(entry.key().is_none());
                assert_eq!(entry.value(), metric_data[j]);
                // Should land on next existing id
                assert!(!it.at_eof(), "still positioned on {id}");
                assert_eq!(it.last_doc_id(), id);
                assert_eq!(it.current().unwrap().doc_id, id);
                probe += 1;
            }
            // Exact match
            it.rewind();
            let Ok(Some(SkipToOutcome::Found(res))) = it.skip_to(probe) else {
                panic!("probe {probe} -> Expected `Found`");
            };
            assert_eq!(res.doc_id, id);
            assert_eq!(res.kind(), RSResultKind::Metric);
            assert_eq!(res.as_numeric().unwrap(), metric_data[j]);

            let metrics = res.metrics_ref();
            let entry = metrics.get(0).expect("should have one entry");
            assert!(entry.key().is_none());
            assert_eq!(entry.value(), metric_data[j]);
            assert!(!it.at_eof(), "still positioned on {id}");
            assert_eq!(it.last_doc_id(), id);
            assert_eq!(it.current().unwrap().doc_id, id);
            probe += 1;
        }

        // After consuming all (by reading past end)
        assert!(matches!(it.read(), Ok(None)));
        assert!(it.at_eof());

        // Rewind and test direct skips to every existing id
        it.rewind();
        for &id in case {
            let Ok(Some(SkipToOutcome::Found(res))) = it.skip_to(id) else {
                panic!("second pass skip_to {id} -> Expected `Found`");
            };
            assert_eq!(res.doc_id, id);
            assert_eq!(it.last_doc_id(), id);
            assert_eq!(it.current().unwrap().doc_id, id);
            assert!(!it.at_eof(), "still positioned on {id}");
        }
    }

    /// Skip between any (ordered) pair of IDs in the list, testing all combinations
    #[apply(id_cases)]
    fn skip_between_any_pair(#[case] case: &[u64]) {
        if case.len() < 2 {
            return;
        }

        let metric_data: Vec<f64> = case.iter().map(|&id| id as f64 * 0.1).collect();
        let mut it = ContractChecker::new(MetricSortedById::new(case.to_vec(), metric_data));

        for from_idx in 0..case.len() - 1 {
            for to_idx in from_idx + 1..case.len() {
                it.rewind();
                assert_eq!(it.last_doc_id(), 0);
                assert_eq!(it.current().unwrap().doc_id, 0);
                assert!(!it.at_eof());

                let from_id = case[from_idx];
                let to_id = case[to_idx];

                // Skip to from_id
                let Ok(Some(SkipToOutcome::Found(doc_from))) = it.skip_to(from_id) else {
                    panic!("pair ({from_idx},{to_idx}) skip_to({from_id}) expected Found");
                };
                assert_eq!(doc_from.doc_id, from_id);
                assert_eq!(it.last_doc_id(), from_id);
                assert_eq!(it.current().unwrap().doc_id, from_id);
                assert!(!it.at_eof());

                // Skip forward to to_id
                let Ok(Some(SkipToOutcome::Found(doc_to))) = it.skip_to(to_id) else {
                    panic!("pair ({from_idx},{to_idx}) skip_to({to_id}) expected Found");
                };
                assert_eq!(doc_to.doc_id, to_id);
                assert_eq!(it.last_doc_id(), to_id);
                assert_eq!(it.current().unwrap().doc_id, to_id);
                assert!(!it.at_eof(), "still positioned on {to_id}");
            }
        }
    }

    #[apply(id_cases)]
    fn rewind(#[case] case: &[u64]) {
        let metric_data: Vec<f64> = case.iter().map(|&id| id as f64 * 0.1).collect();
        let mut it =
            ContractChecker::new(MetricSortedById::new(case.to_vec(), metric_data.clone()));

        // Skip to each doc ID, verify, then rewind and check reset
        for (j, &id) in case.iter().enumerate() {
            let Ok(Some(SkipToOutcome::Found(res))) = it.skip_to(id) else {
                panic!("skip_to({id}) expected Found");
            };
            assert_eq!(res.doc_id, id);
            assert_eq!(res.as_numeric().unwrap(), metric_data[j]);

            let metrics = res.metrics_ref();
            let entry = metrics.get(0).expect("should have one entry");
            assert!(entry.key().is_none());
            assert_eq!(entry.value(), metric_data[j]);
            assert_eq!(it.last_doc_id(), id);
            it.rewind();
            assert_eq!(it.last_doc_id(), 0);
            assert!(!it.at_eof());
        }

        // Read all docs sequentially
        for (j, &id) in case.iter().enumerate() {
            let res = it.read().unwrap().unwrap();
            assert_eq!(res.doc_id, id);
            assert_eq!(res.as_numeric().unwrap(), metric_data[j]);

            let metrics = res.metrics_ref();
            let entry = metrics.get(0).expect("should have one entry");
            assert!(entry.key().is_none());
            assert_eq!(entry.value(), metric_data[j]);
            assert_eq!(it.last_doc_id(), id);
        }

        // Read past EOF
        assert!(matches!(it.read(), Ok(None)));
        assert!(it.at_eof());
        assert_eq!(it.last_doc_id(), *case.last().unwrap());

        // Rewind after EOF
        it.rewind();
        assert_eq!(it.last_doc_id(), 0);
        assert!(!it.at_eof());
    }
}

#[test]
fn revalidate() {
    let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
    let metric_data = vec![0.1, 0.2, 0.3];
    let mut it = ContractChecker::new(MetricSortedById::new(vec![1, 2, 3], metric_data));
    let status = it.revalidate(&*mock_ctx.spec_read()).unwrap();
    assert_eq!(status, RQEValidateStatus::Ok);
}

mod via_resume {
    use super::*;
    use rlookup::RLookupKeyHandle;
    use rqe_iterators::TypeErasedRQEIterator;
    use rqe_iterators_test_utils::{ResumeOutcomeExt, revalidate_via_resume};
    use std::ptr::NonNull;

    #[test]
    fn revalidate() {
        let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
        let metric_data = vec![0.1, 0.2, 0.3];
        let mut handle = RLookupKeyHandle {
            key_ptr: std::ptr::null_mut(),
            is_valid: true,
        };
        let mut it = MetricSortedById::new(vec![1, 2, 3], metric_data);
        // SAFETY: handle_ptr points to a valid, stack-allocated RLookupKeyHandle.
        unsafe { it.set_handle(Some(NonNull::from(&mut handle))) };

        let _it = revalidate_via_resume(
            TypeErasedRQEIterator::new(Box::new(it)),
            &mock_ctx.spec_read(),
        )
        .expect("resume should not fail")
        .expect_ok();

        assert!(handle.is_valid);
    }
}

#[test]
fn metric_type_returns_vector_distance() {
    let it = MetricSortedById::new(vec![1], vec![0.5]);
    assert_eq!(
        it.metric_type(),
        rqe_iterators::metric::MetricType::VectorDistance
    );
}

#[test]
fn key_mut_ref_initially_null() {
    let mut it = MetricSortedById::new(vec![1], vec![0.5]);
    assert!(it.key_mut_ref().is_null());
}

#[test]
fn set_handle_non_null_invalidates_on_drop() {
    use rlookup::RLookupKeyHandle;
    use std::ptr::NonNull;

    let mut handle = RLookupKeyHandle {
        key_ptr: std::ptr::null_mut(),
        is_valid: true,
    };
    let handle_ptr = NonNull::from(&mut handle);

    {
        let mut it = MetricSortedById::new(vec![1], vec![0.5]);
        // SAFETY: handle_ptr points to a valid, stack-allocated RLookupKeyHandle.
        unsafe { it.set_handle(Some(handle_ptr)) };
        // it is dropped here
    }

    // After drop, the handle should be invalidated
    assert!(!handle.is_valid);
}

/// [`Metric`](rqe_iterators::Metric) delegates `current()` to an inner
/// [`IdList`](rqe_iterators::IdList), as do `MetricLazy` and `IdListLazy`.
#[test]
fn metric_upholds_current_contract() {
    use rqe_iterators_test_utils::{assert_current_contract, assert_current_contract_via_skip_to};
    let mut it = ContractChecker::new(MetricSortedById::new(vec![1u64, 3, 5], vec![0.1, 0.3, 0.5]));
    assert_eq!(assert_current_contract(&mut it), [1, 3, 5]);
    assert_current_contract_via_skip_to(&mut it, 6);
}

/// The header dispatch behind [`own_key_ref`](rqe_iterators::metric::own_key_ref)
/// and [`set_key_handle`](rqe_iterators::metric::set_key_handle): the four metric
/// flavours are distinct Rust types behind one C-ABI header, so each has to land
/// on its own fields.
mod header_dispatch {
    use std::ptr::NonNull;

    use ffi::QueryIterator;
    use rlookup::{RLookupKey, RLookupKeyHandle};
    use rqe_iterators::{
        deferred::Producer,
        interop::RQEIteratorWrapper,
        metric::{self, MetricSortedById, MetricSortedByScore, MetricType},
        metric_lazy::{MetricLazySortedById, MetricLazySortedByScore},
    };

    use crate::utils::Mock;

    /// A producer that must never run: these tests wire keys, they never read.
    fn unused_producer() -> Producer<'static> {
        Box::new(|| unreachable!("wiring a key must not read the iterator"))
    }

    /// A lowered iterator, released through the C-ABI callback `boxed_new`
    /// populated.
    ///
    /// Owning it rather than freeing by hand is what keeps a test that unwinds
    /// from leaking: `drop` runs during the unwind, which is the safe point a
    /// panicking dispatch otherwise leaves no room for. Both leak checks in CI
    /// — miri and the sanitized run — fail on a leak, so a `#[should_panic]`
    /// test cannot simply abandon its iterator.
    struct OwnedHeader(Option<NonNull<QueryIterator>>);

    impl OwnedHeader {
        fn new(raw: NonNull<QueryIterator>) -> Self {
            Self(Some(raw))
        }

        fn as_non_null(&self) -> NonNull<QueryIterator> {
            self.0.expect("iterator has already been released")
        }

        /// Release now, for a test that has to observe what freeing did — the
        /// handle invalidation is only visible once the iterator has gone.
        fn free(&mut self) {
            let Some(it) = self.0.take() else {
                return;
            };
            // SAFETY: `it` is a live header from `boxed_new`, taken out of the
            // option so this runs exactly once, and unused afterwards.
            let free = unsafe { it.as_ref() }.Free.expect("Free must be populated");
            // SAFETY: `boxed_new` populates every callback, and ownership passes here.
            unsafe { free(it.as_ptr()) };
        }
    }

    impl Drop for OwnedHeader {
        fn drop(&mut self) {
            self.free();
        }
    }

    /// One lowered iterator per metric flavour, named for the assertion messages.
    fn headers() -> [(&'static str, OwnedHeader); 4] {
        let raw = [
            RQEIteratorWrapper::boxed_new(MetricSortedById::new(vec![1], vec![0.5])),
            RQEIteratorWrapper::boxed_new(MetricSortedByScore::new(vec![1], vec![0.5])),
            RQEIteratorWrapper::boxed_new(MetricLazySortedById::new(
                unused_producer(),
                1,
                MetricType::VectorDistance,
            )),
            RQEIteratorWrapper::boxed_new(MetricLazySortedByScore::new(
                unused_producer(),
                1,
                MetricType::VectorDistance,
            )),
        ];
        let names = [
            "sorted by id",
            "sorted by score",
            "lazy sorted by id",
            "lazy sorted by score",
        ];

        std::array::from_fn(|i| (names[i], OwnedHeader::new(raw[i])))
    }

    #[test]
    fn own_key_ref_reaches_each_flavours_own_slot() {
        let headers = headers();
        // Stand-ins for the keys the pipeline resolves: distinct, never dereferenced.
        let mut keys = [0u64; 4];

        for (i, (name, it)) in headers.iter().enumerate() {
            // SAFETY: `it` is a live metric iterator, held exclusively here.
            let slot = unsafe { metric::own_key_ref(it.as_non_null()) };
            // SAFETY: `slot` is that iterator's own key slot, live and initialised.
            let key = unsafe { &mut *slot.as_ptr() };
            assert!(key.is_null(), "{name}: a fresh iterator has no key yet");
            *key = (&raw mut keys[i]).cast::<RLookupKey<'_>>();
        }

        // A second dispatch reads the slot back: it belongs to the iterator rather
        // than to the call, and no flavour writes into another's.
        for (i, (name, it)) in headers.iter().enumerate() {
            // SAFETY: as above.
            let slot = unsafe { metric::own_key_ref(it.as_non_null()) };
            // SAFETY: as above.
            let key = unsafe { &mut *slot.as_ptr() };
            assert_eq!(
                *key,
                (&raw mut keys[i]).cast::<RLookupKey<'_>>(),
                "{name}: dispatched to the wrong iterator's key slot"
            );
            // The stand-ins are not real keys: clear them before `headers` drops.
            *key = std::ptr::null_mut();
        }
    }

    #[test]
    fn set_key_handle_invalidates_each_flavours_own_handle() {
        let mut headers = headers();
        let mut handles = [(); 4].map(|()| RLookupKeyHandle {
            key_ptr: std::ptr::null_mut(),
            is_valid: true,
        });

        for ((name, it), handle) in headers.iter().zip(&mut handles) {
            // SAFETY: `it` is a live metric iterator held exclusively, and `handle`
            // outlives it — the iterators are freed before `handles` goes out of scope.
            unsafe { metric::set_key_handle(it.as_non_null(), Some(NonNull::from(&mut *handle))) };
            assert!(handle.is_valid, "{name}: wiring a handle must not clear it");
        }

        // Released here rather than left to drop: the invalidation below is only
        // observable once the iterators have gone.
        for (_, it) in &mut headers {
            it.free();
        }

        // Each iterator must have invalidated the handle it was given — a flavour
        // dispatched as another would write the handle at the wrong offset, and
        // this one would still read as valid.
        for ((name, _), handle) in headers.iter().zip(&handles) {
            assert!(
                !handle.is_valid,
                "{name}: freeing the iterator must invalidate its own handle"
            );
        }
    }

    #[test]
    #[should_panic(expected = "expected a metric iterator")]
    fn own_key_ref_rejects_a_non_metric_iterator() {
        let it = OwnedHeader::new(RQEIteratorWrapper::boxed_new(Mock::new([1, 2, 3])));

        // SAFETY: the header is a live, exclusively-held wrapper reporting its
        // type honestly — `Mock` tags itself `IteratorType::Mock`. That is the
        // whole pre-condition; being a metric iterator is not one, it is the
        // documented panic condition, and this asserts it fires. The dispatch
        // reads the type tag and nothing else, so the iterator is untouched and
        // `it` can still free it as the unwind passes through.
        unsafe { metric::own_key_ref(it.as_non_null()) };
    }
}
