/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Tests for deferred (lazily-produced) ID-list and metric iterators. See [`rqe_iterators::deferred`].

use std::cell::Cell;
use std::rc::Rc;

use rqe_iterators::{
    RQEIterator, RQEIteratorError, SkipToOutcome,
    deferred::{ProducedResults, Producer},
    id_list_lazy::IdListLazy,
    metric::MetricType,
    metric_lazy::MetricLazySortedById,
};

/// Build a [`Producer`] closure yielding `ids`/`metrics` (or a timeout), incrementing `calls`
/// each time it runs so tests can assert the query is deferred to (and run only on) first access.
fn producer(
    ids: Vec<u64>,
    metrics: Option<Vec<f64>>,
    timed_out: bool,
    calls: Rc<Cell<u32>>,
) -> Producer<'static> {
    // The owning iterator calls the producer once but retains it ([`Producer`] is `FnMut`), so the
    // payload is taken out via an `Option` rather than moved directly out of the closure captures.
    let mut payload = Some((ids, metrics));
    Box::new(move || {
        calls.set(calls.get() + 1);
        if timed_out {
            return Err(RQEIteratorError::TimedOut);
        }
        let (ids, metrics) = payload.take().expect("producer should only be run once");
        Ok(ProducedResults {
            ids: ids.into(),
            metrics: metrics.map(Into::into),
        })
    })
}

#[test]
fn id_list_is_not_produced_until_first_read() {
    let calls = Rc::new(Cell::new(0));
    let mut it = IdListLazy::<true>::new(
        producer(vec![1, 4, 9], None, false, calls.clone()),
        100,
        index_result::RSIndexResult::build_virt().build(),
    );

    // Building the iterator must not run the query, but it must look readable so the engine
    // will read it (and the estimate falls back to the supplied hint).
    assert_eq!(calls.get(), 0);
    assert!(!it.at_eof());
    assert_eq!(it.num_estimated(), 100);

    // First read triggers production exactly once.
    assert_eq!(it.read().unwrap().unwrap().doc_id, 1);
    assert_eq!(calls.get(), 1);
    assert_eq!(it.num_estimated(), 3); // now the real count

    assert_eq!(it.read().unwrap().unwrap().doc_id, 4);
    assert_eq!(it.read().unwrap().unwrap().doc_id, 9);
    assert!(matches!(it.read(), Ok(None)));
    assert!(it.at_eof());

    // No further production happened.
    assert_eq!(calls.get(), 1);
}

/// Suspend/resume round-trips for the lazy ID list. These exercise the
/// `RQEIteratorBoxed`/`RQESuspendedIterator` impls (whose suspended form erases
/// the retained producer's lifetime to `'static` and restores it on resume) via
/// the dyn path used by the production FFI wrapper.
mod id_list_suspend_resume {
    use super::*;
    use rqe_iterators::TypeErasedRQEIterator;
    use rqe_iterators_test_utils::{MockContext, ResumeOutcomeExt, revalidate_via_resume};

    #[test]
    fn survives_suspend_resume_after_production() {
        let mock_ctx = MockContext::new(0, 0);
        let calls = Rc::new(Cell::new(0));
        let mut it = IdListLazy::<true>::new(
            producer(vec![1, 4, 9], None, false, calls.clone()),
            100,
            index_result::RSIndexResult::build_virt().build(),
        );
        // Produce and consume the first result.
        assert_eq!(it.read().unwrap().unwrap().doc_id, 1);
        assert_eq!(calls.get(), 1);

        // Suspend + resume across a (mock) lock release/reacquire cycle.
        let mut it = revalidate_via_resume(
            TypeErasedRQEIterator::new(Box::new(it)),
            &mock_ctx.spec_read(),
        )
        .expect("resume should not fail")
        .expect_ok();

        // The retained producer is not re-run; the remaining data survives at the
        // preserved position.
        assert_eq!(it.read().unwrap().unwrap().doc_id, 4);
        assert_eq!(it.read().unwrap().unwrap().doc_id, 9);
        assert!(matches!(it.read(), Ok(None)));
        assert_eq!(calls.get(), 1);
    }

    #[test]
    fn suspend_resume_before_production_keeps_deferral() {
        let mock_ctx = MockContext::new(0, 0);
        let calls = Rc::new(Cell::new(0));
        let it = IdListLazy::<true>::new(
            producer(vec![7, 8], None, false, calls.clone()),
            5,
            index_result::RSIndexResult::build_virt().build(),
        );

        // Suspend/resume before any read leaves production deferred.
        let mut it = revalidate_via_resume(
            TypeErasedRQEIterator::new(Box::new(it)),
            &mock_ctx.spec_read(),
        )
        .expect("resume should not fail")
        .expect_ok();
        assert_eq!(calls.get(), 0);

        // The producer still runs on the first read after resume.
        assert_eq!(it.read().unwrap().unwrap().doc_id, 7);
        assert_eq!(calls.get(), 1);
    }
}

#[test]
fn metric_produces_ids_and_metrics_on_first_read() {
    let calls = Rc::new(Cell::new(0));
    let mut it = MetricLazySortedById::new(
        producer(
            vec![2, 5, 8],
            Some(vec![0.2, 0.5, 0.8]),
            false,
            calls.clone(),
        ),
        42,
        MetricType::VectorDistance,
    );
    assert_eq!(calls.get(), 0);
    assert!(!it.at_eof());
    assert_eq!(it.num_estimated(), 42);

    for (expected_id, expected_metric) in [(2u64, 0.2), (5, 0.5), (8, 0.8)] {
        let res = it.read().unwrap().unwrap();
        assert_eq!(res.doc_id, expected_id);
        assert_eq!(res.as_numeric(), Some(expected_metric));
        let entry = res.metrics_ref().get(0).expect("one metric entry");
        assert_eq!(entry.value(), expected_metric);
    }
    assert!(matches!(it.read(), Ok(None)));
    assert_eq!(calls.get(), 1);
}

#[test]
fn empty_production_reports_eof() {
    let calls = Rc::new(Cell::new(0));
    let mut it = MetricLazySortedById::new(
        producer(vec![], Some(vec![]), false, calls.clone()),
        7,
        MetricType::VectorDistance,
    );
    assert!(!it.at_eof()); // not known to be empty until produced
    assert!(matches!(it.read(), Ok(None)));
    assert!(it.at_eof());
    assert_eq!(calls.get(), 1);
}

#[test]
fn timeout_propagates_on_first_read() {
    let calls = Rc::new(Cell::new(0));
    let mut it = IdListLazy::<true>::new(
        producer(vec![], None, true, calls.clone()),
        10,
        index_result::RSIndexResult::build_virt().build(),
    );
    assert!(matches!(it.read(), Err(RQEIteratorError::TimedOut)));
    // The producer was consumed even on timeout, so a subsequent read yields EOF (no re-run).
    assert!(matches!(it.read(), Ok(None)));
    assert_eq!(calls.get(), 1);
}

#[test]
fn skip_to_triggers_production() {
    let calls = Rc::new(Cell::new(0));
    let mut it = IdListLazy::<true>::new(
        producer(vec![3, 6, 9, 12], None, false, calls.clone()),
        50,
        index_result::RSIndexResult::build_virt().build(),
    );
    assert_eq!(calls.get(), 0);

    let outcome = it.skip_to(6).unwrap().unwrap();
    assert_eq!(calls.get(), 1);
    match outcome {
        SkipToOutcome::Found(res) => assert_eq!(res.doc_id, 6),
        other => panic!("expected Found(6), got {other:?}"),
    }
}
