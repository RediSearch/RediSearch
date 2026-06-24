/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Tests for deferred (lazily-produced) ID-list and metric iterators. See [`rqe_iterators::deferred`].

use std::ffi::c_void;

use rqe_iterators::{
    RQEIterator, RQEIteratorError,
    deferred::{ResultsProducer, VectorRangeResults},
    id_list::IdListSorted,
    metric::{MetricSortedById, MetricType},
};

/// What a [`test_produce`] invocation should return, plus a counter recording how many times
/// it was actually called (to prove the query is deferred to the first read).
struct TestProducerCtx {
    ids: Vec<u64>,
    metrics: Vec<f64>,
    yields_metric: bool,
    timed_out: bool,
    call_count: *mut u32,
}

/// Allocate a copy of `data` with the Redis allocator (mocked by the test harness), so the
/// produced [`VectorRangeResults`] can be freed by the iterator via `RedisModule_Free`.
fn alloc_c<T: Copy>(data: &[T]) -> *mut T {
    if data.is_empty() {
        return std::ptr::null_mut();
    }
    // SAFETY: `RedisModule_Alloc` is provided by `mock_or_stub_missing_redis_c_symbols!`.
    let alloc = unsafe { ffi::RedisModule_Alloc }.expect("RedisModule_Alloc set by test harness");
    // SAFETY: `alloc` returns a buffer of at least the requested byte size.
    let ptr = unsafe { alloc(std::mem::size_of_val(data)) } as *mut T;
    // SAFETY: `ptr` has room for `data.len()` elements; source and destination don't overlap.
    unsafe { std::ptr::copy_nonoverlapping(data.as_ptr(), ptr, data.len()) };
    ptr
}

/// C-ABI producer callback used by the tests below.
unsafe extern "C" fn test_produce(ctxp: *mut c_void) -> VectorRangeResults {
    // SAFETY: `ctxp` is the `Box<TestProducerCtx>` leaked in `make_producer`.
    let ctx = unsafe { &mut *(ctxp as *mut TestProducerCtx) };
    // SAFETY: `call_count` points to a live `u32` owned by the test.
    unsafe { *ctx.call_count += 1 };

    if ctx.timed_out {
        return VectorRangeResults {
            ids: std::ptr::null_mut(),
            metrics: std::ptr::null_mut(),
            num: 0,
            timed_out: true,
        };
    }

    let num = ctx.ids.len();
    VectorRangeResults {
        ids: alloc_c(&ctx.ids),
        metrics: if ctx.yields_metric {
            alloc_c(&ctx.metrics)
        } else {
            std::ptr::null_mut()
        },
        num,
        timed_out: false,
    }
}

/// C-ABI free callback: reclaims the leaked context box.
unsafe extern "C" fn test_free(ctxp: *mut c_void) {
    // SAFETY: `ctxp` is the `Box<TestProducerCtx>` leaked in `make_producer`.
    drop(unsafe { Box::from_raw(ctxp as *mut TestProducerCtx) });
}

fn make_producer(ctx: TestProducerCtx) -> ResultsProducer {
    let ctx_ptr = Box::into_raw(Box::new(ctx)) as *mut c_void;
    // SAFETY: the callbacks honor the `ResultsProducer::new` contract; `ctx_ptr` is a leaked box
    // freed exactly once by `test_free`.
    unsafe { ResultsProducer::new(test_produce, test_free, ctx_ptr) }
}

#[test]
fn id_list_is_not_produced_until_first_read() {
    let mut call_count = 0u32;
    let producer = make_producer(TestProducerCtx {
        ids: vec![1, 4, 9],
        metrics: vec![],
        yields_metric: false,
        timed_out: false,
        call_count: &mut call_count,
    });

    let mut it = IdListSorted::with_producer(
        producer,
        100,
        index_result::RSIndexResult::build_virt().build(),
    );

    // Building the iterator must not run the query, but it must look readable so the engine
    // will read it (and the estimate falls back to the supplied hint).
    assert_eq!(call_count, 0);
    assert!(!it.at_eof());
    assert_eq!(it.num_estimated(), 100);

    // First read triggers production exactly once.
    assert_eq!(it.read().unwrap().unwrap().doc_id, 1);
    assert_eq!(call_count, 1);
    assert_eq!(it.num_estimated(), 3); // now the real count

    assert_eq!(it.read().unwrap().unwrap().doc_id, 4);
    assert_eq!(it.read().unwrap().unwrap().doc_id, 9);
    assert!(matches!(it.read(), Ok(None)));
    assert!(it.at_eof());

    // No further production happened.
    assert_eq!(call_count, 1);
}

#[test]
fn metric_produces_ids_and_metrics_on_first_read() {
    let mut call_count = 0u32;
    let producer = make_producer(TestProducerCtx {
        ids: vec![2, 5, 8],
        metrics: vec![0.2, 0.5, 0.8],
        yields_metric: true,
        timed_out: false,
        call_count: &mut call_count,
    });

    let mut it = MetricSortedById::with_producer(producer, 42, MetricType::VectorDistance);
    assert_eq!(call_count, 0);
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
    assert_eq!(call_count, 1);
}

#[test]
fn empty_production_reports_eof() {
    let mut call_count = 0u32;
    let producer = make_producer(TestProducerCtx {
        ids: vec![],
        metrics: vec![],
        yields_metric: true,
        timed_out: false,
        call_count: &mut call_count,
    });

    let mut it = MetricSortedById::with_producer(producer, 7, MetricType::VectorDistance);
    assert!(!it.at_eof()); // not known to be empty until produced
    assert!(matches!(it.read(), Ok(None)));
    assert!(it.at_eof());
    assert_eq!(call_count, 1);
}

#[test]
fn timeout_propagates_on_first_read() {
    let mut call_count = 0u32;
    let producer = make_producer(TestProducerCtx {
        ids: vec![],
        metrics: vec![],
        yields_metric: false,
        timed_out: true,
        call_count: &mut call_count,
    });

    let mut it = IdListSorted::with_producer(
        producer,
        10,
        index_result::RSIndexResult::build_virt().build(),
    );
    assert!(matches!(it.read(), Err(RQEIteratorError::TimedOut)));
    // The producer was consumed even on timeout, so a subsequent read yields EOF (no re-run).
    assert!(matches!(it.read(), Ok(None)));
    assert_eq!(call_count, 1);
}

#[test]
fn skip_to_triggers_production() {
    let mut call_count = 0u32;
    let producer = make_producer(TestProducerCtx {
        ids: vec![3, 6, 9, 12],
        metrics: vec![],
        yields_metric: false,
        timed_out: false,
        call_count: &mut call_count,
    });

    let mut it = IdListSorted::with_producer(
        producer,
        50,
        index_result::RSIndexResult::build_virt().build(),
    );
    assert_eq!(call_count, 0);

    let outcome = it.skip_to(6).unwrap().unwrap();
    assert_eq!(call_count, 1);
    match outcome {
        rqe_iterators::SkipToOutcome::Found(res) => assert_eq!(res.doc_id, 6),
        other => panic!("expected Found(6), got {other:?}"),
    }
}
