/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ffi::c_void;

use ffi::QueryIterator;
use index_result::RSIndexResult;
use rqe_iterators::deferred::{FreeProducerCtxFn, ProduceResultsFn, ResultsProducer};
use rqe_iterators::interop::RQEIteratorWrapper;
use rqe_iterators::{
    id_list::IdList,
    metric::{Metric, MetricType},
};

/// Creates a lazily-evaluated vector range iterator.
///
/// Unlike [`NewMetricIteratorSortedById`](crate::metric::NewMetricIteratorSortedById) and the
/// other ID-list/metric constructors, the matching documents are **not** computed here. Instead
/// the `produce` callback runs the underlying vector range query on the first `Read`/`SkipTo`,
/// after which the resulting iterator behaves exactly like an eagerly-built metric (when
/// `yields_metric`) or ID-list iterator. Deferring the query lets the caller release the spec
/// lock before it executes, so writes can proceed concurrently (see MOD-16437).
///
/// `sorted_by_id` selects between the by-ID and by-score variants; `num_estimated` is the
/// upper-bound estimate reported until the query runs; `type_` is the metric type (only used
/// when `yields_metric`).
///
/// # Safety
///
/// 1. `produce` and `free_ctx` must be valid C callbacks satisfying the contract of
///    [`ResultsProducer::new`].
/// 2. `ctx` must remain valid until the iterator is freed; ownership transfers to the iterator,
///    which frees it via `free_ctx`.
/// 3. `produce` must return arrays allocated with the Redis allocator; the iterator takes
///    ownership and frees them via `RedisModule_Free`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NewLazyVectorRangeIterator(
    produce: ProduceResultsFn,
    free_ctx: FreeProducerCtxFn,
    ctx: *mut c_void,
    yields_metric: bool,
    sorted_by_id: bool,
    num_estimated: usize,
    type_: MetricType,
) -> *mut QueryIterator {
    // SAFETY: callbacks and ctx are valid per preconditions 1 + 2.
    let producer = unsafe { ResultsProducer::new(produce, free_ctx, ctx) };

    match (yields_metric, sorted_by_id) {
        (true, true) => RQEIteratorWrapper::boxed_new(Metric::<true>::with_producer(
            producer,
            num_estimated,
            type_,
        )),
        (true, false) => RQEIteratorWrapper::boxed_new(Metric::<false>::with_producer(
            producer,
            num_estimated,
            type_,
        )),
        (false, true) => RQEIteratorWrapper::boxed_new(IdList::<true>::with_producer(
            producer,
            num_estimated,
            RSIndexResult::build_virt().weight(1.0).build(),
        )),
        (false, false) => RQEIteratorWrapper::boxed_new(IdList::<false>::with_producer(
            producer,
            num_estimated,
            RSIndexResult::build_virt().weight(1.0).build(),
        )),
    }
}
