/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! C entry point for lazily-evaluated vector range iterators.
//!
//! The VecSim range query is captured by the iterator's [`Producer`] and runs on its first read
//! (see [`rqe_iterators::deferred`]).

use std::{ffi::c_void, ptr::NonNull};

use ffi::{
    QueryIterator, QueryRequestTimeout, VecSimIndex, VecSimQueryParams, VecSimQueryReply_Order,
};
use index_result::RSIndexResult;
use rqe_iterators::deferred::{ProducedResults, Producer};
use rqe_iterators::interop::RQEIteratorWrapper;
use rqe_iterators::{
    RQEIteratorError, id_list_lazy::IdListLazy, metric::MetricType, metric_lazy::MetricLazy,
};
use vecsim::{IndexRef, QueryError, QueryReply, QueryVector, ReplyOrder};

/// Creates a lazily-evaluated vector range iterator.
///
/// Unlike [`NewMetricIteratorSortedById`](crate::metric::NewMetricIteratorSortedById) and the
/// other ID-list/metric constructors, the matching documents are **not** computed here: the
/// VecSim range query runs on the first `Read`/`SkipTo`, which the caller may issue after
/// releasing the spec lock, so writes can proceed concurrently. The iterator then behaves like
/// an eagerly-built metric iterator (when `yields_metric`) or ID-list iterator, sorted by id
/// when `order` is `BY_ID`. Until the query runs, its estimate is the index size at
/// construction.
///
/// `query_vector` is copied and `query_params` is taken by value, so neither has to outlive this
/// call.
///
/// Aborts if `order` is neither `BY_SCORE` nor `BY_ID`, and on the first read if `radius` is
/// negative, instead of passing VecSim a value it rejects.
///
/// # Safety
///
/// 1. `index` is non-null and [valid], and outlives the returned iterator.
/// 2. `query_vector` is [valid] for reads of `vector_byte_len` bytes, and `vector_byte_len`
///    equals the index's expected query-vector size.
/// 3. `timeout` is non-null and remains [valid] for the returned iterator's lifetime.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NewLazyVectorRangeIteratorFromParams(
    index: *mut VecSimIndex,
    query_vector: *const c_void,
    vector_byte_len: usize,
    radius: f64,
    mut query_params: VecSimQueryParams,
    order: VecSimQueryReply_Order,
    yields_metric: bool,
    timeout: *mut QueryRequestTimeout,
) -> *mut QueryIterator {
    debug_assert!(!timeout.is_null(), "timeout must be non-null");
    let order = ReplyOrder::from_raw(order).expect("a range query is ordered BY_SCORE or BY_ID");

    // SAFETY: guaranteed by 1.
    let index = unsafe { NonNull::new_unchecked(index) };
    // SAFETY: 1 keeps the index valid for the iterator's lifetime, and the producer closure
    // that holds this reference is owned by the iterator, so it cannot outlive the index.
    let index = unsafe { IndexRef::<'static>::from_raw(index) };
    // SAFETY: guaranteed by 2.
    let blob =
        unsafe { std::slice::from_raw_parts(query_vector.cast::<u8>(), vector_byte_len) }.to_vec();
    // SAFETY: guaranteed by 2.
    let query_vector = unsafe { QueryVector::new(index, blob) };
    query_params.timeoutCtx = timeout.cast();
    // Read now, while the caller still holds the spec lock.
    let num_estimated = index.size();

    // The query may return vectors added after construction. They are not filtered here: their
    // documents are dropped downstream, when the doc-table lookup finds no metadata for them.
    let producer: Producer<'static> = Box::new(move || {
        // SAFETY: `query_params.timeoutCtx` is `timeout`, which 3 keeps valid for the iterator's
        // lifetime and so for this call, made by the iterator.
        let reply = unsafe { index.range_query(&query_vector, radius, &mut query_params, order) }
            .map_err(|QueryError::TimedOut| RQEIteratorError::TimedOut)?;
        Ok(collect_results(reply, yields_metric))
    });

    let type_ = MetricType::VectorDistance;
    let id_list_result = || RSIndexResult::build_virt().weight(1.0).build();
    match (yields_metric, order) {
        (true, ReplyOrder::ById) => {
            RQEIteratorWrapper::boxed_new(MetricLazy::<true>::new(producer, num_estimated, type_))
        }
        (true, ReplyOrder::ByScore) => {
            RQEIteratorWrapper::boxed_new(MetricLazy::<false>::new(producer, num_estimated, type_))
        }
        (false, ReplyOrder::ById) => RQEIteratorWrapper::boxed_new(IdListLazy::<true>::new(
            producer,
            num_estimated,
            id_list_result(),
        )),
        (false, ReplyOrder::ByScore) => RQEIteratorWrapper::boxed_new(IdListLazy::<false>::new(
            producer,
            num_estimated,
            id_list_result(),
        )),
    }
}

/// Drains a range-query `reply` into the iterator's ids and, when `yields_metric`, the parallel
/// distances. A missing reply yields no results.
fn collect_results(reply: Option<QueryReply>, yields_metric: bool) -> ProducedResults {
    let len = reply.as_ref().map_or(0, QueryReply::len);
    let mut ids = Vec::with_capacity(len);
    let mut distances = Vec::with_capacity(if yields_metric { len } else { 0 });
    if let Some(mut results) = reply.and_then(QueryReply::into_results) {
        if yields_metric {
            for (id, distance) in results {
                ids.push(id);
                distances.push(distance);
            }
        } else {
            ids.extend(std::iter::from_fn(|| results.next_id()));
        }
    }
    ProducedResults {
        ids: ids.into(),
        metrics: yields_metric.then(|| distances.into()),
    }
}
