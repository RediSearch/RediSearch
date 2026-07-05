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
//! This is the C-ABI glue around the pure-Rust [`rqe_iterators::deferred`] machinery: it
//! adapts a C producer callback (plus its context) into a Rust [`Producer`] closure, which
//! the iterator runs on its first read.

use std::ffi::c_void;

use ffi::QueryIterator;
use index_result::RSIndexResult;
use rqe_iterators::deferred::{ProducedResults, Producer};
use rqe_iterators::interop::RQEIteratorWrapper;
use rqe_iterators::utils::OwnedSlice;
use rqe_iterators::{
    RQEIteratorError,
    id_list::IdListLazy,
    metric::{MetricLazy, MetricType},
};

/// Results returned by a [`ProduceResultsFn`].
///
/// The `ids` and `metrics` arrays are allocated by the C producer using the Redis allocator;
/// ownership transfers to the iterator, which frees them via `RedisModule_Free` (see
/// [`OwnedSlice::from_c`]).
#[repr(C)]
#[derive(Debug)]
#[cheadergen::config(export)]
pub struct VectorRangeResults {
    /// Pointer to the array of `num` matching document IDs. May be null when
    /// `num` is zero or `timed_out` is set.
    pub ids: *mut rqe_core::DocId,
    /// Pointer to the array of `num` metric (distance) values, parallel to `ids`.
    /// Null when the query does not yield a metric or `timed_out` is set.
    pub metrics: *mut f64,
    /// Number of entries in `ids` (and `metrics`, when non-null).
    pub num: usize,
    /// Set when the underlying query timed out before producing results.
    pub timed_out: bool,
}

/// Type of the C callback that runs the deferred query and returns its results.
pub type ProduceResultsFn = unsafe extern "C" fn(ctx: *mut c_void) -> VectorRangeResults;
/// Type of the C callback that frees the producer context.
pub type FreeProducerCtxFn = unsafe extern "C" fn(ctx: *mut c_void);

/// Owns the C producer context and frees it exactly once, when dropped — i.e. after the
/// [`Producer`] closure runs, or when the iterator is freed without ever being read.
struct CtxGuard {
    ctx: *mut c_void,
    free_ctx: FreeProducerCtxFn,
}

impl Drop for CtxGuard {
    fn drop(&mut self) {
        // SAFETY: `free_ctx` and `ctx` are valid per the contract of `NewLazyVectorRangeIterator`,
        // and this is the only place the context is freed, so it is freed exactly once.
        unsafe { (self.free_ctx)(self.ctx) };
    }
}

/// Wrap a C produce/free callback pair into a Rust [`Producer`] closure.
///
/// # Safety
///
/// See [`NewLazyVectorRangeIterator`].
unsafe fn c_producer(
    produce: ProduceResultsFn,
    free_ctx: FreeProducerCtxFn,
    ctx: *mut c_void,
) -> Producer<'static> {
    let guard = CtxGuard { ctx, free_ctx };
    Box::new(move || {
        // Bind the *whole* `guard` by reference so the closure captures all of it. Without this,
        // edition-2024 disjoint closure captures would capture only the `Copy` field `guard.ctx`
        // and drop the `CtxGuard` (freeing the context) the moment `c_producer` returns — long
        // before the deferred query runs, leaving the closure with a dangling context pointer.
        // Capturing the whole guard ties the context's lifetime to the closure (and its iterator).
        let guard = &guard;
        // SAFETY: `produce` and `ctx` are valid per the contract of `NewLazyVectorRangeIterator`.
        let results = unsafe { (produce)(guard.ctx) };
        // Take ownership of any arrays the producer handed back *before* inspecting `timed_out`, so
        // they are freed even on the timeout path (where the iterator drops these `OwnedSlice`s).
        let ids = if results.ids.is_null() {
            OwnedSlice::default()
        } else {
            // SAFETY: the producer guarantees `ids` points to `num` initialized `DocId`s
            // allocated with the Redis allocator, and transfers ownership to us.
            unsafe { OwnedSlice::from_c(results.ids, results.num) }
        };
        let metrics = if results.metrics.is_null() {
            None
        } else {
            // SAFETY: the producer guarantees `metrics` points to `num` initialized `f64`s
            // allocated with the Redis allocator, and transfers ownership to us.
            Some(unsafe { OwnedSlice::from_c(results.metrics, results.num) })
        };
        if results.timed_out {
            // `ids`/`metrics` drop here, freeing anything the producer allocated alongside the flag.
            return Err(RQEIteratorError::TimedOut);
        }
        Ok(ProducedResults { ids, metrics })
    })
}

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
/// 1. `produce` must run the query against `ctx` and return a valid [`VectorRangeResults`]
///    (arrays allocated with the Redis allocator, or `timed_out`); it must not free `ctx`.
/// 2. `free_ctx` must free `ctx` and be safe to call exactly once.
/// 3. `ctx` must remain valid until the iterator is freed; ownership transfers to the iterator.
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
    // SAFETY: callbacks and ctx are valid per preconditions 1 + 2 + 3.
    let producer = unsafe { c_producer(produce, free_ctx, ctx) };

    match (yields_metric, sorted_by_id) {
        (true, true) => {
            RQEIteratorWrapper::boxed_new(MetricLazy::<true>::new(producer, num_estimated, type_))
        }
        (true, false) => {
            RQEIteratorWrapper::boxed_new(MetricLazy::<false>::new(producer, num_estimated, type_))
        }
        (false, true) => RQEIteratorWrapper::boxed_new(IdListLazy::<true>::new(
            producer,
            num_estimated,
            RSIndexResult::build_virt().weight(1.0).build(),
        )),
        (false, false) => RQEIteratorWrapper::boxed_new(IdListLazy::<false>::new(
            producer,
            num_estimated,
            RSIndexResult::build_virt().weight(1.0).build(),
        )),
    }
}
