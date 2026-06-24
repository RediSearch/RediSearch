/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Deferred result production for lazily-populated iterators.
//!
//! Some iterators (e.g. a vector range query) are cheap to build but expensive to
//! populate, and the population step must run *outside* the spec lock so writes can
//! proceed concurrently (see MOD-16437). A [`ResultsProducer`] wraps a C callback that
//! runs the underlying query, deferring it until the first `read`/`skip_to` of the
//! owning iterator — by which point the caller has released the lock.

use std::ffi::c_void;

use rqe_core::DocId;

use crate::{RQEIteratorError, utils::OwnedSlice};

/// Results produced by a [`ResultsProducer`] when it is first run.
///
/// The `ids` and `metrics` arrays are allocated by the C producer using the Redis
/// allocator; ownership transfers to the iterator, which frees them via
/// `RedisModule_Free` (see [`OwnedSlice::from_c`]).
#[repr(C)]
#[derive(Debug)]
#[cheadergen::config(export)]
pub struct VectorRangeResults {
    /// Pointer to the array of `num` matching document IDs. May be null when
    /// `num` is zero or `timed_out` is set.
    pub ids: *mut DocId,
    /// Pointer to the array of `num` metric (distance) values, parallel to `ids`.
    /// Null when the query does not yield a metric.
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

/// A deferred producer of iterator results.
///
/// Wraps a C callback (`produce`) plus its context (`ctx`). The query is only executed
/// when [`produce`](Self::produce) is called — typically on the first `read`/`skip_to`
/// of the owning iterator, *after* the spec lock has been released. The context is freed
/// exactly once, when the producer is dropped (either after a single production or if the
/// iterator is freed without ever being read).
pub struct ResultsProducer {
    produce: ProduceResultsFn,
    free_ctx: FreeProducerCtxFn,
    ctx: *mut c_void,
}

impl ResultsProducer {
    /// Create a new deferred producer.
    ///
    /// # Safety
    ///
    /// 1. `produce` must run the query against `ctx` and return a valid
    ///    [`VectorRangeResults`] (arrays allocated with the Redis allocator, or `timed_out`).
    /// 2. `free_ctx` must free `ctx` and be safe to call exactly once.
    /// 3. `ctx` must remain valid until the producer is dropped. `produce` must not free it.
    pub unsafe fn new(
        produce: ProduceResultsFn,
        free_ctx: FreeProducerCtxFn,
        ctx: *mut c_void,
    ) -> Self {
        Self {
            produce,
            free_ctx,
            ctx,
        }
    }

    /// Run the deferred query, returning the produced IDs and (optionally) metrics.
    ///
    /// Returns [`RQEIteratorError::TimedOut`] if the query timed out before producing results.
    pub fn produce(
        &self,
    ) -> Result<(OwnedSlice<DocId>, Option<OwnedSlice<f64>>), RQEIteratorError> {
        // SAFETY: `produce` and `ctx` are valid per the contract of `new`.
        let results = unsafe { (self.produce)(self.ctx) };
        if results.timed_out {
            return Err(RQEIteratorError::TimedOut);
        }
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
        Ok((ids, metrics))
    }
}

impl Drop for ResultsProducer {
    fn drop(&mut self) {
        // SAFETY: `free_ctx` and `ctx` are valid per the contract of `new`, and this is the
        // only place the context is freed, so it is freed exactly once.
        unsafe { (self.free_ctx)(self.ctx) };
    }
}
