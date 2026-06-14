/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! FFI entry point for the geometry (GEOSHAPE) query iterator.

use std::ptr::NonNull;

use ffi::{QueryIterator, RedisSearchCtx};
use field::FieldFilterContext;
use rqe_core::DocId;
use rqe_iterators::{
    FieldExpirationChecker, MemTracker, NoTracker,
    geo_shape::GeoShape,
    interop::RQEIteratorWrapper,
    utils::{AnyTimeoutContext, OwnedSlice},
};

/// Probe the query deadline once every this many candidate documents.
const TIMEOUT_CHECK_GRANULARITY: u32 = 100;

/// A [`MemTracker`] that adjusts an externally-owned `usize` counter through a
/// raw pointer — the geometry index's allocation counter, shared from C across
/// the FFI boundary. Use [`NoTracker`] when there is no counter to track.
struct ExternalCounter {
    counter: NonNull<usize>,
}

impl ExternalCounter {
    /// Creates a tracker over an externally-owned counter.
    ///
    /// # Safety
    ///
    /// The pointed-to `usize` must:
    /// 1. be valid and initialized;
    /// 2. remain valid for the entire lifetime of the [`GeoShape`] iterator this
    ///    tracker is given to; and
    /// 3. only be accessed single-threaded (it is mutated without
    ///    synchronization, which holds under the index/spec lock).
    #[inline(always)]
    const unsafe fn new(counter: NonNull<usize>) -> Self {
        Self { counter }
    }
}

impl MemTracker for ExternalCounter {
    #[inline(always)]
    fn add(&self, bytes: usize) {
        // SAFETY: the safety contract of `ExternalCounter::new` guarantees the
        // pointer is valid for the iterator's lifetime and accessed
        // single-threaded.
        unsafe { *self.counter.as_ptr() += bytes };
    }

    #[inline(always)]
    fn sub(&self, bytes: usize) {
        // SAFETY: see [`ExternalCounter::add`].
        unsafe { *self.counter.as_ptr() -= bytes };
    }
}

/// Creates a new geometry-query iterator over a list of matching document IDs.
///
/// `ids` is the set of documents matched by the geometry index, in arbitrary
/// order; the iterator sorts them on construction and yields them in ascending
/// order, skipping documents whose queried field has expired and aborting on
/// query timeout.
///
/// Ownership of `ids` is transferred to the iterator, which frees it with
/// `RedisModule_Free` when dropped. When `allocated` is non-null, the byte size
/// of the iterator is added to `*allocated` on construction and subtracted on
/// drop, keeping the geometry index's memory accounting in sync.
///
/// # Safety
///
/// 1. `sctx` must be a non-null pointer to a valid [`RedisSearchCtx`] whose
///    `spec` is a valid [`IndexSpec`](ffi::IndexSpec); both must outlive the
///    returned iterator.
/// 2. `filter_ctx` must be a non-null pointer to a valid [`FieldFilterContext`].
/// 3. `ids` must be null, or point to `num` initialized [`DocId`]s allocated via
///    `RedisModule_Alloc`. Ownership is transferred to the iterator. When `ids`
///    is null, `num` must be zero.
/// 4. `allocated`, when non-null, must point to a valid, initialized `usize`
///    (it is read-modify-written) that outlives the iterator and is only
///    accessed single-threaded (it is mutated without synchronization, which
///    holds under the spec lock).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NewGeometryQueryIterator(
    sctx: *const RedisSearchCtx,
    filter_ctx: *const FieldFilterContext,
    ids: *mut DocId,
    num: usize,
    allocated: *mut usize,
) -> *mut QueryIterator {
    let sctx_nn = NonNull::new(sctx as *mut RedisSearchCtx).expect("sctx must be non-null");
    // SAFETY: precondition 1.
    let sctx_ref = unsafe { sctx_nn.as_ref() };

    let filter_ctx_nn =
        NonNull::new(filter_ctx as *mut FieldFilterContext).expect("filter_ctx must be non-null");
    // SAFETY: precondition 2. The fields are `Copy`, so we snapshot them.
    let fc = unsafe { filter_ctx_nn.as_ref() };
    let filter = FieldFilterContext {
        field: fc.field,
        predicate: fc.predicate,
    };

    // Geometry queries filter by a single field *index* (never a mask), so the
    // wide-schema flag is irrelevant; pass empty reader flags.
    // SAFETY: precondition 1 guarantees `sctx` and `sctx.spec` are valid and outlive the returned iterator.
    let expiration_checker = unsafe { FieldExpirationChecker::new(sctx_nn, filter, 0) };

    let timeout_ctx = AnyTimeoutContext::from_sctx(sctx_ref, TIMEOUT_CHECK_GRANULARITY);

    let ids_list = if !ids.is_null() {
        // SAFETY: precondition 3.
        unsafe { OwnedSlice::from_c(ids, num) }
    } else {
        debug_assert_eq!(num, 0, "null id array must have a zero count");
        OwnedSlice::default()
    };

    // A null counter means the caller opted out of memory accounting, so use a
    // no-op tracker; otherwise track through the externally-owned counter.
    match NonNull::new(allocated) {
        Some(counter) => {
            // SAFETY: precondition 4 upholds the counter contract of `ExternalCounter::new`.
            let mem_tracker = unsafe { ExternalCounter::new(counter) };
            RQEIteratorWrapper::boxed_new(GeoShape::new(
                ids_list,
                timeout_ctx,
                expiration_checker,
                mem_tracker,
            ))
        }
        None => RQEIteratorWrapper::boxed_new(GeoShape::new(
            ids_list,
            timeout_ctx,
            expiration_checker,
            NoTracker,
        )),
    }
}
