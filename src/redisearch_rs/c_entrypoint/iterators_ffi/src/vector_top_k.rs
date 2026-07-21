/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Rust implementation of the vector top-k iterator and its C accessor functions.
//!
//! This module provides [`NewVectorTopKIterator`] (a drop-in replacement for the
//! C `NewHybridVectorIterator`) together with the accessor shims that C callers
//! use for query-plan construction and profiling.

use std::{
    ffi::c_void,
    ptr::{self, NonNull},
};

use ffi::{
    QueryIterator, RLookupKey, RLookupKeyHandle, RedisSearchCtx, VecSimIndex, VecSimQueryParams,
    VecSimSearchMode_VECSIM_HYBRID_ADHOC_BF, VecSimSearchMode_VECSIM_HYBRID_BATCHES,
    VecSimSearchMode_VECSIM_HYBRID_BATCHES_TO_ADHOC_BF, VecSimSearchMode_VECSIM_STANDARD_KNN,
    timespec,
};
use field::FieldFilterContext;
use rqe_iterators::{
    ExpirationChecker, FieldExpirationChecker,
    c2rust::CRQEIterator,
    interop::{RQEIteratorWrapper, patch_vtable},
};
use top_k::TopKMode;
use vector_score_source::{NewVectorTopK, VectorTopKIterator, new_vector_top_k};

/// Construct a vector top-k iterator and expose it as a C [`QueryIterator`].
///
/// This call can reduce to an `Empty` iterator.
///
/// Pass `child = NULL` for a pure KNN query; pass a valid owning child iterator
/// for a hybrid (filtered) query.
///
/// The `query_params` pointer is read once to copy the parameters into the
/// iterator; it is not retained after this call.
///
/// `can_trim_deep_results` applies only to filtered queries: when `true`, the
/// pipeline needs no rich results, so each match yields a metric-only result
/// carrying just the vector score instead of a deep copy of the child's scoring
/// subtree. It has no effect on a pure KNN query, which is metric-only anyway.
///
/// # Safety
///
/// 1. `index` is non-null and [valid], and outlives the returned iterator.
/// 2. `query_vector` is [valid] for `vector_byte_len` bytes, and
///    `vector_byte_len` equals the index's expected query-vector size.
/// 3. `query_params` is non-null and [valid] for a [`VecSimQueryParams`].
/// 4. `child`, when non-null, is a [valid], owning `QueryIterator *` with every
///    callback populated.
/// 5. `filter_ctx` is non-null and [valid] for a [`FieldFilterContext`] for the
///    duration of this call.
/// 6. `sctx` is non-null and [valid] for a [`RedisSearchCtx`] with a [valid]
///    `spec`, both outliving the returned iterator.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NewVectorTopKIterator(
    index: *mut VecSimIndex,
    query_vector: *const c_void,
    vector_byte_len: usize,
    query_params: *const VecSimQueryParams,
    k: usize,
    can_trim_deep_results: bool,
    child: *mut QueryIterator,
    timeout: timespec,
    skip_timeout_checks: bool,
    sctx: *mut RedisSearchCtx,
    filter_ctx: *const FieldFilterContext,
) -> *mut QueryIterator {
    // SAFETY: guaranteed by 2.
    let query_vector =
        unsafe { std::slice::from_raw_parts(query_vector as *const u8, vector_byte_len).to_vec() };
    // SAFETY: guaranteed by 3.
    let query_params = unsafe { *query_params };

    // Adopt the filter child as an owning Rust iterator. A null child means a pure KNN query.
    // SAFETY: guaranteed by 4.
    let child = NonNull::new(child).map(|c| unsafe { CRQEIterator::new(c) });

    // SAFETY: guaranteed by 1.
    let index = unsafe { NonNull::new_unchecked(index) };

    // The accessor shims cast the returned handle back to the
    // `FieldExpirationChecker` monomorphization, so this is the only checker the
    // iterator may carry; a differently-typed checker would give those casts an
    // incompatible layout. Requirements 5 and 6 guarantee both inputs are present.
    debug_assert!(!sctx.is_null(), "sctx must be non-null");
    debug_assert!(!filter_ctx.is_null(), "filter_ctx must be non-null");

    // SAFETY: guaranteed by 6.
    let sctx_nn = unsafe { NonNull::new_unchecked(sctx) };
    // SAFETY: guaranteed by 5.
    let filter_ctx_val = unsafe { *filter_ctx };
    // Wide-schema flag is irrelevant here: the vector field expiration
    // check uses `FieldMaskOrIndex::Index`, not the mask path.
    // SAFETY: guaranteed by 6.
    let checker = unsafe { FieldExpirationChecker::new(sctx_nn, filter_ctx_val, 0) };
    // SAFETY: `index` and `query_vector` are guaranteed by 1 and 2.
    let reduced = unsafe {
        new_vector_top_k(
            index,
            query_vector,
            query_params,
            k,
            timeout,
            skip_timeout_checks,
            can_trim_deep_results,
            checker,
            child,
        )
    };
    box_reduced(reduced)
}

/// Box a [`NewVectorTopK`] into the C [`QueryIterator`] handle, clearing the
/// root-only `SkipTo` vtable slot so the C highlighter falls back to sequential
/// reads. Generic over the expiration checker to keep the boxing path
/// independent of the checker strategy.
fn box_reduced<'index, E: ExpirationChecker + 'index>(
    reduced: NewVectorTopK<'index, E>,
) -> *mut QueryIterator {
    let clear_skip_to = |ptr| {
        // SAFETY: `ptr` comes from `boxed_new`/`boxed_new_compound` below and has no
        // other alias yet, satisfying 1 and 2.
        unsafe { patch_vtable(ptr, |h| h.SkipTo = None) }
    };
    match reduced {
        NewVectorTopK::ReducedEmpty => RQEIteratorWrapper::boxed_new(rqe_iterators::Empty),
        NewVectorTopK::Unfiltered(it) => {
            let ptr = RQEIteratorWrapper::boxed_new(it);
            clear_skip_to(ptr);
            ptr
        }
        NewVectorTopK::Filtered(it) => {
            // `boxed_new_compound` registers the `ProfileChildren` callback so
            // `FT.PROFILE` recurses into and counts the filter subtree.
            let ptr = RQEIteratorWrapper::boxed_new_compound(it);
            clear_skip_to(ptr);
            ptr
        }
    }
}

/// Cast a `*mut QueryIterator` back to
/// its full Rust wrapper so accessors can reach `VectorScoreSource`.
///
/// # Safety
///
/// `it` must be a [`VectorTopKIterator`] and remain valid.
unsafe fn wrapper_mut(
    it: *mut QueryIterator,
) -> &'static mut RQEIteratorWrapper<VectorTopKIterator<'static>> {
    // SAFETY: `it` was created by `RQEIteratorWrapper::boxed_new` with inner type
    // `VectorTopKIterator<'static>`, so the cast is valid per `mut_ref_from_header_ptr`'s contract.
    unsafe { RQEIteratorWrapper::<VectorTopKIterator<'static>>::mut_ref_from_header_ptr(it) }
}

/// Cast a `*const QueryIterator` produced by [`NewVectorTopKIterator`] back to
/// its full Rust wrapper (shared).
///
/// # Safety
///
/// 1. `it` must be a valid, non-null pointer to a [`VectorTopKIterator`] that was
///    created by [`NewVectorTopKIterator`].
unsafe fn wrapper_ref(
    it: *const QueryIterator,
) -> &'static RQEIteratorWrapper<VectorTopKIterator<'static>> {
    // SAFETY: same contract as `wrapper_mut`.
    unsafe { RQEIteratorWrapper::<VectorTopKIterator<'static>>::ref_from_header_ptr(it) }
}

/// Return a mutable reference to the `RLookupKey *` stored inside this iterator.
///
/// The key is initially `NULL`; the metrics-loader result processor writes
/// through this pointer to set the iterator's score-output key.
///
/// # Safety
///
/// 1. `it` must be a valid, non-null pointer to a [`VectorTopKIterator`] that was
///    created by [`NewVectorTopKIterator`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn VectorTopK_GetOwnKeyRef(it: *mut QueryIterator) -> *mut *mut RLookupKey {
    debug_assert!(!it.is_null());
    // SAFETY: guaranteed by 1.
    let wrapper = unsafe { wrapper_mut(it) };
    // `own_key` boxes a `*mut RLookupKey<'static>`; return the address of the
    // boxed slot (the pointee), not of the `Box` field, so the pointer handed to
    // C stays valid across iterator moves (e.g. the `FT.PROFILE` rebox). The slot
    // holds an `RLookupKey<'static>`, whose `#[repr(C)]` prefix is `ffi::RLookupKey`,
    // so the pointer-to-pointer reinterprets to the C type the caller expects.
    (&raw mut *wrapper.inner.source_mut().own_key).cast::<*mut RLookupKey>()
}

/// Set the [`RLookupKeyHandle`] back-reference on this iterator.
///
/// The handle is used to invalidate the key pointer when the iterator is freed.
///
/// # Safety
///
/// 1. `it` must be a valid, non-null pointer to a [`VectorTopKIterator`] that was
///    created by [`NewVectorTopKIterator`].
/// 2. `handle` is either null or a valid pointer to a [`RLookupKeyHandle`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn VectorTopK_SetKeyHandle(
    it: *mut QueryIterator,
    handle: *mut RLookupKeyHandle,
) {
    debug_assert!(!it.is_null());
    // SAFETY: guaranteed by 1.
    let wrapper = unsafe { wrapper_mut(it) };
    wrapper.inner.source_mut().key_handle = handle;
}

/// Return a C string describing the search mode that was used (or is being used) for this query.
///
/// - [`Unfiltered`](TopKMode::Unfiltered) → `VECSIM_STANDARD_KNN`
/// - [`Batches`](TopKMode::Batches) → `VECSIM_HYBRID_BATCHES`
/// - [`AdhocBF`](TopKMode::AdhocBF) → `VECSIM_HYBRID_ADHOC_BF` or
///   `VECSIM_HYBRID_BATCHES_TO_ADHOC_BF` (when the mode was switched mid-execution)
///
/// The returned pointer is a static, null-terminated C string. It is valid for
/// the lifetime of the program and must not be freed by the caller.
///
/// # Safety
///
/// 1. `it` must be a valid, non-null pointer to a [`VectorTopKIterator`] that was
///    created by [`NewVectorTopKIterator`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn VectorTopK_GetSearchModeString(
    it: *const QueryIterator,
) -> *const ::std::os::raw::c_char {
    debug_assert!(!it.is_null());
    // SAFETY: guaranteed by 1.
    let wrapper = unsafe { wrapper_ref(it) };
    let mode = wrapper.inner.mode();
    let switches = wrapper.inner.metrics().strategy_switches;
    let mode = match mode {
        TopKMode::Unfiltered => VecSimSearchMode_VECSIM_STANDARD_KNN,
        TopKMode::Batches | TopKMode::ForcedBatches => VecSimSearchMode_VECSIM_HYBRID_BATCHES,
        TopKMode::AdhocBF if switches > 0 => VecSimSearchMode_VECSIM_HYBRID_BATCHES_TO_ADHOC_BF,
        TopKMode::AdhocBF => VecSimSearchMode_VECSIM_HYBRID_ADHOC_BF,
    };
    // SAFETY: `VecSimSearchMode` and `VecSearchMode` are distinct bindgen types generated from
    // two C enums that are kept in sync (same underlying integer type, matching variant values).
    unsafe { ffi::VecSimSearchMode_ToString(mode) }
}

/// Return `true` if the iterator is, or has ever been, in batches mode.
///
/// This includes queries that started as batches before switching to ad-hoc BF.
///
/// # Safety
///
/// 1. `it` must be a valid, non-null pointer to a [`VectorTopKIterator`] that was
///    created by [`NewVectorTopKIterator`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn VectorTopK_IsBatchMode(it: *const QueryIterator) -> bool {
    debug_assert!(!it.is_null());
    // SAFETY: guaranteed by 1.
    let wrapper = unsafe { wrapper_ref(it) };
    let mode = wrapper.inner.mode();
    let switches = wrapper.inner.metrics().strategy_switches;
    matches!(mode, TopKMode::Batches | TopKMode::ForcedBatches)
        || matches!(mode, TopKMode::AdhocBF if switches > 0)
}

/// Return the number of batch iterations performed so far (Batches mode only).
///
/// # Safety
///
/// 1. `it` must be a valid, non-null pointer to a [`VectorTopKIterator`] that was
///    created by [`NewVectorTopKIterator`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn VectorTopK_GetNumIterations(it: *const QueryIterator) -> usize {
    debug_assert!(!it.is_null());
    // SAFETY: guaranteed by 1.
    let wrapper = unsafe { wrapper_ref(it) };
    wrapper.inner.source().num_iterations
}

/// Return the largest batch size used during Batches mode.
///
/// # Safety
///
/// 1. `it` must be a valid, non-null pointer to a [`VectorTopKIterator`] that was
///    created by [`NewVectorTopKIterator`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn VectorTopK_GetMaxBatchSize(it: *const QueryIterator) -> usize {
    debug_assert!(!it.is_null());
    // SAFETY: guaranteed by 1.
    let wrapper = unsafe { wrapper_ref(it) };
    wrapper.inner.source().max_batch_size
}

/// Return the zero-based batch index at which the maximum batch size occurred.
///
/// # Safety
///
/// 1. `it` must be a valid, non-null pointer to a [`VectorTopKIterator`] that was
///    created by [`NewVectorTopKIterator`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn VectorTopK_GetMaxBatchIteration(it: *const QueryIterator) -> usize {
    debug_assert!(!it.is_null());
    // SAFETY: guaranteed by 1.
    let wrapper = unsafe { wrapper_ref(it) };
    wrapper.inner.source().max_batch_iteration
}

/// Return the filter child iterator, or `NULL` for a pure KNN query.
///
/// The returned pointer is non-owning; its lifetime is that of `it`.
///
/// # Safety
///
/// 1. `it` must be a valid, non-null pointer to a [`VectorTopKIterator`] that was
///    created by [`NewVectorTopKIterator`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn VectorTopK_GetChild(it: *const QueryIterator) -> *mut QueryIterator {
    debug_assert!(!it.is_null());
    // SAFETY: guaranteed by 1.
    let wrapper = unsafe { wrapper_ref(it) };
    wrapper.inner.child().map_or(ptr::null_mut(), |c| {
        c.as_ref() as *const QueryIterator as *mut QueryIterator
    })
}
