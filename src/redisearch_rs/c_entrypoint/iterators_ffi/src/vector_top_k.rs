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

use std::{ffi::c_void, num::NonZeroUsize, ptr::NonNull};

use ffi::{
    IteratorType, QueryIterator, RLookupKey, RLookupKeyHandle, RedisSearchCtx, VecSimIndex,
    VecSimQueryParams, VecSimSearchMode_VECSIM_HYBRID_ADHOC_BF,
    VecSimSearchMode_VECSIM_HYBRID_BATCHES, VecSimSearchMode_VECSIM_HYBRID_BATCHES_TO_ADHOC_BF,
    VecSimSearchMode_VECSIM_STANDARD_KNN, timespec,
};
use field::FieldFilterContext;
use rqe_iterators::{
    FieldExpirationChecker, RQEIterator, c2rust::CRQEIterator, interop::RQEIteratorWrapper,
};
use top_k::TopKMode;
use vector_score_source::{
    VectorScoreSource, VectorTopKIterator, new_vector_top_k_filtered_boxed,
    new_vector_top_k_unfiltered,
};

// ── Constructor ───────────────────────────────────────────────────────────────

/// Construct a vector top-k iterator and expose it as a C [`QueryIterator`].
///
/// Pass `child = NULL` for a pure KNN query; pass a valid owning child iterator
/// for a hybrid (filtered) query.
///
/// The `query_params` pointer is read once to copy the parameters into the
/// iterator; it is not retained after this call.
///
/// # Profiling note
///
/// [`RQEIteratorWrapper::boxed_new`] (not `boxed_new_compound`) is used because
/// [`VectorScoreSource`] stores its filter child as `Box<dyn RQEIterator>` (type-erased),
/// which prevents implementing [`ProfileChildren`] today.
/// The `ProfileChildren` vtable entry is therefore `NULL`, meaning the filter child
/// won't be recursively profiled via `FT.PROFILE`.
/// This is the same behaviour as the C `HybridIterator` and can be addressed
/// when `TopKIterator` gains a typed-child variant.
///
/// [`ProfileChildren`]: rqe_iterators::interop::ProfileChildren
///
/// # Safety
///
/// - `index` must be a valid, non-null pointer that remains alive for the
///   duration of the returned iterator.
/// - `query_vector` must point to `vector_byte_len` valid, readable bytes.
/// - `query_params` must be a valid, non-null pointer to a [`VecSimQueryParams`].
/// - `child`, when non-null, must be a valid, owning `QueryIterator *` with all
///   required callbacks populated.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NewVectorTopKIterator(
    index: *mut VecSimIndex,
    query_vector: *const c_void,
    vector_byte_len: usize,
    query_params: *const VecSimQueryParams,
    k: usize,
    child: *mut QueryIterator,
    timeout: timespec,
    skip_timeout_checks: bool,
    is_disk: bool,
    sctx: *mut RedisSearchCtx,
    filter_ctx: *const FieldFilterContext,
) -> *mut QueryIterator {
    // SAFETY: valid for `vector_byte_len` bytes per caller's contract.
    let query_bytes =
        unsafe { std::slice::from_raw_parts(query_vector as *const u8, vector_byte_len).to_vec() };
    // SAFETY: non-null and valid per caller's contract.
    let query_params = unsafe { *query_params };

    // ── Reducer ───────────────────────────
    // Re-bind as mutable so we can null it out for the wildcard case.
    let mut child = child;
    if !child.is_null() {
        // SAFETY: child is valid per caller's contract.
        let child_type = unsafe { (*child).type_ };

        // Short-circuit 1: empty child — no documents can ever match.
        if child_type == IteratorType::Empty {
            return child;
        }

        // Short-circuit 2: wildcard child — every document matches, so the
        // filter is a no-op; treat as an unfiltered KNN query and free it.
        if matches!(
            child_type,
            IteratorType::Wildcard | IteratorType::InvIdxWildcard
        ) {
            // SAFETY: child is a valid, owning pointer per caller's contract.
            unsafe {
                if let Some(free) = (*child).Free {
                    free(child);
                }
            }
            child = std::ptr::null_mut();
        }
    }

    let Some(k) = NonZeroUsize::new(k) else {
        return std::ptr::null_mut();
    };

    // Build the per-field expiration checker when caller supplied an `sctx` and
    // `filter_ctx`. Mirrors the C `HybridIterator`'s post-yield check on
    // `filterCtx.field.value.index`.
    let expiration = match (NonNull::new(sctx), filter_ctx.is_null()) {
        (Some(sctx_nn), false) => {
            // SAFETY: caller's contract guarantees `filter_ctx` is valid when non-null.
            let filter_ctx_val = unsafe { *filter_ctx };
            // Wide-schema flag is irrelevant here: the vector field expiration
            // check uses `FieldMaskOrIndex::Index`, not the mask path.
            // SAFETY: caller's contract guarantees `sctx` and `sctx.spec` are valid.
            Some(unsafe { FieldExpirationChecker::new(sctx_nn, filter_ctx_val, 0) })
        }
        _ => None,
    };

    // `shouldRerank` lives in the disk-HNSW arm of the query-params union, so it
    // is only meaningful for disk indexes; reading it otherwise is gated by
    // `is_disk` (as is `VectorScoreSource`'s own use of it). Rerank runs only
    // when explicitly enabled, never for the UNSET/FALSE states.
    let should_rerank = is_disk && {
        // SAFETY: `is_disk` guarantees the union holds `hnswDiskRuntimeParams`.
        let rerank = unsafe {
            query_params
                .__bindgen_anon_1
                .hnswDiskRuntimeParams
                .shouldRerank
        };
        rerank == ffi::VecSimBool_VecSimBool_TRUE
    };

    // SAFETY: `index` is non-null per caller's contract.
    let index = unsafe { NonNull::new_unchecked(index) };

    if child.is_null() {
        // SAFETY: `index` remains valid for the iterator's lifetime per caller's contract.
        let source = unsafe {
            VectorScoreSource::new(
                index,
                query_bytes,
                query_params,
                k,
                timeout,
                skip_timeout_checks,
                is_disk,
                should_rerank,
                0, // no child
                0, // dynamic batch size
                expiration,
            )
        };
        RQEIteratorWrapper::boxed_new(new_vector_top_k_unfiltered(source, k))
    } else {
        // Store raw child pointer before ownership transfer (used by `HybridIterator_GetChild`).
        let child_raw = child;
        // SAFETY: `child` is non-null (checked above).
        let child_nn = unsafe { NonNull::new_unchecked(child) };
        // SAFETY: `child` is a valid, owning pointer with all callbacks populated per caller's contract.
        let child_iter = unsafe { CRQEIterator::new(child_nn) };
        let child_est = child_iter.num_estimated();
        // SAFETY: `index` remains valid for the iterator's lifetime per caller's contract.
        let mut source = unsafe {
            VectorScoreSource::new(
                index,
                query_bytes,
                query_params,
                k,
                timeout,
                skip_timeout_checks,
                is_disk,
                should_rerank,
                child_est,
                0, // dynamic batch size
                expiration,
            )
        };
        source.child_raw = child_raw;
        RQEIteratorWrapper::boxed_new(new_vector_top_k_filtered_boxed(
            source,
            Box::new(child_iter),
            k,
        ))
    }
}

// ── Accessor helpers ──────────────────────────────────────────────────────────

/// Cast a `*mut QueryIterator` produced by [`NewVectorTopKIterator`] back to
/// its full Rust wrapper so accessors can reach `VectorScoreSource`.
///
/// # Safety
///
/// `it` must have been produced by [`NewVectorTopKIterator`] and remain valid.
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
/// `it` must have been produced by [`NewVectorTopKIterator`] and remain valid.
unsafe fn wrapper_ref(
    it: *const QueryIterator,
) -> &'static RQEIteratorWrapper<VectorTopKIterator<'static>> {
    // SAFETY: same contract as `wrapper_mut`.
    unsafe { RQEIteratorWrapper::<VectorTopKIterator<'static>>::ref_from_header_ptr(it) }
}

// ── C accessor functions ──────────────────────────────────────────────────────

/// Return a mutable reference to the `RLookupKey *` stored inside this iterator.
///
/// The key is initially `NULL`; the metrics-loader result processor writes
/// through this pointer to set the iterator's score-output key.
///
/// # Safety
///
/// 1. `it` must be a valid, non-null pointer to a [`QueryIterator`] that was
///    created by [`NewVectorTopKIterator`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn HybridIterator_GetOwnKeyRef(
    it: *mut QueryIterator,
) -> *mut *mut RLookupKey {
    debug_assert!(!it.is_null());
    // SAFETY: guaranteed by 1.
    let wrapper = unsafe { wrapper_mut(it) };
    // `own_key` is `*mut RLookupKey<'static>`; `ffi::RLookupKey` is its `#[repr(C)]`
    // prefix, so the pointer-to-pointer reinterprets to the C type the caller expects.
    (&raw mut wrapper.inner.source_mut().own_key).cast::<*mut RLookupKey>()
}

/// Set the [`RLookupKeyHandle`] back-reference on this iterator.
///
/// The handle is used to invalidate the key pointer when the iterator is freed.
///
/// # Safety
///
/// 1. `it` must be a valid, non-null pointer to a [`QueryIterator`] that was
///    created by [`NewVectorTopKIterator`].
/// 2. `handle` is either null or a valid pointer to a [`RLookupKeyHandle`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn HybridIterator_SetKeyHandle(
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
/// 1. `it` must be a valid, non-null pointer to a [`QueryIterator`] that was
///    created by [`NewVectorTopKIterator`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn HybridIterator_GetSearchModeString(
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
/// 1. `it` must be a valid, non-null pointer to a [`QueryIterator`] that was
///    created by [`NewVectorTopKIterator`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn HybridIterator_IsBatchMode(it: *const QueryIterator) -> bool {
    debug_assert!(!it.is_null());
    // SAFETY: guaranteed by 1.
    let wrapper = unsafe { wrapper_ref(it) };
    let mode = wrapper.inner.mode();
    let switches = wrapper.inner.metrics().strategy_switches;
    matches!(mode, TopKMode::Batches | TopKMode::AdhocBF if switches > 0 )
}

/// Return the number of batch iterations performed so far (Batches mode only).
///
/// # Safety
///
/// 1. `it` must be a valid, non-null pointer to a [`QueryIterator`] that was
///    created by [`NewVectorTopKIterator`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn HybridIterator_GetNumIterations(it: *const QueryIterator) -> usize {
    debug_assert!(!it.is_null());
    // SAFETY: guaranteed by 1.
    let wrapper = unsafe { wrapper_ref(it) };
    wrapper.inner.source().num_iterations
}

/// Return the largest batch size used during Batches mode.
///
/// # Safety
///
/// 1. `it` must be a valid, non-null pointer to a [`QueryIterator`] that was
///    created by [`NewVectorTopKIterator`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn HybridIterator_GetMaxBatchSize(it: *const QueryIterator) -> usize {
    debug_assert!(!it.is_null());
    // SAFETY: guaranteed by 1.
    let wrapper = unsafe { wrapper_ref(it) };
    wrapper.inner.source().max_batch_size
}

/// Return the zero-based batch index at which the maximum batch size occurred.
///
/// # Safety
///
/// 1. `it` must be a valid, non-null pointer to a [`QueryIterator`] that was
///    created by [`NewVectorTopKIterator`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn HybridIterator_GetMaxBatchIteration(it: *const QueryIterator) -> usize {
    debug_assert!(!it.is_null());
    // SAFETY: guaranteed by 1.
    let wrapper = unsafe { wrapper_ref(it) };
    wrapper.inner.source().max_batch_iteration
}

/// Return the raw C child iterator pointer, or `NULL` for a pure KNN query.
///
/// The returned pointer is non-owning; its lifetime is that of `it`.
///
/// # Safety
///
/// 1. `it` must be a valid, non-null pointer to a [`QueryIterator`] that was
///    created by [`NewVectorTopKIterator`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn HybridIterator_GetChild(it: *const QueryIterator) -> *mut QueryIterator {
    debug_assert!(!it.is_null());
    // SAFETY: guaranteed by 1.
    let wrapper = unsafe { wrapper_ref(it) };
    wrapper.inner.source().child_raw
}
