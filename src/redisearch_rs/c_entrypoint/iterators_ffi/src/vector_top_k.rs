/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Rust implementation of the vector top-k iterator, exposed to C.

use std::{ffi::c_void, ptr::NonNull};

use ffi::{QueryIterator, QueryRequestTimeout, RedisSearchCtx, VecSimIndex, VecSimQueryParams};
use field::FieldFilterContext;
use rqe_iterators::{
    ExpirationChecker, FieldExpirationChecker,
    c2rust::CRQEIterator,
    interop::{RQEIteratorWrapper, patch_vtable},
};
use vector_score_source::{NewVectorTopK, new_vector_top_k};

/// Construct a vector top-k iterator and expose it as a C [`QueryIterator`].
///
/// This call can reduce to an `Empty` iterator, whose `type_` is
/// [`IteratorType::Empty`] rather than [`IteratorType::Hybrid`]. The accessors in
/// [`vector_score_source::interop`] must not be called on such a handle.
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
/// 7. `timeout` is non-null and remains valid for the returned iterator's lifetime.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
/// [`IteratorType::Empty`]: rqe_iterators::IteratorType::Empty
/// [`IteratorType::Hybrid`]: rqe_iterators::IteratorType::Hybrid
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NewVectorTopKIterator(
    index: *mut VecSimIndex,
    query_vector: *const c_void,
    vector_byte_len: usize,
    query_params: *const VecSimQueryParams,
    k: usize,
    can_trim_deep_results: bool,
    child: *mut QueryIterator,
    timeout: *mut QueryRequestTimeout,
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
    // SAFETY: `index`, `query_vector`, and `timeout` are guaranteed by 1, 2, and 7.
    let reduced = unsafe {
        new_vector_top_k(
            index,
            query_vector,
            query_params,
            k,
            timeout,
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
