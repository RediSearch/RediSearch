/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ptr::NonNull;

use field::FieldMaskOrIndex;
use index_result::RSQueryTerm;
use rqe_iterators::FieldExpirationChecker;
use rqe_iterators::interop::RQEIteratorWrapper;
use rqe_iterators::inverted_index::{Term, TermIndexReader, build_term_iterator};

/// The concrete term iterator type produced by [`NewInvIndIterator_TermQuery`]
/// and wrapped in an [`RQEIteratorWrapper`]. Aliased so the FFI accessor path
/// can recover the wrapped iterator without spelling out the full generics.
pub(super) type TermIterator<'index> =
    Term<'index, TermIndexReader<'index>, FieldExpirationChecker>;

/// Creates a new term inverted index iterator for querying term fields.
///
/// # Parameters
///
/// * `idx` - Pointer to the inverted index to query.
/// * `sctx` - Pointer to the Redis search context.
/// * `field_mask_or_index` - Field mask or field index to filter on.
/// * `term` - Pointer to the query term. Ownership is transferred to the iterator.
/// * `weight` - Weight to apply to the term results.
///
/// # Returns
///
/// A pointer to a `QueryIterator` that can be used from C code.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
///
/// 1. `idx` must be a valid pointer to a term `InvertedIndex` and cannot be NULL.
/// 2. `idx` must remain valid between `revalidate()` calls, since the revalidation
///    mechanism detects when the index has been replaced via `Redis_OpenInvertedIndex()`
///    pointer comparison.
/// 3. `sctx` must be a valid pointer to a `RedisSearchCtx` and cannot be NULL.
/// 4. `sctx` and `sctx.spec` must remain valid for the lifetime of the returned iterator.
/// 5. `term` must be a valid pointer to a heap-allocated `RSQueryTerm` (e.g. created by
///    `NewQueryTerm`) and cannot be NULL. Ownership is transferred to the iterator.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NewInvIndIterator_TermQuery(
    idx: *const ffi::InvertedIndex,
    sctx: *const ffi::RedisSearchCtx,
    field_mask_or_index: FieldMaskOrIndex,
    term: *mut RSQueryTerm,
    weight: f64,
) -> *mut ffi::QueryIterator {
    debug_assert!(!idx.is_null(), "idx must not be null");
    debug_assert!(!sctx.is_null(), "sctx must not be null");
    debug_assert!(!term.is_null(), "term must not be null");

    // SAFETY: 3. guarantees `sctx` is valid and non-null.
    let sctx = unsafe { NonNull::new_unchecked(sctx as *mut _) };

    // SAFETY: 5. guarantees `term` is a heap-allocated `RSQueryTerm`; ownership
    // is transferred to the iterator.
    let term = unsafe { Box::from_raw(term) };

    // SAFETY: this function's preconditions map directly onto
    // `build_term_iterator`'s: (1)/(2) for `idx`, (3)/(4) for `sctx`, (5) for
    // `term`.
    let iterator = unsafe { build_term_iterator(idx, sctx, field_mask_or_index, term, weight) };

    RQEIteratorWrapper::boxed_new(iterator)
}
