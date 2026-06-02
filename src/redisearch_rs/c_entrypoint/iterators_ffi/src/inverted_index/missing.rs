/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ptr::NonNull;

use rqe_core::FieldIndex;
use rqe_iterators::{interop::RQEIteratorWrapper, inverted_index::new_missing_iterator};

/// Creates a new missing-field inverted index iterator.
///
/// # Parameters
///
/// * `idx` - Pointer to the missing-field inverted index (DocIdsOnly or RawDocIdsOnly encoded).
/// * `sctx` - Pointer to the Redis search context.
/// * `field_index` - The index of the field in `spec.fields` whose missing documents are tracked.
///
/// # Returns
///
/// A pointer to a `QueryIterator` that can be used from C code.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
///
/// 1. `idx` must be a valid pointer to an `InvertedIndex` and cannot be NULL.
/// 2. `idx` must remain valid between `revalidate()` calls, since the revalidation
///    mechanism detects when the index has been replaced via `spec.missingFieldDict`
///    lookup.
/// 3. `sctx` must be a valid pointer to a `RedisSearchCtx` and cannot be NULL.
/// 4. `sctx` and `sctx.spec` must remain valid for the lifetime of the returned iterator.
/// 5. `field_index` must be a valid index into `sctx.spec.fields`.
/// 6. `sctx.spec.missingFieldDict` must be a non-null, valid dict pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NewInvIndIterator_MissingQuery(
    idx: *const ffi::InvertedIndex,
    sctx: *const ffi::RedisSearchCtx,
    field_index: FieldIndex,
) -> *mut ffi::QueryIterator {
    debug_assert!(!idx.is_null(), "idx must not be null");
    debug_assert!(!sctx.is_null(), "sctx must not be null");

    let idx_ffi: *const inverted_index_ffi::InvertedIndex = idx.cast();
    // SAFETY: 1. guarantees idx is valid and non-null.
    let ii_ref = unsafe { &*idx_ffi };

    // SAFETY: 3. guarantees sctx is valid and non-null.
    let sctx_nn = unsafe { NonNull::new_unchecked(sctx as *mut _) };

    // SAFETY: 3.-6. guarantee sctx, spec, field_index, and
    // missingFieldDict validity.
    let it = unsafe { new_missing_iterator(ii_ref, sctx_nn, field_index) };
    RQEIteratorWrapper::boxed_new(it)
}
