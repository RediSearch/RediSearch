/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ptr::NonNull;

use ffi::{
    IteratorType_INV_IDX_WILDCARD_ITERATOR, IteratorType_WILDCARD_ITERATOR, QueryIterator, t_docId,
};
use rqe_iterators::{RQEIterator, Wildcard};
use rqe_iterators_interop::RQEIteratorWrapper;

/// Creates a new non-optimized wildcard iterator over the `[0, max_id]` document id range.
#[unsafe(no_mangle)]
pub extern "C" fn NewWildcardIterator_NonOptimized(
    max_id: t_docId,
    weight: f64,
) -> *mut QueryIterator {
    RQEIteratorWrapper::boxed_new(
        IteratorType_WILDCARD_ITERATOR,
        Wildcard::new(max_id, weight),
    )
}

/// Returns `true` if `it` is a wildcard iterator (either optimized or non-optimized).
///
/// # Safety
///
/// `it`, when non-null, must point to a valid [`QueryIterator`].
#[unsafe(no_mangle)]
#[allow(non_upper_case_globals)]
pub const unsafe extern "C" fn IsWildcardIterator(it: *const QueryIterator) -> bool {
    // SAFETY: Caller guarantees `it`, when non-null, points to a valid `QueryIterator`.
    let Some(it) = (unsafe { it.as_ref() }) else {
        return false;
    };
    matches!(
        it.type_,
        IteratorType_WILDCARD_ITERATOR | IteratorType_INV_IDX_WILDCARD_ITERATOR
    )
}

/// Creates a new optimized wildcard iterator.
///
/// This can only be used when the index is configured to index all documents
/// ([`SchemaRule`](ffi::SchemaRule)`.index_all` is set).
///
/// # Safety
///
/// 1. `sctx` must be a non-null pointer to a valid [`RedisSearchCtx`](ffi::RedisSearchCtx)
///    that remains valid for the lifetime of the returned iterator.
/// 2. `sctx.spec` must be a non-null pointer to a valid [`IndexSpec`](ffi::IndexSpec) that
///    remains valid for the lifetime of the returned iterator.
/// 3. `sctx.spec.rule` must be a non-null pointer to a valid [`SchemaRule`](ffi::SchemaRule) with
///    [`index_all`](ffi::SchemaRule::index_all) set to `true`.
/// 4. `sctx.spec.existingDocs`, when non-null, must point to a valid
///    [`InvertedIndex`](ffi::InvertedIndex) with either
///    [`DocIdsOnly`](inverted_index::codec::doc_ids_only::DocIdsOnly) or
///    [`RawDocIdsOnly`](inverted_index::codec::raw_doc_ids_only::RawDocIdsOnly) encoding.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NewWildcardIterator_Optimized(
    sctx: *const ffi::RedisSearchCtx,
    weight: f64,
) -> *mut QueryIterator {
    let sctx = NonNull::new(sctx.cast_mut()).expect("sctx is null");
    // SAFETY: Caller guarantees all preconditions of `new_wildcard_iterator_optimized`.
    let (it, iter_type) =
        unsafe { rqe_iterators::wildcard::new_wildcard_iterator_optimized(sctx, weight) };
    let it: Box<dyn RQEIterator + '_> = it;
    RQEIteratorWrapper::boxed_new(iter_type, it)
}

/// Creates a new wildcard iterator from a query evaluation context.
///
/// There are three possible code paths:
///
/// 1. **Disk index** — when [`spec.diskSpec`](ffi::IndexSpec::diskSpec) is non-null, delegates to the C
///    function `SearchDisk_NewWildcardIterator`.
/// 2. **[`index_all`](ffi::SchemaRule::index_all) optimized** — when [`SchemaRule`](ffi::SchemaRule)`.index_all` is set, delegates to
///    [`rqe_iterators::wildcard::new_wildcard_iterator_optimized`].
/// 3. **Fallback** — creates a simple [`Wildcard`] iterator that yields all
///    document ids up to [`docTable.maxDocId`](ffi::DocTable::maxDocId).
///
/// # Safety
///
/// 1. `q` must be a non-null pointer to a valid [`QueryEvalCtx`](ffi::QueryEvalCtx)
///    that remains valid for the lifetime of the returned iterator.
/// 2. `q.sctx` must be a non-null pointer to a valid
///    [`RedisSearchCtx`](ffi::RedisSearchCtx) that remains valid for the lifetime
///    of the returned iterator.
/// 3. `q.sctx.spec` must be a non-null pointer to a valid [`IndexSpec`](ffi::IndexSpec) that
///    remains valid for the lifetime of the returned iterator.
/// 4. `q.sctx.spec.rule`, when non-null, must point to a valid [`SchemaRule`](ffi::SchemaRule).
/// 5. When [`SchemaRule`](ffi::SchemaRule)`.index_all` is true, the preconditions of
///    [`rqe_iterators::wildcard::new_wildcard_iterator_optimized`] must also hold.
/// 6. `q.docTable` must be a non-null pointer to a valid [`DocTable`](ffi::DocTable).
/// 7. `q.sctx.spec.diskSpec`, when non-null, must point to a valid
///    [`RedisSearchDiskIndexSpec`](ffi::RedisSearchDiskIndexSpec). `SearchDisk_NewWildcardIterator` must return
///    a valid, owning `QueryIterator` pointer with all required callbacks set.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NewWildcardIterator(
    q: *const ffi::QueryEvalCtx,
    weight: f64,
) -> *mut QueryIterator {
    let query = NonNull::new(q.cast_mut()).expect("q is null");
    // SAFETY: Caller guarantees all preconditions of `new_wildcard_iterator`.
    let (it, iter_type) = unsafe { rqe_iterators::wildcard::new_wildcard_iterator(query, weight) };
    let it: Box<dyn RQEIterator + '_> = it;
    RQEIteratorWrapper::boxed_new(iter_type, it)
}
