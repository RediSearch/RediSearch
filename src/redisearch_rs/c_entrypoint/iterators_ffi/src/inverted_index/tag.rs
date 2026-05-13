/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{fmt::Debug, ptr::NonNull};

use field::{FieldExpirationPredicate, FieldFilterContext, FieldMaskOrIndex};
use inverted_index::{
    IndexReader, RSIndexResult, RSQueryTerm, doc_ids_only::DocIdsOnly,
    raw_doc_ids_only::RawDocIdsOnly, t_docId,
};
use rqe_iterators::{
    FieldExpirationChecker, IteratorType, interop::RQEIteratorWrapper, inverted_index::Tag,
};

/// Wrapper around different tag iterator encoding types to avoid generics in FFI code.
///
/// Tag inverted indices are always created with `DocIdsOnly` flags, so only
/// the standard variable-length encoding ([`DocIdsOnly`]) and the fixed 4-byte
/// raw encoding ([`RawDocIdsOnly`]) are supported.
pub(super) enum TagIterator<'index> {
    Encoded(Tag<'index, DocIdsOnly, FieldExpirationChecker>),
    Raw(Tag<'index, RawDocIdsOnly, FieldExpirationChecker>),
}

impl Debug for TagIterator<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let variant = match self {
            TagIterator::Encoded(_) => "Encoded",
            TagIterator::Raw(_) => "Raw",
        };
        write!(f, "TagIterator({variant})")
    }
}

impl<'index> TagIterator<'index> {
    /// Get the flags from the underlying reader.
    pub(super) fn flags(&self) -> ffi::IndexFlags {
        match self {
            TagIterator::Encoded(t) => t.reader().flags(),
            TagIterator::Raw(t) => t.reader().flags(),
        }
    }
}

/// Macro to dispatch RQEIterator methods across all TagIterator variants.
macro_rules! tag_it_dispatch {
    ($self:expr, $method:ident $(, $arg:expr)*) => {
        match $self {
            TagIterator::Encoded(t) => t.$method($($arg),*),
            TagIterator::Raw(t) => t.$method($($arg),*),
        }
    };
}

impl<'index> rqe_iterators::RQEIterator<'index> for TagIterator<'index> {
    #[inline(always)]
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        tag_it_dispatch!(self, current)
    }

    #[inline(always)]
    fn read(
        &mut self,
    ) -> Result<Option<&mut RSIndexResult<'index>>, rqe_iterators::RQEIteratorError> {
        tag_it_dispatch!(self, read)
    }

    #[inline(always)]
    fn skip_to(
        &mut self,
        doc_id: t_docId,
    ) -> Result<Option<rqe_iterators::SkipToOutcome<'_, 'index>>, rqe_iterators::RQEIteratorError>
    {
        tag_it_dispatch!(self, skip_to, doc_id)
    }

    #[inline(always)]
    fn rewind(&mut self) {
        tag_it_dispatch!(self, rewind)
    }

    #[inline(always)]
    fn num_estimated(&self) -> usize {
        tag_it_dispatch!(self, num_estimated)
    }

    #[inline(always)]
    fn last_doc_id(&self) -> t_docId {
        tag_it_dispatch!(self, last_doc_id)
    }

    #[inline(always)]
    fn at_eof(&self) -> bool {
        tag_it_dispatch!(self, at_eof)
    }

    #[inline(always)]
    unsafe fn revalidate(
        &mut self,
        spec: std::ptr::NonNull<ffi::IndexSpec>,
    ) -> Result<rqe_iterators::RQEValidateStatus<'_, 'index>, rqe_iterators::RQEIteratorError> {
        // SAFETY: Delegating to variant with the same `spec` passed by our caller.
        unsafe { tag_it_dispatch!(self, revalidate, spec) }
    }

    #[inline(always)]
    fn type_(&self) -> IteratorType {
        IteratorType::InvIdxTag
    }

    fn intersection_sort_weight(&self, _prioritize_union_children: bool) -> f64 {
        1.0
    }
}

/// Creates a new tag inverted index iterator.
///
/// # Parameters
///
/// * `idx` - Pointer to the tag's inverted index ([`DocIdsOnly`] or [`RawDocIdsOnly`] encoded).
/// * `tag_idx` - Pointer to the [`TagIndex`](ffi::TagIndex) containing the `TrieMap` of tag values.
/// * `sctx` - Pointer to the Redis search context.
/// * `field_mask_or_index` - Field mask or field index to filter on.
/// * `term` - Pointer to the query term representing the tag value. Ownership is
///   transferred to the iterator.
/// * `weight` - Weight to apply to the term results.
///
/// # Returns
///
/// A pointer to a heap-allocated [`QueryIterator`](ffi::QueryIterator) that can be used from C
/// code. The caller is responsible for freeing the iterator by calling its `Free` callback
/// (i.e. `it->Free(it)`).
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
///
/// 1. `idx` must be a valid pointer to a [`DocIdsOnly`] or [`RawDocIdsOnly`]
///    [`InvertedIndex`](ffi::InvertedIndex) and cannot be NULL.
/// 2. `idx` must remain valid between [`revalidate()`](rqe_iterators::RQEIterator::revalidate) calls, since the revalidation
///    mechanism detects when the index has been replaced via [`TagIndex`](ffi::TagIndex) `TrieMap` lookup.
/// 3. `tag_idx` must be a valid pointer to a [`TagIndex`](ffi::TagIndex) and cannot be NULL.
/// 4. `tag_idx` and `tag_idx.values` must remain valid for the lifetime of the returned
///    iterator.
/// 5. `sctx` must be a valid pointer to a [`RedisSearchCtx`](ffi::RedisSearchCtx) and cannot be NULL.
/// 6. `sctx` and `sctx.spec` must remain valid for the lifetime of the returned iterator.
/// 7. `term` must be a valid pointer to a heap-allocated [`RSQueryTerm`] (e.g. created by
///    `NewQueryTerm`) and cannot be NULL. Ownership is transferred to the iterator.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NewInvIndIterator_TagQuery(
    idx: *const ffi::InvertedIndex,
    tag_idx: *const ffi::TagIndex,
    sctx: *const ffi::RedisSearchCtx,
    field_mask_or_index: FieldMaskOrIndex,
    term: *mut RSQueryTerm,
    weight: f64,
) -> *mut ffi::QueryIterator {
    debug_assert!(!idx.is_null(), "idx must not be null");
    debug_assert!(!tag_idx.is_null(), "tag_idx must not be null");
    debug_assert!(!sctx.is_null(), "sctx must not be null");
    debug_assert!(!term.is_null(), "term must not be null");

    // Cast to the FFI wrapper enum which handles type dispatch
    let idx_ffi: *const inverted_index_ffi::InvertedIndex = idx.cast();
    // SAFETY: 1. guarantees idx is valid and non-null
    let ii_ref = unsafe { &*idx_ffi };

    // SAFETY: 3. guarantees tag_idx is valid and non-null
    let tag_idx_nn = unsafe { NonNull::new_unchecked(tag_idx as *mut _) };

    // SAFETY: 5. guarantees sctx is valid and non-null
    let sctx_nn = unsafe { NonNull::new_unchecked(sctx as *mut _) };

    // SAFETY: 7. guarantees term is a heap-allocated RSQueryTerm
    let term = unsafe { Box::from_raw(term) };

    // Build the expiration checker for the Default predicate
    let filter_ctx = FieldFilterContext {
        field: field_mask_or_index,
        predicate: FieldExpirationPredicate::Default,
    };

    // Create the appropriate tag iterator variant based on encoding type.
    let iterator = match ii_ref {
        inverted_index_ffi::InvertedIndex::DocIdsOnly(ii) => {
            let reader = ii.reader();
            // SAFETY: 5., 6. guarantee context/spec validity for the lifetime of the checker.
            let checker =
                unsafe { FieldExpirationChecker::new(sctx_nn, filter_ctx, reader.flags()) };
            // SAFETY: 1., 2. guarantee idx validity and revalidation semantics.
            // 3., 4. guarantee tag_index and TrieMap validity.
            // 5., 6. guarantee context/spec validity.
            // 7. guarantees term ownership transfer.
            // The DocIdsOnly match arm ensures the encoding variant matches.
            TagIterator::Encoded(unsafe {
                Tag::new(reader, sctx_nn, tag_idx_nn, term, weight, checker)
            })
        }
        inverted_index_ffi::InvertedIndex::RawDocIdsOnly(ii) => {
            let reader = ii.reader();
            // SAFETY: 5., 6. guarantee context/spec validity for the lifetime of the checker.
            let checker =
                unsafe { FieldExpirationChecker::new(sctx_nn, filter_ctx, reader.flags()) };
            // SAFETY: 1., 2. guarantee idx validity and revalidation semantics.
            // 3., 4. guarantee tag_index and TrieMap validity.
            // 5., 6. guarantee context/spec validity.
            // 7. guarantees term ownership transfer.
            // The RawDocIdsOnly match arm ensures the encoding variant matches.
            TagIterator::Raw(unsafe {
                Tag::new(reader, sctx_nn, tag_idx_nn, term, weight, checker)
            })
        }
        _ => panic!(
            "Tag iterator requires a DocIdsOnly or RawDocIdsOnly inverted index, got: {:?}",
            std::mem::discriminant(ii_ref)
        ),
    };

    RQEIteratorWrapper::boxed_new(iterator)
}
