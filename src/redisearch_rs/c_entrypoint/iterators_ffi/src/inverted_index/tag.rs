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
use index_result::{RSIndexResult, RSQueryTerm};
use ffi::{
    ValidateStatus_VALIDATE_ABORTED, ValidateStatus_VALIDATE_MOVED, ValidateStatus_VALIDATE_OK,
    t_docId,
};
use inverted_index::{
    IndexReader, RefreshOutcome, doc_ids_only::DocIdsOnly, raw_doc_ids_only::RawDocIdsOnly,
};
use rqe_core::DocId;
use rqe_iterators::{
    FieldExpirationChecker, IteratorType, RQEIteratorBoxed, RQEIteratorError, RQESuspendedIterator,
    ResumeOutcome, interop::RQEIteratorWrapper, inverted_index::Tag,
    profile_print,
};

/// Suspended counterpart of [`TagIterator`] — produced by
/// [`RQEIteratorBoxed::suspend`] and consumed by [`RQESuspendedIterator::resume`].
///
/// `#[repr(C, u8)]` matches the layout of [`TagIterator`] so that
/// [`RQEIteratorBoxed::suspend`] and [`RQESuspendedIterator::resume`]
/// can perform whole-`Box` pointer casts between the two — the heap
/// allocation is preserved across suspend/resume, which is critical
/// because the FFI wrapper's `header.current` field on the
/// containing [`RQEIteratorWrapper`] is a borrowed pointer into the
/// inner iterator's `result.current` slot. Re-allocating the inner
/// iterator (which the previous `match` + `Box::new` shape did) would
/// leave that pointer dangling. See also
/// `RawInvIndIterator::suspend` for the same argument at the leaf level.
///
/// Retains the `'query` lifetime so query-attached borrows stay valid across
/// the suspend/resume cycle.
#[repr(C, u8)]
#[expect(
    dead_code,
    reason = "variants are constructed via the #[repr(C, u8)] whole-Box cast in `suspend`, never by name"
)]
pub(super) enum TagIteratorSuspended<'query> {
    Encoded(<Tag<'query, DocIdsOnly, FieldExpirationChecker> as RQEIteratorBoxed<'query>>::Suspended),
    Raw(<Tag<'query, RawDocIdsOnly, FieldExpirationChecker> as RQEIteratorBoxed<'query>>::Suspended),
}

/// Local 3-state outcome carrying the work done while still on the
/// suspended form (`should_abort` + `refresh_pointers`) into the active
/// form for the optional reseek dispatch.
enum TagResumeOutcome {
    Abort,
    Ok,
    NeedsReseek { last_doc_id: t_docId },
}

impl<'index> RQEIteratorBoxed<'index> for TagIterator<'index> {
    type Suspended = TagIteratorSuspended<'index>;

    fn suspend(self: Box<Self>) -> Box<Self::Suspended> {
        let raw = Box::into_raw(self);
        // SAFETY: `TagIterator<'index>` and `TagIteratorSuspended` are
        // both `#[repr(C, u8)]` with the same variant order and
        // layout-compatible payloads (the underlying `RawTag<Active, E, C>`
        // / `RawTag<Suspended, E, C>` instantiations are layout-compatible
        // via `#[repr(C)]` + the `SharedPtr` argument; see
        // `RawInvIndIterator::suspend`). The discriminant byte is shared
        // identically. `Box::from_raw` reuses the same heap allocation,
        // so any external borrowed pointer into the iterator's interior
        // (e.g. the wrapper's `header.current`) stays valid across the
        // cast.
        unsafe { Box::from_raw(raw as *mut TagIteratorSuspended<'index>) }
    }
}

impl<'query> RQESuspendedIterator<'query> for TagIteratorSuspended<'query> {
    type Resumed<'a>
        = TagIterator<'a>
    where
        'query: 'a;

    fn resume<'a>(
        mut self: Box<Self>,
        guard: &index_spec::IndexSpecReadGuard<'a>,
    ) -> Result<ResumeOutcome<Box<Self::Resumed<'a>>>, RQEIteratorError>
    where
        'query: 'a,
    {
        // Step 1: should_abort + refresh_pointers on the suspended
        // variant. Both methods are mode-independent and read only
        // mode-independent state (cached term, `tag_index`,
        // `points_to_ii`, the inverted-index `gc_marker`); no `&'a`
        // borrow of any potentially-stale buffer is materialized.
        let outcome = match &mut *self {
            TagIteratorSuspended::Encoded(t) => {
                if t.should_abort() {
                    TagResumeOutcome::Abort
                } else {
                    match t.refresh_pointers(guard) {
                        RefreshOutcome::Ok => TagResumeOutcome::Ok,
                        RefreshOutcome::NeedsReseek { last_doc_id } => {
                            TagResumeOutcome::NeedsReseek { last_doc_id }
                        }
                    }
                }
            }
            TagIteratorSuspended::Raw(t) => {
                if t.should_abort() {
                    TagResumeOutcome::Abort
                } else {
                    match t.refresh_pointers(guard) {
                        RefreshOutcome::Ok => TagResumeOutcome::Ok,
                        RefreshOutcome::NeedsReseek { last_doc_id } => {
                            TagResumeOutcome::NeedsReseek { last_doc_id }
                        }
                    }
                }
            }
        };

        // Step 2: whole-box cast Suspended → Active. Heap address
        // preserved; external borrows into the inner iterator stay valid.
        let raw = Box::into_raw(self);
        // SAFETY: layout-compatible — see `suspend`. The per-variant
        // `refresh_pointers` step above ensures every pointer inside is
        // valid for `'a`.
        let mut active = unsafe { Box::from_raw(raw as *mut TagIterator<'a>) };

        // Step 3: dispatch the outcome into a ValidateStatus, reseeking if
        // needed (only defined on the active form).
        let status = match outcome {
            TagResumeOutcome::Abort => ValidateStatus_VALIDATE_ABORTED,
            TagResumeOutcome::Ok => ValidateStatus_VALIDATE_OK,
            TagResumeOutcome::NeedsReseek { last_doc_id } => match &mut *active {
                TagIterator::Encoded(t) => t.reseek_after_refresh(last_doc_id),
                TagIterator::Raw(t) => t.reseek_after_refresh(last_doc_id),
            },
        };

        // Map the status to the generic `ResumeOutcome`; on Aborted `active` drops.
        #[expect(non_upper_case_globals)]
        Ok(match status {
            ValidateStatus_VALIDATE_OK => ResumeOutcome::Ok(active),
            ValidateStatus_VALIDATE_MOVED => ResumeOutcome::Moved(active),
            _ => ResumeOutcome::Aborted,
        })
    }

    fn last_doc_id(&self) -> DocId {
        match self {
            TagIteratorSuspended::Encoded(s) => s.last_doc_id(),
            TagIteratorSuspended::Raw(s) => s.last_doc_id(),
        }
    }

    fn num_estimated(&self) -> usize {
        match self {
            TagIteratorSuspended::Encoded(s) => s.num_estimated(),
            TagIteratorSuspended::Raw(s) => s.num_estimated(),
        }
    }
}

/// Wrapper around different tag iterator encoding types to avoid generics in FFI code.
///
/// Tag inverted indices are always created with `DocIdsOnly` flags, so only
/// the standard variable-length encoding ([`DocIdsOnly`]) and the fixed 4-byte
/// raw encoding ([`RawDocIdsOnly`]) are supported.
///
/// `#[repr(C, u8)]` to make the layout match
/// [`TagIteratorSuspended`] — see that type's docs for the heap-address
/// preservation argument.
#[repr(C, u8)]
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
    /// Get the cached flags of the underlying reader.
    pub(super) const fn flags(&self) -> ffi::IndexFlags {
        match self {
            TagIterator::Encoded(t) => t.flags(),
            TagIterator::Raw(t) => t.flags(),
        }
    }
}

impl<'query> TagIteratorSuspended<'query> {
    /// Mirror of [`TagIterator::flags`] on the suspended side — see
    /// [`rqe_iterators::interop::RQEIteratorWrapper::state`].
    pub(super) const fn flags(&self) -> ffi::IndexFlags {
        match self {
            TagIteratorSuspended::Encoded(t) => t.flags(),
            TagIteratorSuspended::Raw(t) => t.flags(),
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
        doc_id: DocId,
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
    fn last_doc_id(&self) -> DocId {
        tag_it_dispatch!(self, last_doc_id)
    }

    #[inline(always)]
    fn at_eof(&self) -> bool {
        tag_it_dispatch!(self, at_eof)
    }

    #[inline(always)]
    fn revalidate(
        &mut self,
        spec: &index_spec::IndexSpecReadGuard,
    ) -> Result<rqe_iterators::RQEValidateStatus<'_, 'index>, rqe_iterators::RQEIteratorError> {
        tag_it_dispatch!(self, revalidate, spec)
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

impl profile_print::ProfilePrint for TagIterator<'_> {
    fn print_profile(
        &self,
        map: &mut redis_reply::MapBuilder<'_>,
        ctx: &mut profile_print::ProfilePrintCtx<'_>,
    ) {
        tag_it_dispatch!(self, print_profile, map, ctx);
    }
}
