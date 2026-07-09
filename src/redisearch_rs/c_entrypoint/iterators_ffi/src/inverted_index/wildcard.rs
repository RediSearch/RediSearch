/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::fmt::Debug;

use ffi::{
    ValidateStatus_VALIDATE_ABORTED, ValidateStatus_VALIDATE_MOVED, ValidateStatus_VALIDATE_OK,
    t_docId,
};
use index_result::RSIndexResult;
use inverted_index::{RefreshOutcome, doc_ids_only::DocIdsOnly, raw_doc_ids_only::RawDocIdsOnly};
use rqe_core::DocId;
use rqe_iterators::{
    IteratorType, RQEIterator, RQEIteratorError, RQESuspendedIterator, ResumeOutcome,
    interop::RQEIteratorWrapper, inverted_index::Wildcard, profile_print,
};

/// Suspended counterpart of [`WildcardIterator`] — produced by
/// [`RQEIterator::suspend`] and consumed by [`RQESuspendedIterator::resume`].
///
/// `#[repr(C, u8)]` matches the layout of [`WildcardIterator`] so that
/// [`RQEIterator::suspend`] / [`RQESuspendedIterator::resume`]
/// can perform whole-`Box` pointer casts between the two — see
/// [`super::tag::TagIteratorSuspended`] for the heap-address
/// preservation argument.
///
/// Retains the `'query` lifetime so query-attached borrows stay valid across
/// the suspend/resume cycle.
#[repr(C, u8)]
#[expect(
    dead_code,
    reason = "variants are constructed via the #[repr(C, u8)] whole-Box cast in `suspend`, never by name"
)]
pub(super) enum WildcardIteratorSuspended<'query> {
    Encoded(<Wildcard<'query, DocIdsOnly> as RQEIterator<'query>>::Suspended),
    Raw(<Wildcard<'query, RawDocIdsOnly> as RQEIterator<'query>>::Suspended),
}

/// Local 3-state outcome carrying the work done while still on the
/// suspended form into the active form for the optional reseek dispatch.
enum WildcardResumeOutcome {
    Abort,
    Ok,
    NeedsReseek { last_doc_id: t_docId },
}


impl<'query> RQESuspendedIterator<'query> for WildcardIteratorSuspended<'query> {
    type Resumed<'a>
        = WildcardIterator<'a>
    where
        'query: 'a;

    fn resume<'a>(
        mut self: Box<Self>,
        spec: &index_spec::IndexSpecReadGuard<'a>,
    ) -> Result<ResumeOutcome<Box<Self::Resumed<'a>>>, RQEIteratorError>
    where
        'query: 'a,
    {
        // Step 1: should_abort + refresh_pointers on the suspended
        // variant (see `TagIteratorSuspended::resume` for the
        // mode-independence argument).
        let outcome = match &mut *self {
            WildcardIteratorSuspended::Encoded(w) => {
                if w.should_abort(spec) {
                    WildcardResumeOutcome::Abort
                } else {
                    match w.refresh_pointers(spec) {
                        RefreshOutcome::Ok => WildcardResumeOutcome::Ok,
                        RefreshOutcome::NeedsReseek { last_doc_id } => {
                            WildcardResumeOutcome::NeedsReseek { last_doc_id }
                        }
                    }
                }
            }
            WildcardIteratorSuspended::Raw(w) => {
                if w.should_abort(spec) {
                    WildcardResumeOutcome::Abort
                } else {
                    match w.refresh_pointers(spec) {
                        RefreshOutcome::Ok => WildcardResumeOutcome::Ok,
                        RefreshOutcome::NeedsReseek { last_doc_id } => {
                            WildcardResumeOutcome::NeedsReseek { last_doc_id }
                        }
                    }
                }
            }
        };

        // Step 2: whole-box cast Suspended → Active.
        let raw = Box::into_raw(self);
        // SAFETY: layout-compatible — see `suspend`.
        let mut active = unsafe { Box::from_raw(raw as *mut WildcardIterator<'a>) };

        // Step 3: dispatch the outcome into a ValidateStatus, reseeking if needed.
        let status = match outcome {
            WildcardResumeOutcome::Abort => ValidateStatus_VALIDATE_ABORTED,
            WildcardResumeOutcome::Ok => ValidateStatus_VALIDATE_OK,
            WildcardResumeOutcome::NeedsReseek { last_doc_id } => match &mut *active {
                WildcardIterator::Encoded(w) => w.reseek_after_refresh(last_doc_id),
                WildcardIterator::Raw(w) => w.reseek_after_refresh(last_doc_id),
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
            WildcardIteratorSuspended::Encoded(s) => s.last_doc_id(),
            WildcardIteratorSuspended::Raw(s) => s.last_doc_id(),
        }
    }

    fn num_estimated(&self) -> usize {
        match self {
            WildcardIteratorSuspended::Encoded(s) => s.num_estimated(),
            WildcardIteratorSuspended::Raw(s) => s.num_estimated(),
        }
    }
}

/// Wrapper around different II wildcard iterator encoding types to avoid generics in FFI code.
///
/// Handles both the standard variable-length encoding ([`DocIdsOnly`]) and the
/// fixed 4-byte raw encoding ([`RawDocIdsOnly`]).
///
/// `#[repr(C, u8)]` to make the layout match
/// [`WildcardIteratorSuspended`].
#[repr(C, u8)]
pub(super) enum WildcardIterator<'index> {
    Encoded(Wildcard<'index, DocIdsOnly>),
    Raw(Wildcard<'index, RawDocIdsOnly>),
}

impl Debug for WildcardIterator<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let variant = match self {
            WildcardIterator::Encoded(_) => "Encoded",
            WildcardIterator::Raw(_) => "Raw",
        };
        write!(f, "WildcardIterator({variant})")
    }
}

impl<'index> RQEIterator<'index> for WildcardIterator<'index> {
    type Suspended = WildcardIteratorSuspended<'index>;

    fn suspend(self: Box<Self>) -> Box<Self::Suspended> {
        let raw = Box::into_raw(self);
        // SAFETY: layout-compatible enums (see `WildcardIteratorSuspended`).
        unsafe { Box::from_raw(raw as *mut WildcardIteratorSuspended<'index>) }
    }

    #[inline(always)]
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        match self {
            WildcardIterator::Encoded(w) => w.current(),
            WildcardIterator::Raw(w) => w.current(),
        }
    }

    #[inline(always)]
    fn read(
        &mut self,
    ) -> Result<Option<&mut RSIndexResult<'index>>, rqe_iterators::RQEIteratorError> {
        match self {
            WildcardIterator::Encoded(w) => w.read(),
            WildcardIterator::Raw(w) => w.read(),
        }
    }

    #[inline(always)]
    fn skip_to(
        &mut self,
        doc_id: DocId,
    ) -> Result<Option<rqe_iterators::SkipToOutcome<'_, 'index>>, rqe_iterators::RQEIteratorError>
    {
        match self {
            WildcardIterator::Encoded(w) => w.skip_to(doc_id),
            WildcardIterator::Raw(w) => w.skip_to(doc_id),
        }
    }

    #[inline(always)]
    fn rewind(&mut self) {
        match self {
            WildcardIterator::Encoded(w) => w.rewind(),
            WildcardIterator::Raw(w) => w.rewind(),
        }
    }

    #[inline(always)]
    fn num_estimated(&self) -> usize {
        match self {
            WildcardIterator::Encoded(w) => w.num_estimated(),
            WildcardIterator::Raw(w) => w.num_estimated(),
        }
    }

    #[inline(always)]
    fn last_doc_id(&self) -> DocId {
        match self {
            WildcardIterator::Encoded(w) => w.last_doc_id(),
            WildcardIterator::Raw(w) => w.last_doc_id(),
        }
    }

    #[inline(always)]
    fn at_eof(&self) -> bool {
        match self {
            WildcardIterator::Encoded(w) => w.at_eof(),
            WildcardIterator::Raw(w) => w.at_eof(),
        }
    }

    #[inline(always)]
    fn type_(&self) -> IteratorType {
        IteratorType::InvIdxWildcard
    }

    fn intersection_sort_weight(&self, _prioritize_union_children: bool) -> f64 {
        1.0
    }
}

impl profile_print::ProfilePrint for WildcardIterator<'_> {
    fn print_profile(
        &self,
        map: &mut redis_reply::MapBuilder<'_>,
        ctx: &mut profile_print::ProfilePrintCtx<'_>,
    ) {
        match self {
            WildcardIterator::Encoded(w) => w.print_profile(map, ctx),
            WildcardIterator::Raw(w) => w.print_profile(map, ctx),
        }
    }
}

/// Suspended counterpart — required by the `RQESuspendedIterator: ProfilePrint`
/// supertrait so `FT.PROFILE` can print a suspended tree.
impl profile_print::ProfilePrint for WildcardIteratorSuspended<'_> {
    fn print_profile(
        &self,
        map: &mut redis_reply::MapBuilder<'_>,
        ctx: &mut profile_print::ProfilePrintCtx<'_>,
    ) {
        match self {
            WildcardIteratorSuspended::Encoded(w) => w.print_profile(map, ctx),
            WildcardIteratorSuspended::Raw(w) => w.print_profile(map, ctx),
        }
    }
}

/// Creates a new wildcard inverted index iterator for querying all existing documents.
///
/// # Parameters
///
/// * `idx` - Pointer to the existingDocs inverted index (DocIdsOnly or RawDocIdsOnly encoded).
/// * `sctx` - Pointer to the Redis search context.
/// * `weight` - Weight to apply to all results.
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
///    mechanism detects when the index has been replaced via `spec.existingDocs` pointer
///    comparison.
/// 3. `sctx` must be a valid pointer to a `RedisSearchCtx` and cannot be NULL.
/// 4. `sctx` and `sctx.spec` must remain valid for the lifetime of the returned iterator.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NewInvIndIterator_WildcardQuery(
    idx: *const ffi::InvertedIndex,
    sctx: *const ffi::RedisSearchCtx,
    weight: f64,
) -> *mut ffi::QueryIterator {
    debug_assert!(!idx.is_null(), "idx must not be null");

    // Cast to the FFI wrapper enum which handles type dispatch
    let idx_ffi: *const inverted_index_ffi::InvertedIndex = idx.cast();
    // SAFETY: 1. guarantees idx is valid and non-null
    let ii_ref = unsafe { &*idx_ffi };

    debug_assert!(!sctx.is_null(), "sctx must not be null");

    // Create the appropriate wildcard iterator variant based on the encoding type
    let iterator = match ii_ref {
        inverted_index_ffi::InvertedIndex::DocIdsOnly(ii) => {
            WildcardIterator::Encoded(Wildcard::new(ii.reader(), weight))
        }
        inverted_index_ffi::InvertedIndex::RawDocIdsOnly(ii) => {
            WildcardIterator::Raw(Wildcard::new(ii.reader(), weight))
        }
        _ => panic!(
            "Wildcard iterator requires a DocIdsOnly or RawDocIdsOnly inverted index, got: {:?}",
            std::mem::discriminant(ii_ref)
        ),
    };

    RQEIteratorWrapper::boxed_new(iterator)
}
