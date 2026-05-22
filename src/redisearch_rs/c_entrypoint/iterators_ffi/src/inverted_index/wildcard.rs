/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::fmt::Debug;

use index_result::RSIndexResult;
use inverted_index::{doc_ids_only::DocIdsOnly, raw_doc_ids_only::RawDocIdsOnly, t_docId};
use ffi::{
    ValidateStatus, ValidateStatus_VALIDATE_ABORTED, ValidateStatus_VALIDATE_OK,
};
use inverted_index::RefreshOutcome;
use rqe_iterators::{
    IteratorType, RQEIteratorBoxed, RQESuspendedIterator, interop::RQEIteratorWrapper,
    inverted_index::Wildcard,
};

/// Suspended counterpart of [`WildcardIterator`] — produced by
/// [`RQEIteratorBoxed::suspend`] and consumed by [`RQESuspendedIterator::resume`].
///
/// `#[repr(C, u8)]` matches the layout of [`WildcardIterator`] so that
/// [`RQEIteratorBoxed::suspend`] / [`RQESuspendedIterator::resume`]
/// can perform whole-`Box` pointer casts between the two — see
/// [`super::tag::TagIteratorSuspended`] for the heap-address
/// preservation argument.
#[repr(C, u8)]
pub(super) enum WildcardIteratorSuspended {
    Encoded(<Wildcard<'static, DocIdsOnly> as RQEIteratorBoxed<'static>>::Suspended),
    Raw(<Wildcard<'static, RawDocIdsOnly> as RQEIteratorBoxed<'static>>::Suspended),
}

/// Local 3-state outcome carrying the work done while still on the
/// suspended form into the active form for the optional reseek dispatch.
enum WildcardResumeOutcome {
    Abort,
    Ok,
    NeedsReseek { last_doc_id: t_docId },
}

impl<'index> RQEIteratorBoxed<'index> for WildcardIterator<'index> {
    type Suspended = WildcardIteratorSuspended;

    fn suspend(self: Box<Self>) -> Box<Self::Suspended> {
        let raw = Box::into_raw(self);
        // SAFETY: layout-compatible enums (see `WildcardIteratorSuspended`).
        unsafe { Box::from_raw(raw as *mut WildcardIteratorSuspended) }
    }
}

impl RQESuspendedIterator for WildcardIteratorSuspended {
    type Resumed<'a> = WildcardIterator<'a>;

    fn resume<'a>(
        mut self: Box<Self>,
        spec: &'a index_spec::IndexSpecReadGuard<'a>,
    ) -> (Box<Self::Resumed<'a>>, ValidateStatus) {
        // Step 1: should_abort + refresh_pointers on the suspended
        // variant (see `TagIteratorSuspended::resume` for the
        // mode-independence argument).
        let outcome = match &mut *self {
            WildcardIteratorSuspended::Encoded(w) => {
                if w.should_abort(spec) {
                    WildcardResumeOutcome::Abort
                } else {
                    match w.refresh_pointers() {
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
                    match w.refresh_pointers() {
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

        let status = match outcome {
            WildcardResumeOutcome::Abort => ValidateStatus_VALIDATE_ABORTED,
            WildcardResumeOutcome::Ok => ValidateStatus_VALIDATE_OK,
            WildcardResumeOutcome::NeedsReseek { last_doc_id } => match &mut *active {
                WildcardIterator::Encoded(w) => w.reseek_after_refresh(last_doc_id),
                WildcardIterator::Raw(w) => w.reseek_after_refresh(last_doc_id),
            },
        };

        (active, status)
    }

    fn last_doc_id(&self) -> t_docId {
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

impl<'index> rqe_iterators::RQEIterator<'index> for WildcardIterator<'index> {
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
        doc_id: t_docId,
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
    fn last_doc_id(&self) -> t_docId {
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
