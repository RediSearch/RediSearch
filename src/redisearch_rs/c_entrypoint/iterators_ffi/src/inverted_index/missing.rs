/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{fmt::Debug, ptr::NonNull};

use ffi::t_docId;
use field::{FieldExpirationPredicate, FieldFilterContext, FieldMaskOrIndex};
use index_result::RSIndexResult;
use inverted_index::{IndexReader, doc_ids_only::DocIdsOnly, raw_doc_ids_only::RawDocIdsOnly};
use rqe_core::FieldIndex;
use rqe_iterator_type::IteratorType;
use rqe_iterators::{
    FieldExpirationChecker, RQEIteratorBoxed, RQEIteratorError, RQESuspendedIterator, ResumeOutcome,
    interop::RQEIteratorWrapper, inverted_index::Missing,
};

/// Suspended counterpart of [`MissingIterator`] — produced by
/// [`RQEIteratorBoxed::suspend`] and consumed by [`RQESuspendedIterator::resume`].
pub(super) enum MissingIteratorSuspended<'query> {
    Encoded(
        <Missing<'query, DocIdsOnly, FieldExpirationChecker> as RQEIteratorBoxed<'query>>::Suspended,
    ),
    Raw(
        <Missing<'query, RawDocIdsOnly, FieldExpirationChecker> as RQEIteratorBoxed<'query>>::Suspended,
    ),
}

impl<'index> RQEIteratorBoxed<'index> for MissingIterator<'index> {
    type Suspended = MissingIteratorSuspended<'index>;

    fn suspend(self: Box<Self>) -> Box<Self::Suspended> {
        match *self {
            MissingIterator::Encoded(m) => Box::new(MissingIteratorSuspended::Encoded(
                *<Missing<'index, _, _> as RQEIteratorBoxed<'index>>::suspend(Box::new(m)),
            )),
            MissingIterator::Raw(m) => Box::new(MissingIteratorSuspended::Raw(
                *<Missing<'index, _, _> as RQEIteratorBoxed<'index>>::suspend(Box::new(m)),
            )),
        }
    }
}

impl<'query> RQESuspendedIterator<'query> for MissingIteratorSuspended<'query> {
    type Resumed<'a>
        = MissingIterator<'a>
    where
        'query: 'a;

    fn resume<'a>(
        self: Box<Self>,
        guard: &index_spec::IndexSpecReadGuard<'a>,
    ) -> Result<ResumeOutcome<Box<Self::Resumed<'a>>>, RQEIteratorError>
    where
        'query: 'a,
    {
        // Forward the inner variant's outcome, reconstructing the concrete
        // `MissingIterator` enum and preserving the `Ok`/`Moved`/`Aborted` status.
        let (variant, moved) = match *self {
            MissingIteratorSuspended::Encoded(s) => {
                match <_ as RQESuspendedIterator>::resume(Box::new(s), guard)? {
                    ResumeOutcome::Aborted => return Ok(ResumeOutcome::Aborted),
                    ResumeOutcome::Ok(resumed) => (MissingIterator::Encoded(*resumed), false),
                    ResumeOutcome::Moved(resumed) => (MissingIterator::Encoded(*resumed), true),
                }
            }
            MissingIteratorSuspended::Raw(s) => {
                match <_ as RQESuspendedIterator>::resume(Box::new(s), guard)? {
                    ResumeOutcome::Aborted => return Ok(ResumeOutcome::Aborted),
                    ResumeOutcome::Ok(resumed) => (MissingIterator::Raw(*resumed), false),
                    ResumeOutcome::Moved(resumed) => (MissingIterator::Raw(*resumed), true),
                }
            }
        };
        let active = Box::new(variant);
        Ok(if moved {
            ResumeOutcome::Moved(active)
        } else {
            ResumeOutcome::Ok(active)
        })
    }

    fn last_doc_id(&self) -> t_docId {
        match self {
            MissingIteratorSuspended::Encoded(s) => s.last_doc_id(),
            MissingIteratorSuspended::Raw(s) => s.last_doc_id(),
        }
    }

    fn num_estimated(&self) -> usize {
        match self {
            MissingIteratorSuspended::Encoded(s) => s.num_estimated(),
            MissingIteratorSuspended::Raw(s) => s.num_estimated(),
        }
    }
}

/// Wrapper around different II missing iterator encoding types to avoid generics in FFI code.
///
/// Handles both the standard variable-length encoding ([`DocIdsOnly`]) and the
/// fixed 4-byte raw encoding ([`RawDocIdsOnly`]).
pub(super) enum MissingIterator<'index> {
    Encoded(Missing<'index, DocIdsOnly, FieldExpirationChecker>),
    Raw(Missing<'index, RawDocIdsOnly, FieldExpirationChecker>),
}

impl Debug for MissingIterator<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let variant = match self {
            MissingIterator::Encoded(_) => "Encoded",
            MissingIterator::Raw(_) => "Raw",
        };
        write!(f, "MissingIterator({variant})")
    }
}

impl MissingIterator<'_> {
    /// Get the flags from the underlying reader.
    pub(super) fn flags(&self) -> ffi::IndexFlags {
        match self {
            MissingIterator::Encoded(m) => m.reader().flags(),
            MissingIterator::Raw(m) => m.reader().flags(),
        }
    }

    pub(super) fn field_name(&self) -> (*const std::ffi::c_char, usize) {
        match self {
            MissingIterator::Encoded(m) => m.field_name(),
            MissingIterator::Raw(m) => m.field_name(),
        }
    }
}

/// Macro to dispatch RQEIterator methods across all [`MissingIterator`] variants.
macro_rules! dispatch {
    ($self:expr, $method:ident $(, $arg:expr)*) => {
        match $self {
            MissingIterator::Encoded(m) => m.$method($($arg),*),
            MissingIterator::Raw(m) => m.$method($($arg),*),
        }
    };
}

impl rqe_iterators::profile_print::ProfilePrint for MissingIterator<'_> {
    fn print_profile(
        &self,
        map: &mut redis_reply::MapBuilder<'_>,
        ctx: &mut rqe_iterators::profile_print::ProfilePrintCtx<'_>,
    ) {
        match self {
            MissingIterator::Encoded(m) => m.print_profile(map, ctx),
            MissingIterator::Raw(m) => m.print_profile(map, ctx),
        }
    }
}

impl<'index> rqe_iterators::RQEIterator<'index> for MissingIterator<'index> {
    #[inline(always)]
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        dispatch!(self, current)
    }

    #[inline(always)]
    fn read(
        &mut self,
    ) -> Result<Option<&mut RSIndexResult<'index>>, rqe_iterators::RQEIteratorError> {
        dispatch!(self, read)
    }

    #[inline(always)]
    fn skip_to(
        &mut self,
        doc_id: t_docId,
    ) -> Result<Option<rqe_iterators::SkipToOutcome<'_, 'index>>, rqe_iterators::RQEIteratorError>
    {
        dispatch!(self, skip_to, doc_id)
    }

    #[inline(always)]
    fn rewind(&mut self) {
        dispatch!(self, rewind)
    }

    #[inline(always)]
    fn num_estimated(&self) -> usize {
        dispatch!(self, num_estimated)
    }

    #[inline(always)]
    fn last_doc_id(&self) -> t_docId {
        dispatch!(self, last_doc_id)
    }

    #[inline(always)]
    fn at_eof(&self) -> bool {
        dispatch!(self, at_eof)
    }

    #[inline(always)]
    fn revalidate(
        &mut self,
        spec: &index_spec::IndexSpecReadGuard,
    ) -> Result<rqe_iterators::RQEValidateStatus<'_, 'index>, rqe_iterators::RQEIteratorError> {
        dispatch!(self, revalidate, spec)
    }

    #[inline(always)]
    fn type_(&self) -> IteratorType {
        IteratorType::InvIdxMissing
    }

    fn intersection_sort_weight(&self, _prioritize_union_children: bool) -> f64 {
        1.0
    }
}

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

    // Build the expiration checker for the Missing predicate
    let filter_ctx = FieldFilterContext {
        field: FieldMaskOrIndex::Index(field_index),
        predicate: FieldExpirationPredicate::Missing,
    };

    // Create the appropriate missing iterator variant based on encoding type.
    let iterator = match ii_ref {
        inverted_index_ffi::InvertedIndex::DocIdsOnly(ii) => {
            let reader = ii.reader();
            // SAFETY: 3.-6. guarantee validity for the iterator's lifetime.
            let checker =
                unsafe { FieldExpirationChecker::new(sctx_nn, filter_ctx, reader.flags()) };
            // SAFETY: 3.-6. guarantee validity for the iterator's lifetime.
            MissingIterator::Encoded(unsafe { Missing::new(reader, sctx_nn, field_index, checker) })
        }
        inverted_index_ffi::InvertedIndex::RawDocIdsOnly(ii) => {
            let reader = ii.reader();
            // SAFETY: 3.-6. guarantee validity for the iterator's lifetime.
            let checker =
                unsafe { FieldExpirationChecker::new(sctx_nn, filter_ctx, reader.flags()) };
            // SAFETY: 3.-6. guarantee validity for the iterator's lifetime.
            MissingIterator::Raw(unsafe { Missing::new(reader, sctx_nn, field_index, checker) })
        }
        _ => panic!(
            "Missing iterator requires a DocIdsOnly or RawDocIdsOnly inverted index, got: {:?}",
            std::mem::discriminant(ii_ref)
        ),
    };

    RQEIteratorWrapper::boxed_new(iterator)
}

/// Gets the field name used by a missing-field inverted index iterator.
///
/// # Safety
///
/// 1. `it` must be a valid non-NULL pointer to a `QueryIterator`.
/// 2. `it` must have type [`IteratorType::InvIdxMissing`].
/// 3. `out_len` must be a valid writable pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn InvIndMissingIterator_GetFieldName(
    it: *const ffi::QueryIterator,
    out_len: *mut usize,
) -> *const std::ffi::c_char {
    debug_assert!(!it.is_null(), "it must not be null");
    debug_assert!(!out_len.is_null(), "out_len must not be null");

    // SAFETY: 1. and 2. guarantee the iterator is a valid missing iterator wrapper.
    let wrapper =
        unsafe { RQEIteratorWrapper::<MissingIterator<'static>>::ref_from_header_ptr(it.cast()) };
    let (field_name, field_name_len) = wrapper.inner().field_name();
    // SAFETY: 3. guarantees `out_len` is valid and writable.
    unsafe { *out_len = field_name_len };
    field_name
}
