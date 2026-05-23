/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Supporting types for [`Wildcard`].

use std::ptr::NonNull;

use index_result::{RSIndexResult, RawIndexResult};
use index_spec::IndexSpecReadGuard;
use inverted_index::codec::{doc_ids_only::DocIdsOnly, raw_doc_ids_only::RawDocIdsOnly};
use inverted_index::{DocIdsDecoder, opaque};
use ref_mode::{Active, Ref, Suspended};

use rqe_core::{DocId, RS_FIELDMASK_ALL};

use crate::{
    Empty, RQEIterator, RQEIteratorBoxed, RQEIteratorError, RQESuspendedIterator,
    RQEValidateStatus, ResumeOutcome, SEARCH_ENTERPRISE_ITERATORS, SkipToOutcome,
    profile_print::{ProfilePrint, ProfilePrintCtx},
};
use crate::{IteratorType, QueryError, RQEIteratorPrintable};

/// An iterator that yields all ids within a given range, from 1 to max id
/// (inclusive) in an index.
///
/// Parameterised over a [`Ref`] mode — see [`Wildcard`] for the [`Active`]
/// instantiation that implements [`RQEIterator`]. The struct owns no
/// references into the index (it's a pure counter); the only `Rf`-dependent
/// field is `result`.
#[repr(C)]
pub struct RawWildcard<'query, Rf: Ref> {
    // Supposed to be the max id in the index
    top_id: DocId,

    /// A reusable result object to avoid allocations on each `read` call.
    result: RawIndexResult<'query, Rf>,
}

/// Alias for an [`Active`] [`RawWildcard`] — the only instantiation with an
/// [`RQEIterator`] impl today.
pub type Wildcard<'index> = RawWildcard<'index, Active<'index>>;

impl Wildcard<'_> {
    pub fn new(top_id: DocId, weight: f64) -> Self {
        Wildcard {
            top_id,
            result: RSIndexResult::build_virt()
                .frequency(1)
                .weight(weight)
                .field_mask(RS_FIELDMASK_ALL)
                .build(),
        }
    }
}

impl<'index> RQEIterator<'index> for Wildcard<'index> {
    #[inline(always)]
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        Some(&mut self.result)
    }

    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        if self.at_eof() {
            return Ok(None);
        }

        self.result.doc_id += 1;
        Ok(Some(&mut self.result))
    }

    fn skip_to(
        &mut self,
        doc_id: DocId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        if self.at_eof() {
            return Ok(None);
        }
        debug_assert!(self.last_doc_id() < doc_id);

        if doc_id > self.top_id {
            // skip beyond range - set to EOF
            self.result.doc_id = self.top_id;
            return Ok(None);
        }

        self.result.doc_id = doc_id;
        Ok(Some(SkipToOutcome::Found(&mut self.result)))
    }

    fn rewind(&mut self) {
        self.result.doc_id = 0;
    }

    // This should always return total results from the iterator, even after some yields.
    fn num_estimated(&self) -> usize {
        self.top_id as usize
    }

    fn last_doc_id(&self) -> DocId {
        self.result.doc_id
    }

    fn at_eof(&self) -> bool {
        self.result.doc_id >= self.top_id
    }

    fn revalidate(
        &mut self,
        _spec: &IndexSpecReadGuard,
    ) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        Ok(RQEValidateStatus::Ok)
    }

    #[inline(always)]
    fn type_(&self) -> IteratorType {
        IteratorType::Wildcard
    }

    fn intersection_sort_weight(&self, _prioritize_union_children: bool) -> f64 {
        1.0
    }
}

impl<'index> RQEIteratorBoxed<'index> for Wildcard<'index> {
    type Suspended = RawWildcard<'index, Suspended>;

    fn suspend(self: Box<Self>) -> Box<Self::Suspended> {
        let raw = Box::into_raw(self);
        // SAFETY: `RawWildcard` is `#[repr(C)]` with the only `Rf`-dependent
        // field being `result: RawIndexResult<Rf>`, layout-compatible across
        // `Rf` (see [`crate::inverted_index::Wildcard::suspend`] for the
        // same argument). Box::from_raw reuses the same heap allocation.
        unsafe { Box::from_raw(raw as *mut RawWildcard<'index, Suspended>) }
    }
}

impl<'query> RQESuspendedIterator<'query> for RawWildcard<'query, Suspended> {
    type Resumed<'a>
        = Wildcard<'a>
    where
        'query: 'a;

    fn resume<'a>(
        self: Box<Self>,
        _guard: &IndexSpecReadGuard<'a>,
    ) -> Result<ResumeOutcome<Box<Self::Resumed<'a>>>, RQEIteratorError>
    where
        'query: 'a,
    {
        let raw = Box::into_raw(self);
        // SAFETY: layout-compatible — see `suspend`. The top-level wildcard
        // owns no references into the index (it's just a counter), so there
        // is no state to refresh.
        let active = unsafe { Box::from_raw(raw as *mut Wildcard<'a>) };
        Ok(ResumeOutcome::Ok(active))
    }

    fn last_doc_id(&self) -> DocId {
        self.result.doc_id
    }

    fn num_estimated(&self) -> usize {
        // Mode-independent — mirrors the active `num_estimated`.
        self.top_id as usize
    }
}
/// A marker trait for iterators that match all documents.
pub trait WildcardIterator<'index>: RQEIterator<'index> {}

/// [`Wildcard`] is obviously a wildcard iterator.
impl<'index> WildcardIterator<'index> for Wildcard<'index> {}

/// [`inverted_index::Wildcard`](crate::inverted_index::Wildcard) is used in the optimized version.
impl<'index, E> WildcardIterator<'index> for crate::inverted_index::Wildcard<'index, E>
where
    E: inverted_index::DecodedBy
        + inverted_index::opaque::OpaqueEncoding<Storage = inverted_index::InvertedIndex<E>>
        + 'index,
    <E as inverted_index::DecodedBy>::Decoder: DocIdsDecoder,
{
}

/// A [`Profile`](crate::profile::Profile) wrapper preserves the wildcard property of its child.
impl<'index, I: WildcardIterator<'index>> WildcardIterator<'index>
    for crate::profile::Profile<'index, I>
{
}

/// A [`CRQEIterator`](crate::c2rust::CRQEIterator) may wrap a wildcard iterator
/// at runtime, but this cannot be verified statically.
/// The caller is responsible for only using this impl when the underlying C
/// iterator is actually a wildcard—mirroring the C code's use of an untyped
/// `QueryIterator*` for the `wcii` field.
impl<'index> WildcardIterator<'index> for crate::c2rust::CRQEIterator {}

impl<'index> WildcardIterator<'index> for Box<dyn WildcardIterator<'index> + 'index> {}

/// The result of [`new_wildcard_iterator`], representing the different kinds of
/// wildcard iterators that can be created depending on the index configuration.
pub enum NewWildcardIterator<'index> {
    /// Non-optimized wildcard: yields all document ids from 1 to `maxDocId`.
    NotOptimized(Wildcard<'index>),
    /// Optimized wildcard: reads from the `existingDocs` inverted index.
    Optimized(OptimizedWildcard<'index>),
    /// Empty wildcard: the index has no documents.
    Empty(Empty),
    /// Disk-backed wildcard: delegates to the enterprise disk index iterator.
    Disk(DiskWildcardIterator<'index>),
}

/// An optimized wildcard iterator over the `existingDocs` inverted index.
///
/// The encoding may be either [`DocIdsOnly`] or [`RawDocIdsOnly`], depending on
/// the index configuration.
pub enum OptimizedWildcard<'index> {
    /// Optimized wildcard with [`DocIdsOnly`] encoding.
    DocIdsOnly(crate::inverted_index::Wildcard<'index, DocIdsOnly>),
    /// Optimized wildcard with [`RawDocIdsOnly`] encoding.
    RawDocIdsOnly(crate::inverted_index::Wildcard<'index, RawDocIdsOnly>),
}

/// Delegates each [`RQEIterator`] method to the active variant.
macro_rules! delegate_rqe_iterator {
    ($self:ident, $method:ident $(, $arg:ident)*) => {
        match $self {
            Self::DocIdsOnly(it) => it.$method($($arg),*),
            Self::RawDocIdsOnly(it) => it.$method($($arg),*),
        }
    };
}

impl<'index> RQEIterator<'index> for OptimizedWildcard<'index> {
    #[inline(always)]
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        delegate_rqe_iterator!(self, current)
    }

    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        delegate_rqe_iterator!(self, read)
    }

    fn skip_to(
        &mut self,
        doc_id: DocId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        delegate_rqe_iterator!(self, skip_to, doc_id)
    }

    fn rewind(&mut self) {
        delegate_rqe_iterator!(self, rewind)
    }

    fn num_estimated(&self) -> usize {
        delegate_rqe_iterator!(self, num_estimated)
    }

    fn last_doc_id(&self) -> DocId {
        delegate_rqe_iterator!(self, last_doc_id)
    }

    fn at_eof(&self) -> bool {
        delegate_rqe_iterator!(self, at_eof)
    }

    fn revalidate(
        &mut self,
        spec: &IndexSpecReadGuard,
    ) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        delegate_rqe_iterator!(self, revalidate, spec)
    }

    #[inline(always)]
    fn type_(&self) -> IteratorType {
        delegate_rqe_iterator!(self, type_)
    }

    fn intersection_sort_weight(&self, prioritize_union_children: bool) -> f64 {
        delegate_rqe_iterator!(self, intersection_sort_weight, prioritize_union_children)
    }
}

impl<'index> WildcardIterator<'index> for OptimizedWildcard<'index> {}

impl crate::profile_print::ProfilePrint for OptimizedWildcard<'_> {
    fn print_profile(
        &self,
        map: &mut redis_reply::MapBuilder<'_>,
        ctx: &mut crate::profile_print::ProfilePrintCtx<'_>,
    ) {
        match self {
            Self::DocIdsOnly(it) => it.print_profile(map, ctx),
            Self::RawDocIdsOnly(it) => it.print_profile(map, ctx),
        }
    }
}

/// [`Suspended`]-mode counterpart of [`OptimizedWildcard`] used as its
/// `RQEIteratorBoxed::Suspended` type. Each variant holds the `Suspended`
/// form of the corresponding inverted-index wildcard reader, retaining the
/// `'query` lifetime so query-attached borrows stay valid across the
/// suspend/resume cycle.
///
/// `#[repr(C, u8)]` matches [`OptimizedWildcard`]'s layout.
#[repr(C, u8)]
pub enum OptimizedWildcardSuspended<'query> {
    /// Suspended counterpart of [`OptimizedWildcard::DocIdsOnly`].
    DocIdsOnly(crate::inverted_index::RawWildcard<'query, Suspended, DocIdsOnly>),
    /// Suspended counterpart of [`OptimizedWildcard::RawDocIdsOnly`].
    RawDocIdsOnly(crate::inverted_index::RawWildcard<'query, Suspended, RawDocIdsOnly>),
}

/// Local 3-state outcome carrying the work done while still on the
/// suspended form into the active form for the optional reseek dispatch.
enum OptimizedResumeOutcome {
    Abort,
    Ok,
    NeedsReseek { last_doc_id: DocId },
}

impl<'index> RQEIteratorBoxed<'index> for OptimizedWildcard<'index> {
    type Suspended = OptimizedWildcardSuspended<'index>;

    fn suspend(self: Box<Self>) -> Box<Self::Suspended> {
        let raw = Box::into_raw(self);
        // SAFETY: both enums are `#[repr(C, u8)]`; corresponding variants
        // hold `RawWildcard<'index, Active<'index>, E>` / `RawWildcard<'index,
        // Suspended, E>`, which are layout-compatible by the [`RQEIteratorBoxed`]
        // contract on `inverted_index::Wildcard`. Box::from_raw reuses the
        // heap allocation.
        unsafe { Box::from_raw(raw as *mut OptimizedWildcardSuspended<'index>) }
    }
}

impl<'query> RQESuspendedIterator<'query> for OptimizedWildcardSuspended<'query> {
    type Resumed<'a>
        = OptimizedWildcard<'a>
    where
        'query: 'a;

    fn resume<'a>(
        mut self: Box<Self>,
        spec: &IndexSpecReadGuard<'a>,
    ) -> Result<ResumeOutcome<Box<Self::Resumed<'a>>>, RQEIteratorError>
    where
        'query: 'a,
    {
        // Step 1: should_abort + refresh_pointers on the suspended
        // variant. Preserves the heap allocation across the cycle so
        // external borrows into the iterator's interior (FFI wrapper's
        // `header.current`) stay valid.
        let outcome = match &mut *self {
            OptimizedWildcardSuspended::DocIdsOnly(w) => {
                if w.should_abort(spec) {
                    OptimizedResumeOutcome::Abort
                } else {
                    match w.refresh_pointers() {
                        inverted_index::RefreshOutcome::Ok => OptimizedResumeOutcome::Ok,
                        inverted_index::RefreshOutcome::NeedsReseek { last_doc_id } => {
                            OptimizedResumeOutcome::NeedsReseek { last_doc_id }
                        }
                    }
                }
            }
            OptimizedWildcardSuspended::RawDocIdsOnly(w) => {
                if w.should_abort(spec) {
                    OptimizedResumeOutcome::Abort
                } else {
                    match w.refresh_pointers() {
                        inverted_index::RefreshOutcome::Ok => OptimizedResumeOutcome::Ok,
                        inverted_index::RefreshOutcome::NeedsReseek { last_doc_id } => {
                            OptimizedResumeOutcome::NeedsReseek { last_doc_id }
                        }
                    }
                }
            }
        };

        // An aborting variant is dropped without promotion to Active —
        // nothing is materialized.
        if let OptimizedResumeOutcome::Abort = outcome {
            return Ok(ResumeOutcome::Aborted);
        }

        // Step 2: whole-box cast.
        let raw = Box::into_raw(self);
        // SAFETY: layout-compatible — see `suspend`.
        let mut active = unsafe { Box::from_raw(raw as *mut OptimizedWildcard<'a>) };

        let moved = match outcome {
            OptimizedResumeOutcome::Abort => unreachable!("aborted above"),
            OptimizedResumeOutcome::Ok => false,
            OptimizedResumeOutcome::NeedsReseek { last_doc_id } => {
                let status = match &mut *active {
                    OptimizedWildcard::DocIdsOnly(w) => w.reseek_after_refresh(last_doc_id),
                    OptimizedWildcard::RawDocIdsOnly(w) => w.reseek_after_refresh(last_doc_id),
                };
                status == ffi::ValidateStatus_VALIDATE_MOVED
            }
        };
        Ok(if moved {
            ResumeOutcome::Moved(active)
        } else {
            ResumeOutcome::Ok(active)
        })
    }

    fn last_doc_id(&self) -> DocId {
        match self {
            OptimizedWildcardSuspended::DocIdsOnly(it) => RQESuspendedIterator::last_doc_id(it),
            OptimizedWildcardSuspended::RawDocIdsOnly(it) => RQESuspendedIterator::last_doc_id(it),
        }
    }

    fn num_estimated(&self) -> usize {
        match self {
            OptimizedWildcardSuspended::DocIdsOnly(it) => RQESuspendedIterator::num_estimated(it),
            OptimizedWildcardSuspended::RawDocIdsOnly(it) => {
                RQESuspendedIterator::num_estimated(it)
            }
        }
    }
}

/// Delegates each [`RQEIterator`] method to the active variant.
macro_rules! delegate_wildcard_iterator {
    ($self:ident, $method:ident $(, $arg:ident)*) => {
        match $self {
            Self::NotOptimized(it) => it.$method($($arg),*),
            Self::Optimized(it) => it.$method($($arg),*),
            Self::Empty(it) => it.$method($($arg),*),
            Self::Disk(it) => it.$method($($arg),*),
        }
    };
}

impl<'index> RQEIterator<'index> for NewWildcardIterator<'index> {
    #[inline(always)]
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        delegate_wildcard_iterator!(self, current)
    }

    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        delegate_wildcard_iterator!(self, read)
    }

    fn skip_to(
        &mut self,
        doc_id: DocId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        delegate_wildcard_iterator!(self, skip_to, doc_id)
    }

    fn rewind(&mut self) {
        delegate_wildcard_iterator!(self, rewind)
    }

    fn num_estimated(&self) -> usize {
        // Disambiguated against `RQESuspendedIterator::num_estimated` for the
        // `Empty` variant (whose Suspended counterpart is `Empty` itself).
        match self {
            Self::NotOptimized(it) => RQEIterator::num_estimated(it),
            Self::Optimized(it) => RQEIterator::num_estimated(it),
            Self::Empty(it) => RQEIterator::num_estimated(it),
            Self::Disk(it) => RQEIterator::num_estimated(it),
        }
    }

    fn last_doc_id(&self) -> DocId {
        // Disambiguated against `RQESuspendedIterator::last_doc_id` for the
        // `Empty` variant (whose Suspended counterpart is `Empty` itself).
        match self {
            Self::NotOptimized(it) => RQEIterator::last_doc_id(it),
            Self::Optimized(it) => RQEIterator::last_doc_id(it),
            Self::Empty(it) => RQEIterator::last_doc_id(it),
            Self::Disk(it) => RQEIterator::last_doc_id(it),
        }
    }

    fn at_eof(&self) -> bool {
        delegate_wildcard_iterator!(self, at_eof)
    }

    fn revalidate(
        &mut self,
        spec: &IndexSpecReadGuard,
    ) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        delegate_wildcard_iterator!(self, revalidate, spec)
    }

    #[inline(always)]
    fn type_(&self) -> IteratorType {
        delegate_wildcard_iterator!(self, type_)
    }

    fn intersection_sort_weight(&self, prioritize_union_children: bool) -> f64 {
        delegate_wildcard_iterator!(self, intersection_sort_weight, prioritize_union_children)
    }
}

impl<'index> WildcardIterator<'index> for NewWildcardIterator<'index> {}

/// [`Suspended`]-mode counterpart of [`NewWildcardIterator`] used as
/// its `RQEIteratorBoxed::Suspended` type. Each variant holds the
/// `Suspended` form of the corresponding active variant, retaining the
/// `'query` lifetime so query-attached borrows stay valid across the
/// suspend/resume cycle.
pub enum NewWildcardSuspended<'query> {
    /// Suspended counterpart of [`NewWildcardIterator::NotOptimized`].
    NotOptimized(RawWildcard<'query, Suspended>),
    /// Suspended counterpart of [`NewWildcardIterator::Optimized`].
    Optimized(OptimizedWildcardSuspended<'query>),
    /// Suspended counterpart of [`NewWildcardIterator::Empty`].
    Empty(Empty),
    /// Suspended counterpart of [`NewWildcardIterator::Disk`].
    Disk(DiskWildcardSuspended),
}

impl<'index> RQEIteratorBoxed<'index> for NewWildcardIterator<'index> {
    type Suspended = NewWildcardSuspended<'index>;

    fn suspend(self: Box<Self>) -> Box<Self::Suspended> {
        match *self {
            NewWildcardIterator::NotOptimized(it) => {
                let suspended = RQEIteratorBoxed::suspend(Box::new(it));
                Box::new(NewWildcardSuspended::NotOptimized(*suspended))
            }
            NewWildcardIterator::Optimized(it) => {
                let suspended = RQEIteratorBoxed::suspend(Box::new(it));
                Box::new(NewWildcardSuspended::Optimized(*suspended))
            }
            NewWildcardIterator::Empty(it) => {
                let suspended = RQEIteratorBoxed::suspend(Box::new(it));
                Box::new(NewWildcardSuspended::Empty(*suspended))
            }
            NewWildcardIterator::Disk(it) => {
                let suspended = RQEIteratorBoxed::suspend(Box::new(it));
                Box::new(NewWildcardSuspended::Disk(*suspended))
            }
        }
    }
}

impl<'query> RQESuspendedIterator<'query> for NewWildcardSuspended<'query> {
    type Resumed<'a>
        = NewWildcardIterator<'a>
    where
        'query: 'a;

    fn resume<'a>(
        self: Box<Self>,
        guard: &IndexSpecReadGuard<'a>,
    ) -> Result<ResumeOutcome<Box<Self::Resumed<'a>>>, RQEIteratorError>
    where
        'query: 'a,
    {
        // Forward the inner variant's outcome: an aborted inner aborts the
        // whole wrapper; otherwise reconstruct the concrete
        // `NewWildcardIterator` and preserve the `Ok`/`Moved` status.
        let (variant, moved) = match *self {
            NewWildcardSuspended::NotOptimized(it) => match Box::new(it).resume(guard)? {
                ResumeOutcome::Aborted => return Ok(ResumeOutcome::Aborted),
                ResumeOutcome::Ok(active) => (NewWildcardIterator::NotOptimized(*active), false),
                ResumeOutcome::Moved(active) => (NewWildcardIterator::NotOptimized(*active), true),
            },
            NewWildcardSuspended::Optimized(it) => match Box::new(it).resume(guard)? {
                ResumeOutcome::Aborted => return Ok(ResumeOutcome::Aborted),
                ResumeOutcome::Ok(active) => (NewWildcardIterator::Optimized(*active), false),
                ResumeOutcome::Moved(active) => (NewWildcardIterator::Optimized(*active), true),
            },
            NewWildcardSuspended::Empty(it) => match Box::new(it).resume(guard)? {
                ResumeOutcome::Aborted => return Ok(ResumeOutcome::Aborted),
                ResumeOutcome::Ok(active) => (NewWildcardIterator::Empty(*active), false),
                ResumeOutcome::Moved(active) => (NewWildcardIterator::Empty(*active), true),
            },
            NewWildcardSuspended::Disk(it) => match Box::new(it).resume(guard)? {
                ResumeOutcome::Aborted => return Ok(ResumeOutcome::Aborted),
                ResumeOutcome::Ok(active) => (NewWildcardIterator::Disk(*active), false),
                ResumeOutcome::Moved(active) => (NewWildcardIterator::Disk(*active), true),
            },
        };
        let active = Box::new(variant);
        Ok(if moved {
            ResumeOutcome::Moved(active)
        } else {
            ResumeOutcome::Ok(active)
        })
    }

    fn last_doc_id(&self) -> DocId {
        match self {
            NewWildcardSuspended::NotOptimized(it) => RQESuspendedIterator::last_doc_id(it),
            NewWildcardSuspended::Optimized(it) => RQESuspendedIterator::last_doc_id(it),
            NewWildcardSuspended::Empty(it) => RQESuspendedIterator::last_doc_id(it),
            NewWildcardSuspended::Disk(it) => RQESuspendedIterator::last_doc_id(it),
        }
    }

    fn num_estimated(&self) -> usize {
        match self {
            NewWildcardSuspended::NotOptimized(it) => RQESuspendedIterator::num_estimated(it),
            NewWildcardSuspended::Optimized(it) => RQESuspendedIterator::num_estimated(it),
            NewWildcardSuspended::Empty(it) => RQESuspendedIterator::num_estimated(it),
            NewWildcardSuspended::Disk(it) => RQESuspendedIterator::num_estimated(it),
        }
    }
}

/// Create a [`WildcardIterator`] for an index whose spec has
/// [`SchemaRule`](ffi::SchemaRule)`.index_all` set.
///
/// When [`spec.existingDocs`](ffi::IndexSpec::existingDocs) is non-null, the returned iterator
/// reads from the existing-documents inverted index (either
/// [`DocIdsOnly`] or [`RawDocIdsOnly`]
/// encoding). When it is null (no documents indexed yet), an [`Empty`] iterator
/// is returned instead.
///
/// # Safety
///
/// 1. `sctx` must point to a valid [`RedisSearchCtx`](ffi::RedisSearchCtx) that
///    remains valid for `'index`.
/// 2. `sctx.spec` must be a non-null pointer to a valid [`IndexSpec`](ffi::IndexSpec) that
///    remains valid for `'index`.
/// 3. `sctx.spec.rule` must be a non-null pointer to a valid [`SchemaRule`](ffi::SchemaRule) with
///    [`index_all`](ffi::SchemaRule::index_all) set to `true`.
/// 4. `sctx.spec.existingDocs`, when non-null, must point to a valid
///    [`opaque::InvertedIndex`] with either
///    [`DocIdsOnly`] or [`RawDocIdsOnly`]
///    encoding.
pub unsafe fn new_wildcard_iterator_optimized<'index>(
    sctx: NonNull<ffi::RedisSearchCtx>,
    weight: f64,
) -> NewWildcardIterator<'index> {
    // SAFETY: Caller guarantees `sctx` points to a valid `RedisSearchCtx` (1).
    let sctx_ref = unsafe { sctx.as_ref() };
    let spec = NonNull::new(sctx_ref.spec).expect("sctx.spec is null");
    // SAFETY: Caller guarantees `sctx.spec` is a valid, non-null pointer (2).
    let spec_ref = unsafe { spec.as_ref() };
    let rule = NonNull::new(spec_ref.rule).expect("sctx.spec.rule is null");
    // SAFETY: Caller guarantees `sctx.spec.rule` is a valid, non-null pointer (3).
    let rule_ref = unsafe { rule.as_ref() };
    debug_assert!(rule_ref.index_all);

    match NonNull::new(spec_ref.existingDocs) {
        Some(existing_docs) => {
            let ii = existing_docs.cast::<opaque::InvertedIndex>();
            // SAFETY: Caller guarantees `existingDocs` points to a valid
            // `opaque::InvertedIndex` with `DocIdsOnly` or `RawDocIdsOnly`
            // encoding (4).
            let ii_ref = unsafe { ii.as_ref() };
            let optimized = match ii_ref {
                opaque::InvertedIndex::DocIdsOnly(ii) => OptimizedWildcard::DocIdsOnly(
                    crate::inverted_index::Wildcard::new(ii.reader(), weight),
                ),
                opaque::InvertedIndex::RawDocIdsOnly(ii) => OptimizedWildcard::RawDocIdsOnly(
                    crate::inverted_index::Wildcard::new(ii.reader(), weight),
                ),
                _ => panic!("spec.existingDocs has the wrong inverted index type: {ii_ref:?}"),
            };
            NewWildcardIterator::Optimized(optimized)
        }
        None => NewWildcardIterator::Empty(Empty),
    }
}

/// Create a [`WildcardIterator`] backed by an on-disk index implementation.
///
/// This delegates to [`SEARCH_ENTERPRISE_ITERATORS`]'s
/// [`new_wildcard_on_disk`](crate::SearchEnterpriseIterators::new_wildcard_on_disk)
/// and wraps the resulting iterator in a [`DiskWildcardIterator`].
///
/// If the enterprise iterator cannot be created, this function populates
/// `status` (when non-null) with the cause and falls back to an empty iterator;
/// the query then aborts with an error rather than returning empty results.
///
/// # Safety
///
/// 1. `disk_spec` must reference a valid [`RedisSearchDiskIndexSpec`](ffi::RedisSearchDiskIndexSpec)
///    that remains valid for `'index`.
/// 2. [`SEARCH_ENTERPRISE_ITERATORS`] must be initialized before calling this function.
/// 3. `snapshot` must be a [`RedisSearchDiskSnapshot`](ffi::RedisSearchDiskSnapshot) handle
///    for `disk_spec` and must remain valid for `'index`.
/// 4. `status`, when non-null, must point to a valid [`QueryError`](ffi::QueryError).
pub unsafe fn new_wildcard_iterator_on_disk<'index>(
    disk_spec: &'index mut ffi::RedisSearchDiskIndexSpec,
    weight: f64,
    snapshot: std::ptr::NonNull<ffi::RedisSearchDiskSnapshot>,
    status: *mut ffi::QueryError,
) -> NewWildcardIterator<'index> {
    // SAFETY: Caller guarantees `SEARCH_ENTERPRISE_ITERATORS` is
    // initialized when `spec.diskSpec` is non-null (8).
    let enterprise_iters_api = SEARCH_ENTERPRISE_ITERATORS
        .get()
        .expect("SEARCH_ENTERPRISE_ITERATORS not initialized");
    // SAFETY: caller guarantees `status`, when non-null, points to a valid `QueryError` (4).
    let status = unsafe { QueryError::from_opaque_mut_ptr(status.cast()) };
    // On failure the enterprise implementation populates `status` with the
    // cause; we just fall back to an empty iterator so the query aborts via the
    // existing `QueryError_HasError` check rather than returning empty results.
    match enterprise_iters_api.new_wildcard_on_disk(disk_spec, weight, snapshot, status) {
        Ok(it) => NewWildcardIterator::Disk(it),
        Err(err) => {
            tracing::warn!(
                "Failed to create a disk wildcard iterator ({err}); falling back to empty iterator."
            );
            NewWildcardIterator::Empty(Empty)
        }
    }
}

/// Create a [`WildcardIterator`] from a query evaluation context.
///
/// There are three possible code paths:
///
/// 1. **Disk index** — when [`spec.diskSpec`](ffi::IndexSpec::diskSpec) is non-null, delegates to
///    [`SEARCH_ENTERPRISE_ITERATORS`]'s [`new_wildcard_on_disk`](crate::SearchEnterpriseIterators::new_wildcard_on_disk)
///    and wraps the result in a [`DiskWildcardIterator`].
/// 2. **[`index_all`](ffi::SchemaRule::index_all) optimized** — when
///    [`SchemaRule`](ffi::SchemaRule)`.index_all` is set, delegates to
///    [`new_wildcard_iterator_optimized`] which reads from the
///    [`existingDocs`](ffi::IndexSpec::existingDocs) inverted index.
/// 3. **Fallback** — creates a simple [`Wildcard`] iterator that yields all
///    document ids up to [`docTable.maxDocId`](ffi::DocTable::maxDocId).
///
/// # Safety
///
/// 1. `query` must point to a valid [`QueryEvalCtx`](ffi::QueryEvalCtx) that
///    remains valid for `'index`.
/// 2. `query.sctx` must be a non-null pointer to a valid
///    [`RedisSearchCtx`](ffi::RedisSearchCtx) that remains valid for `'index`.
/// 3. `query.sctx.spec` must be a non-null pointer to a valid [`IndexSpec`](ffi::IndexSpec) that
///    remains valid for `'index`.
/// 4. `query.sctx.spec.rule`, when non-null, must point to a valid [`SchemaRule`](ffi::SchemaRule).
/// 5. When [`SchemaRule`](ffi::SchemaRule)`.index_all` is true, the preconditions of
///    [`new_wildcard_iterator_optimized`] must also hold.
/// 6. `query.docTable` must be a non-null pointer to a valid [`DocTable`](ffi::DocTable) that
///    remains valid for `'index`.
/// 7. `query.sctx.spec.diskSpec`, when non-null, must point to a valid
///    [`RedisSearchDiskIndexSpec`](ffi::RedisSearchDiskIndexSpec) that remains valid for `'index`.
/// 8. When `query.sctx.spec.diskSpec` is non-null, [`SEARCH_ENTERPRISE_ITERATORS`] must be
///    initialized.
/// 9. When `query.sctx.spec.diskSpec` is non-null, `query.sctx.diskSnapshot` must be a
///    non-null [`RedisSearchDiskSnapshot`](ffi::RedisSearchDiskSnapshot) handle for
///    `query.sctx.spec.diskSpec` and must remain valid for `'index`.
pub unsafe fn new_wildcard_iterator<'index>(
    query: NonNull<ffi::QueryEvalCtx>,
    weight: f64,
) -> NewWildcardIterator<'index> {
    // SAFETY: Caller guarantees `query` points to a valid `QueryEvalCtx` (1).
    let query = unsafe { query.as_ref() };
    let sctx = NonNull::new(query.sctx).expect("query.sctx is null");
    // SAFETY: Caller guarantees `query.sctx` is a valid, non-null pointer (2).
    let sctx_ref = unsafe { sctx.as_ref() };
    // SAFETY: Caller guarantees `query.sctx.spec` is a valid, non-null pointer (3).
    let spec = unsafe { &*sctx_ref.spec };

    if !spec.diskSpec.is_null() {
        // SAFETY: Caller guarantees `spec.diskSpec` is a valid, non-null
        // pointer to a `RedisSearchDiskIndexSpec` that remains valid for
        // `'index` (7).
        let disk_spec = unsafe { &mut *spec.diskSpec };
        let snapshot = NonNull::new(sctx_ref.diskSnapshot)
            .expect("query.sctx.diskSnapshot is null for a disk-backed wildcard query");
        // SAFETY: Caller guarantees all preconditions of
        // `new_wildcard_iterator_on_disk` hold (7, 8, 9); `query.status` is the
        // valid `QueryError` of the evaluating query.
        return unsafe { new_wildcard_iterator_on_disk(disk_spec, weight, snapshot, query.status) };
    }

    let index_all = NonNull::new(spec.rule)
        .map(|rule| {
            // SAFETY: Caller guarantees `spec.rule`, when non-null, points to
            // a valid `SchemaRule` (4).
            let rule_ref = unsafe { rule.as_ref() };
            rule_ref.index_all
        })
        .unwrap_or_default();

    if index_all {
        // SAFETY: Caller guarantees the preconditions of
        // `new_wildcard_iterator_optimized` hold when `rule.index_all` is
        // true (5).
        unsafe { new_wildcard_iterator_optimized(sctx, weight) }
    } else {
        // SAFETY: Caller guarantees `query.docTable` is a valid, non-null
        // pointer (6).
        let doc_table = unsafe { &*query.docTable };
        NewWildcardIterator::NotOptimized(Wildcard::new(doc_table.maxDocId, weight))
    }
}

/// A wildcard iterator backed by an enterprise disk index iterator.
///
/// This is a thin wrapper around a [`Box<dyn RQEIterator>`] provided by
/// [`SEARCH_ENTERPRISE_ITERATORS`] that implements [`WildcardIterator`],
/// allowing disk-based wildcard queries to be used interchangeably with
/// in-memory ones.
pub type DiskWildcardIterator<'index> = Box<dyn RQEIteratorPrintable<'index> + 'index>;

/// [`DiskWildcardIterator`] matches all documents on the disk index.
impl<'index> WildcardIterator<'index> for DiskWildcardIterator<'index> {}

impl ProfilePrint for Wildcard<'_> {
    fn print_profile(&self, map: &mut redis_reply::MapBuilder<'_>, ctx: &mut ProfilePrintCtx<'_>) {
        ctx.print_leaf(c"WILDCARD", map);
    }
}

impl ProfilePrint for NewWildcardIterator<'_> {
    fn print_profile(&self, map: &mut redis_reply::MapBuilder<'_>, ctx: &mut ProfilePrintCtx<'_>) {
        match self {
            Self::NotOptimized(it) => it.print_profile(map, ctx),
            Self::Optimized(it) => it.print_profile(map, ctx),
            Self::Empty(it) => it.print_profile(map, ctx),
            Self::Disk(it) => it.print_profile(map, ctx),
        }
    }
}

/// `'static`-typed counterpart of [`DiskWildcardIterator`] used as its
/// `RQEIteratorBoxed::Suspended` type.
///
/// Wraps a `Box<dyn RQEIterator<'static> + 'static>` — the `'static`
/// here is a **lifetime lie**: the actual borrowed lifetime is `'index`,
/// inherited from the original [`DiskWildcardIterator`]. The lie is
/// closed by the FFI-side discipline: while a `DiskWildcardSuspended`
/// exists, no code dereferences the inner iterator. On [`resume`](RQESuspendedIterator::resume) the
/// lifetime contracts back to the guard's lifetime `'a` (which the
/// caller proves is still valid for the underlying index).
#[repr(transparent)]
pub struct DiskWildcardSuspended(pub(crate) Box<dyn RQEIterator<'static> + 'static>);

impl<'index> RQEIteratorBoxed<'index> for DiskWildcardIterator<'index> {
    type Suspended = DiskWildcardSuspended;

    fn suspend(self: Box<Self>) -> Box<Self::Suspended> {
        let raw = Box::into_raw(self);
        // SAFETY: `DiskWildcardIterator<'index>` is `#[repr(transparent)]`
        // over `Box<dyn RQEIterator<'index> + 'index>`, and
        // `DiskWildcardSuspended` is `#[repr(transparent)]` over
        // `Box<dyn RQEIterator<'static> + 'static>`. The two are byte-
        // identical (a `Box` of a trait object is two pointers regardless
        // of lifetime). Lifetime-extending the inner trait object from
        // `'index` to `'static` is a lie that is closed at resume time;
        // the suspended value is opaque (no read/skip path).
        unsafe { Box::from_raw(raw as *mut DiskWildcardSuspended) }
    }
}

impl<'query> RQESuspendedIterator<'query> for DiskWildcardSuspended {
    type Resumed<'a>
        = DiskWildcardIterator<'a>
    where
        'query: 'a;

    fn resume<'a>(
        self: Box<Self>,
        spec: &IndexSpecReadGuard<'a>,
    ) -> Result<ResumeOutcome<Box<Self::Resumed<'a>>>, RQEIteratorError>
    where
        'query: 'a,
    {
        let raw = Box::into_raw(self);
        // SAFETY: contract the lifetime back from the suspended `'static`
        // to the caller-provided `'a`. The caller's read lock on `spec`
        // witnesses that the underlying index data is dereferenceable at
        // `'a`. Box::from_raw reuses the same heap allocation.
        let mut active = unsafe { Box::from_raw(raw as *mut DiskWildcardIterator<'a>) };
        // Drive validity recovery through the inner trait object's
        // `revalidate` callback — the same path the legacy
        // `Suspendable::resume` used. Reduce the borrowing `RQEValidateStatus`
        // to a `Copy` status discriminant first so the mutable borrow of
        // `active` ends before we move it into the outcome; propagate a
        // revalidate error (e.g. timeout) rather than masking it.
        let status = match active.revalidate(spec)? {
            RQEValidateStatus::Ok => ffi::ValidateStatus_VALIDATE_OK,
            RQEValidateStatus::Moved { .. } => ffi::ValidateStatus_VALIDATE_MOVED,
            RQEValidateStatus::Aborted => ffi::ValidateStatus_VALIDATE_ABORTED,
        };
        Ok(match status {
            ffi::ValidateStatus_VALIDATE_OK => ResumeOutcome::Ok(active),
            ffi::ValidateStatus_VALIDATE_MOVED => ResumeOutcome::Moved(active),
            // `Aborted`: `active` is not moved into the outcome, so the inner
            // trait object drops here.
            _ => ResumeOutcome::Aborted,
        })
    }

    fn last_doc_id(&self) -> DocId {
        // SAFETY: `last_doc_id` reads a cached primitive — it does not
        // dereference any borrowed index pointer. Forwarding to the inner
        // dyn's `RQEIterator::last_doc_id` is therefore sound despite the
        // `'static` lifetime lie.
        RQEIterator::last_doc_id(&*self.0)
    }

    fn num_estimated(&self) -> usize {
        // `num_estimated` reads a cached count — it does not dereference any
        // borrowed index pointer, so forwarding to the inner dyn is sound
        // despite the `'static` lifetime lie.
        RQEIterator::num_estimated(&*self.0)
    }
}
