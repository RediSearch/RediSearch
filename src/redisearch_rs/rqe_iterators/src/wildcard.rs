/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Supporting types for [`Wildcard`].

use std::ptr::{self, NonNull};

use ffi::{
    RS_FIELDMASK_ALL, ValidateStatus, ValidateStatus_VALIDATE_ABORTED, ValidateStatus_VALIDATE_OK,
    t_docId,
};
use index_result::{RSIndexResult, RawIndexResult};
use index_spec::IndexSpecReadGuard;
use inverted_index::codec::{doc_ids_only::DocIdsOnly, raw_doc_ids_only::RawDocIdsOnly};
use inverted_index::{DocIdsDecoder, opaque};
use ref_mode::{Active, Ref, Suspended};

use crate::IteratorType;
use crate::{
    Empty, RQEIterator, RQEIteratorBoxed, RQEIteratorError, RQESuspendedIterator, SEARCH_ENTERPRISE_ITERATORS, SkipToOutcome,
};

/// An iterator that yields all ids within a given range, from 1 to max id
/// (inclusive) in an index.
///
/// Parameterised over a [`Ref`] mode — see [`Wildcard`] for the [`Active`]
/// instantiation that implements [`RQEIterator`]. The struct owns no
/// references into the index (it's a pure counter); the only `Rf`-dependent
/// field is `result`.
#[repr(C)]
pub struct RawWildcard<Rf: Ref> {
    // Supposed to be the max id in the index
    top_id: t_docId,

    /// A reusable result object to avoid allocations on each `read` call.
    result: RawIndexResult<Rf>,
}

/// Alias for an [`Active`] [`RawWildcard`] — the only instantiation with an
/// [`RQEIterator`] impl today.
pub type Wildcard<'index> = RawWildcard<Active<'index>>;

impl Wildcard<'_> {
    pub fn new(top_id: t_docId, weight: f64) -> Self {
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
        doc_id: t_docId,
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

    fn last_doc_id(&self) -> t_docId {
        self.result.doc_id
    }

    fn at_eof(&self) -> bool {
        self.result.doc_id >= self.top_id
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
    type Suspended = RawWildcard<Suspended>;

    fn suspend(self: Box<Self>) -> Box<Self::Suspended> {
        let raw = Box::into_raw(self);
        // SAFETY: `RawWildcard` is `#[repr(C)]` with the only `Rf`-dependent
        // field being `result: RawIndexResult<Rf>`, layout-compatible across
        // `Rf` (see [`crate::inverted_index::Wildcard::suspend`] for the
        // same argument). Box::from_raw reuses the same heap allocation.
        unsafe { Box::from_raw(raw as *mut RawWildcard<Suspended>) }
    }
}

impl RQESuspendedIterator for RawWildcard<Suspended> {
    type Resumed<'a> = Wildcard<'a>;

    fn resume<'a>(
        self: Box<Self>,
        _guard: &'a IndexSpecReadGuard<'a>,
    ) -> (Box<Self::Resumed<'a>>, ValidateStatus) {
        let raw = Box::into_raw(self);
        // SAFETY: layout-compatible — see `suspend`. The top-level wildcard
        // owns no references into the index (it's just a counter), so there
        // is no state to refresh.
        let active = unsafe { Box::from_raw(raw as *mut Wildcard<'a>) };
        (active, ValidateStatus_VALIDATE_OK)
    }

    fn last_doc_id(&self) -> t_docId {
        self.result.doc_id
    }

    fn num_estimated(&self) -> usize {
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

impl<'index> RQEIterator<'index> for Box<dyn WildcardIterator<'index> + 'index> {
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        (**self).current()
    }

    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        (**self).read()
    }

    fn skip_to(
        &mut self,
        doc_id: t_docId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        (**self).skip_to(doc_id)
    }

    fn rewind(&mut self) {
        (**self).rewind()
    }

    fn num_estimated(&self) -> usize {
        (**self).num_estimated()
    }

    fn last_doc_id(&self) -> t_docId {
        (**self).last_doc_id()
    }

    fn at_eof(&self) -> bool {
        (**self).at_eof()
    }

    #[inline(always)]
    fn type_(&self) -> IteratorType {
        (**self).type_()
    }

    fn as_c_iterator(&self) -> Option<&crate::c2rust::CRQEIterator> {
        (**self).as_c_iterator()
    }

    fn intersection_sort_weight(&self, prioritize_union_children: bool) -> f64 {
        (**self).intersection_sort_weight(prioritize_union_children)
    }
}

impl<'index> WildcardIterator<'index> for Box<dyn WildcardIterator<'index> + 'index> {}

/// The result of [`new_wildcard_iterator`], representing the different kinds of
/// wildcard iterators that can be created depending on the index configuration.
///
/// `#[repr(C, u8)]` matches [`NewWildcardSuspended`] so that
/// suspend/resume use whole-`Box` casts that preserve the heap
/// allocation across the cycle.
#[repr(C, u8)]
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
///
/// `#[repr(C, u8)]` so the layout matches [`OptimizedWildcardSuspended`] —
/// suspend/resume use whole-`Box` pointer casts that preserve the heap
/// allocation across the cycle. See
/// [`crate::inverted_index::Wildcard`] for the same argument at the leaf level.
#[repr(C, u8)]
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
        doc_id: t_docId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        delegate_rqe_iterator!(self, skip_to, doc_id)
    }

    fn rewind(&mut self) {
        delegate_rqe_iterator!(self, rewind)
    }

    fn num_estimated(&self) -> usize {
        delegate_rqe_iterator!(self, num_estimated)
    }

    fn last_doc_id(&self) -> t_docId {
        delegate_rqe_iterator!(self, last_doc_id)
    }

    fn at_eof(&self) -> bool {
        delegate_rqe_iterator!(self, at_eof)
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

/// Parallel `'static`-typed counterpart of [`OptimizedWildcard`] used as
/// its `RQEIteratorBoxed::Suspended` type. Each variant holds the
/// `Suspended` form of the corresponding inverted-index wildcard reader.
///
/// `#[repr(C, u8)]` matches [`OptimizedWildcard`]'s layout.
#[repr(C, u8)]
pub enum OptimizedWildcardSuspended {
    /// Suspended counterpart of [`OptimizedWildcard::DocIdsOnly`].
    DocIdsOnly(crate::inverted_index::RawWildcard<Suspended, DocIdsOnly>),
    /// Suspended counterpart of [`OptimizedWildcard::RawDocIdsOnly`].
    RawDocIdsOnly(crate::inverted_index::RawWildcard<Suspended, RawDocIdsOnly>),
}

impl<'index> RQEIteratorBoxed<'index> for OptimizedWildcard<'index> {
    type Suspended = OptimizedWildcardSuspended;

    fn suspend(self: Box<Self>) -> Box<Self::Suspended> {
        let raw = Box::into_raw(self);
        // SAFETY: both enums are `#[repr(C)]`; corresponding variants hold
        // `RawWildcard<Active<'index>, E>` / `RawWildcard<Suspended, E>`,
        // which are layout-compatible by the [`RQEIteratorBoxed`]
        // contract on `inverted_index::Wildcard`. Box::from_raw reuses the
        // heap allocation.
        unsafe { Box::from_raw(raw as *mut OptimizedWildcardSuspended) }
    }
}

/// Local 3-state outcome carrying the work done while still on the
/// suspended form into the active form for the optional reseek dispatch.
enum OptimizedResumeOutcome {
    Abort,
    Ok,
    NeedsReseek { last_doc_id: t_docId },
}

impl RQESuspendedIterator for OptimizedWildcardSuspended {
    type Resumed<'a> = OptimizedWildcard<'a>;

    fn resume<'a>(
        mut self: Box<Self>,
        spec: &'a IndexSpecReadGuard<'a>,
    ) -> (Box<Self::Resumed<'a>>, ValidateStatus) {
        // Step 1: should_abort + refresh_pointers on the suspended
        // variant (same pattern as `TagIteratorSuspended::resume` in
        // iterators_ffi). Preserves the heap allocation across the
        // cycle so external borrows into the iterator's interior
        // (FFI wrapper's `header.current`) stay valid.
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

        // Step 2: whole-box cast.
        let raw = Box::into_raw(self);
        // SAFETY: layout-compatible — see `suspend`.
        let mut active = unsafe { Box::from_raw(raw as *mut OptimizedWildcard<'a>) };

        let status = match outcome {
            OptimizedResumeOutcome::Abort => ValidateStatus_VALIDATE_ABORTED,
            OptimizedResumeOutcome::Ok => ValidateStatus_VALIDATE_OK,
            OptimizedResumeOutcome::NeedsReseek { last_doc_id } => match &mut *active {
                OptimizedWildcard::DocIdsOnly(w) => w.reseek_after_refresh(last_doc_id),
                OptimizedWildcard::RawDocIdsOnly(w) => w.reseek_after_refresh(last_doc_id),
            },
        };
        (active, status)
    }

    fn last_doc_id(&self) -> t_docId {
        match self {
            OptimizedWildcardSuspended::DocIdsOnly(it) => RQESuspendedIterator::last_doc_id(it),
            OptimizedWildcardSuspended::RawDocIdsOnly(it) => RQESuspendedIterator::last_doc_id(it),
        }
    }

    fn num_estimated(&self) -> usize {
        match self {
            OptimizedWildcardSuspended::DocIdsOnly(it) => RQESuspendedIterator::num_estimated(it),
            OptimizedWildcardSuspended::RawDocIdsOnly(it) => RQESuspendedIterator::num_estimated(it),
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
        doc_id: t_docId,
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

    fn last_doc_id(&self) -> t_docId {
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

    #[inline(always)]
    fn type_(&self) -> IteratorType {
        delegate_wildcard_iterator!(self, type_)
    }

    fn intersection_sort_weight(&self, prioritize_union_children: bool) -> f64 {
        delegate_wildcard_iterator!(self, intersection_sort_weight, prioritize_union_children)
    }
}

impl<'index> WildcardIterator<'index> for NewWildcardIterator<'index> {}

/// Parallel `'static`-typed counterpart of [`NewWildcardIterator`] used as
/// its `RQEIteratorBoxed::Suspended` type. Each variant holds the
/// `Suspended` form of the corresponding active variant.
///
/// `#[repr(C, u8)]` matches [`NewWildcardIterator`]'s layout.
#[repr(C, u8)]
pub enum NewWildcardSuspended {
    /// Suspended counterpart of [`NewWildcardIterator::NotOptimized`].
    NotOptimized(RawWildcard<Suspended>),
    /// Suspended counterpart of [`NewWildcardIterator::Optimized`].
    Optimized(OptimizedWildcardSuspended),
    /// Suspended counterpart of [`NewWildcardIterator::Empty`].
    Empty(Empty),
    /// Suspended counterpart of [`NewWildcardIterator::Disk`].
    Disk(DiskWildcardSuspended),
}

impl<'index> RQEIteratorBoxed<'index> for NewWildcardIterator<'index> {
    type Suspended = NewWildcardSuspended;

    fn suspend(self: Box<Self>) -> Box<Self::Suspended> {
        let raw = Box::into_raw(self);
        // SAFETY: layout-compatible enums (`#[repr(C, u8)]`).
        unsafe { Box::from_raw(raw as *mut NewWildcardSuspended) }
    }
}

impl RQESuspendedIterator for NewWildcardSuspended {
    type Resumed<'a> = NewWildcardIterator<'a>;

    fn resume<'a>(
        self: Box<Self>,
        guard: &'a IndexSpecReadGuard<'a>,
    ) -> (Box<Self::Resumed<'a>>, ValidateStatus) {
        // Preserve the outer Box's heap allocation across the cycle.
        // The FFI wrapper's `header.current` is a borrowed pointer
        // into the inner iterator's `result.current` slot, whose
        // address is at a fixed offset within this outer Box (since
        // the variant payload sits inline per `#[repr(C, u8)]`).
        // Re-allocating the outer Box would shift that offset and
        // leave `header.current` dangling.
        //
        // Strategy: `ptr::read` the suspended value out of the outer
        // slot, drive the per-variant resume, then `ptr::write` the
        // active value back into the same slot. `Box::from_raw` at
        // the end reuses the same heap allocation.
        let raw = Box::into_raw(self);
        // SAFETY: `raw` came from `Box::into_raw` (valid pointer,
        // exclusive ownership). The bytes are a valid
        // `NewWildcardSuspended`. `ptr::read` moves them out, leaving
        // the slot's bytes typed-but-moved-from; we overwrite via
        // `ptr::write` before reconstituting the Box.
        let suspended_val = unsafe { ptr::read(raw) };

        let (active_val, status) = match suspended_val {
            NewWildcardSuspended::NotOptimized(it) => {
                let (active, status) = Box::new(it).resume(guard);
                (NewWildcardIterator::NotOptimized(*active), status)
            }
            NewWildcardSuspended::Optimized(it) => {
                let (active, status) = Box::new(it).resume(guard);
                (NewWildcardIterator::Optimized(*active), status)
            }
            NewWildcardSuspended::Empty(it) => {
                let (active, status) = Box::new(it).resume(guard);
                (NewWildcardIterator::Empty(*active), status)
            }
            NewWildcardSuspended::Disk(it) => {
                let (active, status) = Box::new(it).resume(guard);
                (NewWildcardIterator::Disk(*active), status)
            }
        };

        let active_raw = raw as *mut NewWildcardIterator<'a>;
        // SAFETY: `active_raw` is the same heap allocation as `raw`,
        // retyped as `NewWildcardIterator<'a>` (layout-compatible by
        // `#[repr(C, u8)]`). The slot is uninitialised after the
        // earlier `ptr::read`; writing a valid `NewWildcardIterator<'a>`
        // reinitialises it.
        unsafe { ptr::write(active_raw, active_val) };
        // SAFETY: outer Box reconstituted on the same heap allocation.
        let active = unsafe { Box::from_raw(active_raw) };
        (active, status)
    }

    fn last_doc_id(&self) -> t_docId {
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
/// If the enterprise iterator cannot be created, this function logs a warning
/// and falls back to an empty iterator.
///
/// # Safety
///
/// 1. `disk_spec` must reference a valid [`RedisSearchDiskIndexSpec`](ffi::RedisSearchDiskIndexSpec)
///    that remains valid for `'index`.
/// 2. [`SEARCH_ENTERPRISE_ITERATORS`] must be initialized before calling this function.
pub unsafe fn new_wildcard_iterator_on_disk<'index>(
    disk_spec: &'index mut ffi::RedisSearchDiskIndexSpec,
    weight: f64,
) -> NewWildcardIterator<'index> {
    // SAFETY: Caller guarantees `SEARCH_ENTERPRISE_ITERATORS` is
    // initialized when `spec.diskSpec` is non-null (8).
    let enterprise_iters_api = SEARCH_ENTERPRISE_ITERATORS
        .get()
        .expect("SEARCH_ENTERPRISE_ITERATORS not initialized");
    match enterprise_iters_api.new_wildcard_on_disk(disk_spec, weight) {
        Ok(it) => NewWildcardIterator::Disk(DiskWildcardIterator(it)),
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
        // SAFETY: Caller guarantees all preconditions of
        // `new_wildcard_iterator_on_disk` hold (7, 8).
        return unsafe { new_wildcard_iterator_on_disk(disk_spec, weight) };
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
/// This is a thin wrapper around a [`BoxedRQEIterator`] provided by
/// [`SEARCH_ENTERPRISE_ITERATORS`] that implements [`WildcardIterator`],
/// allowing disk-based wildcard queries to be used interchangeably with
/// in-memory ones.
#[repr(transparent)]
pub struct DiskWildcardIterator<'index>(crate::BoxedRQEIterator<'index>);

impl<'index> RQEIterator<'index> for DiskWildcardIterator<'index> {
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        self.0.current()
    }

    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        self.0.read()
    }

    fn skip_to(
        &mut self,
        doc_id: t_docId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        self.0.skip_to(doc_id)
    }

    fn rewind(&mut self) {
        self.0.rewind()
    }

    fn num_estimated(&self) -> usize {
        self.0.num_estimated()
    }

    fn last_doc_id(&self) -> t_docId {
        self.0.last_doc_id()
    }

    fn at_eof(&self) -> bool {
        self.0.at_eof()
    }

    #[inline(always)]
    fn type_(&self) -> IteratorType {
        self.0.type_()
    }

    fn intersection_sort_weight(&self, prioritize_union_children: bool) -> f64 {
        self.0.intersection_sort_weight(prioritize_union_children)
    }
}

/// [`DiskWildcardIterator`] matches all documents on the disk index.
impl<'index> WildcardIterator<'index> for DiskWildcardIterator<'index> {}

/// `'static`-typed counterpart of [`DiskWildcardIterator`] used as its
/// `RQEIteratorBoxed::Suspended` type.
///
/// A thin wrapper around [`BoxedRQESuspendedIterator`] — the
/// dyn-erased suspended counterpart of the disk iterator. On resume
/// the lifetime is taken from the guard, then the inner is unwrapped
/// and wrapped back into a `BoxedRQEIterator<'a>` to construct the
/// resumed [`DiskWildcardIterator`].
#[repr(transparent)]
pub struct DiskWildcardSuspended(pub(crate) crate::BoxedRQESuspendedIterator);

impl<'index> RQEIteratorBoxed<'index> for DiskWildcardIterator<'index> {
    type Suspended = DiskWildcardSuspended;

    fn suspend(self: Box<Self>) -> Box<Self::Suspended> {
        // Drive the inner dyn-erased iterator's suspend via the
        // `RQEDynIterator` vtable, then wrap the resulting
        // `BoxedRQESuspendedIterator` as `DiskWildcardSuspended`. The
        // explicit `Box::new` here is a small overhead the disk path
        // can afford; see `boxed::suspend_child_slot_in_place` for the
        // no-allocation pattern composites use on their hot children.
        let inner_suspended = self.0.0.suspend();
        Box::new(DiskWildcardSuspended(inner_suspended))
    }
}

impl RQESuspendedIterator for DiskWildcardSuspended {
    type Resumed<'a> = DiskWildcardIterator<'a>;

    fn resume<'a>(
        self: Box<Self>,
        spec: &'a IndexSpecReadGuard<'a>,
    ) -> (Box<Self::Resumed<'a>>, ValidateStatus) {
        // Drive the inner dyn-erased resume; whatever it reports is the
        // disk iterator's validity. The disk crate's iterators are
        // read-only snapshots so the inner currently reports OK; once
        // the disk crate's iterators implement `RQESuspendedIterator`
        // natively this will surface their genuine status.
        let (active_inner, status) = self.0.0.resume(spec);
        (Box::new(DiskWildcardIterator(active_inner)), status)
    }

    fn last_doc_id(&self) -> t_docId {
        // SAFETY: `last_doc_id` reads a cached primitive — it does not
        // dereference any borrowed index pointer. Forwarding to the inner
        // dyn's `RQEIterator::last_doc_id` is therefore sound despite the
        // `'static` lifetime lie.
        crate::boxed::RQEDynSuspendedIterator::last_doc_id(&*self.0.0)
    }

    fn num_estimated(&self) -> usize {
        crate::boxed::RQEDynSuspendedIterator::num_estimated(&*self.0.0)
    }
}

