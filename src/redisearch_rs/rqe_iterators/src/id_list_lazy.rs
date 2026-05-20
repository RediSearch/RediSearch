/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Supporting types for [`IdListLazy`].

use index_result::RSIndexResult;
use index_spec::IndexSpecReadGuard;
use ref_mode::Suspended;
use rqe_core::DocId;

use crate::{
    IteratorType, RQEIterator, RQEIteratorBoxed, RQEIteratorError, RQESuspendedIterator,
    RQEValidateStatus, ResumeOutcome, SkipToOutcome,
    deferred::{ProducedResults, Producer},
    id_list::{IdList, RawIdList},
    profile_print::{ProfilePrint, ProfilePrintCtx},
    utils::OwnedSlice,
};

/// A lazily-populated [`IdList`]: the IDs are not supplied up front but produced on the first
/// [`read`](RQEIterator::read)/[`skip_to`](RQEIterator::skip_to) by a deferred [`Producer`]
/// (see [`crate::deferred`]). This keeps the eager [`IdList`] hot path free of any
/// "produced yet?" branch — that check lives only here.
///
/// Once produced it delegates entirely to the wrapped [`IdList`], so it reports the same
/// [`IteratorType`] and is interchangeable with an eagerly-built one.
///
/// `#[repr(C)]` so it is layout-compatible with its suspended counterpart
/// [`IdListLazySuspended`], enabling the allocation-preserving whole-struct cast
/// in [`RQEIteratorBoxed::suspend`].
#[repr(C)]
pub struct IdListLazy<'index, const SORTED: bool> {
    /// The wrapped ID list, empty until [`producer`](Self::producer) runs.
    inner: IdList<'index, SORTED>,
    /// The deferred producer. Run once on the first read/skip_to (guarded by
    /// [`produced`](Self::produced)) but **retained** so any state it owns lives as long as this
    /// iterator (see [`Producer`]).
    ///
    /// `'static` rather than `'index`: the deferred producer captures only raw C pointers and C
    /// function pointers (never Rust references — see the `NewLazyVectorRangeIterator` FFI
    /// constructor), so it borrows nothing tied to `'index`.
    producer: Producer<'static>,
    /// Whether [`producer`](Self::producer) has already run.
    produced: bool,
    /// Upper-bound estimate reported while the producer is still pending (the real count is
    /// unknown until it runs).
    num_estimated_hint: usize,
}

impl<'index, const SORTED: bool> IdListLazy<'index, SORTED> {
    /// Create a lazy ID list. `result` is the reusable result object the inner list yields,
    /// and `num_estimated_hint` is the estimate reported until `producer` runs.
    pub fn new(
        producer: Producer<'static>,
        num_estimated_hint: usize,
        result: RSIndexResult<'index>,
    ) -> Self {
        Self {
            inner: IdList::with_result(OwnedSlice::default(), result),
            producer,
            produced: false,
            num_estimated_hint,
        }
    }

    /// Run the producer on the first call, populating the inner list; a no-op afterwards. The
    /// producer is retained either way. On timeout the error is propagated and the inner list
    /// stays empty (so the next read reports EOF).
    #[inline]
    fn ensure_produced(&mut self) -> Result<(), RQEIteratorError> {
        if !self.produced {
            self.produced = true;
            let ProducedResults { ids, .. } = (self.producer)()?;
            self.inner.set_ids(ids);
        }
        Ok(())
    }
}

impl<'index, const SORTED: bool> RQEIterator<'index> for IdListLazy<'index, SORTED> {
    #[inline(always)]
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        self.inner.current()
    }

    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        self.ensure_produced()?;
        self.inner.read()
    }

    fn skip_to(
        &mut self,
        doc_id: DocId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        self.ensure_produced()?;
        self.inner.skip_to(doc_id)
    }

    #[inline(always)]
    fn rewind(&mut self) {
        self.inner.rewind();
    }

    #[inline(always)]
    fn num_estimated(&self) -> usize {
        if self.produced {
            self.inner.num_estimated()
        } else {
            self.num_estimated_hint
        }
    }

    #[inline(always)]
    fn last_doc_id(&self) -> DocId {
        self.inner.last_doc_id()
    }

    #[inline(always)]
    fn at_eof(&self) -> bool {
        // Before production the list is not (necessarily) exhausted — report not-at-EOF so
        // callers read it and trigger production.
        self.produced && self.inner.at_eof()
    }

    #[inline(always)]
    fn revalidate(
        &mut self,
        spec: &IndexSpecReadGuard,
    ) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        self.inner.revalidate(spec)
    }

    #[inline(always)]
    fn type_(&self) -> IteratorType {
        self.inner.type_()
    }

    fn intersection_sort_weight(&self, prioritize_union_children: bool) -> f64 {
        self.inner
            .intersection_sort_weight(prioritize_union_children)
    }
}

impl<const SORTED: bool> ProfilePrint for IdListLazy<'_, SORTED> {
    fn print_profile(&self, map: &mut redis_reply::MapBuilder<'_>, ctx: &mut ProfilePrintCtx<'_>) {
        self.inner.print_profile(map, ctx);
    }
}

/// Suspended counterpart of [`IdListLazy`], used as its
/// [`RQEIteratorBoxed::Suspended`] type.
///
/// Layout-compatible with [`IdListLazy`] — both are `#[repr(C)]` with the same
/// field order. The only differing field is the inner list: the active
/// `RawIdList<Active, _>` is layout-compatible with `RawIdList<Suspended, _>`
/// (see [`IdList`]'s [`RQEIteratorBoxed::suspend`]). The `producer`,
/// `produced`, and `num_estimated_hint` fields are identical — in particular the
/// producer is `Producer<'static>` on both sides (it captures only raw C pointers
/// and C function pointers, never Rust references — see the
/// `NewLazyVectorRangeIterator` FFI constructor), so the whole-struct cast touches
/// no lifetime.
#[repr(C)]
pub struct IdListLazySuspended<'query, const SORTED: bool> {
    inner: RawIdList<'query, Suspended, SORTED>,
    producer: Producer<'static>,
    produced: bool,
    num_estimated_hint: usize,
}

impl<'index, const SORTED: bool> RQEIteratorBoxed<'index> for IdListLazy<'index, SORTED> {
    type Suspended = IdListLazySuspended<'index, SORTED>;

    fn suspend(self: Box<Self>) -> Box<Self::Suspended> {
        let raw = Box::into_raw(self);
        // SAFETY: `IdListLazy` and `IdListLazySuspended` are both `#[repr(C)]` with
        // identical field layout: the inner `RawIdList<Active, _>` is layout-compatible
        // with `RawIdList<Suspended, _>` (see `IdList::suspend`), the `producer` field is
        // `Producer<'static>` on both sides, and `produced`/`num_estimated_hint` carry no
        // lifetime. Casting the box's heap pointer preserves the allocation, so any
        // interior pointers into the inner list's result stay valid across the
        // suspend/resume cycle.
        unsafe { Box::from_raw(raw as *mut IdListLazySuspended<'index, SORTED>) }
    }
}

impl<'query, const SORTED: bool> RQESuspendedIterator<'query> for IdListLazySuspended<'query, SORTED> {
    type Resumed<'index>
        = IdListLazy<'index, SORTED>
    where
        'query: 'index;

    fn resume<'index>(
        self: Box<Self>,
        _guard: &IndexSpecReadGuard<'index>,
    ) -> Result<ResumeOutcome<Box<Self::Resumed<'index>>>, RQEIteratorError>
    where
        'query: 'index,
    {
        let raw = Box::into_raw(self);
        // SAFETY: layout-compatible — see `suspend`. The inner list owns all its data
        // (the `OwnedSlice<DocId>`) and the virtual result has no aliased pointers, so
        // promoting it back to `Active<'index>` is unconditionally sound (`'query: 'index`
        // keeps any retained query-pipeline pointers valid). `Box::from_raw` reuses the
        // same heap allocation as `suspend`'s `Box::into_raw`.
        let active = unsafe { Box::from_raw(raw as *mut IdListLazy<'index, SORTED>) };
        Ok(ResumeOutcome::Ok(active))
    }

    fn last_doc_id(&self) -> DocId {
        RawIdList::<'query, Suspended, SORTED>::suspended_result_doc_id(&self.inner)
    }

    fn num_estimated(&self) -> usize {
        // Snapshot from construction; the real count is only known once the producer
        // has run. Acceptable for the FFI display-only consumer.
        self.num_estimated_hint
    }
}
