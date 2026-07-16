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
use ref_mode::{Active, Ref, Suspended};
use rqe_core::DocId;

use crate::{
    IdList, IteratorType, RQEIterator, RQEIteratorBoxed, RQEIteratorError, RQESuspendedIterator,
    RQEValidateStatus, ResumeOutcome, SkipToOutcome,
    deferred::{ProducedResults, Producer},
    id_list::{RawIdList, SuspendedIdList},
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
/// `#[repr(C)]` so the active/suspended versions are layout-compatible, enabling the
/// allocation-preserving whole-struct cast
/// in [`RQEIteratorBoxed::suspend`].
#[repr(C)]
pub struct RawIdListLazy<'query, Rf: Ref, const SORTED: bool> {
    /// The wrapped ID list, empty until [`producer`](Self::producer) runs.
    inner: RawIdList<'query, Rf, SORTED>,
    /// The deferred producer. Run once on the first read/skip_to (guarded by
    /// [`produced`](Self::produced)) but **retained** so any state it owns lives as long as this
    /// iterator (see [`Producer`]).
    producer: Producer<'static>,
    /// Whether [`producer`](Self::producer) has already run.
    produced: bool,
    /// Upper-bound estimate reported while the producer is still pending (the real count is
    /// unknown until it runs).
    num_estimated_hint: usize,
}

/// Alias for an [`Active`] [`RawIdListLazy`] — the only instantiation with an
/// [`RQEIterator`] impl today.
pub type IdListLazy<'index, const SORTED: bool> = RawIdListLazy<'index, Active<'index>, SORTED>;
/// Alias for a [`Suspended`] [`RawIdListLazy`].
pub type SuspendedIdListLazy<'query, const SORTED: bool> = RawIdListLazy<'query, Suspended, SORTED>;

// Compile-time proof that the `IdListLazy` and its suspended counterpart are layout-identical.
const _: () = {
    use std::mem::offset_of;

    const SORTED: bool = true;
    type A<'a> = IdListLazy<'a, SORTED>;
    type S<'a> = SuspendedIdListLazy<'a, SORTED>;

    // Every field starts at the same offset.
    assert!(offset_of!(A, inner) == offset_of!(S, inner));
    assert!(offset_of!(A, producer) == offset_of!(S, producer));
    assert!(offset_of!(A, produced) == offset_of!(S, produced));
    assert!(offset_of!(A, num_estimated_hint) == offset_of!(S, num_estimated_hint));

    assert!(size_of::<A>() == size_of::<S>());
    assert!(align_of::<A>() == align_of::<S>());
};

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

impl<'query, Rf: Ref, const SORTED: bool> RawIdListLazy<'query, Rf, SORTED> {
    #[inline(always)]
    fn _num_estimated(&self) -> usize {
        if self.produced {
            self.inner._num_estimated()
        } else {
            self.num_estimated_hint
        }
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
        self._num_estimated()
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

    #[inline(always)]
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

impl<'index, const SORTED: bool> RQEIteratorBoxed<'index> for IdListLazy<'index, SORTED> {
    type Suspended = SuspendedIdListLazy<'index, SORTED>;

    fn suspend(self: Box<Self>) -> Box<Self::Suspended> {
        let active = Box::into_raw(self);

        // Suspend the wrapped id list in place, leaving the outer
        // allocation (and the `Rf`-independent `producer`) untouched.
        //
        // SAFETY: `&raw mut` forms a field pointer without creating a reference,
        // preserving `active`'s provenance over the whole allocation for the outer
        // cast below.
        let inner_slot = unsafe { &raw mut (*active).inner };
        // SAFETY: `suspend_in_place`'s contract is met — `inner_slot` is initialized and
        // unaliased (this function owns `self`). Suspending is a safe widening conversion.
        unsafe { IdList::<'index, SORTED>::suspend_in_place(inner_slot) };

        // SAFETY: `IdListLazy` and `SuspendedIdListLazy` are both `#[repr(C)]` with identical
        // field layout. `Box::from_raw` reuses the same heap allocation as `Box::into_raw`,
        // so the address is unchanged.
        unsafe { Box::from_raw(active.cast::<SuspendedIdListLazy<'index, SORTED>>()) }
    }
}

impl<'query, const SORTED: bool> RQESuspendedIterator<'query>
    for SuspendedIdListLazy<'query, SORTED>
{
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
        let suspended = Box::into_raw(self);

        // Promote the wrapped id list's `result` field in place, leaving the outer
        // allocation (and the `Rf`-independent `producer`) untouched.
        //
        // SAFETY: `&raw mut` forms a field pointer without creating a reference,
        // preserving `suspended`'s provenance over the whole allocation for the
        // outer cast below.
        let inner_slot = unsafe { &raw mut (*suspended).inner };
        // SAFETY: `resume_in_place`'s contract is met — `inner_slot` is initialized and
        // unaliased (this function owns `self`). We reclaim the outer allocation via `suspended`
        // in either branch, so the inner pointer it returns is not needed.
        match unsafe { SuspendedIdList::<'query, SORTED>::resume_in_place::<'index>(inner_slot) } {
            Ok(_) => {
                // SAFETY: layout-compatible — see `suspend`. `inner`'s `result` is now valid at
                // the active type for `'index`; the remaining fields carry no `Rf`. `Box::from_raw`
                // reuses the same heap allocation as `suspend`'s `Box::into_raw`, so the address
                // is unchanged.
                let active =
                    unsafe { Box::from_raw(suspended.cast::<IdListLazy<'index, SORTED>>()) };
                Ok(ResumeOutcome::Ok(active))
            }
            Err(_) => {
                // `resume_in_place` left the inner id list untouched (the stored result kind was
                // neither metric nor virtual), so the outer allocation is still a valid
                // `SuspendedIdListLazy`.
                // SAFETY: `suspended` came from `Box::into_raw` above and was not mutated; reclaim
                // ownership so it is dropped.
                drop(unsafe { Box::from_raw(suspended) });
                Ok(ResumeOutcome::Aborted)
            }
        }
    }

    fn last_doc_id(&self) -> DocId {
        RawIdList::<'query, Suspended, SORTED>::suspended_result_doc_id(&self.inner)
    }

    fn num_estimated(&self) -> usize {
        self._num_estimated()
    }
}
