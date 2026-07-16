/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Supporting types for [`MetricLazy`].

use crate::{
    IteratorType, Metric, RQEIterator, RQEIteratorBoxed, RQEIteratorError, RQESuspendedIterator,
    RQEValidateStatus, ResumeOutcome, SkipToOutcome,
    deferred::{ProducedResults, Producer},
    metric::{MetricType, RawMetric, SuspendedMetric},
    profile_print::{ProfilePrint, ProfilePrintCtx},
};
use ffi::{RLookupKey, RLookupKeyHandle};
use index_result::RSIndexResult;
use index_spec::IndexSpecReadGuard;
use ref_mode::{Active, Ref, Suspended};
use rqe_core::DocId;

/// A lazily-populated metric iterator sorted by document id.
pub type MetricLazySortedById<'index> = MetricLazy<'index, true>;
/// A lazily-populated metric iterator sorted by metric value.
pub type MetricLazySortedByScore<'index> = MetricLazy<'index, false>;

/// A lazily-populated [`Metric`]: the doc ids and metric values are produced on the first
/// [`read`](RQEIterator::read)/[`skip_to`](RQEIterator::skip_to) by a deferred [`Producer`]
/// (see [`crate::deferred`]), keeping the eager [`Metric`] hot path branch-free.
///
/// It wraps an initially-empty [`Metric`], so the metric's `own_key`/`key_handle` wiring is in
/// place from construction — the query pipeline resolves the distance RLookup key (which the
/// caller wires via [`set_handle`](Self::set_handle)/[`key_mut_ref`](Self::key_mut_ref)) before
/// the producer ever runs. Once produced it delegates entirely to the inner [`Metric`], so the
/// metric value is yielded against the same key.
///
/// `#[repr(C)]` so it is layout-compatible with its suspended counterpart
/// [`SuspendedMetricLazy`], enabling the allocation-preserving whole-struct
/// cast in [`RQEIteratorBoxed::suspend`].
#[repr(C)]
pub struct RawMetricLazy<'query, Rf: Ref, const SORTED_BY_ID: bool> {
    /// The wrapped metric iterator, empty until [`producer`](Self::producer) runs.
    inner: RawMetric<'query, Rf, SORTED_BY_ID>,
    /// The deferred producer. Run once on the first read/skip_to (guarded by
    /// [`produced`](Self::produced)) but **retained** so any state it owns lives as long as this
    /// iterator (see [`Producer`]).
    producer: Producer<'static>,
    /// Whether [`producer`](Self::producer) has already run.
    produced: bool,
    /// Upper-bound estimate reported while the producer is still pending.
    num_estimated_hint: usize,
}

/// Alias for an [`Active`] [`RawMetricLazy`] — the only instantiation with an
/// [`RQEIterator`] impl today.
pub type MetricLazy<'index, const SORTED: bool> = RawMetricLazy<'index, Active<'index>, SORTED>;
/// Alias for an [`Active`] [`RawMetricLazy`] — the only instantiation with an
/// [`RQEIterator`] impl today.
pub type SuspendedMetricLazy<'query, const SORTED: bool> = RawMetricLazy<'query, Suspended, SORTED>;

// Compile-time proof that the `MetricLazy` and its suspended counterpart are layout-identical.
const _: () = {
    use std::mem::offset_of;

    const SORTED: bool = true;
    type A<'a> = MetricLazy<'a, SORTED>;
    type S<'a> = SuspendedMetricLazy<'a, SORTED>;

    // Every field starts at the same offset.
    assert!(offset_of!(A, inner) == offset_of!(S, inner));
    assert!(offset_of!(A, producer) == offset_of!(S, producer));
    assert!(offset_of!(A, produced) == offset_of!(S, produced));
    assert!(offset_of!(A, num_estimated_hint) == offset_of!(S, num_estimated_hint));

    assert!(size_of::<A>() == size_of::<S>());
    assert!(align_of::<A>() == align_of::<S>());
};

impl<'index, const SORTED_BY_ID: bool> MetricLazy<'index, SORTED_BY_ID> {
    /// Create a lazy metric iterator. `num_estimated_hint` is the estimate reported until the
    /// `producer` runs.
    pub fn new(producer: Producer<'static>, num_estimated_hint: usize, type_: MetricType) -> Self {
        Self {
            inner: Metric::empty(type_),
            producer,
            produced: false,
            num_estimated_hint,
        }
    }

    /// Return a mutable reference to the (inner) iterator's key. See [`Metric::key_mut_ref`].
    pub const fn key_mut_ref(&mut self) -> &mut *mut RLookupKey {
        self.inner.key_mut_ref()
    }

    /// Set the (inner) iterator's [`RLookupKeyHandle`]. See [`Metric::set_handle`].
    ///
    /// # Safety
    ///
    /// Same contract as [`Metric::set_handle`].
    pub const unsafe fn set_handle(&mut self, key_handle: *mut RLookupKeyHandle) {
        // SAFETY: contract forwarded to the caller of this function.
        unsafe { self.inner.set_handle(key_handle) };
    }

    /// Get the metric type used by this iterator.
    pub const fn metric_type(&self) -> MetricType {
        self.inner.metric_type()
    }

    /// Run the producer on the first call, populating the inner iterator; a no-op afterwards. The
    /// producer is retained either way. On timeout the error is propagated and the inner iterator
    /// stays empty (so the next read reports EOF).
    #[inline]
    fn ensure_produced(&mut self) -> Result<(), RQEIteratorError> {
        if !self.produced {
            self.produced = true;
            let ProducedResults { ids, metrics } = (self.producer)()?;
            self.inner.set_results(ids, metrics.unwrap_or_default());
        }
        Ok(())
    }
}

impl<'query, Rf: Ref, const SORTED_BY_ID: bool> RawMetricLazy<'query, Rf, SORTED_BY_ID> {
    #[inline(always)]
    fn _num_estimated(&self) -> usize {
        if self.produced {
            self.inner._num_estimated()
        } else {
            self.num_estimated_hint
        }
    }
}

impl<'index, const SORTED_BY_ID: bool> RQEIterator<'index> for MetricLazy<'index, SORTED_BY_ID> {
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
        // Before production the iterator is not (necessarily) exhausted — report not-at-EOF so
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
        if SORTED_BY_ID {
            IteratorType::MetricLazySortedById
        } else {
            IteratorType::MetricLazySortedByScore
        }
    }

    fn intersection_sort_weight(&self, prioritize_union_children: bool) -> f64 {
        self.inner
            .intersection_sort_weight(prioritize_union_children)
    }
}

impl<const SORTED_BY_ID: bool> ProfilePrint for MetricLazy<'_, SORTED_BY_ID> {
    fn print_profile(&self, map: &mut redis_reply::MapBuilder<'_>, ctx: &mut ProfilePrintCtx<'_>) {
        self.inner.print_profile(map, ctx);
    }
}

impl<'index, const SORTED_BY_ID: bool> RQEIteratorBoxed<'index>
    for MetricLazy<'index, SORTED_BY_ID>
{
    type Suspended = SuspendedMetricLazy<'index, SORTED_BY_ID>;

    fn suspend(self: Box<Self>) -> Box<Self::Suspended> {
        let active = Box::into_raw(self);

        // Suspend the wrapped metric's base id list `result` field in place, leaving the
        // outer allocation (and the `Rf`-independent `producer`) untouched.
        //
        // SAFETY: `&raw mut` forms a field pointer without creating a reference,
        // preserving `active`'s provenance over the whole allocation for the outer
        // cast below.
        let inner_slot = unsafe { &raw mut (*active).inner };
        // SAFETY: `suspend_in_place`'s contract is met — `inner_slot` is initialized and
        // unaliased (this function owns `self`). Suspending is a safe widening conversion.
        let _ = unsafe { Metric::<'index, SORTED_BY_ID>::suspend_in_place(inner_slot) };

        // SAFETY: `MetricLazy` and `SuspendedMetricLazy` are both `#[repr(C)]` with identical
        // field layout: `inner` is now the suspended metric, `producer` is `Producer<'static>` in
        // both variants, and `produced`/`num_estimated_hint` carry no lifetime. `Box::from_raw`
        // reuses the same heap allocation as `Box::into_raw`, so the address is unchanged.
        unsafe { Box::from_raw(active.cast::<SuspendedMetricLazy<'index, SORTED_BY_ID>>()) }
    }
}

impl<'query, const SORTED_BY_ID: bool> RQESuspendedIterator<'query>
    for SuspendedMetricLazy<'query, SORTED_BY_ID>
{
    type Resumed<'index>
        = MetricLazy<'index, SORTED_BY_ID>
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

        // Promote the wrapped metric's base id list `result` field in place, leaving the
        // outer allocation (and the `Rf`-independent `producer`) untouched.
        //
        // SAFETY: `&raw mut` forms a field pointer without creating a reference,
        // preserving `suspended`'s provenance over the whole allocation for the
        // outer cast below.
        let inner_slot = unsafe { &raw mut (*suspended).inner };
        // SAFETY: `resume_in_place`'s contract is met — `inner_slot` is initialized and
        // unaliased (this function owns `self`). We reclaim the outer allocation via `suspended`
        // in either branch, so the inner pointer it returns is not needed.
        match unsafe {
            SuspendedMetric::<'query, SORTED_BY_ID>::resume_in_place::<'index>(inner_slot)
        } {
            Ok(_) => {
                // SAFETY: layout-compatible — see `suspend`. `inner`'s base `result` is now valid
                // at the active type for `'index`; the remaining fields carry no `Rf`.
                // `Box::from_raw` reuses the same heap allocation as `suspend`'s `Box::into_raw`,
                // so the address is unchanged.
                let active =
                    unsafe { Box::from_raw(suspended.cast::<MetricLazy<'index, SORTED_BY_ID>>()) };
                Ok(ResumeOutcome::Ok(active))
            }
            Err(_) => {
                // `resume_in_place` left the inner metric untouched (the stored result kind was
                // neither metric nor virtual), so the outer allocation is still a valid
                // `SuspendedMetricLazy`.
                // SAFETY: `suspended` came from `Box::into_raw` above and was not mutated; reclaim
                // ownership so it is dropped.
                drop(unsafe { Box::from_raw(suspended) });
                Ok(ResumeOutcome::Aborted)
            }
        }
    }

    #[inline(always)]
    fn last_doc_id(&self) -> DocId {
        self.inner.last_doc_id()
    }

    fn num_estimated(&self) -> usize {
        self._num_estimated()
    }
}
