/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Supporting types for [`MetricLazy`].

use ffi::{RLookupKey, RLookupKeyHandle};
use index_result::RSIndexResult;
use index_spec::IndexSpecReadGuard;
use rqe_core::DocId;

use crate::{
    IteratorType, RQEIterator, RQEIteratorError, RQEValidateStatus, SkipToOutcome,
    deferred::{ProducedResults, Producer},
    metric::{Metric, MetricType},
    profile_print::{ProfilePrint, ProfilePrintCtx},
};

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
pub struct MetricLazy<'index, const SORTED_BY_ID: bool> {
    /// The wrapped metric iterator, empty until [`producer`](Self::producer) runs.
    inner: Metric<'index, SORTED_BY_ID>,
    /// The deferred producer. Run once on the first read/skip_to (guarded by
    /// [`produced`](Self::produced)) but **retained** so any state it owns lives as long as this
    /// iterator (see [`Producer`]).
    producer: Producer<'index>,
    /// Whether [`producer`](Self::producer) has already run.
    produced: bool,
    /// Upper-bound estimate reported while the producer is still pending.
    num_estimated_hint: usize,
}

impl<'index, const SORTED_BY_ID: bool> MetricLazy<'index, SORTED_BY_ID> {
    /// Create a lazy metric iterator. `num_estimated_hint` is the estimate reported until the
    /// `producer` runs.
    pub fn new(producer: Producer<'index>, num_estimated_hint: usize, type_: MetricType) -> Self {
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
