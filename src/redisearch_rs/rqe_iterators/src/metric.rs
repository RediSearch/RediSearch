/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Supporting types for [`Metric`].

use crate::{
    IteratorType, RQEIterator, RQEIteratorError, RQEValidateStatus, SkipToOutcome,
    deferred::{ProducedResults, Producer},
    id_list::IdList,
    profile_print::{ProfilePrint, ProfilePrintCtx},
    utils::OwnedSlice,
};
use ffi::{RLookupKey, RLookupKeyHandle};
use index_result::RSIndexResult;
use index_spec::IndexSpecReadGuard;
use rqe_core::DocId;

/// The different types of metrics.
/// At the moment, only vector distance is supported.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cheadergen::config(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MetricType {
    VectorDistance,
}

/// An iterator that yields document ids alongside a metric value (e.g. a score or a distance),
/// sorted by document id.
pub type MetricSortedById<'index> = Metric<'index, true>;
/// An iterator that yields document ids alongside a metric value (e.g. a score or a distance),
/// sorted by metric value.
pub type MetricSortedByScore<'index> = Metric<'index, false>;

/// An iterator that yields document ids alongside a metric value (e.g. a score or a distance).
/// The iterator can be sorted by document id or by metric value,
/// but the choice is made at compile time.
pub struct Metric<'index, const SORTED_BY_ID: bool> {
    base: IdList<'index, SORTED_BY_ID>,
    metric_data: OwnedSlice<f64>,
    type_: MetricType,
    own_key: *mut RLookupKey,
    /// # Invariants
    ///
    /// The handle is either:
    ///
    /// - A null pointer, indicating that the iterator is not associated with a key.
    /// - A valid pointer to a [`RLookupKeyHandle`] instance.
    key_handle: *mut RLookupKeyHandle,
}

impl<'index, const SORTED_BY_ID: bool> Drop for Metric<'index, SORTED_BY_ID> {
    fn drop(&mut self) {
        if !self.key_handle.is_null() {
            // Safety: thanks to [`Self::key_handle`]'s invariant, we can safely
            // dereference it if it's not null.
            unsafe {
                (*self.key_handle).is_valid = false;
            }
        }
    }
}

#[inline(always)]
fn set_result_metrics(result: &mut RSIndexResult, val: f64, key: *mut RLookupKey) {
    if let Some(num) = result.as_numeric_mut() {
        *num = val;
    } else {
        panic!("Result is not numeric");
    }

    let metrics = result.metrics_mut();
    metrics.reset();
    if key.is_null() {
        metrics.push_without_key(val);
    } else {
        // SAFETY: `key` is non-null per the check above, and a valid `RLookupKey`
        // pointer that outlives this result (upheld by callers in `read` and `skip_to`).
        metrics.push_with_key(unsafe { &*key }, val);
    };
}

impl<'index, const SORTED_BY_ID: bool> Metric<'index, SORTED_BY_ID> {
    pub fn new(ids: impl Into<OwnedSlice<DocId>>, metric_data: impl Into<OwnedSlice<f64>>) -> Self {
        let ids = ids.into();
        let metric_data = metric_data.into();

        debug_assert!(ids.len() == metric_data.len());

        Self {
            base: IdList::with_result(ids, RSIndexResult::build_metric(0.0).build()),
            metric_data,
            type_: MetricType::VectorDistance,
            own_key: std::ptr::null_mut(),
            key_handle: std::ptr::null_mut(),
        }
    }

    /// Creates an empty metric iterator with no results, to be populated later via
    /// [`set_results`](Self::set_results).
    ///
    /// Used by [`MetricLazy`] to construct the iterator (with its `own_key`/`key_handle` wiring
    /// in place) before the deferred producer has run.
    pub fn empty(type_: MetricType) -> Self {
        Self {
            base: IdList::with_result(OwnedSlice::default(), RSIndexResult::build_metric().build()),
            metric_data: OwnedSlice::default(),
            type_,
            own_key: std::ptr::null_mut(),
            key_handle: std::ptr::null_mut(),
        }
    }

    /// Populate the (initially empty) iterator with `ids` and their parallel `metric_data`.
    /// Used by [`MetricLazy`] once the deferred producer has run.
    pub(crate) fn set_results(&mut self, ids: OwnedSlice<DocId>, metric_data: OwnedSlice<f64>) {
        debug_assert!(ids.len() == metric_data.len());
        self.base.set_ids(ids);
        self.metric_data = metric_data;
    }

    /// Set the [`RLookupKeyHandle`] for the metric iterator.
    ///
    /// # Safety
    ///
    /// The provided `key_handle` can either be:
    ///
    /// - A null pointer, indicating that the metric iterator does not have a key.
    /// - A valid pointer to a [`RLookupKeyHandle`] instance.
    pub const unsafe fn set_handle(&mut self, key_handle: *mut RLookupKeyHandle) {
        self.key_handle = key_handle;
    }

    /// Get the metric type used by this iterator.
    pub const fn metric_type(&self) -> MetricType {
        self.type_
    }

    /// Return a mutable reference to the key for this metric iterator.
    pub const fn key_mut_ref(&mut self) -> &mut *mut RLookupKey {
        &mut self.own_key
    }
}

impl<'index, const SORTED_BY_ID: bool> RQEIterator<'index> for Metric<'index, SORTED_BY_ID> {
    #[inline(always)]
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        self.base.current()
    }

    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        if self.base.at_eof() {
            return Ok(None);
        }

        let Some((result, offset)) = self.base.read_and_get_offset()? else {
            return Ok(None);
        };
        let val = self.metric_data[offset - 1];

        set_result_metrics(result, val, self.own_key);
        Ok(Some(result))
    }

    fn skip_to(
        &mut self,
        doc_id: DocId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        let Some(found) = self.base._skip_to(doc_id) else {
            return Ok(None);
        };
        let val = self.metric_data[self.base.offset() - 1];
        let current = self
            .base
            .current()
            .expect("The underlying ID list skipped successfully, so it shouldn't be at EOF");
        set_result_metrics(current, val, self.own_key);
        let outcome = if found {
            SkipToOutcome::Found(current)
        } else {
            SkipToOutcome::NotFound(current)
        };
        Ok(Some(outcome))
    }

    #[inline(always)]
    fn rewind(&mut self) {
        self.base.rewind();
    }

    #[inline(always)]
    // This should always return total results from the iterator, even after some yields.
    fn num_estimated(&self) -> usize {
        self.base.num_estimated()
    }

    #[inline(always)]
    fn last_doc_id(&self) -> DocId {
        self.base.last_doc_id()
    }

    #[inline(always)]
    fn at_eof(&self) -> bool {
        self.base.at_eof()
    }

    #[inline(always)]
    fn revalidate(
        &mut self,
        spec: &IndexSpecReadGuard,
    ) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        self.base.revalidate(spec)
    }

    #[inline(always)]
    fn type_(&self) -> IteratorType {
        if SORTED_BY_ID {
            IteratorType::MetricSortedById
        } else {
            IteratorType::MetricSortedByScore
        }
    }

    fn intersection_sort_weight(&self, _prioritize_union_children: bool) -> f64 {
        1.0
    }
}

impl<const SORTED_BY_ID: bool> ProfilePrint for Metric<'_, SORTED_BY_ID> {
    fn print_profile(&self, map: &mut redis_reply::MapBuilder<'_>, ctx: &mut ProfilePrintCtx<'_>) {
        let metric_type = self.metric_type();

        let type_prefix = if SORTED_BY_ID {
            "METRIC SORTED BY ID"
        } else {
            "METRIC SORTED BY SCORE"
        };
        let type_str = match metric_type {
            MetricType::VectorDistance => {
                format!("{type_prefix} - VECTOR DISTANCE")
            }
        };
        let type_cstr = std::ffi::CString::new(type_str).unwrap();
        map.kv_simple_string(c"Type", &type_cstr);

        ctx.print_optional_counters(map);

        if matches!(metric_type, MetricType::VectorDistance) {
            map.kv_simple_string(c"Vector search mode", c"RANGE_QUERY");
        }
    }
}

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
