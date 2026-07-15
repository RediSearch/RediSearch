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
    IteratorType, RQEIterator, RQEIteratorBoxed, RQEIteratorError, RQESuspendedIterator,
    ResumeOutcome, SkipToOutcome,
    id_list::{IdList, RawIdList, SuspendedIdList},
    profile_print::{ProfilePrint, ProfilePrintCtx},
    utils::OwnedSlice,
};
use ffi::{RLookupKey, RLookupKeyHandle};
use index_result::RSIndexResult;
use index_spec::IndexSpecReadGuard;
use ref_mode::{Active, Ref, Suspended};
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
///
/// Parameterised over a [`Ref`] mode — see [`Metric`] for the [`Active`]
/// instantiation that implements [`RQEIterator`]. The `Rf` flows down into
/// the wrapped `RawIdList` (whose `result` field is `Rf`-typed); the metric
/// data is owned and has no `Rf` dependency.
#[repr(C)]
pub struct RawMetric<'query, Rf: Ref, const SORTED_BY_ID: bool> {
    base: RawIdList<'query, Rf, SORTED_BY_ID>,
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

// Compile-time proof that the `Metric` and its suspended counterpart are layout-identical.
const _: () = {
    use std::mem::offset_of;

    const SORTED_BY_ID: bool = true;
    type A<'a> = Metric<'a, SORTED_BY_ID>;
    type S<'a> = RawMetric<'a, Suspended, SORTED_BY_ID>;

    // Every field starts at the same offset.
    assert!(offset_of!(A, base) == offset_of!(S, base));
    assert!(offset_of!(A, metric_data) == offset_of!(S, metric_data));
    assert!(offset_of!(A, type_) == offset_of!(S, type_));
    assert!(offset_of!(A, own_key) == offset_of!(S, own_key));
    assert!(offset_of!(A, key_handle) == offset_of!(S, key_handle));

    assert!(size_of::<A>() == size_of::<S>());
    assert!(align_of::<A>() == align_of::<S>());
};

/// Alias for an [`Active`] [`RawMetric`] — the only instantiation with an
/// [`RQEIterator`] impl today.
pub type Metric<'index, const SORTED_BY_ID: bool> = RawMetric<'index, Active<'index>, SORTED_BY_ID>;
/// Alias for a [`Suspended`] [`RawMetric`].
pub type SuspendedMetric<'query, const SORTED_BY_ID: bool> =
    RawMetric<'query, Suspended, SORTED_BY_ID>;

impl<'query, Rf: Ref, const SORTED_BY_ID: bool> RawMetric<'query, Rf, SORTED_BY_ID> {
    #[inline(always)]
    pub(super) fn _num_estimated(&self) -> usize {
        self.base._num_estimated()
    }
}

impl<'query, Rf: Ref, const SORTED_BY_ID: bool> Drop for RawMetric<'query, Rf, SORTED_BY_ID> {
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
    /// Used by [`MetricLazy`](crate::metric_lazy::MetricLazy) to construct the iterator (with its
    /// `own_key`/`key_handle` wiring in place) before the deferred producer has run.
    pub fn empty(type_: MetricType) -> Self {
        Self {
            base: IdList::with_result(
                OwnedSlice::default(),
                RSIndexResult::build_metric(0.0).build(),
            ),
            metric_data: OwnedSlice::default(),
            type_,
            own_key: std::ptr::null_mut(),
            key_handle: std::ptr::null_mut(),
        }
    }

    /// Populate the (initially empty) iterator with `ids` and their parallel `metric_data`.
    /// Used by [`MetricLazy`](crate::metric_lazy::MetricLazy) once the deferred producer has run.
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

    /// Return a mutable reference to the key for this metric iterator.
    pub const fn key_mut_ref(&mut self) -> &mut *mut RLookupKey {
        &mut self.own_key
    }
}

impl<'query, Rf: Ref, const SORTED_BY_ID: bool> RawMetric<'query, Rf, SORTED_BY_ID> {
    /// Get the metric type used by this iterator. Mode-independent — the
    /// metric type is a `Copy` enum value set at construction time.
    pub const fn metric_type(&self) -> MetricType {
        self.type_
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
        self._num_estimated()
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

impl<'query, const SORTED_BY_ID: bool> RawMetric<'query, Suspended, SORTED_BY_ID> {
    /// Read the suspended iterator's `doc_id` without exposing the private
    /// `base` field to other modules. Used by
    /// [`SuspendedMetricLazy`](crate::metric_lazy::SuspendedMetricLazy)'s
    /// [`RQESuspendedIterator`] impl, which can't reach into the inner
    /// `RawMetric` directly.
    pub(crate) const fn suspended_result_doc_id(&self) -> DocId {
        RawIdList::<'query, Suspended, SORTED_BY_ID>::suspended_result_doc_id(&self.base)
    }
}

impl<'index, const SORTED_BY_ID: bool> Metric<'index, SORTED_BY_ID> {
    /// Suspend the active metric at `slot` in place.
    /// Returns the same slot reinterpreted as the suspended `RawMetric`.
    ///
    /// # Safety
    ///
    /// The caller must guarantee that:
    ///
    /// 1. `slot` is non-null, aligned, and points to an initialized
    ///    `Metric<'index, SORTED_BY_ID>`.
    /// 2. `slot` is unaliased for the duration of the call.
    pub(crate) unsafe fn suspend_in_place(
        slot: *mut Self,
    ) -> *mut SuspendedMetric<'index, SORTED_BY_ID> {
        // SAFETY: `slot` is non-null, aligned, and initialized (caller contract 1).
        // `&raw mut` forms a field pointer without creating a reference, leaving
        // `slot`'s provenance over the whole allocation intact for the cast below.
        let base_slot = unsafe { &raw mut (*slot).base };
        // SAFETY: `IdList::suspend_in_place`'s contract is met — `base_slot` is
        // initialized and unaliased (caller contracts 1 and 2).
        unsafe { IdList::<'index, SORTED_BY_ID>::suspend_in_place(base_slot) };

        slot.cast::<SuspendedMetric<'index, SORTED_BY_ID>>()
    }
}

impl<'index, const SORTED_BY_ID: bool> RQEIteratorBoxed<'index> for Metric<'index, SORTED_BY_ID> {
    type Suspended = SuspendedMetric<'index, SORTED_BY_ID>;

    fn suspend(self: Box<Self>) -> Box<Self::Suspended> {
        let active: *mut Self = Box::into_raw(self);

        // SAFETY: `suspend_in_place`'s contract is met — `active` is non-null, aligned, and
        // initialized (it just came from a `Box`), and unaliased (this function owns `self`).
        let suspended_ptr = unsafe { Metric::<'index, SORTED_BY_ID>::suspend_in_place(active) };

        // SAFETY: `suspended_ptr` reuses the same allocation from `Box::into_raw` above, so the
        // address is unchanged and every field is now valid at the suspended type.
        unsafe { Box::from_raw(suspended_ptr) }
    }
}

impl<'query, const SORTED_BY_ID: bool> SuspendedMetric<'query, SORTED_BY_ID> {
    /// Resume the suspended metric at `slot` in place, promoting its wrapped
    /// [`base`](Self::base) id list to `Active<'a>` without moving the allocation.
    ///
    /// On success returns `Ok(ptr)`, the same slot reinterpreted as the active
    /// [`Metric`]. If the stored result kind is neither metric nor virtual it
    /// returns `Err(ptr)` — the same slot, **left untouched and still a valid
    /// [`SuspendedMetric`]** — with a warning logged by the wrapped id list; see
    /// [`SuspendedIdList::resume_in_place`].
    ///
    /// # Safety
    ///
    /// The caller must guarantee that:
    ///
    /// 1. `slot` is non-null, aligned, and points to an initialized
    ///    `RawMetric<'query, Suspended, SORTED_BY_ID>`.
    /// 2. `slot` is unaliased for the duration of the call.
    ///
    /// The returned pointer aliases `slot`. In the `Ok` case every field is valid
    /// at the active type for `'a`; in the `Err` case the slot is byte-for-byte
    /// unchanged and remains a valid `SuspendedMetric<'query, SORTED_BY_ID>`.
    pub(crate) unsafe fn resume_in_place<'a>(
        slot: *mut Self,
    ) -> Result<*mut Metric<'a, SORTED_BY_ID>, *mut Self>
    where
        'query: 'a,
    {
        // SAFETY: `slot` is non-null, aligned, and initialized (caller contract 1).
        // `&raw mut` forms a field pointer without creating a reference, leaving
        // `slot`'s provenance over the whole allocation intact for the casts below.
        let base_slot = unsafe { &raw mut (*slot).base };
        // SAFETY: `SuspendedIdList::resume_in_place`'s contract is met — `base_slot`
        // is initialized and unaliased (caller contracts 1 and 2).
        match unsafe { SuspendedIdList::<'query, SORTED_BY_ID>::resume_in_place::<'a>(base_slot) } {
            // The base id list was resumed in place; the rest of the metric carries no `Rf`.
            Ok(_) => Ok(slot.cast::<Metric<'a, SORTED_BY_ID>>()),
            // The base — and therefore the whole metric slot — was left untouched.
            Err(_) => Err(slot),
        }
    }
}

impl<'query, const SORTED_BY_ID: bool> RQESuspendedIterator<'query>
    for SuspendedMetric<'query, SORTED_BY_ID>
{
    type Resumed<'index>
        = Metric<'index, SORTED_BY_ID>
    where
        'query: 'index;

    fn resume<'index>(
        self: Box<Self>,
        _guard: &IndexSpecReadGuard<'index>,
    ) -> Result<ResumeOutcome<Box<Self::Resumed<'index>>>, RQEIteratorError>
    where
        'query: 'index,
    {
        let suspended: *mut Self = Box::into_raw(self);

        // SAFETY: `resume_in_place`'s contract is met:
        // 1. `suspended` is non-null, aligned, and initialized — it just came from a `Box`.
        // 2. `suspended` is not aliased, since this function has ownership of `self`.
        match unsafe {
            SuspendedMetric::<'query, SORTED_BY_ID>::resume_in_place::<'index>(suspended)
        } {
            Ok(active_ptr) => {
                // SAFETY: `active_ptr` reuses the same allocation from `Box::into_raw` above, so
                // the address is unchanged and every field is now valid at the active type for
                // `'index`.
                Ok(ResumeOutcome::Ok(unsafe { Box::from_raw(active_ptr) }))
            }
            Err(suspended_ptr) => {
                // SAFETY: `suspended_ptr` is the same allocation, left untouched and still a valid
                // `SuspendedMetric`. Reclaim ownership so it is dropped.
                drop(unsafe { Box::from_raw(suspended_ptr) });
                Ok(ResumeOutcome::Aborted)
            }
        }
    }

    fn last_doc_id(&self) -> DocId {
        self.suspended_result_doc_id()
    }

    fn num_estimated(&self) -> usize {
        self._num_estimated()
    }
}
