/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Supporting types for [`Metric`].

use std::ptr::NonNull;

use crate::{
    IteratorType, RQEIterator, RQEIteratorBoxed, RQEIteratorError, RQESuspendedIterator,
    RQEValidateStatus, ResumeOutcome, SkipToOutcome,
    id_list::{IdList, RawIdList, SuspendedIdList},
    interop::RQEIteratorWrapper,
    metric_lazy::{MetricLazySortedById, MetricLazySortedByScore},
    profile_print::{ProfilePrint, ProfilePrintCtx},
    utils::OwnedSlice,
};
use ffi::QueryIterator;
use index_result::RSIndexResult;
use index_spec::IndexSpecReadGuard;
use ref_mode::{Active, Ref, Suspended};
use rlookup::{RLookupKey, RLookupKeyHandle};
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
    own_key: *mut RLookupKey<'query>,
    /// # Invariants
    ///
    /// The handle is either:
    ///
    /// - [`None`], indicating that the iterator is not associated with a key.
    /// - A valid pointer to a [`RLookupKeyHandle`] instance.
    key_handle: Option<NonNull<RLookupKeyHandle<'query>>>,
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
        if let Some(mut key_handle) = self.key_handle {
            // Safety: thanks to [`Self::key_handle`]'s invariant, we can safely
            // dereference the handle if it is present.
            unsafe {
                key_handle.as_mut().is_valid = false;
            }
        }
    }
}

#[inline(always)]
fn set_result_metrics(result: &mut RSIndexResult, val: f64, key: *mut RLookupKey<'_>) {
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
        // The metrics vector types the key the way C sees it: the header
        // `RLookupKey` is exported under, which is this key's first field.
        //
        // SAFETY: `key` is non-null per the check above, and a valid `RLookupKey`
        // pointer that outlives this result (upheld by callers in `read` and `skip_to`).
        metrics.push_with_key(unsafe { &*key.cast::<ffi::RLookupKey>() }, val);
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
            key_handle: None,
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
            key_handle: None,
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
    /// - [`None`], indicating that the metric iterator does not have a key.
    /// - A valid pointer to a [`RLookupKeyHandle`] instance.
    pub const unsafe fn set_handle(
        &mut self,
        key_handle: Option<NonNull<RLookupKeyHandle<'index>>>,
    ) {
        self.key_handle = key_handle;
    }

    /// Get the metric type used by this iterator.
    pub const fn metric_type(&self) -> MetricType {
        self.type_
    }

    /// Return a mutable reference to the key for this metric iterator.
    pub const fn key_mut_ref(&mut self) -> &mut *mut RLookupKey<'index> {
        &mut self.own_key
    }
}

impl<'index, const SORTED_BY_ID: bool> RQEIterator<'index> for Metric<'index, SORTED_BY_ID> {
    #[inline(always)]
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        self.base.current()
    }

    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        // The read below yields `None` once the base has nothing left, and records
        // the step past the end as it does so; returning early would skip that
        // bookkeeping.
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
    /// 1. `slot` is aligned and points to an initialized
    ///    `Metric<'index, SORTED_BY_ID>`.
    /// 2. `slot` is unaliased for the duration of the call.
    pub(crate) unsafe fn suspend_in_place(
        slot: NonNull<Self>,
    ) -> NonNull<SuspendedMetric<'index, SORTED_BY_ID>> {
        // SAFETY: `slot` is aligned and initialized (caller contract 1).
        // `&raw mut` forms a field pointer without creating a reference, leaving
        // `slot`'s provenance over the whole allocation intact for the cast below.
        let base_slot = unsafe { &raw mut (*slot.as_ptr()).base };
        // SAFETY: `base_slot` is a field pointer derived from the non-null `slot`, so it is
        // non-null too.
        let base_slot = unsafe { NonNull::new_unchecked(base_slot) };
        // SAFETY: `IdList::suspend_in_place`'s contract is met — `base_slot` is
        // initialized and unaliased (caller contracts 1 and 2).
        unsafe { IdList::<'index, SORTED_BY_ID>::suspend_in_place(base_slot) };

        slot.cast::<SuspendedMetric<'index, SORTED_BY_ID>>()
    }
}

impl<'index, const SORTED_BY_ID: bool> RQEIteratorBoxed<'index> for Metric<'index, SORTED_BY_ID> {
    type Suspended = SuspendedMetric<'index, SORTED_BY_ID>;

    fn suspend(self: Box<Self>) -> Box<Self::Suspended> {
        let active = NonNull::from(Box::leak(self));

        // SAFETY: `suspend_in_place`'s contract is met — `active` is aligned and
        // initialized (it just came from a `Box`), and unaliased (this function owns `self`).
        let suspended_ptr = unsafe { Metric::<'index, SORTED_BY_ID>::suspend_in_place(active) };

        // SAFETY: `suspended_ptr` reuses the same allocation from `Box::leak` above, so the
        // address is unchanged and every field is now valid at the suspended type.
        unsafe { Box::from_raw(suspended_ptr.as_ptr()) }
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
    /// 1. `slot` is aligned and points to an initialized
    ///    `RawMetric<'query, Suspended, SORTED_BY_ID>`.
    /// 2. `slot` is unaliased for the duration of the call.
    ///
    /// The returned pointer aliases `slot`. In the `Ok` case every field is valid
    /// at the active type for `'a`; in the `Err` case the slot is byte-for-byte
    /// unchanged and remains a valid `SuspendedMetric<'query, SORTED_BY_ID>`.
    pub(crate) unsafe fn resume_in_place<'a>(
        slot: NonNull<Self>,
    ) -> Result<NonNull<Metric<'a, SORTED_BY_ID>>, NonNull<Self>>
    where
        'query: 'a,
    {
        // SAFETY: `slot` is aligned and initialized (caller contract 1).
        // `&raw mut` forms a field pointer without creating a reference, leaving
        // `slot`'s provenance over the whole allocation intact for the casts below.
        let base_slot = unsafe { &raw mut (*slot.as_ptr()).base };
        // SAFETY: `base_slot` is a field pointer derived from the non-null `slot`, so it is
        // non-null too.
        let base_slot = unsafe { NonNull::new_unchecked(base_slot) };
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
        let suspended = NonNull::from(Box::leak(self));

        // SAFETY: `resume_in_place`'s contract is met:
        // 1. `suspended` is aligned and initialized — it just came from a `Box`.
        // 2. `suspended` is not aliased, since this function has ownership of `self`.
        match unsafe {
            SuspendedMetric::<'query, SORTED_BY_ID>::resume_in_place::<'index>(suspended)
        } {
            Ok(active_ptr) => {
                // SAFETY: `active_ptr` reuses the same allocation from `Box::leak` above, so
                // the address is unchanged and every field is now valid at the active type for
                // `'index`.
                Ok(ResumeOutcome::Ok(unsafe {
                    Box::from_raw(active_ptr.as_ptr())
                }))
            }
            Err(suspended_ptr) => {
                // SAFETY: `suspended_ptr` is the same allocation, left untouched and still a valid
                // `SuspendedMetric`. Reclaim ownership so it is dropped.
                drop(unsafe { Box::from_raw(suspended_ptr.as_ptr()) });
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

/// A metric iterator's own [`RLookupKey`] slot, whatever its sortedness and
/// laziness.
///
/// The four metric flavours are distinct Rust types behind one C-ABI header, so
/// reaching the slot means dispatching on the header's
/// [`type_`](QueryIterator::type_).
///
/// # Safety
///
/// 1. `header` points to a live iterator built by
///    [`RQEIteratorWrapper::boxed_new`] whose type tag names the type it
///    actually wraps. Being a *metric* iterator is not required — that is the
///    run-time check under [Panics](#panics) — but honesty is. The tag is what
///    the downcast below keys off, and the wrapper copies it from the safe
///    [`RQEIterator::type_`], so an iterator reporting a flavour it is not
///    would be downcast to a type it is not, and its `inner` read as that type.
///    Every iterator in this crate reports honestly.
/// 2. The caller holds that iterator exclusively for the duration of the call.
/// 3. `'index` must not outlive the storage the slot's key borrows. A
///    [`NonNull<QueryIterator>`] carries no lifetime, so — as for
///    [`std::slice::from_raw_parts`] — there is nothing here to infer one from
///    and the caller picks it. The choice is not cosmetic: [`RLookupKey`] owns
///    its name as a `Cow<'index, CStr>` behind the safe [`name`] accessor, so
///    `'static` over a query-local key hands out a `Cow<'static, CStr>` that
///    still borrows it, and safe code can then outlive the storage. A caller
///    with no lifetime to offer — C, across the FFI shim — should discard it by
///    casting to the erased [`ffi::RLookupKey`] header rather than naming
///    `'static`.
///
/// The returned pointer aliases the iterator's own slot, so it is valid only
/// for as long as the iterator itself, and writes through it must not be
/// interleaved with use of the iterator.
///
/// [`name`]: RLookupKey::name
///
/// # Panics
///
/// Panics unless `header` wraps one of [`MetricSortedById`],
/// [`MetricSortedByScore`], [`MetricLazySortedById`] or
/// [`MetricLazySortedByScore`] — the four flavours that own a key slot.
// TODO: this API should use proper Rust types instead of QueryIterator once top-k has been ported
// to Rust (MOD-17755).
pub unsafe fn own_key_ref<'index>(
    header: NonNull<QueryIterator>,
) -> NonNull<*mut RLookupKey<'index>> {
    // SAFETY: safe thanks to 1.
    let iterator_type = unsafe { header.as_ref().type_ };

    match iterator_type {
        IteratorType::MetricSortedById => {
            // SAFETY: safe thanks to 1 + 2.
            let wrapper =
                unsafe { RQEIteratorWrapper::<MetricSortedById>::mut_ref_from_header_ptr(header) };
            NonNull::from(wrapper.inner.key_mut_ref())
        }
        IteratorType::MetricSortedByScore => {
            // SAFETY: safe thanks to 1 + 2.
            let wrapper = unsafe {
                RQEIteratorWrapper::<MetricSortedByScore>::mut_ref_from_header_ptr(header)
            };
            NonNull::from(wrapper.inner.key_mut_ref())
        }
        IteratorType::MetricLazySortedById => {
            // SAFETY: safe thanks to 1 + 2.
            let wrapper = unsafe {
                RQEIteratorWrapper::<MetricLazySortedById>::mut_ref_from_header_ptr(header)
            };
            NonNull::from(wrapper.inner.key_mut_ref())
        }
        IteratorType::MetricLazySortedByScore => {
            // SAFETY: safe thanks to 1 + 2.
            let wrapper = unsafe {
                RQEIteratorWrapper::<MetricLazySortedByScore>::mut_ref_from_header_ptr(header)
            };
            NonNull::from(wrapper.inner.key_mut_ref())
        }
        _ => unreachable!(
            "expected a metric iterator, either sorted by ID or Score (metric value): unexpected type: {iterator_type}"
        ),
    }
}

/// Give a metric iterator the [`RLookupKeyHandle`] it invalidates when freed,
/// dispatching on the header exactly as [`own_key_ref`] does.
///
/// # Safety
///
/// 1. `header` points to a live iterator whose type tag is honest, exactly as
///    for [`own_key_ref`] — metric-ness is a run-time check here too, not a
///    pre-condition.
/// 2. The caller holds that iterator exclusively for the duration of the call.
/// 3. `handle` is [`None`], or points to a valid [`RLookupKeyHandle`] that outlives
///    the iterator. The iterator clears the handle's validity flag on its way
///    out, so freeing the handle first is a use-after-free at a distance: the
///    write lands whenever the iterator is dropped, not during this call.
///
/// # Panics
///
/// Panics on any header [`own_key_ref`] would panic on.
pub unsafe fn set_key_handle(
    header: NonNull<QueryIterator>,
    handle: Option<NonNull<RLookupKeyHandle<'_>>>,
) {
    // SAFETY: safe thanks to 1.
    let iterator_type = unsafe { header.as_ref().type_ };

    match iterator_type {
        IteratorType::MetricSortedById => {
            // SAFETY: safe thanks to 1 + 2.
            let wrapper =
                unsafe { RQEIteratorWrapper::<MetricSortedById>::mut_ref_from_header_ptr(header) };
            // SAFETY: safe thanks to 3.
            unsafe { wrapper.inner.set_handle(handle) };
        }
        IteratorType::MetricSortedByScore => {
            // SAFETY: safe thanks to 1 + 2.
            let wrapper = unsafe {
                RQEIteratorWrapper::<MetricSortedByScore>::mut_ref_from_header_ptr(header)
            };
            // SAFETY: safe thanks to 3.
            unsafe { wrapper.inner.set_handle(handle) };
        }
        IteratorType::MetricLazySortedById => {
            // SAFETY: safe thanks to 1 + 2.
            let wrapper = unsafe {
                RQEIteratorWrapper::<MetricLazySortedById>::mut_ref_from_header_ptr(header)
            };
            // SAFETY: safe thanks to 3.
            unsafe { wrapper.inner.set_handle(handle) };
        }
        IteratorType::MetricLazySortedByScore => {
            // SAFETY: safe thanks to 1 + 2.
            let wrapper = unsafe {
                RQEIteratorWrapper::<MetricLazySortedByScore>::mut_ref_from_header_ptr(header)
            };
            // SAFETY: safe thanks to 3.
            unsafe { wrapper.inner.set_handle(handle) };
        }
        _ => unreachable!(
            "expected a metric iterator, either sorted by ID or Score (metric value): unexpected type: {iterator_type}"
        ),
    }
}
