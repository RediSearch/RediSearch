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
    RQEValidateStatus, SkipToOutcome,
    id_list::{IdList, RawIdList},
    profile_print::{ProfilePrint, ProfilePrintCtx},
    utils::OwnedSlice,
};
use ffi::{RLookupKey, RLookupKeyHandle, ValidateStatus, ValidateStatus_VALIDATE_OK};
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
pub struct RawMetric<Rf: Ref, const SORTED_BY_ID: bool> {
    base: RawIdList<Rf, SORTED_BY_ID>,
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

/// Alias for an [`Active`] [`RawMetric`] — the only instantiation with an
/// [`RQEIterator`] impl today.
pub type Metric<'index, const SORTED_BY_ID: bool> = RawMetric<Active<'index>, SORTED_BY_ID>;

impl<Rf: Ref, const SORTED_BY_ID: bool> Drop for RawMetric<Rf, SORTED_BY_ID> {
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
            base: IdList::with_result(ids, RSIndexResult::build_metric().build()),
            metric_data,
            type_: MetricType::VectorDistance,
            own_key: std::ptr::null_mut(),
            key_handle: std::ptr::null_mut(),
        }
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

impl<Rf: Ref, const SORTED_BY_ID: bool> RawMetric<Rf, SORTED_BY_ID> {
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

impl<'index, const SORTED_BY_ID: bool> RQEIteratorBoxed<'index> for Metric<'index, SORTED_BY_ID> {
    type Suspended = RawMetric<Suspended, SORTED_BY_ID>;

    fn suspend(self: Box<Self>) -> Box<Self::Suspended> {
        let raw = Box::into_raw(self);
        // SAFETY: `RawMetric` is `#[repr(C)]`. The only `Rf`-dependent field
        // is the inner `RawIdList<Rf, SORTED_BY_ID>`, layout-compatible
        // across `Rf` (its only `Rf` field is `result: RawIndexResult<Rf>`,
        // backed by `SharedPtr` transparency). The remaining fields
        // (`metric_data`, `type_`, `own_key`, `key_handle`) carry no `Rf`.
        // Suspend is widening, so casting the box's heap pointer is sound
        // and preserves the heap allocation; the active `Drop` impl
        // (`key_handle` nullification) does not run on these bytes —
        // ownership of the allocation transfers to the suspended box.
        unsafe { Box::from_raw(raw as *mut RawMetric<Suspended, SORTED_BY_ID>) }
    }
}

impl<const SORTED_BY_ID: bool> RQESuspendedIterator for RawMetric<Suspended, SORTED_BY_ID> {
    type Resumed<'a> = Metric<'a, SORTED_BY_ID>;

    fn resume<'a>(
        self: Box<Self>,
        _guard: &'a IndexSpecReadGuard<'a>,
    ) -> (Box<Self::Resumed<'a>>, ValidateStatus) {
        let raw = Box::into_raw(self);
        // SAFETY: layout-compatible — see `suspend`. `Metric` owns all its
        // pointee data (the `OwnedSlice` and the inner `IdList`'s
        // `OwnedSlice`); the virtual `result` has no aliased pointers, so
        // promotion back to `Active<'a>` is unconditionally sound and
        // re-uses the same heap allocation.
        let active = unsafe { Box::from_raw(raw as *mut Metric<'a, SORTED_BY_ID>) };
        (active, ValidateStatus_VALIDATE_OK)
    }

    fn last_doc_id(&self) -> DocId {
        RawIdList::<Suspended, SORTED_BY_ID>::suspended_result_doc_id(&self.base)
    }

    fn num_estimated(&self) -> usize {
        self.base.num_estimated()
    }
}
