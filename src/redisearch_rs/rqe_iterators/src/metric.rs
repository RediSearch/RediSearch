/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::{RQEIterator, RQEIteratorError, RQEValidateStatus, SkipToOutcome, id_list::IdList};
use ffi::{RLookupKey, RLookupKeyHandle, RSYieldableMetric, array_ensure_append_n_func, t_docId};
use inverted_index::{RSIndexResult, ResultMetrics_Reset_func};
use value::{RSValueFFI, RSValueTrait};

/// The different types of metrics.
/// At the moment, only vector distance is supported.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MetricType {
    VectorDistance,
}

/// An iterator that yields document ids alongside a metric value (e.g. a score or a distance),
/// sorted by document id.
pub type MetricIteratorSortedById<'index> = MetricIterator<'index, true>;
/// An iterator that yields document ids alongside a metric value (e.g. a score or a distance),
/// sorted by metric value.
pub type MetricIteratorSortedByScore<'index> = MetricIterator<'index, false>;

/// An iterator that yields document ids alongside a metric value (e.g. a score or a distance).
/// The iterator can be sorted by document id or by metric value,
/// but the choice is made at compile time.
pub struct MetricIterator<'index, const SORTED_BY_ID: bool> {
    base: IdList<'index, SORTED_BY_ID>,
    metric_data: Vec<f64>,
    #[allow(dead_code)]
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

impl<'index, const SORTED_BY_ID: bool> Drop for MetricIterator<'index, SORTED_BY_ID> {
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
        // Safety: we created a metric result, which is numeric, in the constructor
        panic!("Result is not numeric");
    }

    // SAFETY: reset the metrics c_array
    unsafe {
        ResultMetrics_Reset_func(result);
    }

    let value = RSValueFFI::create_num(val);
    let new_metrics: *const RSYieldableMetric = &RSYieldableMetric {
        key,
        value: value.as_ptr(),
    };
    // Prevent value::drop() from being called to avoid use-after-free as the C code now owns this value.
    std::mem::forget(value);
    // SAFETY: calling a C function to append a new metric to the result's metrics array
    unsafe {
        result.metrics = array_ensure_append_n_func(
            result.metrics as *mut _,
            new_metrics as *mut _,
            1,
            std::mem::size_of::<RSYieldableMetric>() as u16,
        ) as *mut RSYieldableMetric;
    }
}

impl<'index, const SORTED_BY_ID: bool> MetricIterator<'index, SORTED_BY_ID> {
    pub fn new(ids: Vec<t_docId>, metric_data: Vec<f64>) -> Self {
        debug_assert!(ids.len() == metric_data.len());

        Self {
            base: IdList::with_result(ids, RSIndexResult::metric()),
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

    /// Get the metric type used by this iterator.
    pub const fn metric_type(&self) -> MetricType {
        self.type_
    }

    /// Return a mutable reference to the key for this metric iterator.
    pub const fn key_mut_ref(&mut self) -> &mut *mut RLookupKey {
        &mut self.own_key
    }
}

impl<'index, const SORTED_BY_ID: bool> RQEIterator<'index>
    for MetricIterator<'index, SORTED_BY_ID>
{
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
        doc_id: t_docId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        let skip_outcome = self.base.skip_to_and_get_offset(doc_id)?;
        match skip_outcome {
            Some((SkipToOutcome::Found(result), offset)) => {
                let val = self.metric_data[offset - 1];
                set_result_metrics(result, val, self.own_key);
                Ok(Some(SkipToOutcome::Found(result)))
            }
            Some((SkipToOutcome::NotFound(result), offset)) => {
                let val = self.metric_data[offset - 1];
                set_result_metrics(result, val, self.own_key);
                Ok(Some(SkipToOutcome::NotFound(result)))
            }
            None => Ok(None),
        }
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
    fn last_doc_id(&self) -> t_docId {
        self.base.last_doc_id()
    }

    #[inline(always)]
    fn at_eof(&self) -> bool {
        self.base.at_eof()
    }

    #[inline(always)]
    fn revalidate(&mut self) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        self.base.revalidate()
    }
}
