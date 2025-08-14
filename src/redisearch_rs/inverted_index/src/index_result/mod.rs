/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{ffi::c_char, fmt::Debug, marker::PhantomData, ptr};

use ffi::{FieldMask, RS_FIELDMASK_ALL, RSQueryTerm, t_docId, t_fieldMask};
use low_memory_thin_vec::LowMemoryThinVec;
use raw::{
    RSAggregateResultRaw, RSAggregateResultRawIter, RSIndexResultRaw, RSNumericRecordRaw,
    RSOffsetVectorRaw, RSResultDataRaw, RSResultKindMaskRaw, RSResultKindRaw, RSTermRecordRaw,
    RSVirtualResultRaw,
};

pub mod raw;

// Manually define some C functions, because we'll create a circular dependency if we use the FFI
// crate to make them automatically.
unsafe extern "C" {
    /// Adds the metrics of a child [`RSIndexResultRaw`] to the parent [`RSIndexResultRaw`].
    ///
    /// # Safety
    /// Both should be valid `RSIndexResultRaw` instances.
    #[allow(improper_ctypes)] // The doc_id in `RSIndexResultRaw` might be a u128
    unsafe fn IndexResult_ConcatMetrics(
        parent: *mut RSIndexResultRaw,
        child: *const RSIndexResultRaw,
    );

    /// Free the metrics inside an [`RSIndexResultRaw`]
    ///
    /// # Safety
    /// The caller must ensure that the `result` pointer is valid and points to an `RSIndexResultRaw`.
    #[allow(improper_ctypes)] // The doc_id in `RSIndexResultRaw` might be a u128
    unsafe fn ResultMetrics_Free(result: *mut RSIndexResultRaw);

    /// Free the data inside a [`RSTermRecordRaw`]'s offset
    ///
    /// # Safety
    /// The caller must ensure that the `tr` pointer is valid and points to an `RSTermRecordRaw`.
    unsafe fn Term_Offset_Data_Free(tr: *mut raw::RSTermRecordRaw);

    /// Free a [`RSQueryTerm`]
    ///
    /// # Safety
    /// The caller must ensure that the `t` pointer is valid and points to an `RSQueryTerm`.
    unsafe fn Term_Free(t: *mut RSQueryTerm);
}

impl Debug for RSOffsetVectorRaw<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.data.is_null() {
            return write!(f, "RSOffsetVectorRaw(null)");
        }
        // SAFETY: `len` is guaranteed to be a valid length for the data pointer.
        let offsets =
            unsafe { std::slice::from_raw_parts(self.data as *const i8, self.len as usize) };

        write!(f, "RSOffsetVectorRaw {offsets:?}")
    }
}

impl RSOffsetVectorRaw<'_> {
    /// Create a new, empty offset vector ready to receive data
    pub fn empty() -> Self {
        Self {
            data: ptr::null_mut(),
            len: 0,
            _phantom: PhantomData,
        }
    }

    /// Create a new offset vector with the given data pointer and length.
    pub fn with_data(data: *mut c_char, len: u32) -> Self {
        Self {
            data,
            len,
            _phantom: PhantomData,
        }
    }
}

impl<'index> RSTermRecordRaw<'index> {
    /// Create a new term record without term pointer and offsets.
    pub fn new() -> Self {
        Self {
            is_copy: false,
            term: ptr::null_mut(),
            offsets: RSOffsetVectorRaw::empty(),
        }
    }

    /// Create a new term with the given term pointer and offsets.
    pub fn with_term(
        term: *mut RSQueryTerm,
        offsets: RSOffsetVectorRaw<'index>,
    ) -> RSTermRecordRaw<'index> {
        Self {
            is_copy: false,
            term,
            offsets,
        }
    }
}

/// Wrapper to provide better Debug output for `RSQueryTerm`.
/// Can be removed once `RSQueryTerm` is fully ported to Rust.
struct QueryTermDebug(*mut RSQueryTerm);

impl Debug for QueryTermDebug {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.0.is_null() {
            return write!(f, "RSQueryTerm(null)");
        }
        // SAFETY: we just checked that `self.0` is not null.
        let term = unsafe { &*self.0 };

        let term_str = if term.str_.is_null() {
            "<null>"
        } else {
            // SAFETY: we just checked than `str_` is not null and `len`
            // is guaranteed to be a valid length for the data pointer.
            let slice = unsafe { std::slice::from_raw_parts(term.str_ as *const u8, term.len) };
            // SAFETY: term.str_ is used as a string in the C code.
            unsafe { std::str::from_utf8_unchecked(slice) }
        };

        f.debug_struct("RSQueryTerm")
            .field("str", &term_str)
            .field("idf", &term.idf)
            .field("id", &term.id)
            .field("flags", &term.flags)
            .field("bm25_idf", &term.bm25_idf)
            .finish()
    }
}

impl Debug for RSTermRecordRaw<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RSTermRecordRaw")
            .field("term", &QueryTermDebug(self.term))
            .field("offsets", &self.offsets)
            .finish()
    }
}

impl Default for RSTermRecordRaw<'_> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'index, 'children> RSAggregateResultRaw<'index, 'children> {
    /// Create a new empty aggregate result with the given capacity
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            is_copy: false,
            records: LowMemoryThinVec::with_capacity(cap),
            kind_mask: RSResultKindMaskRaw::empty(),
            _phantom: PhantomData,
        }
    }

    /// The number of results in this aggregate result
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// Check whether this aggregate result is empty
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// The capacity of the aggregate result
    pub fn capacity(&self) -> usize {
        self.records.capacity()
    }

    /// The current type mask of the aggregate result
    pub fn kind_mask(&self) -> RSResultKindMaskRaw {
        self.kind_mask
    }

    /// Get an iterator over the children of this aggregate result
    pub fn iter(&'index self) -> RSAggregateResultRawIter<'index, 'children> {
        RSAggregateResultRawIter {
            agg: self,
            index: 0,
        }
    }

    /// Get the child at the given index, if it exists
    ///
    /// # Safety
    /// The caller must ensure that the memory at the given index is still valid
    pub fn get(&self, index: usize) -> Option<&RSIndexResultRaw<'index, 'children>> {
        if let Some(result_addr) = self.records.get(index) {
            // SAFETY: The caller is to guarantee that the memory at `result_addr` is still valid.
            Some(unsafe { &**result_addr })
        } else {
            None
        }
    }

    /// Reset the aggregate result, clearing all children and resetting the kind mask.
    ///
    /// Note, this does not deallocate the children pointers, it just resets the count and kind
    /// mask. The owner of the children pointers is responsible for deallocating them when needed.
    pub fn reset(&mut self) {
        self.records.clear();
        self.kind_mask = RSResultKindMaskRaw::empty();
    }

    /// Add a child to the aggregate result and update the kind mask
    ///
    /// # Safety
    /// The given `child` has to stay valid for the lifetime of this aggregate result. Else reading
    /// the child with [`Self::get()`] will cause undefined behavior.
    pub fn push(&mut self, child: &RSIndexResultRaw) {
        self.records.push(child as *const _ as *mut _);

        self.kind_mask |= child.data.kind();
    }
}

/// An owned iterator over the results in an [`RSAggregateResultRaw`].
pub struct RSAggregateResultIterOwned<'index, 'aggregate_children> {
    agg: RSAggregateResultRaw<'index, 'aggregate_children>,
    index: usize,
}

impl<'index, 'aggregate_children> Iterator
    for RSAggregateResultIterOwned<'index, 'aggregate_children>
{
    type Item = Box<RSIndexResultRaw<'index, 'aggregate_children>>;

    /// Get the next item as a `Box<RSIndexResultRaw>`
    ///
    /// # Safety
    /// The box can only be taken if the items in this aggregate result have been cloned and is
    /// therefore owned by the `RSAggregateResultRaw`.
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(result_ptr) = self.agg.records.get(self.index) {
            self.index += 1;

            // SAFETY: The caller is to ensure the `RSAggregateResultRaw` was cloned to allow getting
            // the pointer as a `Box<RSIndexResultRaw>`.
            unsafe { Some(Box::from_raw(*result_ptr as *mut _)) }
        } else {
            None
        }
    }
}

impl<'index, 'children> IntoIterator for RSAggregateResultRaw<'index, 'children> {
    type Item = Box<RSIndexResultRaw<'index, 'children>>;

    type IntoIter = RSAggregateResultIterOwned<'index, 'children>;

    fn into_iter(self) -> Self::IntoIter {
        RSAggregateResultIterOwned {
            agg: self,
            index: 0,
        }
    }
}

impl RSResultDataRaw<'_, '_> {
    pub fn kind(&self) -> RSResultKindRaw {
        match self {
            RSResultDataRaw::Union(_) => RSResultKindRaw::Union,
            RSResultDataRaw::Intersection(_) => RSResultKindRaw::Intersection,
            RSResultDataRaw::Term(_) => RSResultKindRaw::Term,
            RSResultDataRaw::Virtual(_) => RSResultKindRaw::Virtual,
            RSResultDataRaw::Numeric(_) => RSResultKindRaw::Numeric,
            RSResultDataRaw::Metric(_) => RSResultKindRaw::Metric,
            RSResultDataRaw::HybridMetric(_) => RSResultKindRaw::HybridMetric,
        }
    }
}

impl Default for RSIndexResultRaw<'_, '_> {
    fn default() -> Self {
        Self::virt()
    }
}

impl<'index, 'aggregate_children> RSIndexResultRaw<'index, 'aggregate_children> {
    /// Create a new virtual index result
    pub fn virt() -> Self {
        Self {
            doc_id: 0,
            dmd: ptr::null(),
            field_mask: 0,
            freq: 1,
            offsets_sz: 0,
            data: RSResultDataRaw::Virtual(RSVirtualResultRaw),
            metrics: ptr::null_mut(),
            weight: 0.0,
        }
    }

    /// Create a new numeric index result with the given number
    pub fn numeric(num: f64) -> Self {
        Self {
            field_mask: RS_FIELDMASK_ALL,
            freq: 1,
            data: RSResultDataRaw::Numeric(RSNumericRecordRaw(num)),
            weight: 1.0,
            ..Default::default()
        }
    }

    /// Create a new metric index result
    pub fn metric() -> Self {
        Self {
            field_mask: RS_FIELDMASK_ALL,
            data: RSResultDataRaw::Metric(RSNumericRecordRaw(0.0)),
            weight: 1.0,
            ..Default::default()
        }
    }

    /// Create a new intersection index result with the given capacity
    pub fn intersect(cap: usize) -> Self {
        Self {
            data: RSResultDataRaw::Intersection(RSAggregateResultRaw::with_capacity(cap)),
            ..Default::default()
        }
    }

    /// Create a new union index result with the given capacity
    pub fn union(cap: usize) -> Self {
        Self {
            data: RSResultDataRaw::Union(RSAggregateResultRaw::with_capacity(cap)),
            ..Default::default()
        }
    }

    /// Create a new hybrid metric index result
    pub fn hybrid_metric() -> Self {
        Self {
            data: RSResultDataRaw::HybridMetric(RSAggregateResultRaw::with_capacity(2)),
            weight: 1.0,
            ..Default::default()
        }
    }

    /// Create a new term index result.
    pub fn term() -> Self {
        Self {
            data: RSResultDataRaw::Term(RSTermRecordRaw::new()),
            freq: 1,
            ..Default::default()
        }
    }

    /// Create a new `RSIndexResultRaw` with a given `term`, `offsets`, `doc_id`, `field_mask`, and `freq`.
    pub fn term_with_term_ptr(
        term: *mut RSQueryTerm,
        offsets: RSOffsetVectorRaw<'index>,
        doc_id: t_docId,
        field_mask: t_fieldMask,
        freq: u32,
    ) -> RSIndexResultRaw<'index, 'aggregate_children> {
        let offsets_sz = offsets.len;
        Self {
            data: RSResultDataRaw::Term(RSTermRecordRaw::with_term(term, offsets)),
            doc_id,
            field_mask,
            freq,
            offsets_sz,
            dmd: std::ptr::null(),
            metrics: std::ptr::null_mut(),
            weight: 0.0,
        }
    }

    /// Set the document ID of this record
    pub fn doc_id(mut self, doc_id: t_docId) -> Self {
        self.doc_id = doc_id;

        self
    }

    /// Set the field mask of this record
    pub fn field_mask(mut self, field_mask: FieldMask) -> Self {
        self.field_mask = field_mask;

        self
    }

    /// Set the weight of this record
    pub fn weight(mut self, weight: f64) -> Self {
        self.weight = weight;

        self
    }

    /// Set the frequency of this record
    pub fn frequency(mut self, frequency: u32) -> Self {
        self.freq = frequency;

        self
    }

    /// Get the kind of this index result
    pub fn kind(&self) -> RSResultKindRaw {
        self.data.kind()
    }

    /// Get this record as a numeric record if possible. If the record is not numeric, returns
    /// `None`.
    pub fn as_numeric(&self) -> Option<&RSNumericRecordRaw> {
        match &self.data {
            RSResultDataRaw::Numeric(numeric) | RSResultDataRaw::Metric(numeric) => Some(numeric),
            RSResultDataRaw::HybridMetric(_)
            | RSResultDataRaw::Union(_)
            | RSResultDataRaw::Intersection(_)
            | RSResultDataRaw::Term(_)
            | RSResultDataRaw::Virtual(_) => None,
        }
    }

    /// Get this record as a mutable numeric record if possible. If the record is not numeric,
    /// returns `None`.
    pub fn as_numeric_mut(&mut self) -> Option<&mut RSNumericRecordRaw> {
        match &mut self.data {
            RSResultDataRaw::Numeric(numeric) | RSResultDataRaw::Metric(numeric) => Some(numeric),
            RSResultDataRaw::HybridMetric(_)
            | RSResultDataRaw::Union(_)
            | RSResultDataRaw::Intersection(_)
            | RSResultDataRaw::Term(_)
            | RSResultDataRaw::Virtual(_) => None,
        }
    }

    /// Get this record as a term record if possible. If the record is not term, returns
    /// `None`.
    pub fn as_term(&self) -> Option<&RSTermRecordRaw<'index>> {
        match &self.data {
            RSResultDataRaw::Term(term) => Some(term),
            RSResultDataRaw::Union(_)
            | RSResultDataRaw::Intersection(_)
            | RSResultDataRaw::Virtual(_)
            | RSResultDataRaw::Numeric(_)
            | RSResultDataRaw::Metric(_)
            | RSResultDataRaw::HybridMetric(_) => None,
        }
    }

    /// Get this record as a mutable term record if possible. If the record is not a term,
    /// returns `None`.
    pub fn as_term_mut(&mut self) -> Option<&mut RSTermRecordRaw<'index>> {
        match &mut self.data {
            RSResultDataRaw::Term(term) => Some(term),
            RSResultDataRaw::Union(_)
            | RSResultDataRaw::Intersection(_)
            | RSResultDataRaw::Virtual(_)
            | RSResultDataRaw::Numeric(_)
            | RSResultDataRaw::Metric(_)
            | RSResultDataRaw::HybridMetric(_) => None,
        }
    }

    /// Get this record as an aggregate result if possible. If the record is not an aggregate,
    /// returns `None`.
    pub fn as_aggregate(&self) -> Option<&RSAggregateResultRaw<'index, 'aggregate_children>> {
        match &self.data {
            RSResultDataRaw::Union(agg)
            | RSResultDataRaw::Intersection(agg)
            | RSResultDataRaw::HybridMetric(agg) => Some(agg),
            RSResultDataRaw::Term(_)
            | RSResultDataRaw::Virtual(_)
            | RSResultDataRaw::Numeric(_)
            | RSResultDataRaw::Metric(_) => None,
        }
    }

    /// Get this record as a mutable aggregate result if possible. If the record is not an
    /// aggregate, returns `None`.
    pub fn as_aggregate_mut(
        &mut self,
    ) -> Option<&mut RSAggregateResultRaw<'index, 'aggregate_children>> {
        match &mut self.data {
            RSResultDataRaw::Union(agg)
            | RSResultDataRaw::Intersection(agg)
            | RSResultDataRaw::HybridMetric(agg) => Some(agg),
            RSResultDataRaw::Term(_)
            | RSResultDataRaw::Virtual(_)
            | RSResultDataRaw::Numeric(_)
            | RSResultDataRaw::Metric(_) => None,
        }
    }

    /// True if this is an aggregate kind
    pub fn is_aggregate(&self) -> bool {
        matches!(
            self.data,
            RSResultDataRaw::Intersection(_)
                | RSResultDataRaw::Union(_)
                | RSResultDataRaw::HybridMetric(_)
        )
    }

    /// If this is an aggregate result, then add a child to it. Also updates the following of this
    /// record:
    /// - The document ID will inherit the new child added
    /// - The child's frequency will contribute to this result
    /// - The child's field mask will contribute to this result's field mask
    /// - If the child has metrics, then they will be concatenated to this result's metrics
    ///
    /// If this is not an aggregate result, then nothing happens. Use [`Self::is_aggregate()`] first
    /// to make sure this is an aggregate result.
    ///
    /// # Safety
    ///
    /// The given `result` has to stay valid for the lifetime of this index result. Else reading
    /// from this result will cause undefined behaviour.
    pub fn push(&mut self, child: &RSIndexResultRaw) {
        match &mut self.data {
            RSResultDataRaw::Union(agg)
            | RSResultDataRaw::Intersection(agg)
            | RSResultDataRaw::HybridMetric(agg) => {
                agg.push(child);

                self.doc_id = child.doc_id;
                self.freq += child.freq;
                self.field_mask |= child.field_mask;

                // SAFETY: we know both arguments are valid `RSIndexResultRaw` types
                unsafe {
                    IndexResult_ConcatMetrics(self, child);
                }
            }
            RSResultDataRaw::Term(_)
            | RSResultDataRaw::Virtual(_)
            | RSResultDataRaw::Numeric(_)
            | RSResultDataRaw::Metric(_) => {}
        }
    }

    /// Get a child at the given index if this is an aggregate record. Returns `None` if this is not
    /// an aggregate record or if the index is out-of-bounds.
    pub fn get(&self, index: usize) -> Option<&RSIndexResultRaw<'index, 'aggregate_children>> {
        match &self.data {
            RSResultDataRaw::Union(agg)
            | RSResultDataRaw::Intersection(agg)
            | RSResultDataRaw::HybridMetric(agg) => agg.get(index),
            RSResultDataRaw::Term(_)
            | RSResultDataRaw::Virtual(_)
            | RSResultDataRaw::Numeric(_)
            | RSResultDataRaw::Metric(_) => None,
        }
    }
}

impl Drop for RSIndexResultRaw<'_, '_> {
    fn drop(&mut self) {
        // SAFETY: we know `self` still exists because we are in `drop`. We also know the C type is
        // the same since it was autogenerated from the Rust type
        unsafe {
            ResultMetrics_Free(self);
        }

        // Take ownership of the internal data to be able to call `into_iter()` below.
        // `into_iter()` will convert each pointer back to a `Box` to allow it to be cleaned up
        // correctly.
        let mut data = RSResultDataRaw::Virtual(RSVirtualResultRaw);
        std::mem::swap(&mut self.data, &mut data);

        match data {
            RSResultDataRaw::Union(agg)
            | RSResultDataRaw::Intersection(agg)
            | RSResultDataRaw::HybridMetric(agg) => {
                if agg.is_copy {
                    for child in agg.into_iter() {
                        drop(child);
                    }
                }
            }
            RSResultDataRaw::Term(mut term) => {
                if term.is_copy {
                    // SAFETY: we know the C type is the same because it was autogenerated from the Rust type
                    unsafe {
                        Term_Offset_Data_Free(&mut term);
                    }
                } else {
                    // SAFETY: we know the C type is the same because it was autogenerated from the Rust type
                    unsafe {
                        Term_Free(term.term);
                    }
                }
            }
            RSResultDataRaw::Numeric(_numeric) | RSResultDataRaw::Metric(_numeric) => {}
            RSResultDataRaw::Virtual(_virtual) => {}
        }
    }
}
