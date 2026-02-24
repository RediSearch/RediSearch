/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ptr;

use ffi::{
    FieldMask, RS_FIELDMASK_ALL, RSDocumentMetadata, RSYieldableMetric, t_docId, t_fieldMask,
};
use query_term::RSQueryTerm;

use super::aggregate::RSAggregateResult;
use super::kind::RSResultKind;
use super::offsets::RSOffsetSlice;
use super::result_data::RSResultData;
use super::term_record::RSTermRecord;

// Manually define some C functions, because we'll create a circular dependency if we use the FFI
// crate to make them automatically.
unsafe extern "C" {
    /// Adds the metrics of a child [`RSYieldableMetric`] to the parent [`RSYieldableMetric`].
    ///
    /// # Safety
    /// Both should be valid `RSYieldableMetric` instances.
    #[allow(improper_ctypes)] // The doc_id in `RSIndexResult` might be a u128
    unsafe fn RSYieldableMetric_Concat(
        parent: *mut *mut RSYieldableMetric,
        child: *const RSYieldableMetric,
    );

    /// Free the metrics
    ///
    /// # Safety
    /// The caller must ensure that the `metrics` pointer is either `null` or valid and points to a `*mut RSYieldableMetric`.
    unsafe fn ResultMetrics_Free(metrics: *mut RSYieldableMetric);

    /// reset the metrics
    ///
    /// # Safety
    /// The caller must ensure that the `metrics` pointer is either `null` or valid and points to a `*mut RSYieldableMetric`.
    #[expect(
        improper_ctypes,
        reason = "RSQueryTerm is opaque - accessed via FFI functions only"
    )]
    pub unsafe fn ResultMetrics_Reset_func(result: *mut RSIndexResult);

    /// Make a complete clone of the metrics array and increment the reference count of each value
    ///
    /// # Safety
    /// The caller must ensure that the `src` pointer is valid and points to an `RSYieldableMetric`.
    /// The caller must also not free the returned pointer, but should use `ResultMetrics_Free` instead.
    unsafe fn RSYieldableMetrics_Clone(src: *mut RSYieldableMetric) -> *mut RSYieldableMetric;
}

/// The result of an inverted index
/// cbindgen:rename-all=CamelCase
#[repr(C)]
#[derive(Debug, PartialEq)]
pub struct RSIndexResult<'index> {
    /// The document ID of the result
    pub doc_id: t_docId,

    /// Some metadata about the result document
    pub dmd: *const RSDocumentMetadata,

    /// The aggregate field mask of all the records in this result
    pub field_mask: t_fieldMask,

    /// The total frequency of all the records in this result
    pub freq: u32,

    /// The actual data of the result
    pub data: RSResultData<'index>,

    /// Holds an array of metrics yielded by the different iterators in the AST
    pub metrics: *mut RSYieldableMetric,

    /// Relative weight for scoring calculations. This is derived from the result's iterator weight
    pub weight: f64,
}

impl Default for RSIndexResult<'_> {
    fn default() -> Self {
        Self::virt()
    }
}

impl<'index> RSIndexResult<'index> {
    /// Create a new virtual index result
    pub const fn virt() -> Self {
        Self {
            doc_id: 0,
            dmd: ptr::null(),
            field_mask: 0,
            freq: 0,
            data: RSResultData::Virtual,
            metrics: ptr::null_mut(),
            weight: 0.0,
        }
    }

    /// Create a new numeric index result with the given number
    pub fn numeric(num: f64) -> Self {
        Self {
            field_mask: RS_FIELDMASK_ALL,
            freq: 1,
            data: RSResultData::Numeric(num),
            weight: 1.0,
            ..Default::default()
        }
    }

    /// Create a new metric index result
    pub fn metric() -> Self {
        Self {
            field_mask: RS_FIELDMASK_ALL,
            data: RSResultData::Metric(0f64),
            weight: 1.0,
            ..Default::default()
        }
    }

    /// Create a new intersection index result with the given capacity
    pub fn intersect(cap: usize) -> Self {
        Self {
            data: RSResultData::Intersection(RSAggregateResult::borrowed_with_capacity(cap)),
            ..Default::default()
        }
    }

    /// Create a new union index result with the given capacity
    pub fn union(cap: usize) -> Self {
        Self {
            data: RSResultData::Union(RSAggregateResult::borrowed_with_capacity(cap)),
            ..Default::default()
        }
    }

    /// Create a new hybrid metric index result
    pub fn hybrid_metric() -> Self {
        Self {
            data: RSResultData::HybridMetric(RSAggregateResult::owned_with_capacity(2)),
            weight: 1.0,
            ..Default::default()
        }
    }

    /// Create a new term index result with a `None` term.
    pub fn term() -> Self {
        Self {
            data: RSResultData::Term(RSTermRecord::new()),
            freq: 1,
            ..Default::default()
        }
    }

    /// Create a new `RSIndexResult` with a given `term`, `offsets`, `doc_id`, `field_mask`, and `freq`.
    pub const fn with_term(
        term: Option<Box<RSQueryTerm>>,
        offsets: RSOffsetSlice<'index>,
        doc_id: t_docId,
        field_mask: t_fieldMask,
        freq: u32,
    ) -> RSIndexResult<'index> {
        Self {
            data: RSResultData::Term(RSTermRecord::with_term(term, offsets)),
            doc_id,
            field_mask,
            freq,
            dmd: std::ptr::null(),
            metrics: std::ptr::null_mut(),
            weight: 0.0,
        }
    }

    /// Set the document ID of this record
    pub const fn doc_id(mut self, doc_id: t_docId) -> Self {
        self.doc_id = doc_id;

        self
    }

    /// Set the field mask of this record
    pub const fn field_mask(mut self, field_mask: FieldMask) -> Self {
        self.field_mask = field_mask;

        self
    }

    /// Set the weight of this record
    pub const fn weight(mut self, weight: f64) -> Self {
        self.weight = weight;

        self
    }

    /// Set the frequency of this record
    pub const fn frequency(mut self, frequency: u32) -> Self {
        self.freq = frequency;

        self
    }

    /// Get the kind of this index result
    pub const fn kind(&self) -> RSResultKind {
        self.data.kind()
    }

    /// Get the numeric value of this record without checking its kind. The caller must ensure
    /// that this is a numeric record, else invoking this method will cause undefined behavior.
    ///
    /// # Safety
    ///
    /// 1. `Self::is_numeric()` must return `true` for `self`.
    pub unsafe fn as_numeric_unchecked(&self) -> f64 {
        debug_assert!(
            self.is_numeric(),
            "Invariant violation: `as_numeric_unchecked` was invoked on a non-numeric `RSIndexResult` \
             instance that didn't actually contain a numeric. It was a {}",
            self.data.kind()
        );

        match &self.data {
            RSResultData::Numeric(numeric) | RSResultData::Metric(numeric) => *numeric,
            RSResultData::Union(_)
            | RSResultData::Intersection(_)
            | RSResultData::Term(_)
            | RSResultData::Virtual
            | RSResultData::HybridMetric(_) => {
                // SAFETY: unreachable because of safety condition 1
                unsafe { std::hint::unreachable_unchecked() }
            }
        }
    }

    /// Get a mutable reference to the numeric value of this record without checking its kind.
    /// The caller must ensure that this is a numeric record, else invoking this method will cause
    /// undefined behavior.
    ///
    /// # Safety
    ///
    /// 1. `Self::is_numeric()` must return `true` for `self`.
    pub unsafe fn as_numeric_unchecked_mut(&mut self) -> &mut f64 {
        debug_assert!(
            self.is_numeric(),
            "Invariant violation: `as_numeric_unchecked_mut` was invoked on a non-numeric `RSIndexResult` \
             instance that didn't actually contain a numeric. It was a {}",
            self.data.kind()
        );

        match &mut self.data {
            RSResultData::Numeric(numeric) | RSResultData::Metric(numeric) => numeric,
            RSResultData::Union(_)
            | RSResultData::Intersection(_)
            | RSResultData::Term(_)
            | RSResultData::Virtual
            | RSResultData::HybridMetric(_) => {
                // SAFETY: unreachable because of safety condition 1
                unsafe { std::hint::unreachable_unchecked() }
            }
        }
    }

    /// Get this record as a numeric record if possible. If the record is not numeric, returns
    /// `None`.
    pub const fn as_numeric(&self) -> Option<f64> {
        match &self.data {
            RSResultData::Numeric(numeric) | RSResultData::Metric(numeric) => Some(*numeric),
            RSResultData::HybridMetric(_)
            | RSResultData::Union(_)
            | RSResultData::Intersection(_)
            | RSResultData::Term(_)
            | RSResultData::Virtual => None,
        }
    }

    /// Get this record as a mutable numeric record if possible. If the record is not numeric,
    /// returns `None`.
    pub const fn as_numeric_mut(&mut self) -> Option<&mut f64> {
        match &mut self.data {
            RSResultData::Numeric(numeric) | RSResultData::Metric(numeric) => Some(numeric),
            RSResultData::HybridMetric(_)
            | RSResultData::Union(_)
            | RSResultData::Intersection(_)
            | RSResultData::Term(_)
            | RSResultData::Virtual => None,
        }
    }

    /// Get a reference to the term record of this index result without checking its kind. The caller
    /// must ensure that this is a term record, else invoking this method will cause undefined
    /// behavior.
    ///
    /// # Safety
    ///
    /// 1. `Self::is_term()` must return `true` for `self`.
    pub unsafe fn as_term_unchecked_mut(&mut self) -> &mut RSTermRecord<'index> {
        debug_assert!(
            self.is_term(),
            "Invariant violation: `as_term_unchecked_mut` was invoked on a non-term `RSIndexResult` \
             instance that didn't actually contain a term. It was a {}",
            self.data.kind()
        );

        match &mut self.data {
            RSResultData::Term(term) => term,
            RSResultData::Union(_)
            | RSResultData::Intersection(_)
            | RSResultData::Virtual
            | RSResultData::Numeric(_)
            | RSResultData::Metric(_)
            | RSResultData::HybridMetric(_) => {
                // SAFETY: unreachable because of safety condition 1
                unsafe { std::hint::unreachable_unchecked() }
            }
        }
    }

    /// Get this record as a term record if possible. If the record is not term, returns
    /// `None`.
    pub const fn as_term(&self) -> Option<&RSTermRecord<'index>> {
        match &self.data {
            RSResultData::Term(term) => Some(term),
            RSResultData::Union(_)
            | RSResultData::Intersection(_)
            | RSResultData::Virtual
            | RSResultData::Numeric(_)
            | RSResultData::Metric(_)
            | RSResultData::HybridMetric(_) => None,
        }
    }

    /// Get the aggregate result associated with this record
    /// **without checking the discriminant**.
    ///
    /// # Safety
    ///
    /// 1. `Self::is_aggregate` must return `true` for `self`.
    pub unsafe fn as_aggregate_unchecked(&self) -> Option<&RSAggregateResult<'index>> {
        debug_assert!(
            self.is_aggregate(),
            "Invariant violation: `as_aggregate_unchecked` was invoked on an `IndexResult` \
            instance that didn't actually contain an aggregate! It was a {}",
            self.data.kind()
        );
        match &self.data {
            RSResultData::Union(agg)
            | RSResultData::Intersection(agg)
            | RSResultData::HybridMetric(agg) => Some(agg),
            RSResultData::Term(_)
            | RSResultData::Virtual
            | RSResultData::Numeric(_)
            | RSResultData::Metric(_) => {
                // SAFETY:
                // - Thanks to safety precondition 1., we'll never reach this statement.
                unsafe { std::hint::unreachable_unchecked() }
            }
        }
    }

    /// Get this record as an aggregate result if possible. If the record is not an aggregate,
    /// returns `None`.
    pub const fn as_aggregate(&self) -> Option<&RSAggregateResult<'index>> {
        match &self.data {
            RSResultData::Union(agg)
            | RSResultData::Intersection(agg)
            | RSResultData::HybridMetric(agg) => Some(agg),
            RSResultData::Term(_)
            | RSResultData::Virtual
            | RSResultData::Numeric(_)
            | RSResultData::Metric(_) => None,
        }
    }

    /// Get this record as a mutable aggregate result if possible. If the record is not an
    /// aggregate, returns `None`.
    pub const fn as_aggregate_mut(&mut self) -> Option<&mut RSAggregateResult<'index>> {
        match &mut self.data {
            RSResultData::Union(agg)
            | RSResultData::Intersection(agg)
            | RSResultData::HybridMetric(agg) => Some(agg),
            RSResultData::Term(_)
            | RSResultData::Virtual
            | RSResultData::Numeric(_)
            | RSResultData::Metric(_) => None,
        }
    }

    /// True if this is an aggregate kind
    pub const fn is_aggregate(&self) -> bool {
        matches!(
            self.data,
            RSResultData::Intersection(_) | RSResultData::Union(_) | RSResultData::HybridMetric(_)
        )
    }

    /// True if this is a numeric kind
    const fn is_numeric(&self) -> bool {
        matches!(
            self.data,
            RSResultData::Numeric(_) | RSResultData::Metric(_)
        )
    }

    /// True if this is a term kind
    const fn is_term(&self) -> bool {
        matches!(self.data, RSResultData::Term(_))
    }

    /// Is this result some copy type
    pub const fn is_copy(&self) -> bool {
        match self.data {
            RSResultData::Union(RSAggregateResult::Owned { .. })
            | RSResultData::Intersection(RSAggregateResult::Owned { .. })
            | RSResultData::HybridMetric(RSAggregateResult::Owned { .. })
            | RSResultData::Term(RSTermRecord::Owned { .. }) => true,
            RSResultData::Union(RSAggregateResult::Borrowed { .. })
            | RSResultData::Intersection(RSAggregateResult::Borrowed { .. })
            | RSResultData::HybridMetric(RSAggregateResult::Borrowed { .. })
            | RSResultData::Term(RSTermRecord::Borrowed { .. })
            | RSResultData::Virtual
            | RSResultData::Numeric(_)
            | RSResultData::Metric(_) => false,
        }
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
    pub fn push_borrowed(&mut self, child: &'index RSIndexResult) {
        match &mut self.data {
            RSResultData::Union(agg)
            | RSResultData::Intersection(agg)
            | RSResultData::HybridMetric(agg) => {
                agg.push_borrowed(child);

                self.doc_id = child.doc_id;
                self.freq += child.freq;
                self.field_mask |= child.field_mask;

                // SAFETY: we know both arguments are valid `RSIndexResult` types
                unsafe {
                    RSYieldableMetric_Concat(&mut self.metrics, child.metrics);
                }
            }
            RSResultData::Term(_)
            | RSResultData::Virtual
            | RSResultData::Numeric(_)
            | RSResultData::Metric(_) => {}
        }
    }

    /// Get a child at the given index if this is an aggregate record. Returns `None` if this is not
    /// an aggregate record or if the index is out-of-bounds.
    pub fn get(&self, index: usize) -> Option<&RSIndexResult<'index>> {
        match &self.data {
            RSResultData::Union(agg)
            | RSResultData::Intersection(agg)
            | RSResultData::HybridMetric(agg) => agg.get(index),
            RSResultData::Term(_)
            | RSResultData::Virtual
            | RSResultData::Numeric(_)
            | RSResultData::Metric(_) => None,
        }
    }

    /// Create an owned copy of this index result, allocating new memory for the contained data.
    ///
    /// The returned result may borrow the term data from the original result.
    pub fn to_owned<'a>(&'a self) -> RSIndexResult<'a> {
        let metrics = if !self.metrics.is_null() {
            // SAFETY: we know metric is a valid pointer to `RSYieldableMetric` because we created
            // it in a constructor. We also know it is not NULL because of the check above.
            unsafe { RSYieldableMetrics_Clone(self.metrics) }
        } else {
            ptr::null_mut()
        };

        RSIndexResult {
            doc_id: self.doc_id,
            dmd: self.dmd,
            field_mask: self.field_mask,
            freq: self.freq,
            data: self.data.to_owned(),
            metrics,
            weight: self.weight,
        }
    }

    /// If this is an aggregate result, then add a heap owned child to it. Also updates the
    /// following of this record:
    /// - The document ID will inherit the new child added
    /// - The child's frequency will contribute to this result
    /// - The child's field mask will contribute to this result's field mask
    /// - If the child has metrics, then they will be concatenated to this result's metrics
    ///
    /// If this is not an aggregate result, then nothing happens. Use [`Self::is_aggregate()`] first
    /// to make sure this is an aggregate result.
    pub fn push_boxed(&mut self, child: Box<RSIndexResult<'index>>) {
        match &mut self.data {
            RSResultData::Union(agg)
            | RSResultData::Intersection(agg)
            | RSResultData::HybridMetric(agg) => {
                self.doc_id = child.doc_id;
                self.freq += child.freq;
                self.field_mask |= child.field_mask;

                // SAFETY: we know both arguments are valid `RSIndexResult` types
                unsafe {
                    RSYieldableMetric_Concat(&mut self.metrics, child.metrics);
                }

                agg.push_boxed(child);
            }
            RSResultData::Term(_)
            | RSResultData::Virtual
            | RSResultData::Numeric(_)
            | RSResultData::Metric(_) => {}
        }
    }

    /// Get a mutable reference to the child at the given index, if it is an aggregate record.
    /// `None` is returned if this is not an aggregate record or if the index is out-of-bounds.
    pub fn get_mut(&mut self, index: usize) -> Option<&mut Self> {
        match &mut self.data {
            RSResultData::Union(agg)
            | RSResultData::Intersection(agg)
            | RSResultData::HybridMetric(agg) => agg.get_mut(index),
            RSResultData::Term(_)
            | RSResultData::Virtual
            | RSResultData::Numeric(_)
            | RSResultData::Metric(_) => None,
        }
    }
}

impl Drop for RSIndexResult<'_> {
    fn drop(&mut self) {
        if !self.metrics.is_null() {
            // SAFETY: we know `self` still exists because we are in `drop`. We also know the C type is
            // the same since it was autogenerated from the Rust type
            unsafe {
                ResultMetrics_Free(self.metrics);
            }
        }
    }
}
