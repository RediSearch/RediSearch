/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

mod proximity;

use std::ptr;

use super::aggregate::RawAggregateResult;
use super::kind::RSResultKind;
use super::metrics::{MetricsVec, RawMetricsVec};
use super::offsets::{RSOffsetVector, RawOffsetSlice};
use super::result_data::RawResultData;
use super::term_record::RawTermRecord;
use ffi::RSDocumentMetadata;
use query_term::RSQueryTerm;
use ref_mode::{Active, Ref, SharedPtr};
use rqe_core::{DocId, FieldMask, RS_FIELDMASK_ALL};

/// Builder for creating [`RawIndexResult`] instances.
///
/// Constructed via `RawIndexResult::build_*` methods. For the
/// [`RSResultKind::Term`] kind, [`RawIndexResult::build_term`] returns a
/// specialized [`RawTermResultBuilder`] with additional setters for
/// term-specific fields.
pub struct RawIndexResultBuilder<R: Ref> {
    doc_id: DocId,
    field_mask: FieldMask,
    freq: u32,
    data: RawResultData<R>,
    weight: f64,
}

/// Specialized builder for creating [`RawIndexResult`] instances of the
/// [`RSResultKind::Term`] kind.
///
/// Created via [`RawIndexResult::build_term`]. Use [`Self::borrowed_record`]
/// or [`Self::owned_record`] to set the term record data before calling
/// [`Self::build`].
pub struct RawTermResultBuilder<R: Ref> {
    doc_id: DocId,
    field_mask: FieldMask,
    freq: u32,
    weight: f64,
    record: TermBuilderRecord<R>,
}

/// Internal enum holding the term record data for the builder.
enum TermBuilderRecord<R: Ref> {
    Borrowed {
        term: Option<Box<RSQueryTerm>>,
        offsets: RawOffsetSlice<R>,
    },
    Owned {
        term: Option<SharedPtr<R, RSQueryTerm>>,
        offsets: RSOffsetVector,
    },
    FullyOwned {
        term: Option<Box<RSQueryTerm>>,
        offsets: RSOffsetVector,
    },
}

impl<R: Ref> RawIndexResultBuilder<R> {
    /// Set the document ID of this record
    pub const fn doc_id(mut self, doc_id: DocId) -> Self {
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

    /// Create a builder for a virtual index result
    const fn virt() -> Self {
        Self {
            doc_id: 0,
            field_mask: 0,
            freq: 0,
            data: RawResultData::Virtual,
            weight: 0.0,
        }
    }

    /// Create a builder for a numeric index result with the given number
    const fn numeric(num: f64) -> Self {
        Self {
            doc_id: 0,
            field_mask: RS_FIELDMASK_ALL,
            freq: 1,
            data: RawResultData::Numeric(num),
            weight: 1.0,
        }
    }

    /// Create a builder for a metric index result with the given number
    const fn metric(num: f64) -> Self {
        Self {
            doc_id: 0,
            field_mask: RS_FIELDMASK_ALL,
            freq: 0,
            data: RawResultData::Metric(num),
            weight: 1.0,
        }
    }

    /// Create a builder for an intersection index result with the given capacity
    fn intersect(cap: usize) -> Self {
        Self {
            doc_id: 0,
            field_mask: 0,
            freq: 0,
            data: RawResultData::Intersection(RawAggregateResult::borrowed_with_capacity(cap)),
            weight: 0.0,
        }
    }

    /// Create a builder for a union index result with the given capacity
    fn union(cap: usize) -> Self {
        Self {
            doc_id: 0,
            field_mask: 0,
            freq: 0,
            data: RawResultData::Union(RawAggregateResult::borrowed_with_capacity(cap)),
            weight: 0.0,
        }
    }

    /// Create a builder for a hybrid metric index result
    fn hybrid_metric() -> Self {
        Self {
            doc_id: 0,
            field_mask: 0,
            freq: 0,
            data: RawResultData::HybridMetric(RawAggregateResult::owned_with_capacity(2)),
            weight: 1.0,
        }
    }

    /// Build the final [`RawIndexResult`]
    #[inline]
    pub fn build(self) -> RawIndexResult<R> {
        RawIndexResult {
            doc_id: self.doc_id,
            dmd: ptr::null(),
            field_mask: self.field_mask,
            freq: self.freq,
            data: self.data,
            metrics: RawMetricsVec::new(),
            weight: self.weight,
        }
    }
}

impl<R: Ref> RawTermResultBuilder<R> {
    /// Create a new term result builder
    const fn new() -> Self {
        Self {
            doc_id: 0,
            field_mask: 0,
            freq: 1,
            weight: 0.0,
            record: TermBuilderRecord::Borrowed {
                term: None,
                offsets: RawOffsetSlice::empty(),
            },
        }
    }

    /// Set the document ID of this record
    pub const fn doc_id(mut self, doc_id: DocId) -> Self {
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

    /// Set the term record data with borrowed offsets and an optional query term.
    ///
    /// Produces an [`RawTermRecord::Borrowed`] variant.
    #[inline]
    pub fn borrowed_record(
        mut self,
        term: Option<Box<RSQueryTerm>>,
        offsets: RawOffsetSlice<R>,
    ) -> Self {
        self.record = TermBuilderRecord::Borrowed { term, offsets };
        self
    }

    /// Set the term record data with owned offsets and an optional borrowed query term.
    ///
    /// Produces an [`RawTermRecord::Owned`] variant. Use this when the offset
    /// data does not live long enough to be borrowed by the result.
    #[inline]
    pub fn owned_record(
        mut self,
        term: Option<SharedPtr<R, RSQueryTerm>>,
        offsets: RSOffsetVector,
    ) -> Self {
        self.record = TermBuilderRecord::Owned { term, offsets };
        self
    }

    /// Set the term record data with an owned query term (wrapped in a
    /// [`Box`]) and owned offsets.
    ///
    /// Produces an [`RawTermRecord::FullyOwned`] variant. Use this when the
    /// offsets do not live long enough to be borrowed by the result, but the
    /// caller still wants the record to own the query term (as with
    /// [`Self::borrowed_record`]). This is the right choice for readers that
    /// decode offsets from a transient source such as a disk page.
    #[inline]
    pub fn fully_owned_record(
        mut self,
        term: Option<Box<RSQueryTerm>>,
        offsets: RSOffsetVector,
    ) -> Self {
        self.record = TermBuilderRecord::FullyOwned { term, offsets };
        self
    }

    /// Build the final [`RawIndexResult`]
    #[inline]
    pub fn build(self) -> RawIndexResult<R> {
        let data = match self.record {
            TermBuilderRecord::Borrowed { term, offsets } => {
                RawResultData::Term(RawTermRecord::Borrowed { term, offsets })
            }
            TermBuilderRecord::Owned { term, offsets } => {
                RawResultData::Term(RawTermRecord::Owned { term, offsets })
            }
            TermBuilderRecord::FullyOwned { term, offsets } => {
                RawResultData::Term(RawTermRecord::FullyOwned { term, offsets })
            }
        };
        RawIndexResult {
            doc_id: self.doc_id,
            dmd: ptr::null(),
            field_mask: self.field_mask,
            freq: self.freq,
            data,
            metrics: RawMetricsVec::new(),
            weight: self.weight,
        }
    }
}

/// The result of an inverted index
///
/// The `R: Ref` parameter selects between [`Active<'index>`] mode (the
/// data references inside this result are valid for `'index` and the
/// inverted index read lock is held) and [`ref_mode::Suspended`] mode
/// (the data references are inert raw pointers that survive lock
/// release/reacquire cycles).
#[cheadergen::config(rename_all = "camelCase")]
#[derive(Debug)]
#[repr(C)]
pub struct RawIndexResult<R: Ref> {
    /// The document ID of the result
    pub doc_id: DocId,

    /// Some metadata about the result document
    pub dmd: *const RSDocumentMetadata,

    /// The aggregate field mask of all the records in this result
    pub field_mask: FieldMask,

    /// The total frequency of all the records in this result
    pub freq: u32,

    /// The actual data of the result
    pub(super) data: RawResultData<R>,

    /// Holds an array of metrics yielded by the different iterators in the AST.
    ///
    /// Backed by [`ThinVec`](thin_vec::ThinVec) — pointer-sized, no
    /// allocation when empty.
    pub metrics: RawMetricsVec<R>,

    /// Relative weight for scoring calculations. This is derived from the result's iterator weight
    pub weight: f64,
}

/// The [`Active`] instantiation of [`RawIndexResult`].
///
/// This is the only mode that crosses the C boundary today; the suspended
/// counterpart is forthcoming.
#[cheadergen::config(export)]
pub type RSIndexResult<'a> = RawIndexResult<Active<'a>>;

impl<'a> PartialEq for RSIndexResult<'a> {
    fn eq(&self, other: &Self) -> bool {
        let Self {
            doc_id,
            dmd,
            field_mask,
            freq,
            data,
            metrics,
            weight,
        } = self;
        let Self {
            doc_id: o_doc_id,
            dmd: o_dmd,
            field_mask: o_field_mask,
            freq: o_freq,
            data: o_data,
            metrics: o_metrics,
            weight: o_weight,
        } = other;
        doc_id == o_doc_id
            && dmd == o_dmd
            && field_mask == o_field_mask
            && freq == o_freq
            && data == o_data
            && metrics == o_metrics
            && weight == o_weight
    }
}

impl<'a> Default for RSIndexResult<'a> {
    fn default() -> Self {
        Self::build_virt().build()
    }
}

impl<R: Ref> RawIndexResult<R> {
    /// Create a builder for a virtual index result
    pub const fn build_virt() -> RawIndexResultBuilder<R> {
        RawIndexResultBuilder::virt()
    }

    /// Create a builder for a numeric index result with the given number
    pub const fn build_numeric(num: f64) -> RawIndexResultBuilder<R> {
        RawIndexResultBuilder::numeric(num)
    }

    /// Create a builder for a metric index result with the given number
    pub const fn build_metric(num: f64) -> RawIndexResultBuilder<R> {
        RawIndexResultBuilder::metric(num)
    }

    /// Create a builder for an intersection index result with the given capacity
    pub fn build_intersect(cap: usize) -> RawIndexResultBuilder<R> {
        RawIndexResultBuilder::intersect(cap)
    }

    /// Create a builder for a union index result with the given capacity
    pub fn build_union(cap: usize) -> RawIndexResultBuilder<R> {
        RawIndexResultBuilder::union(cap)
    }

    /// Create a builder for a hybrid metric index result
    pub fn build_hybrid_metric() -> RawIndexResultBuilder<R> {
        RawIndexResultBuilder::hybrid_metric()
    }

    /// Create a specialized builder for a term index result
    pub const fn build_term() -> RawTermResultBuilder<R> {
        RawTermResultBuilder::new()
    }

    /// Get the kind of this index result
    pub const fn kind(&self) -> RSResultKind {
        self.data.kind()
    }

    /// Reset an aggregate result for reuse, clearing children, frequency,
    /// field mask, and metrics.
    pub fn reset_aggregate(&mut self) {
        self.doc_id = 0;
        self.freq = 0;
        self.field_mask = 0;
        match &mut self.data {
            RawResultData::Union(agg)
            | RawResultData::Intersection(agg)
            | RawResultData::HybridMetric(agg) => agg.reset(),
            RawResultData::Term(_)
            | RawResultData::Virtual
            | RawResultData::Numeric(_)
            | RawResultData::Metric(_) => {}
        }
        self.metrics.reset();
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
            "Invariant violation: `as_numeric_unchecked` was invoked on a non-numeric `RawIndexResult` \
             instance that didn't actually contain a numeric. It was a {}",
            self.kind()
        );

        match &self.data {
            RawResultData::Numeric(numeric) | RawResultData::Metric(numeric) => *numeric,
            RawResultData::Union(_)
            | RawResultData::Intersection(_)
            | RawResultData::Term(_)
            | RawResultData::Virtual
            | RawResultData::HybridMetric(_) => {
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
            "Invariant violation: `as_numeric_unchecked_mut` was invoked on a non-numeric `RawIndexResult` \
             instance that didn't actually contain a numeric. It was a {}",
            self.kind()
        );

        match &mut self.data {
            RawResultData::Numeric(numeric) | RawResultData::Metric(numeric) => numeric,
            RawResultData::Union(_)
            | RawResultData::Intersection(_)
            | RawResultData::Term(_)
            | RawResultData::Virtual
            | RawResultData::HybridMetric(_) => {
                // SAFETY: unreachable because of safety condition 1
                unsafe { std::hint::unreachable_unchecked() }
            }
        }
    }

    /// Get this record as a numeric record if possible. If the record is not numeric, returns
    /// `None`.
    pub const fn as_numeric(&self) -> Option<f64> {
        match &self.data {
            RawResultData::Numeric(numeric) | RawResultData::Metric(numeric) => Some(*numeric),
            RawResultData::HybridMetric(_)
            | RawResultData::Union(_)
            | RawResultData::Intersection(_)
            | RawResultData::Term(_)
            | RawResultData::Virtual => None,
        }
    }

    /// Get this record as a mutable numeric record if possible. If the record is not numeric,
    /// returns `None`.
    pub const fn as_numeric_mut(&mut self) -> Option<&mut f64> {
        match &mut self.data {
            RawResultData::Numeric(numeric) | RawResultData::Metric(numeric) => Some(numeric),
            RawResultData::HybridMetric(_)
            | RawResultData::Union(_)
            | RawResultData::Intersection(_)
            | RawResultData::Term(_)
            | RawResultData::Virtual => None,
        }
    }

    /// Get a reference to the term record of this index result without checking its kind. The caller
    /// must ensure that this is a term record, else invoking this method will cause undefined
    /// behavior.
    ///
    /// # Safety
    ///
    /// 1. `Self::is_term()` must return `true` for `self`.
    pub unsafe fn as_term_unchecked_mut(&mut self) -> &mut RawTermRecord<R> {
        debug_assert!(
            self.is_term(),
            "Invariant violation: `as_term_unchecked_mut` was invoked on a non-term `RawIndexResult` \
             instance that didn't actually contain a term. It was a {}",
            self.kind()
        );

        match &mut self.data {
            RawResultData::Term(term) => term,
            RawResultData::Union(_)
            | RawResultData::Intersection(_)
            | RawResultData::Virtual
            | RawResultData::Numeric(_)
            | RawResultData::Metric(_)
            | RawResultData::HybridMetric(_) => {
                // SAFETY: unreachable because of safety condition 1
                unsafe { std::hint::unreachable_unchecked() }
            }
        }
    }

    /// Get this record as a term record if possible. If the record is not term, returns
    /// `None`.
    pub const fn as_term(&self) -> Option<&RawTermRecord<R>> {
        match &self.data {
            RawResultData::Term(term) => Some(term),
            RawResultData::Union(_)
            | RawResultData::Intersection(_)
            | RawResultData::Virtual
            | RawResultData::Numeric(_)
            | RawResultData::Metric(_)
            | RawResultData::HybridMetric(_) => None,
        }
    }

    /// Get this record as a mutable term record if possible. If the record is not term, returns
    /// `None`.
    pub const fn as_term_mut(&mut self) -> Option<&mut RawTermRecord<R>> {
        match &mut self.data {
            RawResultData::Term(term) => Some(term),
            RawResultData::Union(_)
            | RawResultData::Intersection(_)
            | RawResultData::Virtual
            | RawResultData::Numeric(_)
            | RawResultData::Metric(_)
            | RawResultData::HybridMetric(_) => None,
        }
    }

    /// Get the aggregate result associated with this record
    /// **without checking the discriminant**.
    ///
    /// # Safety
    ///
    /// 1. `Self::is_aggregate` must return `true` for `self`.
    pub unsafe fn as_aggregate_unchecked(&self) -> Option<&RawAggregateResult<R>> {
        debug_assert!(
            self.is_aggregate(),
            "Invariant violation: `as_aggregate_unchecked` was invoked on an `IndexResult` \
            instance that didn't actually contain an aggregate! It was a {}",
            self.kind()
        );
        match &self.data {
            RawResultData::Union(agg)
            | RawResultData::Intersection(agg)
            | RawResultData::HybridMetric(agg) => Some(agg),
            RawResultData::Term(_)
            | RawResultData::Virtual
            | RawResultData::Numeric(_)
            | RawResultData::Metric(_) => {
                // SAFETY:
                // - Thanks to safety precondition 1., we'll never reach this statement.
                unsafe { std::hint::unreachable_unchecked() }
            }
        }
    }

    /// Get this record as an aggregate result if possible. If the record is not an aggregate,
    /// returns `None`.
    pub const fn as_aggregate(&self) -> Option<&RawAggregateResult<R>> {
        match &self.data {
            RawResultData::Union(agg)
            | RawResultData::Intersection(agg)
            | RawResultData::HybridMetric(agg) => Some(agg),
            RawResultData::Term(_)
            | RawResultData::Virtual
            | RawResultData::Numeric(_)
            | RawResultData::Metric(_) => None,
        }
    }

    /// Get this record as a mutable aggregate result if possible. If the record is not an
    /// aggregate, returns `None`.
    pub const fn as_aggregate_mut(&mut self) -> Option<&mut RawAggregateResult<R>> {
        match &mut self.data {
            RawResultData::Union(agg)
            | RawResultData::Intersection(agg)
            | RawResultData::HybridMetric(agg) => Some(agg),
            RawResultData::Term(_)
            | RawResultData::Virtual
            | RawResultData::Numeric(_)
            | RawResultData::Metric(_) => None,
        }
    }

    /// Get the mutable aggregate result associated with this record
    /// **without checking the discriminant**.
    ///
    /// # Safety
    ///
    /// 1. `Self::is_aggregate` must return `true` for `self`.
    pub unsafe fn as_aggregate_mut_unchecked(&mut self) -> Option<&mut RawAggregateResult<R>> {
        debug_assert!(
            self.is_aggregate(),
            "Invariant violation: `as_aggregate_mut_unchecked` was invoked on an `IndexResult` \
            instance that didn't actually contain an aggregate! It was a {}",
            self.kind()
        );
        match &mut self.data {
            RawResultData::Union(agg)
            | RawResultData::Intersection(agg)
            | RawResultData::HybridMetric(agg) => Some(agg),
            RawResultData::Term(_)
            | RawResultData::Virtual
            | RawResultData::Numeric(_)
            | RawResultData::Metric(_) => {
                // SAFETY:
                // - Thanks to safety precondition 1., we'll never reach this statement.
                unsafe { std::hint::unreachable_unchecked() }
            }
        }
    }

    /// True if this is an aggregate kind
    pub const fn is_aggregate(&self) -> bool {
        matches!(
            self.data,
            RawResultData::Intersection(_)
                | RawResultData::Union(_)
                | RawResultData::HybridMetric(_)
        )
    }

    /// True if this is a numeric kind
    const fn is_numeric(&self) -> bool {
        matches!(
            self.data,
            RawResultData::Numeric(_) | RawResultData::Metric(_)
        )
    }

    /// True if this is a term kind
    pub const fn is_term(&self) -> bool {
        matches!(self.data, RawResultData::Term(_))
    }

    /// Returns a mutable reference to the metrics collection.
    pub const fn metrics_mut(&mut self) -> &mut RawMetricsVec<R> {
        &mut self.metrics
    }

    /// Returns a reference to the metrics collection.
    pub const fn metrics_ref(&self) -> &RawMetricsVec<R> {
        &self.metrics
    }

    /// Is this result some copy type
    pub const fn is_copy(&self) -> bool {
        match self.data {
            RawResultData::Union(RawAggregateResult::Owned { .. })
            | RawResultData::Intersection(RawAggregateResult::Owned { .. })
            | RawResultData::HybridMetric(RawAggregateResult::Owned { .. })
            | RawResultData::Term(RawTermRecord::Owned { .. })
            | RawResultData::Term(RawTermRecord::FullyOwned { .. }) => true,
            RawResultData::Union(RawAggregateResult::Borrowed { .. })
            | RawResultData::Intersection(RawAggregateResult::Borrowed { .. })
            | RawResultData::HybridMetric(RawAggregateResult::Borrowed { .. })
            | RawResultData::Term(RawTermRecord::Borrowed { .. })
            | RawResultData::Virtual
            | RawResultData::Numeric(_)
            | RawResultData::Metric(_) => false,
        }
    }
}

impl<'a> RSIndexResult<'a> {
    /// Returns `true` when the term positions in this result satisfy the given
    /// proximity constraints.
    ///
    /// - `max_slop`: maximum allowed number of non-matched token slots between
    ///   consecutive terms. `None` disables the check entirely.
    /// - `in_order`: when `true`, terms must appear in the same order as the
    ///   child iterators.
    ///
    /// Returns `true` when `self` is not an aggregate, has ≤ 1 child, or ≤ 1
    /// child has meaningful offsets.
    ///
    /// # Preconditions
    ///
    /// At least one of `max_slop` or `in_order` must impose a constraint:
    /// `max_slop.is_some() || in_order` must hold.  If neither is set, the result
    /// is trivially `true` for every input and the call is pointless; callers are
    /// expected to short-circuit that case before invoking this function.
    pub fn is_within_range(&self, max_slop: Option<u32>, in_order: bool) -> bool {
        proximity::is_within_range(self, max_slop, in_order)
    }

    /// Debug-only assertion that `self.data == other.data`.
    ///
    /// This is a no-op in release builds.
    #[track_caller]
    pub fn assert_data(&self, other: &Self) {
        debug_assert_eq!(self.data, other.data);
    }

    /// If this is an aggregate result, then add a child to it. Also updates the following of this
    /// record:
    /// - `doc_id` is set to the child's doc_id (inherits, not accumulated)
    /// - `freq` is accumulated (`+=`) from the child's frequency
    /// - `field_mask` is OR'd with the child's field mask
    /// - `child_metrics` are concatenated (moved) into this result's metrics
    ///
    /// If this is not an aggregate result, then nothing happens. Use [`Self::is_aggregate()`] first
    /// to make sure this is an aggregate result.
    ///
    /// The caller must drain the child's metrics via `std::mem::take(&mut child.metrics)`
    /// before calling this method, and pass them as `child_metrics`.
    ///
    /// # Safety
    ///
    /// The given `child` has to stay valid for the lifetime of this index result. Else reading
    /// from this result will cause undefined behaviour.
    pub fn push_borrowed(
        &mut self,
        child: &'a RSIndexResult<'a>,
        mut child_metrics: MetricsVec<'a>,
    ) {
        debug_assert!(
            child.metrics.is_empty(),
            "child metrics must be drained by caller via std::mem::take()"
        );
        if !self.is_aggregate() {
            return;
        }

        self.doc_id = child.doc_id;
        self.freq += child.freq;
        self.field_mask |= child.field_mask;

        if !child_metrics.is_empty() {
            self.metrics.concat(&mut child_metrics);
        }

        if let Some(agg) = self.as_aggregate_mut() {
            agg.push_borrowed(child);
        }
    }

    /// Get a child at the given index if this is an aggregate record. Returns `None` if this is not
    /// an aggregate record or if the index is out-of-bounds.
    pub fn get(&self, index: usize) -> Option<&RSIndexResult<'a>> {
        match &self.data {
            RawResultData::Union(agg)
            | RawResultData::Intersection(agg)
            | RawResultData::HybridMetric(agg) => agg.get(index),
            RawResultData::Term(_)
            | RawResultData::Virtual
            | RawResultData::Numeric(_)
            | RawResultData::Metric(_) => None,
        }
    }

    /// Create an owned copy of this index result, allocating new memory for the contained data.
    ///
    /// The returned result may borrow the term data from the original result.
    pub fn to_owned(&'a self) -> RSIndexResult<'a> {
        RawIndexResult {
            doc_id: self.doc_id,
            dmd: self.dmd,
            field_mask: self.field_mask,
            freq: self.freq,
            data: self.data.to_owned(),
            metrics: self.metrics.clone(),
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
    pub fn push_boxed(&mut self, mut child: Box<RSIndexResult<'a>>) {
        if !self.is_aggregate() {
            return;
        }

        self.doc_id = child.doc_id;
        self.freq += child.freq;
        self.field_mask |= child.field_mask;

        let mut child_metrics = std::mem::take(&mut child.metrics);
        if !child_metrics.is_empty() {
            self.metrics.concat(&mut child_metrics);
        }

        if let Some(agg) = self.as_aggregate_mut() {
            agg.push_boxed(child);
        }
    }

    /// Get a mutable reference to the child at the given index, if it is an aggregate record.
    /// `None` is returned if this is not an aggregate record or if the index is out-of-bounds.
    pub fn get_mut(&mut self, index: usize) -> Option<&mut Self> {
        match &mut self.data {
            RawResultData::Union(agg)
            | RawResultData::Intersection(agg)
            | RawResultData::HybridMetric(agg) => agg.get_mut(index),
            RawResultData::Term(_)
            | RawResultData::Virtual
            | RawResultData::Numeric(_)
            | RawResultData::Metric(_) => None,
        }
    }
}
