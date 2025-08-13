/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{
    ffi::c_char,
    fmt::Debug,
    io::{BufRead, Cursor, Seek, Write},
    marker::PhantomData,
    ptr,
};

use enumflags2::{BitFlags, bitflags};
use ffi::{FieldMask, RS_FIELDMASK_ALL};
pub use ffi::{RSDocumentMetadata, RSQueryTerm, RSYieldableMetric, t_docId, t_fieldMask};
use low_memory_thin_vec::LowMemoryThinVec;

pub mod doc_ids_only;
pub mod fields_offsets;
pub mod fields_only;
pub mod freqs_fields;
pub mod freqs_offsets;
pub mod freqs_only;
pub mod full;
pub mod numeric;
pub mod offsets_only;
pub mod raw_doc_ids_only;
#[doc(hidden)]
pub mod test_utils;

// Manually define some C functions, because we'll create a circular dependency if we use the FFI
// crate to make them automatically.
unsafe extern "C" {
    /// Adds the metrics of a child [`RSIndexResult`] to the parent [`RSIndexResult`].
    ///
    /// # Safety
    /// Both should be valid `RSIndexResult` instances.
    #[allow(improper_ctypes)] // The doc_id in `RSIndexResult` might be a u128
    unsafe fn IndexResult_ConcatMetrics(parent: *mut RSIndexResult, child: *const RSIndexResult);

    /// Free the metrics inside an [`RSIndexResult`]
    ///
    /// # Safety
    /// The caller must ensure that the `result` pointer is valid and points to an `RSIndexResult`.
    #[allow(improper_ctypes)] // The doc_id in `RSIndexResult` might be a u128
    unsafe fn ResultMetrics_Free(result: *mut RSIndexResult);

    /// Free the data inside a [`RSTermRecord`]'s offset
    ///
    /// # Safety
    /// The caller must ensure that the `tr` pointer is valid and points to an `RSTermRecord`.
    unsafe fn Term_Offset_Data_Free(tr: *mut RSTermRecord);

    /// Free a [`RSQueryTerm`]
    ///
    /// # Safety
    /// The caller must ensure that the `t` pointer is valid and points to an `RSQueryTerm`.
    unsafe fn Term_Free(t: *mut RSQueryTerm);
}

/// Trait used to correctly derive the delta needed for different encoders
pub trait IdDelta
where
    Self: Sized,
{
    /// Try to convert a `u64` into this delta type. If the `u64` will be too big for this delta
    /// type, then `None` should be returned. Returning `None` will cause the [`InvertedIndex`]
    /// to make a new block so that it can have a zero delta.
    fn from_u64(delta: u64) -> Option<Self>;

    /// The delta value when the [`InvertedIndex`] makes a new block and needs a delta equal to `0`.
    fn zero() -> Self;
}

impl IdDelta for u32 {
    fn from_u64(delta: u64) -> Option<Self> {
        delta.try_into().ok()
    }

    fn zero() -> Self {
        0
    }
}

/// Represents a numeric value in an index record.
/// cbindgen:field-names=[value]
#[allow(rustdoc::broken_intra_doc_links)] // The field rename above breaks the intra-doc link
#[repr(C)]
#[derive(Debug, PartialEq)]
pub struct RSNumericRecord(pub f64);

/// Represents the encoded offsets of a term in a document. You can read the offsets by iterating
/// over it with RSIndexResult_IterateOffsets
#[repr(C)]
#[derive(Eq, PartialEq)]
pub struct RSOffsetVector<'index> {
    /// At this point the data ownership is still managed by the caller.
    // TODO: switch to a Cow once the caller code has been ported to Rust.
    pub data: *mut c_char,
    pub len: u32,
    /// data may be borrowed from the reader.
    /// The data pointer does not allow lifetime so use a PhantomData to carry the lifetime for it instead.
    _phantom: PhantomData<&'index ()>,
}

impl std::fmt::Debug for RSOffsetVector<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.data.is_null() {
            return write!(f, "RSOffsetVector(null)");
        }
        // SAFETY: `len` is guaranteed to be a valid length for the data pointer.
        let offsets =
            unsafe { std::slice::from_raw_parts(self.data as *const i8, self.len as usize) };

        write!(f, "RSOffsetVector {offsets:?}")
    }
}

impl RSOffsetVector<'_> {
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

/// Represents a single record of a document inside a term in the inverted index
#[repr(C)]
#[derive(Eq, PartialEq)]
pub struct RSTermRecord<'index> {
    /// The term that brought up this record
    pub term: *mut RSQueryTerm,

    /// The encoded offsets in which the term appeared in the document
    pub offsets: RSOffsetVector<'index>,
}

impl<'index> RSTermRecord<'index> {
    /// Create a new term record without term pointer and offsets.
    pub fn new() -> Self {
        Self {
            term: ptr::null_mut(),
            offsets: RSOffsetVector::empty(),
        }
    }

    /// Create a new term with the given term pointer and offsets.
    pub fn with_term(
        term: *mut RSQueryTerm,
        offsets: RSOffsetVector<'index>,
    ) -> RSTermRecord<'index> {
        Self { term, offsets }
    }
}

/// Wrapper to provide better Debug output for `RSQueryTerm`.
/// Can be removed once `RSQueryTerm` is fully ported to Rust.
struct QueryTermDebug(*mut RSQueryTerm);

impl std::fmt::Debug for QueryTermDebug {
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

impl std::fmt::Debug for RSTermRecord<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RSTermRecord")
            .field("term", &QueryTermDebug(self.term))
            .field("offsets", &self.offsets)
            .finish()
    }
}

impl Default for RSTermRecord<'_> {
    fn default() -> Self {
        Self::new()
    }
}

pub type RSResultKindMask = BitFlags<RSResultKind, u8>;

/// Represents an aggregate array of values in an index record.
///
/// The C code should always use `AggregateResult_New` to construct a new instance of this type
/// using Rust since the internals cannot be constructed directly in C. The reason is because of
/// the `LowMemoryThinVec` which needs to exist in Rust's memory space to ensure its memory is
/// managed correctly.
/// cbindgen:rename-all=CamelCase
#[repr(C)]
#[derive(Debug, Eq, PartialEq)]
pub struct RSAggregateResult<'index, 'children> {
    /// The records making up this aggregate result
    ///
    /// The `RSAggregateResult` is part of a union in [`RSIndexResultData`], so it needs to have a
    /// known size. The std `Vec` won't have this since it is not `#[repr(C)]`, so we use our
    /// own `LowMemoryThinVec` type which is `#[repr(C)]` and has a known size instead.
    records: LowMemoryThinVec<*const RSIndexResult<'index, 'children>>,

    /// A map of the aggregate kind of the underlying records
    kind_mask: RSResultKindMask,

    /// The lifetime is actually on the `*const RSIndexResult` children stored in the `records`
    /// field. But since these are stored as a pointers which do not support lifetimes, we need to
    /// use a PhantomData to carry the lifetime for each child record instead.
    _phantom: PhantomData<&'children ()>,
}

impl<'index, 'children> RSAggregateResult<'index, 'children> {
    /// Create a new empty aggregate result with the given capacity
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            records: LowMemoryThinVec::with_capacity(cap),
            kind_mask: RSResultKindMask::empty(),
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
    pub fn kind_mask(&self) -> RSResultKindMask {
        self.kind_mask
    }

    /// Get an iterator over the children of this aggregate result
    pub fn iter(&'index self) -> RSAggregateResultIter<'index, 'children> {
        RSAggregateResultIter {
            agg: self,
            index: 0,
        }
    }

    /// Get the child at the given index, if it exists
    ///
    /// # Safety
    /// The caller must ensure that the memory at the given index is still valid
    pub fn get(&self, index: usize) -> Option<&RSIndexResult<'index, 'children>> {
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
        self.kind_mask = RSResultKindMask::empty();
    }

    /// Add a child to the aggregate result and update the kind mask
    ///
    /// # Safety
    /// The given `child` has to stay valid for the lifetime of this aggregate result. Else reading
    /// the child with [`Self::get()`] will cause undefined behavior.
    pub fn push(&mut self, child: &RSIndexResult) {
        self.records.push(child as *const _ as *mut _);

        self.kind_mask |= child.data.kind();
    }
}

/// An iterator over the results in an [`RSAggregateResult`].
pub struct RSAggregateResultIter<'index, 'aggregate_children> {
    agg: &'index RSAggregateResult<'index, 'aggregate_children>,
    index: usize,
}

impl<'index, 'aggregate_children> Iterator for RSAggregateResultIter<'index, 'aggregate_children> {
    type Item = &'index RSIndexResult<'index, 'aggregate_children>;

    /// Get the next item in the iterator
    ///
    /// # Safety
    /// The caller must ensure that all memory pointers in the aggregate result are still valid.
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(result) = self.agg.get(self.index) {
            self.index += 1;
            Some(result)
        } else {
            None
        }
    }
}

/// An owned iterator over the results in an [`RSAggregateResult`].
pub struct RSAggregateResultIterOwned<'index, 'aggregate_children> {
    agg: RSAggregateResult<'index, 'aggregate_children>,
    index: usize,
}

impl<'index, 'aggregate_children> Iterator
    for RSAggregateResultIterOwned<'index, 'aggregate_children>
{
    type Item = Box<RSIndexResult<'index, 'aggregate_children>>;

    /// Get the next item as a `Box<RSIndexResult>`
    ///
    /// # Safety
    /// The box can only be taken if the items in this aggregate result have been cloned and is
    /// therefore owned by the `RSAggregateResult`.
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(result_ptr) = self.agg.records.get(self.index) {
            self.index += 1;

            // SAFETY: The caller is to ensure the `RSAggregateResult` was cloned to allow getting
            // the pointer as a `Box<RSIndexResult>`.
            unsafe { Some(Box::from_raw(*result_ptr as *mut _)) }
        } else {
            None
        }
    }
}

impl<'index, 'children> IntoIterator for RSAggregateResult<'index, 'children> {
    type Item = Box<RSIndexResult<'index, 'children>>;

    type IntoIter = RSAggregateResultIterOwned<'index, 'children>;

    fn into_iter(self) -> Self::IntoIter {
        RSAggregateResultIterOwned {
            agg: self,
            index: 0,
        }
    }
}

/// Represents a virtual result in an index record.
#[repr(C)]
#[derive(Debug, Eq, PartialEq)]
pub struct RSVirtualResult;

/// A C-style discriminant for [`RSResultData`].
///
/// # Implementation notes
///
/// We need a standalone C-style discriminant to get `bitflags` to generate a
/// dedicated bitmask type. Unfortunately, we can't apply `#[bitflags]` directly
/// on [`RSResultData`] since `bitflags` doesn't support enum with data in
/// their variants, nor lifetime parameters.
///
/// The discriminant values must match *exactly* the ones specified
/// on [`RSResultData`].
#[bitflags]
#[repr(u8)]
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum RSResultKind {
    Union = 1,
    Intersection = 2,
    Term = 4,
    Virtual = 8,
    Numeric = 16,
    Metric = 32,
    HybridMetric = 64,
}

/// Holds the actual data of an ['IndexResult']
///
/// These enum values should stay in sync with [`RSResultKind`], so that the C union generated matches
/// the bitflags on [`RSResultKindMask`]
///
/// The `'index` lifetime is linked to the [`IndexBlock`] when decoding borrows from the block.
/// While the `'aggregate_children` lifetime is linked to [`RSAggregateResult`]s that is holding
/// raw pointers to results.
#[repr(u8)]
#[derive(Debug, PartialEq)]
/// cbindgen:prefix-with-name=true
pub enum RSResultData<'index, 'aggregate_children> {
    Union(RSAggregateResult<'index, 'aggregate_children>) = 1,
    Intersection(RSAggregateResult<'index, 'aggregate_children>) = 2,
    Term(RSTermRecord<'index>) = 4,
    Virtual(RSVirtualResult) = 8,
    Numeric(RSNumericRecord) = 16,
    Metric(RSNumericRecord) = 32,
    HybridMetric(RSAggregateResult<'index, 'aggregate_children>) = 64,
}

impl RSResultData<'_, '_> {
    fn kind(&self) -> RSResultKind {
        match self {
            RSResultData::Union(_) => RSResultKind::Union,
            RSResultData::Intersection(_) => RSResultKind::Intersection,
            RSResultData::Term(_) => RSResultKind::Term,
            RSResultData::Virtual(_) => RSResultKind::Virtual,
            RSResultData::Numeric(_) => RSResultKind::Numeric,
            RSResultData::Metric(_) => RSResultKind::Metric,
            RSResultData::HybridMetric(_) => RSResultKind::HybridMetric,
        }
    }
}

/// The result of an inverted index
/// cbindgen:rename-all=CamelCase
#[repr(C)]
#[derive(Debug, PartialEq)]
pub struct RSIndexResult<'index, 'aggregate_children> {
    /// The document ID of the result
    pub doc_id: t_docId,

    /// Some metadata about the result document
    pub dmd: *const RSDocumentMetadata,

    /// The aggregate field mask of all the records in this result
    pub field_mask: t_fieldMask,

    /// The total frequency of all the records in this result
    pub freq: u32,

    /// For term records only. This is used as an optimization, allowing the result to be loaded
    /// directly into memory
    pub offsets_sz: u32,

    /// The actual data of the result
    data: RSResultData<'index, 'aggregate_children>,

    /// We mark copied results so we can treat them a bit differently on deletion, and pool them if
    /// we want
    pub is_copy: bool,

    /// Holds an array of metrics yielded by the different iterators in the AST
    pub metrics: *mut RSYieldableMetric,

    /// Relative weight for scoring calculations. This is derived from the result's iterator weight
    pub weight: f64,
}

impl Default for RSIndexResult<'_, '_> {
    fn default() -> Self {
        Self::virt()
    }
}

impl<'index, 'aggregate_children> RSIndexResult<'index, 'aggregate_children> {
    /// Create a new virtual index result
    pub fn virt() -> Self {
        Self {
            doc_id: 0,
            dmd: ptr::null(),
            field_mask: 0,
            freq: 1,
            offsets_sz: 0,
            data: RSResultData::Virtual(RSVirtualResult),
            is_copy: false,
            metrics: ptr::null_mut(),
            weight: 0.0,
        }
    }

    /// Create a new numeric index result with the given number
    pub fn numeric(num: f64) -> Self {
        Self {
            field_mask: RS_FIELDMASK_ALL,
            freq: 1,
            data: RSResultData::Numeric(RSNumericRecord(num)),
            weight: 1.0,
            ..Default::default()
        }
    }

    /// Create a new metric index result
    pub fn metric() -> Self {
        Self {
            field_mask: RS_FIELDMASK_ALL,
            data: RSResultData::Metric(RSNumericRecord(0.0)),
            weight: 1.0,
            ..Default::default()
        }
    }

    /// Create a new intersection index result with the given capacity
    pub fn intersect(cap: usize) -> Self {
        Self {
            data: RSResultData::Intersection(RSAggregateResult::with_capacity(cap)),
            ..Default::default()
        }
    }

    /// Create a new union index result with the given capacity
    pub fn union(cap: usize) -> Self {
        Self {
            data: RSResultData::Union(RSAggregateResult::with_capacity(cap)),
            ..Default::default()
        }
    }

    /// Create a new hybrid metric index result
    pub fn hybrid_metric() -> Self {
        Self {
            data: RSResultData::HybridMetric(RSAggregateResult::with_capacity(2)),
            weight: 1.0,
            ..Default::default()
        }
    }

    /// Create a new term index result.
    pub fn term() -> Self {
        Self {
            data: RSResultData::Term(RSTermRecord::new()),
            freq: 1,
            ..Default::default()
        }
    }

    /// Create a new `RSIndexResult` with a given `term`, `offsets`, `doc_id`, `field_mask`, and `freq`.
    pub fn term_with_term_ptr(
        term: *mut RSQueryTerm,
        offsets: RSOffsetVector<'index>,
        doc_id: t_docId,
        field_mask: t_fieldMask,
        freq: u32,
    ) -> RSIndexResult<'index, 'aggregate_children> {
        let offsets_sz = offsets.len;
        Self {
            data: RSResultData::Term(RSTermRecord::with_term(term, offsets)),
            doc_id,
            field_mask,
            freq,
            offsets_sz,
            is_copy: false,
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
    pub fn kind(&self) -> RSResultKind {
        self.data.kind()
    }

    /// Get this record as a numeric record if possible. If the record is not numeric, returns
    /// `None`.
    pub fn as_numeric(&self) -> Option<&RSNumericRecord> {
        match &self.data {
            RSResultData::Numeric(numeric) | RSResultData::Metric(numeric) => Some(numeric),
            RSResultData::HybridMetric(_)
            | RSResultData::Union(_)
            | RSResultData::Intersection(_)
            | RSResultData::Term(_)
            | RSResultData::Virtual(_) => None,
        }
    }

    /// Get this record as a mutable numeric record if possible. If the record is not numeric,
    /// returns `None`.
    pub fn as_numeric_mut(&mut self) -> Option<&mut RSNumericRecord> {
        match &mut self.data {
            RSResultData::Numeric(numeric) | RSResultData::Metric(numeric) => Some(numeric),
            RSResultData::HybridMetric(_)
            | RSResultData::Union(_)
            | RSResultData::Intersection(_)
            | RSResultData::Term(_)
            | RSResultData::Virtual(_) => None,
        }
    }

    /// Get this record as a term record if possible. If the record is not term, returns
    /// `None`.
    pub fn as_term(&self) -> Option<&RSTermRecord<'index>> {
        match &self.data {
            RSResultData::Term(term) => Some(term),
            RSResultData::Union(_)
            | RSResultData::Intersection(_)
            | RSResultData::Virtual(_)
            | RSResultData::Numeric(_)
            | RSResultData::Metric(_)
            | RSResultData::HybridMetric(_) => None,
        }
    }

    /// Get this record as a mutable term record if possible. If the record is not a term,
    /// returns `None`.
    pub fn as_term_mut(&mut self) -> Option<&mut RSTermRecord<'index>> {
        match &mut self.data {
            RSResultData::Term(term) => Some(term),
            RSResultData::Union(_)
            | RSResultData::Intersection(_)
            | RSResultData::Virtual(_)
            | RSResultData::Numeric(_)
            | RSResultData::Metric(_)
            | RSResultData::HybridMetric(_) => None,
        }
    }

    /// Get this record as an aggregate result if possible. If the record is not an aggregate,
    /// returns `None`.
    pub fn as_aggregate(&self) -> Option<&RSAggregateResult<'index, 'aggregate_children>> {
        match &self.data {
            RSResultData::Union(agg)
            | RSResultData::Intersection(agg)
            | RSResultData::HybridMetric(agg) => Some(agg),
            RSResultData::Term(_)
            | RSResultData::Virtual(_)
            | RSResultData::Numeric(_)
            | RSResultData::Metric(_) => None,
        }
    }

    /// Get this record as a mutable aggregate result if possible. If the record is not an
    /// aggregate, returns `None`.
    pub fn as_aggregate_mut(
        &mut self,
    ) -> Option<&mut RSAggregateResult<'index, 'aggregate_children>> {
        match &mut self.data {
            RSResultData::Union(agg)
            | RSResultData::Intersection(agg)
            | RSResultData::HybridMetric(agg) => Some(agg),
            RSResultData::Term(_)
            | RSResultData::Virtual(_)
            | RSResultData::Numeric(_)
            | RSResultData::Metric(_) => None,
        }
    }

    /// True if this is an aggregate kind
    pub fn is_aggregate(&self) -> bool {
        matches!(
            self.data,
            RSResultData::Intersection(_) | RSResultData::Union(_) | RSResultData::HybridMetric(_)
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
    pub fn push(&mut self, child: &RSIndexResult) {
        match &mut self.data {
            RSResultData::Union(agg)
            | RSResultData::Intersection(agg)
            | RSResultData::HybridMetric(agg) => {
                agg.push(child);

                self.doc_id = child.doc_id;
                self.freq += child.freq;
                self.field_mask |= child.field_mask;

                // SAFETY: we know both arguments are valid `RSIndexResult` types
                unsafe {
                    IndexResult_ConcatMetrics(self, child);
                }
            }
            RSResultData::Term(_)
            | RSResultData::Virtual(_)
            | RSResultData::Numeric(_)
            | RSResultData::Metric(_) => {}
        }
    }

    /// Get a child at the given index if this is an aggregate record. Returns `None` if this is not
    /// an aggregate record or if the index is out-of-bounds.
    pub fn get(&self, index: usize) -> Option<&RSIndexResult<'index, 'aggregate_children>> {
        match &self.data {
            RSResultData::Union(agg)
            | RSResultData::Intersection(agg)
            | RSResultData::HybridMetric(agg) => agg.get(index),
            RSResultData::Term(_)
            | RSResultData::Virtual(_)
            | RSResultData::Numeric(_)
            | RSResultData::Metric(_) => None,
        }
    }
}

impl Drop for RSIndexResult<'_, '_> {
    fn drop(&mut self) {
        // SAFETY: we know `self` still exists because we are in `drop`. We also know the C type is
        // the same since it was autogenerated from the Rust type
        unsafe {
            ResultMetrics_Free(self);
        }

        // Take ownership of the internal data to be able to call `into_iter()` below.
        // `into_iter()` will convert each pointer back to a `Box` to allow it to be cleaned up
        // correctly.
        let mut data = RSResultData::Virtual(RSVirtualResult);
        std::mem::swap(&mut self.data, &mut data);

        match data {
            RSResultData::Union(agg)
            | RSResultData::Intersection(agg)
            | RSResultData::HybridMetric(agg) => {
                if self.is_copy {
                    for child in agg.into_iter() {
                        drop(child);
                    }
                }
            }
            RSResultData::Term(mut term) => {
                if self.is_copy {
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
            RSResultData::Numeric(_numeric) | RSResultData::Metric(_numeric) => {}
            RSResultData::Virtual(_virtual) => {}
        }
    }
}

/// Encoder to write a record into an index
pub trait Encoder {
    /// Document ids are represented as `u64`s and stored using delta-encoding.
    ///
    /// Some encoders can't encode arbitrarily large id deltas—e.g. they might be limited to `u32::MAX` or
    /// another subset of the `u64` value range.
    ///
    /// This associated type can be used by each encoder to specify which range they support, thus
    /// allowing the inverted index to work around their limitations accordingly (i.e. by creating new blocks).
    type Delta: IdDelta;

    /// Does this encoder allow the same document to appear in the index multiple times. We need to
    /// allow duplicates to support multi-value JSON fields.
    ///
    /// Defaults to `false`.
    const ALLOW_DUPLICATES: bool = false;

    /// The suggested number of entries that can be written in a single block. Defaults to 100.
    const RECOMMENDED_BLOCK_ENTRIES: usize = 100;

    /// Write the record to the writer and return the number of bytes written. The delta is the
    /// pre-computed difference between the current document ID and the last document ID written.
    fn encode<W: Write + Seek>(
        &mut self,
        writer: W,
        delta: Self::Delta,
        record: &RSIndexResult,
    ) -> std::io::Result<usize>;

    /// Returns the base value that should be used for any delta calculations
    fn delta_base(block: &IndexBlock) -> t_docId {
        block.last_doc_id
    }
}

/// Decoder to read records from an index
pub trait Decoder {
    /// Decode the next record from the reader. If any delta values are decoded, then they should
    /// add to the `base` document ID to get the actual document ID.
    fn decode<'index>(
        &self,
        cursor: &mut Cursor<&'index [u8]>,
        base: t_docId,
    ) -> std::io::Result<RSIndexResult<'index, 'static>>;

    /// Like `[Decoder::decode]`, but it skips all entries whose document ID is lower than `target`.
    ///
    /// Returns `None` if no record has a document ID greater than or equal to `target`.
    fn seek<'index>(
        &self,
        cursor: &mut Cursor<&'index [u8]>,
        base: t_docId,
        target: t_docId,
    ) -> std::io::Result<Option<RSIndexResult<'index, 'static>>> {
        loop {
            match self.decode(cursor, base) {
                Ok(record) if record.doc_id >= target => {
                    return Ok(Some(record));
                }
                Ok(_) => continue,
                Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
                Err(err) => return Err(err),
            }
        }
    }

    /// Returns the base value to use for any delta calculations
    fn base_id(_block: &IndexBlock, last_doc_id: t_docId) -> t_docId {
        last_doc_id
    }
}

/// An inverted index is a data structure that maps terms to their occurrences in documents. It is
/// used to efficiently search for documents that contain specific terms.
pub struct InvertedIndex<E> {
    /// The blocks of the index. Each block contains a set of entries for a specific range of
    /// document IDs. The entries and blocks themselves are ordered by document ID, so the first
    /// block contains entries for the lowest document IDs, and the last block contains entries for
    /// the highest document IDs.
    blocks: Vec<IndexBlock>,

    /// Number of unique documents in the index. This is not the total number of entries, but rather the
    /// number of unique documents that have been indexed.
    n_unique_docs: usize,

    /// The encoder to use when adding new entries to the index
    encoder: E,
}

/// Each `IndexBlock` contains a set of entries for a specific range of document IDs. The entries
/// are ordered by document ID, so the first entry in the block has the lowest document ID, and the
/// last entry has the highest document ID. The block also contains a buffer that is used to
/// store the encoded entries. The buffer is dynamically resized as needed when new entries are
/// added to the block.
#[allow(dead_code)] // TODO: remove after the DocId encoder is implemented
pub struct IndexBlock {
    /// The first document ID in this block. This is used to determine the range of document IDs
    /// that this block covers.
    first_doc_id: t_docId,

    /// The last document ID in this block. This is used to determine the range of document IDs
    /// that this block covers.
    last_doc_id: t_docId,

    /// The total number of non-unique entries in this block
    num_entries: usize,

    /// The encoded entries in this block
    buffer: Vec<u8>,
}

impl IndexBlock {
    const SIZE: usize = std::mem::size_of::<Self>();

    /// Make a new index block with primed with the initial doc ID. The next entry written into
    /// the block should be for this doc ID else the block will contain incoherent data.
    ///
    /// This returns the block and how much memory grew by.
    pub fn new(doc_id: t_docId) -> (Self, usize) {
        let this = Self {
            first_doc_id: doc_id,
            last_doc_id: doc_id,
            num_entries: 0,
            buffer: Vec::new(),
        };
        let buf_cap = this.buffer.capacity();

        (this, Self::SIZE + buf_cap)
    }

    fn writer(&mut self) -> Cursor<&mut Vec<u8>> {
        let pos = self.buffer.len();
        let mut buffer = Cursor::new(&mut self.buffer);

        buffer.set_position(pos as u64);

        buffer
    }
}

impl<E: Encoder> InvertedIndex<E> {
    /// Create a new inverted index with the given encoder. The encoder is used to write new
    /// entries to the index.
    pub fn new(encoder: E) -> Self {
        Self {
            blocks: Vec::new(),
            n_unique_docs: 0,
            encoder,
        }
    }

    /// Add a new record to the index and return by how much memory grew. It is expected that
    /// the document ID of the record is greater than or equal the last document ID in the index.
    pub fn add_record(&mut self, record: &RSIndexResult) -> std::io::Result<usize> {
        let doc_id = record.doc_id;

        let same_doc = match (
            E::ALLOW_DUPLICATES,
            self.last_doc_id().map(|d| d == doc_id).unwrap_or_default(),
        ) {
            (true, true) => true,
            (false, true) => {
                // Even though we might allow duplicate document IDs, this encoder does not allow
                // it since it will contain redundant information. Therefore, we are skipping this
                // record.
                return Ok(0);
            }
            (_, false) => false,
        };

        // We take ownership of the block since we are going to keep using self. So we can't have a
        // mutable reference to the block we are working with at the same time.
        let (mut block, mut mem_growth) = self.take_block(doc_id, same_doc);

        let delta_base = E::delta_base(&block);
        debug_assert!(
            doc_id >= delta_base,
            "documents should be encoded in the order of their IDs"
        );
        let delta = doc_id.wrapping_sub(delta_base);

        let delta = match E::Delta::from_u64(delta) {
            Some(delta) => delta,
            None => {
                // The delta is too large for this encoder. We need to create a new block.
                // Since the new block is empty, we'll start with `delta` equal to 0.
                let (new_block, block_size) = IndexBlock::new(doc_id);

                // We won't use the block so make sure to put it back
                self.blocks.push(block);
                block = new_block;
                mem_growth += block_size;

                E::Delta::zero()
            }
        };

        let buf_cap = block.buffer.capacity();
        let writer = block.writer();
        let _bytes_written = self.encoder.encode(writer, delta, record)?;

        // We don't use `_bytes_written` returned by the encoder to determine by how much memory
        // grew because the buffer might have had enough capacity for the bytes in the encoding.
        // Instead we took the capacity of the buffer before the write and now check by how much it
        // has increased (if any).
        let buf_growth = block.buffer.capacity() - buf_cap;

        block.num_entries += 1;
        block.last_doc_id = doc_id;

        // We took ownership of the block so put it back
        self.blocks.push(block);

        if !same_doc {
            self.n_unique_docs += 1;
        }

        Ok(buf_growth + mem_growth)
    }

    fn last_doc_id(&self) -> Option<t_docId> {
        self.blocks.last().map(|b| b.last_doc_id)
    }

    /// Take a block that can be written to and report by how much memory grew
    fn take_block(&mut self, doc_id: t_docId, same_doc: bool) -> (IndexBlock, usize) {
        if self.blocks.is_empty() ||
            // If the block is full
        (!same_doc
            && self
                .blocks
                .last()
                .expect("we just confirmed there are blocks")
                .num_entries
                >= E::RECOMMENDED_BLOCK_ENTRIES)
        {
            IndexBlock::new(doc_id)
        } else {
            (
                self.blocks
                    .pop()
                    .expect("to get the last block since we know there is one"),
                0,
            )
        }
    }
}

/// Reader that is able to read the records from an [`InvertedIndex`]
pub struct IndexReader<'index, D> {
    /// The block of the inverted index that is being read from. This might be used to determine the
    /// base document ID for delta calculations.
    blocks: &'index Vec<IndexBlock>,

    /// The decoder used to decode the records from the index blocks.
    decoder: D,

    /// The current position in the block that is being read from.
    current_buffer: Cursor<&'index [u8]>,

    /// The current block that is being read from. This might be used to determine the base document
    /// ID for delta calculations and to read the next record from the block.
    current_block: &'index IndexBlock,

    /// The index of the current block in the `blocks` vector. This is used to keep track of
    /// which block we are currently reading from, especially when the current buffer is empty and we
    /// need to move to the next block.
    current_block_idx: usize,

    /// The last document ID that was read from the index. This is used to determine the base
    /// document ID for delta calculations.
    last_doc_id: t_docId,
}

impl<'index, D: Decoder> IndexReader<'index, D> {
    /// Create a new index reader that reads from the given blocks using the provided decoder.
    ///
    /// # Panic
    /// This function will panic if the `blocks` vector is empty. The reader expects at least one block to read from.
    pub fn new(blocks: &'index Vec<IndexBlock>, decoder: D) -> Self {
        debug_assert!(
            !blocks.is_empty(),
            "IndexReader should not be created with an empty block list"
        );

        let first_block = blocks.first().expect("to have at least one block");

        Self {
            blocks,
            decoder,
            current_buffer: Cursor::new(&first_block.buffer),
            current_block: first_block,
            current_block_idx: 0,
            last_doc_id: first_block.first_doc_id,
        }
    }

    /// Read the next record from the index. If there are no more records to read, then `None` is returned.
    pub fn next_record(&mut self) -> std::io::Result<Option<RSIndexResult<'index, 'static>>> {
        // Check if the current buffer is empty. The GC might clean out a block so we have to
        // continue checking until we find a block with data.
        while self.current_buffer.fill_buf()?.is_empty() {
            let Some(next_block) = self.blocks.get(self.current_block_idx + 1) else {
                // No more blocks to read from
                return Ok(None);
            };

            self.current_block_idx += 1;
            self.current_block = next_block;
            self.last_doc_id = next_block.first_doc_id;
            self.current_buffer = Cursor::new(&next_block.buffer);
        }

        let base = D::base_id(self.current_block, self.last_doc_id);
        let result = self.decoder.decode(&mut self.current_buffer, base)?;

        self.last_doc_id = result.doc_id;

        Ok(Some(result))
    }
}

/// A reader that skips duplicate records in the index. It is used to ensure that the same document
/// ID is not returned multiple times in the results. This is useful when the index contains
/// multiple entries for the same document ID, such as when the document has multiple values for a
/// field or when the document is indexed multiple times.
pub struct SkipDuplicatesReader<I> {
    /// The last document ID that was read from the index. This is used to determine if the next
    /// record is a duplicate of the last one.
    last_doc_id: t_docId,

    /// The inner reader that is used to read the records from the index.
    inner: I,
}

impl<'index, I: Iterator<Item = RSIndexResult<'index, 'static>>> SkipDuplicatesReader<I> {
    /// Create a new skip duplicates reader over the given inner iterator.
    pub fn new(inner: I) -> Self {
        Self {
            last_doc_id: 0,
            inner,
        }
    }
}

impl<'index, I: Iterator<Item = RSIndexResult<'index, 'static>>> Iterator
    for SkipDuplicatesReader<I>
{
    type Item = RSIndexResult<'index, 'static>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let next = self.inner.next()?;

            if next.doc_id == self.last_doc_id {
                continue;
            }

            self.last_doc_id = next.doc_id;

            return Some(next);
        }
    }
}

/// A reader that filters out records that do not match a given field mask. It is used to
/// filter records in an index based on their field mask, allowing only those that match the
/// specified mask to be returned.
pub struct FilterMaskReader<I> {
    /// Mask which a record needs to match to be valid
    mask: t_fieldMask,

    /// The inner reader that will be used to read the records from the index.
    inner: I,
}

impl<'index, I: Iterator<Item = RSIndexResult<'index, 'static>>> FilterMaskReader<I> {
    /// Create a new filter mask reader with the given mask and inner iterator
    pub fn new(mask: t_fieldMask, inner: I) -> Self {
        Self { mask, inner }
    }
}

impl<'index, I: Iterator<Item = RSIndexResult<'index, 'static>>> Iterator for FilterMaskReader<I> {
    type Item = RSIndexResult<'index, 'static>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let next = self.inner.next()?;

            if next.field_mask & self.mask == 0 {
                continue;
            }

            return Some(next);
        }
    }
}

#[cfg(test)]
mod tests;
