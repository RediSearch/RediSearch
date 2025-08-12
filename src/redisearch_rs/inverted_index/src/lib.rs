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
    mem::ManuallyDrop,
    ops::DerefMut,
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
#[derive(PartialEq)]
pub struct RSOffsetVector<'a> {
    /// At this point the data ownership is still managed by the caller.
    // TODO: switch to a Cow once the caller code has been ported to Rust.
    pub data: *mut c_char,
    pub len: u32,
    /// data may be borrowed from the reader.
    /// The data pointer does not allow lifetime so use a PhantomData to carry the lifetime for it instead.
    _phantom: PhantomData<&'a ()>,
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
#[derive(PartialEq)]
pub struct RSTermRecord<'a> {
    /// The term that brought up this record
    pub term: *mut RSQueryTerm,

    /// The encoded offsets in which the term appeared in the document
    pub offsets: RSOffsetVector<'a>,
}

impl<'a> RSTermRecord<'a> {
    /// Create a new term record without term pointer and offsets.
    pub fn new() -> Self {
        Self {
            term: ptr::null_mut(),
            offsets: RSOffsetVector::empty(),
        }
    }

    /// Create a new term with the given term pointer and offsets.
    pub fn with_term(term: *mut RSQueryTerm, offsets: RSOffsetVector<'a>) -> RSTermRecord<'a> {
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

#[bitflags]
#[repr(u32)]
#[derive(Copy, Clone, Debug, PartialEq)]
/// cbindgen:prefix-with-name=true
pub enum RSResultType {
    Union = 1,
    Intersection = 2,
    Term = 4,
    Virtual = 8,
    Numeric = 16,
    Metric = 32,
    HybridMetric = 64,
}

pub type RSResultTypeMask = BitFlags<RSResultType, u32>;

/// Represents an aggregate array of values in an index record.
///
/// The C code should always use `AggregateResult_New` to construct a new instance of this type
/// using Rust since the internals cannot be constructed directly in C. The reason is because of
/// the `LowMemoryThinVec` which needs to exist in Rust's memory space to ensure its memory is
/// managed correctly.
/// cbindgen:rename-all=CamelCase
#[repr(C)]
#[derive(Debug, PartialEq)]
pub struct RSAggregateResult<'a> {
    /// The records making up this aggregate result
    ///
    /// The `RSAggregateResult` is part of a union in [`RSIndexResultData`], so it needs to have a
    /// known size. The std `Vec` won't have this since it is not `#[repr(C)]`, so we use our
    /// own `LowMemoryThinVec` type which is `#[repr(C)]` and has a known size instead.
    records: LowMemoryThinVec<*const RSIndexResult<'a>>,

    /// A map of the aggregate type of the underlying records
    type_mask: RSResultTypeMask,
    /// The lifetime is actually on `RsIndexResult` but it is stored as a pointer which does not
    /// support lifetimes. So use a PhantomData to carry the lifetime for it instead.
    _phantom: PhantomData<&'a ()>,
}

impl<'a> RSAggregateResult<'a> {
    /// Create a new empty aggregate result with the given capacity
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            records: LowMemoryThinVec::with_capacity(cap),
            type_mask: RSResultTypeMask::empty(),
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
    pub fn type_mask(&self) -> RSResultTypeMask {
        self.type_mask
    }

    /// Get an iterator over the children of this aggregate result
    pub fn iter(&'a self) -> RSAggregateResultIter<'a> {
        RSAggregateResultIter {
            agg: self,
            index: 0,
        }
    }

    /// Get the child at the given index, if it exists
    ///
    /// # Safety
    /// The caller must ensure that the memory at the given index is still valid
    pub fn get(&self, index: usize) -> Option<&RSIndexResult<'_>> {
        if let Some(result_addr) = self.records.get(index) {
            // SAFETY: The caller is to guarantee that the memory at `result_addr` is still valid.
            Some(unsafe { &**result_addr })
        } else {
            None
        }
    }

    /// Reset the aggregate result, clearing all children and resetting the type mask.
    ///
    /// Note, this does not deallocate the children pointers, it just resets the count and type
    /// mask. The owner of the children pointers is responsible for deallocating them when needed.
    pub fn reset(&mut self) {
        self.records.clear();
        self.type_mask = RSResultTypeMask::empty();
    }

    /// Add a child to the aggregate result and update the type mask
    ///
    /// # Safety
    /// The given `child` has to stay valid for the lifetime of this aggregate result. Else reading
    /// the child with [`Self::get()`] will cause undefined behavior.
    pub fn push(&mut self, child: &RSIndexResult) {
        self.records.push(child as *const _ as *mut _);

        self.type_mask |= child.result_type;
    }
}

/// An iterator over the results in an [`RSAggregateResult`].
pub struct RSAggregateResultIter<'a> {
    agg: &'a RSAggregateResult<'a>,
    index: usize,
}

impl<'a> Iterator for RSAggregateResultIter<'a> {
    type Item = &'a RSIndexResult<'a>;

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
pub struct RSAggregateResultIterOwned<'a> {
    agg: RSAggregateResult<'a>,
    index: usize,
}

impl<'a> Iterator for RSAggregateResultIterOwned<'a> {
    type Item = Box<RSIndexResult<'a>>;

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

impl<'a> IntoIterator for RSAggregateResult<'a> {
    type Item = Box<RSIndexResult<'a>>;

    type IntoIter = RSAggregateResultIterOwned<'a>;

    fn into_iter(self) -> Self::IntoIter {
        RSAggregateResultIterOwned {
            agg: self,
            index: 0,
        }
    }
}

/// Represents a virtual result in an index record.
#[repr(C)]
#[derive(Debug, PartialEq)]
pub struct RSVirtualResult;

/// Holds the actual data of an ['IndexResult']
#[repr(C)]
pub union RSIndexResultData<'a> {
    pub agg: ManuallyDrop<RSAggregateResult<'a>>,
    pub term: ManuallyDrop<RSTermRecord<'a>>,
    pub num: ManuallyDrop<RSNumericRecord>,
    pub virt: ManuallyDrop<RSVirtualResult>,
}

/// The result of an inverted index
/// cbindgen:field-names=[docId, dmd, fieldMask, freq, offsetsSz, data, type, isCopy, metrics, weight]
#[repr(C)]
pub struct RSIndexResult<'a> {
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

    data: RSIndexResultData<'a>,

    /// The type of data stored at ['Self::data']
    pub result_type: RSResultType,

    /// We mark copied results so we can treat them a bit differently on deletion, and pool them if
    /// we want
    pub is_copy: bool,

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

impl<'a> RSIndexResult<'a> {
    /// Create a new virtual index result
    pub fn virt() -> Self {
        Self {
            doc_id: 0,
            dmd: ptr::null(),
            field_mask: 0,
            freq: 0,
            offsets_sz: 0,
            data: RSIndexResultData {
                virt: ManuallyDrop::new(RSVirtualResult),
            },
            result_type: RSResultType::Virtual,
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
            data: RSIndexResultData {
                num: ManuallyDrop::new(RSNumericRecord(num)),
            },
            result_type: RSResultType::Numeric,
            weight: 1.0,
            ..Default::default()
        }
    }

    /// Create a new metric index result
    pub fn metric() -> Self {
        Self {
            field_mask: RS_FIELDMASK_ALL,
            data: RSIndexResultData {
                num: ManuallyDrop::new(RSNumericRecord(0.0)),
            },
            result_type: RSResultType::Metric,
            weight: 1.0,
            ..Default::default()
        }
    }

    /// Create a new intersection index result with the given capacity
    pub fn intersect(cap: usize) -> Self {
        Self {
            data: RSIndexResultData {
                agg: ManuallyDrop::new(RSAggregateResult::with_capacity(cap)),
            },
            result_type: RSResultType::Intersection,
            ..Default::default()
        }
    }

    /// Create a new union index result with the given capacity
    pub fn union(cap: usize) -> Self {
        Self {
            data: RSIndexResultData {
                agg: ManuallyDrop::new(RSAggregateResult::with_capacity(cap)),
            },
            result_type: RSResultType::Union,
            ..Default::default()
        }
    }

    /// Create a new hybrid metric index result
    pub fn hybrid_metric() -> Self {
        Self {
            data: RSIndexResultData {
                agg: ManuallyDrop::new(RSAggregateResult::with_capacity(2)),
            },
            result_type: RSResultType::HybridMetric,
            weight: 1.0,
            ..Default::default()
        }
    }

    /// Create a new term index result.
    pub fn term() -> Self {
        Self {
            data: RSIndexResultData {
                term: ManuallyDrop::new(RSTermRecord::new()),
            },
            result_type: RSResultType::Term,
            ..Default::default()
        }
    }

    /// Create a new `RSIndexResult` with a given `term`, `offsets`, `doc_id`, `field_mask`, and `freq`.
    pub fn term_with_term_ptr(
        term: *mut RSQueryTerm,
        offsets: RSOffsetVector<'a>,
        doc_id: t_docId,
        field_mask: t_fieldMask,
        freq: u32,
    ) -> RSIndexResult<'a> {
        let offsets_sz = offsets.len;
        Self {
            data: RSIndexResultData {
                term: ManuallyDrop::new(RSTermRecord::with_term(term, offsets)),
            },
            result_type: RSResultType::Term,
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

    /// Get this record as a numeric record if possible. If the record is not numeric, returns
    /// `None`.
    pub fn as_numeric(&self) -> Option<&RSNumericRecord> {
        if matches!(
            self.result_type,
            RSResultType::Numeric | RSResultType::Metric,
        ) {
            // SAFETY: We are guaranteed the record data is numeric because of the check we just
            // did on the `result_type`.
            Some(unsafe { &self.data.num })
        } else {
            None
        }
    }

    /// Get this record as a mutable numeric record if possible. If the record is not numeric,
    /// returns `None`.
    pub fn as_numeric_mut(&mut self) -> Option<&mut RSNumericRecord> {
        if matches!(
            self.result_type,
            RSResultType::Numeric | RSResultType::Metric,
        ) {
            // SAFETY: We are guaranteed the record data is numeric because of the check we just
            // did on the `result_type`.
            Some(unsafe { &mut self.data.num })
        } else {
            None
        }
    }

    /// Get this record as a term record if possible. If the record is not term, returns
    /// `None`.
    pub fn as_term(&self) -> Option<&RSTermRecord<'_>> {
        if matches!(self.result_type, RSResultType::Term) {
            // SAFETY: We are guaranteed the record data is term because of the check we just
            // did on the `result_type`.
            Some(unsafe { &self.data.term })
        } else {
            None
        }
    }

    /// Get this record as a mutable term record if possible. If the record is not a term,
    /// returns `None`.
    pub fn as_term_mut(&mut self) -> Option<&mut RSTermRecord<'a>> {
        if matches!(self.result_type, RSResultType::Term) {
            // SAFETY: We are guaranteed the record data is term because of the check we just
            // did on the `result_type`.
            Some(unsafe { &mut self.data.term })
        } else {
            None
        }
    }

    /// Get this record as an aggregate result if possible. If the record is not an aggregate,
    /// returns `None`.
    pub fn as_aggregate(&self) -> Option<&RSAggregateResult<'a>> {
        if self.is_aggregate() {
            // SAFETY: We are guaranteed the record data is aggregate because of the check we just
            // did
            Some(unsafe { &self.data.agg })
        } else {
            None
        }
    }

    /// Get this record as a mutable aggregate result if possible. If the record is not an
    /// aggregate, returns `None`.
    pub fn as_aggregate_mut(&mut self) -> Option<&mut RSAggregateResult<'a>> {
        if self.is_aggregate() {
            // SAFETY: We are guaranteed the record data is aggregate because of the check we just
            // did
            Some(unsafe { &mut self.data.agg })
        } else {
            None
        }
    }

    /// True if this is an aggregate type
    pub fn is_aggregate(&self) -> bool {
        matches!(
            self.result_type,
            RSResultType::Intersection | RSResultType::Union | RSResultType::HybridMetric
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
        if self.is_aggregate() {
            // SAFETY: we know the data will be an aggregate because we just checked the type
            let agg = unsafe { &mut self.data.agg };

            agg.push(child);

            self.doc_id = child.doc_id;
            self.freq += child.freq;
            self.field_mask |= child.field_mask;

            // SAFETY: we know both arguments are valid `RSIndexResult` types
            unsafe {
                IndexResult_ConcatMetrics(self, child);
            }
        }
    }

    /// Get a child at the given index if this is an aggregate record. Returns `None` if this is not
    /// an aggregate record or if the index is out-of-bounds.
    pub fn get(&self, index: usize) -> Option<&RSIndexResult<'_>> {
        if self.is_aggregate() {
            // SAFETY: we know the data will be an aggregate because we just checked the type
            let agg = unsafe { &self.data.agg };

            agg.get(index)
        } else {
            None
        }
    }
}

impl Drop for RSIndexResult<'_> {
    fn drop(&mut self) {
        // SAFETY: we know `self` still exists because we are in `drop`. We also know the C type is
        // the same since it was autogenerated from the Rust type
        unsafe {
            ResultMetrics_Free(self);
        }

        match self.result_type {
            RSResultType::Union | RSResultType::Intersection | RSResultType::HybridMetric => {
                // SAFETY: we just checked the type to ensure the union has aggregated data
                let agg = unsafe { &mut self.data.agg };

                // SAFETY: we are in `drop` so nothing else will have access to this union type
                let agg = unsafe { ManuallyDrop::take(agg) };

                if self.is_copy {
                    for child in agg.into_iter() {
                        drop(child);
                    }
                }
            }
            RSResultType::Term => {
                // SAFETY: we just checked the type to ensure the unior has term data
                let term = unsafe { &mut self.data.term };

                if self.is_copy {
                    // SAFETY: we know the C type is the same because it was autogenerated from the Rust type
                    unsafe {
                        Term_Offset_Data_Free(term.deref_mut() as *mut _);
                    }
                } else {
                    // SAFETY: we know the C type is the same because it was autogenerated from the Rust type
                    unsafe {
                        Term_Free(term.term);
                    }
                }

                // SAFETY: we are in `drop` so nothing else will have access to this union type
                unsafe {
                    ManuallyDrop::drop(term);
                }
            }
            RSResultType::Numeric | RSResultType::Metric => {
                // SAFETY: we just checked the type to ensure the union has numeric data
                let num = unsafe { &mut self.data.num };

                // SAFETY: we are in `drop` so nothing else will have access to this union type
                unsafe {
                    ManuallyDrop::drop(num);
                }
            }
            RSResultType::Virtual => {
                // SAFETY: we just checked the type to ensure the union has virtual data
                let virt = unsafe { &mut self.data.virt };

                // SAFETY: we are in `drop` so nothing else will have access to this union type
                unsafe {
                    ManuallyDrop::drop(virt);
                }
            }
        }
    }
}

impl Debug for RSIndexResult<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("RSIndexResult");

        d.field("doc_id", &self.doc_id)
            .field("dmd", &self.dmd)
            .field("field_mask", &self.field_mask)
            .field("freq", &self.freq)
            .field("offsets_sz", &self.offsets_sz);

        match self.result_type {
            RSResultType::Numeric | RSResultType::Metric => {
                d.field(
                    "data.num",
                    // SAFETY: we just checked the type to ensure the data union has numeric data
                    unsafe { &self.data.num },
                );
            }
            RSResultType::Union | RSResultType::Intersection | RSResultType::HybridMetric => {
                d.field(
                    "data.agg",
                    // SAFETY: we just checked the type to ensure the data union has aggregate data
                    unsafe { &self.data.agg },
                );
            }
            RSResultType::Term => {
                d.field(
                    "data.term",
                    // SAFETY: we just checked the type to ensure the data union has term data
                    unsafe { &self.data.term },
                );
            }
            RSResultType::Virtual => {}
        }

        d.field("result_type", &self.result_type)
            .field("is_copy", &self.is_copy)
            .field("metrics", &self.metrics)
            .field("weight", &self.weight)
            .finish()
    }
}

impl PartialEq for RSIndexResult<'_> {
    fn eq(&self, other: &Self) -> bool {
        if !(self.doc_id == other.doc_id
            && self.dmd == other.dmd
            && self.field_mask == other.field_mask
            && self.freq == other.freq
            && self.offsets_sz == other.offsets_sz
            && self.result_type == other.result_type
            && self.is_copy == other.is_copy
            && self.metrics == other.metrics
            && self.weight == other.weight)
        {
            return false;
        }

        match self.result_type {
            RSResultType::Numeric | RSResultType::Metric => {
                // SAFETY: we just checked the type of self to ensure the data union has numeric data
                let self_num = unsafe { &self.data.num };

                // SAFETY: from the previous checks we already know `other` has the same result
                // type as `self`. Therefore `other` also has numeric data in its union.
                let other_num = unsafe { &other.data.num };

                self_num == other_num
            }
            RSResultType::Union | RSResultType::Intersection | RSResultType::HybridMetric => {
                // SAFETY: we just checked the type of self to ensure the data union has aggregate data
                let self_agg = unsafe { &self.data.agg };

                // SAFETY: from the previous checks we already know `other` has the same result
                // type as `self`. Therefore `other` also has aggregate data in its union.
                let other_agg = unsafe { &other.data.agg };

                self_agg == other_agg
            }
            RSResultType::Term => {
                // SAFETY: we just checked the type of self to ensure the data union has term data
                let self_term = unsafe { &self.data.term };

                // SAFETY: from the previous checks we already know `other` has the same result
                // type as `self`. Therefore `other` also has term data in its union.
                let other_term = unsafe { &other.data.term };

                self_term == other_term
            }
            RSResultType::Virtual => true,
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
    fn decode<'a>(
        &self,
        cursor: &mut Cursor<&'a [u8]>,
        base: t_docId,
    ) -> std::io::Result<RSIndexResult<'a>>;

    /// Like `[Decoder::decode]`, but it skips all entries whose document ID is lower than `target`.
    ///
    /// Returns `None` if no record has a document ID greater than or equal to `target`.
    fn seek<'a>(
        &self,
        cursor: &mut Cursor<&'a [u8]>,
        base: t_docId,
        target: t_docId,
    ) -> std::io::Result<Option<RSIndexResult<'a>>> {
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
pub struct IndexReader<'a, D> {
    /// The block of the inverted index that is being read from. This might be used to determine the
    /// base document ID for delta calculations.
    blocks: &'a Vec<IndexBlock>,

    /// The decoder used to decode the records from the index blocks.
    decoder: D,

    /// The current position in the block that is being read from.
    current_buffer: Cursor<&'a [u8]>,

    /// The current block that is being read from. This might be used to determine the base document
    /// ID for delta calculations and to read the next record from the block.
    current_block: &'a IndexBlock,

    /// The index of the current block in the `blocks` vector. This is used to keep track of
    /// which block we are currently reading from, especially when the current buffer is empty and we
    /// need to move to the next block.
    current_block_idx: usize,

    /// The last document ID that was read from the index. This is used to determine the base
    /// document ID for delta calculations.
    last_doc_id: t_docId,
}

impl<'a, D: Decoder> IndexReader<'a, D> {
    /// Create a new index reader that reads from the given blocks using the provided decoder.
    ///
    /// # Panic
    /// This function will panic if the `blocks` vector is empty. The reader expects at least one block to read from.
    pub fn new(blocks: &'a Vec<IndexBlock>, decoder: D) -> Self {
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
    pub fn next_record(&mut self) -> std::io::Result<Option<RSIndexResult<'_>>> {
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

impl<'a, I: Iterator<Item = RSIndexResult<'a>>> SkipDuplicatesReader<I> {
    /// Create a new skip duplicates reader over the given inner iterator.
    pub fn new(inner: I) -> Self {
        Self {
            last_doc_id: 0,
            inner,
        }
    }
}

impl<'a, I: Iterator<Item = RSIndexResult<'a>>> Iterator for SkipDuplicatesReader<I> {
    type Item = RSIndexResult<'a>;

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

impl<'a, I: Iterator<Item = RSIndexResult<'a>>> FilterMaskReader<I> {
    /// Create a new filter mask reader with the given mask and inner iterator
    pub fn new(mask: t_fieldMask, inner: I) -> Self {
        Self { mask, inner }
    }
}

impl<'a, I: Iterator<Item = RSIndexResult<'a>>> Iterator for FilterMaskReader<I> {
    type Item = RSIndexResult<'a>;

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
