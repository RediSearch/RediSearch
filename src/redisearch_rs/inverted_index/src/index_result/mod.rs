use std::{ffi::c_char, fmt::Debug, marker::PhantomData, ptr};

use enumflags2::{BitFlags, bitflags};
use ffi::{
    FieldMask, RS_FIELDMASK_ALL, RSDocumentMetadata, RSQueryTerm, RSYieldableMetric, t_docId,
    t_fieldMask,
};
use low_memory_thin_vec::LowMemoryThinVec;

pub mod raw;

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
    unsafe fn Term_Offset_Data_Free(tr: *mut raw::RSTermRecord);

    /// Free a [`RSQueryTerm`]
    ///
    /// # Safety
    /// The caller must ensure that the `t` pointer is valid and points to an `RSQueryTerm`.
    unsafe fn Term_Free(t: *mut RSQueryTerm);
}

/// Represents the encoded offsets of a term in a document. You can read the offsets by iterating
/// over it with RSIndexResult_IterateOffsets
#[derive(Debug, Eq, PartialEq)]
pub enum RSOffsetVector<'index> {
    Borrowed(RSOffsetVectorRef<'index>),
    Owned(RSOffsetVectorOwned),
}

#[derive(Eq, PartialEq)]
pub struct RSOffsetVectorRef<'index> {
    /// At this point the data ownership is still managed by the caller.
    pub data: *mut c_char,
    pub len: u32,
    /// data may be borrowed from the reader.
    /// The data pointer does not allow lifetime so use a PhantomData to carry the lifetime for it instead.
    _phantom: PhantomData<&'index ()>,
}

#[derive(Eq, PartialEq)]
pub struct RSOffsetVectorOwned {
    /// At this point the data ownership is still managed by the caller.
    pub data: *mut c_char,
    pub len: u32,
}

impl Debug for RSOffsetVectorRef<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.data.is_null() {
            return write!(f, "RSOffsetVectorRef(null)");
        }
        // SAFETY: `len` is guaranteed to be a valid length for the data pointer.
        let offsets =
            unsafe { std::slice::from_raw_parts(self.data as *const i8, self.len as usize) };

        write!(f, "RSOffsetVectorRef {offsets:?}")
    }
}

impl Debug for RSOffsetVectorOwned {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.data.is_null() {
            return write!(f, "RSOffsetVectorOwned(null)");
        }
        // SAFETY: `len` is guaranteed to be a valid length for the data pointer.
        let offsets =
            unsafe { std::slice::from_raw_parts(self.data as *const i8, self.len as usize) };

        write!(f, "RSOffsetVectorOwned {offsets:?}")
    }
}

impl RSOffsetVectorRef<'_> {
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

    fn offsets(&self) -> &[u8] {
        // SAFETY: `len` is guaranteed to be a valid length for the data pointer.
        unsafe { std::slice::from_raw_parts(self.data as *const u8, self.len as usize) }
    }
}

impl RSOffsetVectorOwned {
    /// Create a new, empty offset vector ready to receive data
    pub fn empty() -> Self {
        Self {
            data: ptr::null_mut(),
            len: 0,
        }
    }

    /// Create a new offset vector with the given data pointer and length.
    pub fn with_data(data: *mut c_char, len: u32) -> Self {
        Self { data, len }
    }

    fn offsets(&self) -> &[u8] {
        // SAFETY: `len` is guaranteed to be a valid length for the data pointer.
        unsafe { std::slice::from_raw_parts(self.data as *const u8, self.len as usize) }
    }
}

/// Represents a single record of a document inside a term in the inverted index
#[derive(Debug, Eq, PartialEq)]
pub enum RSTermRecord<'index> {
    Borrowed(RSTermRecordRef<'index>),
    Owned(RSTermRecordOwned),
}

#[derive(Eq, PartialEq)]
pub struct RSTermRecordRef<'index> {
    /// The term that brought up this record
    pub term: *mut RSQueryTerm,

    /// The encoded offsets in which the term appeared in the document
    pub offsets: RSOffsetVectorRef<'index>,
}

#[derive(Eq, PartialEq)]
pub struct RSTermRecordOwned {
    /// The term that brought up this record
    pub term: *mut RSQueryTerm,

    /// The encoded offsets in which the term appeared in the document
    pub offsets: RSOffsetVectorOwned,
}

impl<'index> RSTermRecord<'index> {
    /// Create a new term record without term pointer and offsets.
    pub fn new() -> Self {
        Self::Borrowed(RSTermRecordRef {
            term: ptr::null_mut(),
            offsets: RSOffsetVectorRef::empty(),
        })
    }

    /// Create a new term with the given term pointer and offsets.
    pub fn with_term(
        term: *mut RSQueryTerm,
        offsets: RSOffsetVectorRef<'index>,
    ) -> RSTermRecord<'index> {
        Self::Borrowed(RSTermRecordRef { term, offsets })
    }

    pub fn offsets(&self) -> &[u8] {
        match self {
            RSTermRecord::Borrowed(t) => t.offsets.offsets(),
            RSTermRecord::Owned(t) => t.offsets.offsets(),
        }
    }

    pub fn is_copy(&self) -> bool {
        matches!(self, Self::Owned(_))
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

impl Debug for RSTermRecordRef<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RSTermRecordRef")
            .field("term", &QueryTermDebug(self.term))
            .field("offsets", &self.offsets)
            .finish()
    }
}

impl Debug for RSTermRecordOwned {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RSTermRecordOwned")
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
#[derive(Debug, Eq, PartialEq)]
pub enum RSAggregateResult<'index, 'children> {
    Borrowed(RSAggregateResultRef<'index, 'children>),
    Owned(RSAggregateResultOwned),
}

#[derive(Debug, Eq, PartialEq)]
pub struct RSAggregateResultRef<'index, 'children> {
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

#[derive(Debug, Eq, PartialEq)]
pub struct RSAggregateResultOwned {
    /// The records making up this aggregate result
    ///
    /// The `RSAggregateResult` is part of a union in [`RSIndexResultData`], so it needs to have a
    /// known size. The std `Vec` won't have this since it is not `#[repr(C)]`, so we use our
    /// own `LowMemoryThinVec` type which is `#[repr(C)]` and has a known size instead.
    records: LowMemoryThinVec<*const RSIndexResult<'static, 'static>>,

    /// A map of the aggregate kind of the underlying records
    kind_mask: RSResultKindMask,
}

impl<'index, 'children> RSAggregateResult<'index, 'children> {
    /// Create a new empty aggregate result with the given capacity
    pub fn with_capacity(cap: usize) -> Self {
        Self::Borrowed(RSAggregateResultRef {
            records: LowMemoryThinVec::with_capacity(cap),
            kind_mask: RSResultKindMask::empty(),
            _phantom: PhantomData,
        })
    }

    /// The number of results in this aggregate result
    pub fn len(&self) -> usize {
        match self {
            RSAggregateResult::Borrowed(a) => a.records.len(),
            RSAggregateResult::Owned(a) => a.records.len(),
        }
    }

    /// Check whether this aggregate result is empty
    pub fn is_empty(&self) -> bool {
        match self {
            RSAggregateResult::Borrowed(a) => a.records.is_empty(),
            RSAggregateResult::Owned(a) => a.records.is_empty(),
        }
    }

    /// The capacity of the aggregate result
    pub fn capacity(&self) -> usize {
        match self {
            RSAggregateResult::Borrowed(a) => a.records.capacity(),
            RSAggregateResult::Owned(a) => a.records.capacity(),
        }
    }

    /// The current type mask of the aggregate result
    pub fn kind_mask(&self) -> RSResultKindMask {
        match self {
            RSAggregateResult::Borrowed(a) => a.kind_mask,
            RSAggregateResult::Owned(a) => a.kind_mask,
        }
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
        let records = match self {
            RSAggregateResult::Borrowed(a) => &a.records,
            RSAggregateResult::Owned(a) => &a.records,
        };

        if let Some(result_addr) = records.get(index) {
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
        match self {
            RSAggregateResult::Borrowed(a) => {
                a.records.clear();
                a.kind_mask = RSResultKindMask::empty();
            }
            RSAggregateResult::Owned(a) => {
                a.records.clear();
                a.kind_mask = RSResultKindMask::empty();
            }
        }
    }

    /// Add a child to the aggregate result and update the kind mask
    ///
    /// # Safety
    /// The given `child` has to stay valid for the lifetime of this aggregate result. Else reading
    /// the child with [`Self::get()`] will cause undefined behavior.
    pub fn push(&mut self, child: &RSIndexResult) {
        match self {
            RSAggregateResult::Borrowed(a) => {
                a.records.push(child as *const _ as *mut _);

                a.kind_mask |= child.data.kind();
            }
            RSAggregateResult::Owned(a) => {
                a.records.push(child as *const _ as *mut _);

                a.kind_mask |= child.data.kind();
            }
        }
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
pub struct RSAggregateResultIterOwned {
    agg: RSAggregateResultOwned,
    index: usize,
}

impl Iterator for RSAggregateResultIterOwned {
    type Item = Box<RSIndexResult<'static, 'static>>;

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

impl IntoIterator for RSAggregateResultOwned {
    type Item = Box<RSIndexResult<'static, 'static>>;

    type IntoIter = RSAggregateResultIterOwned;

    fn into_iter(self) -> Self::IntoIter {
        RSAggregateResultIterOwned {
            agg: self,
            index: 0,
        }
    }
}

/// Represents a virtual result in an index record.
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
/// While the `'aggregate_children` lifetime is linked to [`RSAggregateResult`] that is holding
/// raw pointers to results.
#[repr(u8)]
#[derive(Debug, PartialEq)]
pub enum RSResultData<'index, 'aggregate_children> {
    Union(RSAggregateResult<'index, 'aggregate_children>) = 1,
    Intersection(RSAggregateResult<'index, 'aggregate_children>) = 2,
    Term(RSTermRecord<'index>) = 4,
    Virtual(RSVirtualResult) = 8,
    Numeric(f64) = 16,
    Metric(f64) = 32,
    HybridMetric(RSAggregateResult<'index, 'aggregate_children>) = 64,
}

impl RSResultData<'_, '_> {
    pub fn kind(&self) -> RSResultKind {
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
    pub data: RSResultData<'index, 'aggregate_children>,

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
            data: RSResultData::Metric(0.0),
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
        offsets: RSOffsetVectorRef<'index>,
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
    pub fn as_numeric(&self) -> Option<f64> {
        match self.data {
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
    pub fn as_numeric_mut(&mut self) -> Option<&mut f64> {
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
            RSResultData::Union(RSAggregateResult::Owned(agg))
            | RSResultData::Intersection(RSAggregateResult::Owned(agg))
            | RSResultData::HybridMetric(RSAggregateResult::Owned(agg)) => {
                for child in agg.into_iter() {
                    drop(child);
                }
            }
            RSResultData::Union(_)
            | RSResultData::Intersection(RSAggregateResult::Borrowed(_))
            | RSResultData::HybridMetric(RSAggregateResult::Borrowed(_)) => {}
            RSResultData::Term(RSTermRecord::Owned(_term)) => {
                // SAFETY: we know the C type is the same because it was autogenerated from the Rust type
                // unsafe {
                //     Term_Offset_Data_Free(&mut term);
                // }
            }
            RSResultData::Term(RSTermRecord::Borrowed(term)) => {
                // SAFETY: we know the C type is the same because it was autogenerated from the Rust type
                unsafe {
                    Term_Free(term.term);
                }
            }
            RSResultData::Numeric(_numeric) | RSResultData::Metric(_numeric) => {}
            RSResultData::Virtual(_virtual) => {}
        }
    }
}
