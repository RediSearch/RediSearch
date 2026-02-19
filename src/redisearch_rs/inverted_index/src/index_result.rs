/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{alloc::Layout, borrow::Borrow, fmt::Debug, marker::PhantomData, ptr};

use enumflags2::{BitFlags, bitflags};
use ffi::{
    FieldMask, RS_FIELDMASK_ALL, RSDocumentMetadata, RSYieldableMetric, t_docId, t_fieldMask,
};
pub use query_term::RSQueryTerm;
use thin_vec::SmallThinVec;

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

/// Borrowed view of the encoded offsets of a term in a document. You can read the offsets by
/// iterating over it with RSIndexResult_IterateOffsets.
///
/// This is a borrowed, `Copy` type — it does not own the data and will not free it on drop.
/// Use [`RSOffsetVector`] for owned offset data.
#[repr(C)]
#[derive(Copy, Clone)]
pub struct RSOffsetSlice<'index> {
    /// Pointer to the borrowed offset data.
    pub data: *mut u8,
    pub len: u32,
    /// The data pointer does not carry a lifetime, so use a `PhantomData` to track it instead.
    _phantom: PhantomData<&'index ()>,
}

impl PartialEq for RSOffsetSlice<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.as_bytes() == other.as_bytes()
    }
}

impl Eq for RSOffsetSlice<'_> {}

impl Debug for RSOffsetSlice<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.data.is_null() {
            return write!(f, "RSOffsetSlice(null)");
        }
        // SAFETY: `len` is guaranteed to be a valid length for the data pointer.
        let offsets =
            unsafe { std::slice::from_raw_parts(self.data.cast_const(), self.len as usize) };

        write!(f, "RSOffsetSlice {offsets:?}")
    }
}

impl AsRef<[u8]> for RSOffsetSlice<'_> {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl Borrow<[u8]> for RSOffsetSlice<'_> {
    fn borrow(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl<'index> RSOffsetSlice<'index> {
    /// Create an offset slice borrowing from the given byte slice.
    ///
    /// # Panics
    ///
    /// Panics if `bytes.len() > u32::MAX as usize`.
    pub fn from_slice(bytes: &'index [u8]) -> Self {
        assert!(
            bytes.len() <= u32::MAX as usize,
            "offset slice length exceeds u32::MAX"
        );
        Self {
            data: bytes.as_ptr().cast_mut(),
            len: bytes.len() as u32,
            _phantom: PhantomData,
        }
    }
}

impl RSOffsetSlice<'_> {
    /// Create a new, empty offset slice.
    pub const fn empty() -> Self {
        Self {
            data: ptr::null_mut(),
            len: 0,
            _phantom: PhantomData,
        }
    }

    /// Return the offset data as a byte slice.
    pub const fn as_bytes(&self) -> &[u8] {
        if self.data.is_null() {
            &[]
        } else {
            // SAFETY: We checked that data is not NULL and `len` is guaranteed to be a valid
            // length for the data pointer.
            unsafe { std::slice::from_raw_parts(self.data, self.len as usize) }
        }
    }

    /// Create an owned copy of this offset slice, allocating new memory for the data.
    pub fn to_owned(&self) -> RSOffsetVector {
        let data = if self.len > 0 {
            debug_assert!(!self.data.is_null(), "data must not be null");
            let layout = Layout::array::<u8>(self.len as usize).unwrap();
            // SAFETY: we just checked that len > 0
            let data = unsafe { std::alloc::alloc(layout) };
            if data.is_null() {
                std::alloc::handle_alloc_error(layout)
            };
            // SAFETY:
            // - The source buffer and the destination buffer don't overlap because
            //   they belong to distinct non-overlapping allocations.
            // - The destination buffer is valid for writes of `src.len` elements
            //   since it was just allocated with capacity `src.len`.
            // - The source buffer is valid for reads of `src.len` elements as a call invariant.
            unsafe { std::ptr::copy_nonoverlapping(self.data, data, self.len as usize) };

            data
        } else {
            ptr::null_mut()
        };

        RSOffsetVector {
            data,
            len: self.len,
        }
    }
}

/// Owned encoded offsets of a term in a document.
///
/// This type owns the data and will free it on drop. Use [`RSOffsetSlice`] for borrowed offset
/// data.
///
/// The `#[repr(C)]` layout is identical to [`RSOffsetSlice`] (minus the zero-sized `PhantomData`),
/// so a `&RSOffsetVector` can be safely cast to `&RSOffsetSlice<'_>`.
#[repr(C)]
pub struct RSOffsetVector {
    /// Pointer to the owned offset data, allocated via the global allocator.
    pub data: *mut u8,
    pub len: u32,
}

impl PartialEq for RSOffsetVector {
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl Eq for RSOffsetVector {}

impl Debug for RSOffsetVector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.data.is_null() {
            return write!(f, "RSOffsetVector(null)");
        }
        // SAFETY: `len` is guaranteed to be a valid length for the data pointer.
        let offsets =
            unsafe { std::slice::from_raw_parts(self.data.cast_const(), self.len as usize) };

        write!(f, "RSOffsetVector {offsets:?}")
    }
}

impl AsRef<[u8]> for RSOffsetVector {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl Borrow<[u8]> for RSOffsetVector {
    fn borrow(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl RSOffsetVector {
    /// Create a new, empty offset vector.
    pub const fn empty() -> Self {
        Self {
            data: ptr::null_mut(),
            len: 0,
        }
    }

    /// Return a borrowed view of this owned offset vector.
    pub const fn as_slice<'a>(&'a self) -> RSOffsetSlice<'a> {
        RSOffsetSlice {
            data: self.data,
            len: self.len,
            _phantom: PhantomData,
        }
    }

    /// Return the offset data as a byte slice.
    pub const fn as_bytes(&self) -> &[u8] {
        if self.data.is_null() {
            &[]
        } else {
            // SAFETY: We checked that data is not NULL and `len` is guaranteed to be a valid
            // length for the data pointer.
            unsafe { std::slice::from_raw_parts(self.data, self.len as usize) }
        }
    }
}

impl Drop for RSOffsetVector {
    fn drop(&mut self) {
        if !self.data.is_null() {
            let layout = Layout::array::<u8>(self.len as usize).unwrap();
            // SAFETY: Data was allocated via the global allocator with the matching layout.
            unsafe { std::alloc::dealloc(self.data, layout) };
        }
    }
}

/// Represents a single record of a document inside a term in the inverted index
/// cbindgen:prefix-with-name=true
#[repr(u8)]
pub enum RSTermRecord<'index> {
    Borrowed {
        /// The term that brought up this record.
        ///
        /// The term is owned by the record. The name of the variant, `Borrowed`,
        /// refers to the `offsets` field.
        ///
        /// The term is wrapped in a `Box` to ensure that both `Owned` and `Borrowed`
        /// variants have the same memory layout.
        term: Option<Box<RSQueryTerm>>,

        /// The encoded offsets in which the term appeared in the document
        ///
        /// A decoder can choose to borrow this data from the index block, hence the `'index` lifetime.
        offsets: RSOffsetSlice<'index>,
    },
    Owned {
        /// The term that brought up this record.
        ///
        /// It borrows the term from another record.
        /// The name of the variant, `Owned`, refers to the `offsets` field.
        term: *mut RSQueryTerm,

        /// The encoded offsets in which the term appeared in the document
        ///
        /// The owned version owns a copy of the offsets data, which is freed on drop.
        offsets: RSOffsetVector,
    },
}

impl PartialEq for RSTermRecord<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.query_term() == other.query_term() && self.offsets() == other.offsets()
    }
}

impl Eq for RSTermRecord<'_> {}

impl<'index> RSTermRecord<'index> {
    /// Create a new term record without term pointer and offsets.
    pub const fn new() -> Self {
        Self::Borrowed {
            term: None,
            offsets: RSOffsetSlice::empty(),
        }
    }

    /// Create a new borrowed term record with the given term and offsets.
    pub const fn with_term(
        term: Option<Box<RSQueryTerm>>,
        offsets: RSOffsetSlice<'index>,
    ) -> RSTermRecord<'index> {
        Self::Borrowed { term, offsets }
    }

    /// Is this term record borrowed or owned?
    pub const fn is_copy(&self) -> bool {
        matches!(self, RSTermRecord::Owned { .. })
    }

    /// Get the offsets of this term record as a byte slice.
    pub const fn offsets(&self) -> &[u8] {
        match self {
            RSTermRecord::Borrowed { offsets, .. } => offsets.as_bytes(),
            RSTermRecord::Owned { offsets, .. } => offsets.as_bytes(),
        }
    }

    /// Get a reference to the query term of this term record, if one is set.
    pub fn query_term(&self) -> Option<&RSQueryTerm> {
        match self {
            RSTermRecord::Borrowed { term, .. } => term.as_deref(),
            RSTermRecord::Owned { term, .. } => {
                if term.is_null() {
                    None
                } else {
                    // SAFETY: `Owned::term` is either null or points to a valid
                    // `RSQueryTerm` owned by the source `Borrowed` variant.
                    Some(unsafe { &**term })
                }
            }
        }
    }

    /// Create an owned copy of this term record, allocating new memory for the offsets, but reusing the term.
    pub fn to_owned(&self) -> RSTermRecord<'static> {
        RSTermRecord::Owned {
            term: self
                .query_term()
                .map_or(ptr::null_mut(), |t| ptr::from_ref(t).cast_mut()),
            offsets: match self {
                RSTermRecord::Borrowed { offsets, .. } => offsets.to_owned(),
                RSTermRecord::Owned { offsets, .. } => offsets.as_slice().to_owned(),
            },
        }
    }

    /// Set the offsets of this term record, replacing any existing offsets.
    pub fn set_offsets(&mut self, offsets: RSOffsetSlice<'index>) {
        match self {
            RSTermRecord::Borrowed { offsets: o, .. } => {
                *o = offsets;
            }
            RSTermRecord::Owned { offsets: o, .. } => {
                // Assign the new owned copy; the old value is auto-dropped, freeing old data.
                *o = offsets.to_owned();
            }
        }
    }
}

impl Debug for RSTermRecord<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RSTermRecord::Borrowed { offsets, .. } => f
                .debug_struct("RSTermRecord(Borrowed)")
                .field("term", &self.query_term())
                .field("offsets", offsets)
                .finish(),
            RSTermRecord::Owned { offsets, .. } => f
                .debug_struct("RSTermRecord(Owned)")
                .field("term", &self.query_term())
                .field("offsets", offsets)
                .finish(),
        }
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
/// the `ThinVec` which needs to exist in Rust's memory space to ensure its memory is
/// managed correctly.
/// cbindgen:prefix-with-name=true
#[repr(u8)]
#[derive(Debug, PartialEq)]
pub enum RSAggregateResult<'index> {
    Borrowed {
        /// The records making up this aggregate result
        ///
        /// The `RSAggregateResult` is part of a union in [`RSResultData`], so it needs to have a
        /// known size. The std `Vec` won't have this since it is not `#[repr(C)]`, so we use our
        /// own `ThinVec` type which is `#[repr(C)]` and has a known size instead.
        ///
        /// This requires `'index` on the reference because adding a new lifetime will cause the
        /// type to be `ThinVec<&'refs RSIndexResult<'index, 'refs>>` which will require
        /// `'index: 'refs` else it would mean the `'index` can be cleaned up while some reference
        /// will still try to access it (ie a dangling pointer). Now the decoders will never return
        /// any aggregate results so `'refs == 'static` when decoding. Because of the requirement
        /// above, this means `'index: 'static` which is just incorrect since the index data will
        /// never be `'static` when decoding.
        records: SmallThinVec<&'index RSIndexResult<'index>>,

        /// A map of the aggregate kind of the underlying records
        kind_mask: RSResultKindMask,
    },
    Owned {
        /// The records making up this aggregate result
        ///
        /// The `RSAggregateResult` is part of a union in [`RSResultData`], so it needs to have a
        /// known size. The std `Vec` won't have this since it is not `#[repr(C)]`, so we use our
        /// own `ThinVec` type which is `#[repr(C)]` and has a known size instead.
        records: SmallThinVec<Box<RSIndexResult<'static>>>,

        /// A map of the aggregate kind of the underlying records
        kind_mask: RSResultKindMask,
    },
}

impl<'index> RSAggregateResult<'index> {
    /// Create a new empty aggregate result with the given capacity
    pub fn with_capacity(cap: usize) -> Self {
        Self::Borrowed {
            records: SmallThinVec::with_capacity(cap),
            kind_mask: RSResultKindMask::empty(),
        }
    }

    /// The number of results in this aggregate result
    pub fn len(&self) -> usize {
        match self {
            RSAggregateResult::Borrowed { records, .. } => records.len(),
            RSAggregateResult::Owned { records, .. } => records.len(),
        }
    }

    /// Check whether this aggregate result is empty
    pub fn is_empty(&self) -> bool {
        match self {
            RSAggregateResult::Borrowed { records, .. } => records.is_empty(),
            RSAggregateResult::Owned { records, .. } => records.is_empty(),
        }
    }

    /// The capacity of the aggregate result
    pub fn capacity(&self) -> usize {
        match self {
            RSAggregateResult::Borrowed { records, .. } => records.capacity(),
            RSAggregateResult::Owned { records, .. } => records.capacity(),
        }
    }

    /// The current type mask of the aggregate result
    pub const fn kind_mask(&self) -> RSResultKindMask {
        match self {
            RSAggregateResult::Borrowed { kind_mask, .. } => *kind_mask,
            RSAggregateResult::Owned { kind_mask, .. } => *kind_mask,
        }
    }

    /// Get an iterator over the children of this aggregate result
    pub const fn iter(&'index self) -> RSAggregateResultIter<'index> {
        RSAggregateResultIter {
            agg: self,
            index: 0,
        }
    }

    /// Get the child at the given index, if it exists.
    pub fn get(&self, index: usize) -> Option<&RSIndexResult<'index>> {
        match self {
            RSAggregateResult::Borrowed { records, .. } => records.get(index).copied(),
            RSAggregateResult::Owned { records, .. } => records.get(index).map(AsRef::as_ref),
        }
    }

    /// Get the child at the given index, if it exists.
    ///
    /// # Safety
    ///
    /// 1. The index must be within the bounds of the children vector.
    pub unsafe fn get_unchecked(&self, index: usize) -> &RSIndexResult<'index> {
        match self {
            RSAggregateResult::Borrowed { records, .. } => {
                debug_assert!(
                    index < records.len(),
                    "Safety violation: trying to access an aggregate result child at an out-of-bounds index, {index}. Length: {}",
                    records.len()
                );
                // SAFETY:
                // - Thanks to precondition 1., we know that the index is within bounds.
                unsafe { records.get_unchecked(index) }
            }
            RSAggregateResult::Owned { records, .. } => {
                debug_assert!(
                    index < records.len(),
                    "Safety violation: trying to access an aggregate result child at an out-of-bounds index, {index}. Length: {}",
                    records.len()
                );
                // SAFETY:
                // - Thanks to precondition 1., we know that the index is within bounds.
                unsafe { records.get_unchecked(index) }
            }
        }
    }

    /// Reset the aggregate result, clearing the children vector and resetting the kind mask.
    pub fn reset(&mut self) {
        match self {
            RSAggregateResult::Borrowed {
                records, kind_mask, ..
            } => {
                records.clear();
                *kind_mask = RSResultKindMask::empty();
            }
            RSAggregateResult::Owned { records, kind_mask } => {
                records.clear();
                *kind_mask = RSResultKindMask::empty();
            }
        }
    }

    /// Add a child to the aggregate result and update the kind mask
    ///
    /// # Safety
    /// The given `child` has to stay valid for the lifetime of this aggregate result. Else reading
    /// the child with [`Self::get()`] will cause undefined behavior.
    pub fn push_borrowed(&mut self, child: &'index RSIndexResult) {
        match self {
            RSAggregateResult::Borrowed {
                records, kind_mask, ..
            } => {
                records.push(child);

                *kind_mask |= child.data.kind();
            }
            RSAggregateResult::Owned { .. } => {
                panic!("Cannot push a borrowed child to an owned aggregate result");
            }
        }
    }

    /// Create an owned copy of this aggregate result, allocating new memory for the records.
    pub fn to_owned(&self) -> RSAggregateResult<'static> {
        match self {
            RSAggregateResult::Borrowed { records, kind_mask } => {
                let mut new_records = SmallThinVec::with_capacity(records.len());

                new_records.extend(
                    records
                        .iter()
                        .map(|c| RSIndexResult::to_owned(c))
                        .map(Box::new),
                );

                RSAggregateResult::Owned {
                    records: new_records,
                    kind_mask: *kind_mask,
                }
            }
            RSAggregateResult::Owned { records, kind_mask } => {
                let mut new_records = SmallThinVec::with_capacity(records.len());

                new_records.extend(
                    records
                        .iter()
                        .map(|c| RSIndexResult::to_owned(c))
                        .map(Box::new),
                );

                RSAggregateResult::Owned {
                    records: new_records,
                    kind_mask: *kind_mask,
                }
            }
        }
    }
}

impl RSAggregateResult<'static> {
    /// Add a heap owned child to the aggregate result and update the kind mask
    pub fn push_boxed(&mut self, child: Box<RSIndexResult<'static>>) {
        match self {
            RSAggregateResult::Borrowed { .. } => {
                panic!("Cannot push a borrowed child to an owned aggregate result");
            }
            RSAggregateResult::Owned { records, kind_mask } => {
                *kind_mask |= child.data.kind();
                records.push(child);
            }
        }
    }

    /// Get a mutable reference to the child at the given index, if it exists
    pub fn get_mut(&mut self, index: usize) -> Option<&mut RSIndexResult<'static>> {
        match self {
            RSAggregateResult::Borrowed { .. } => {
                panic!("Cannot get a mutable reference to a borrowed aggregate result");
            }
            RSAggregateResult::Owned { records, .. } => records.get_mut(index).map(AsMut::as_mut),
        }
    }
}

/// An iterator over the results in an [`RSAggregateResult`].
pub struct RSAggregateResultIter<'index> {
    agg: &'index RSAggregateResult<'index>,
    index: usize,
}

impl<'index> Iterator for RSAggregateResultIter<'index> {
    type Item = &'index RSIndexResult<'index>;

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
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum RSResultKind {
    Union = 1,
    Intersection = 2,
    Term = 4,
    Virtual = 8,
    Numeric = 16,
    Metric = 32,
    HybridMetric = 64,
}

impl std::fmt::Display for RSResultKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let k = match self {
            RSResultKind::Union => "Union",
            RSResultKind::Intersection => "Intersection",
            RSResultKind::Term => "Term",
            RSResultKind::Virtual => "Virtual",
            RSResultKind::Numeric => "Numeric",
            RSResultKind::Metric => "Metric",
            RSResultKind::HybridMetric => "HybridMetric",
        };
        write!(f, "{k}")
    }
}

/// Holds the actual data of an ['IndexResult']
///
/// These enum values should stay in sync with [`RSResultKind`], so that the C union generated matches
/// the bitflags on [`RSResultKindMask`]
///
/// The `'index` lifetime is linked to the [`crate::IndexBlock`] when decoding borrows from the block.
#[repr(u8)]
#[derive(Debug, PartialEq)]
/// cbindgen:prefix-with-name=true
pub enum RSResultData<'index> {
    Union(RSAggregateResult<'index>) = 1,
    Intersection(RSAggregateResult<'index>) = 2,
    Term(RSTermRecord<'index>) = 4,
    Virtual = 8,
    Numeric(f64) = 16,
    Metric(f64) = 32,
    HybridMetric(RSAggregateResult<'index>) = 64,
}

impl RSResultData<'_> {
    pub const fn kind(&self) -> RSResultKind {
        match self {
            RSResultData::Union(_) => RSResultKind::Union,
            RSResultData::Intersection(_) => RSResultKind::Intersection,
            RSResultData::Term(_) => RSResultKind::Term,
            RSResultData::Virtual => RSResultKind::Virtual,
            RSResultData::Numeric(_) => RSResultKind::Numeric,
            RSResultData::Metric(_) => RSResultKind::Metric,
            RSResultData::HybridMetric(_) => RSResultKind::HybridMetric,
        }
    }

    /// Create an owned copy of this result data, allocating new memory for the contained data.
    pub fn to_owned(&self) -> RSResultData<'static> {
        match self {
            Self::Union(agg) => RSResultData::Union(agg.to_owned()),
            Self::Intersection(agg) => RSResultData::Intersection(agg.to_owned()),
            Self::Term(term) => RSResultData::Term(term.to_owned()),
            Self::Virtual => RSResultData::Virtual,
            Self::Numeric(num) => RSResultData::Numeric(*num),
            Self::Metric(num) => RSResultData::Metric(*num),
            Self::HybridMetric(agg) => RSResultData::HybridMetric(agg.to_owned()),
        }
    }
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
    pub fn to_owned(&self) -> RSIndexResult<'static> {
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
}

impl RSIndexResult<'static> {
    /// If this is an aggregate result, then add a heap owned child to it. Also updates the
    /// following of this record:
    /// - The document ID will inherit the new child added
    /// - The child's frequency will contribute to this result
    /// - The child's field mask will contribute to this result's field mask
    /// - If the child has metrics, then they will be concatenated to this result's metrics
    ///
    /// If this is not an aggregate result, then nothing happens. Use [`Self::is_aggregate()`] first
    /// to make sure this is an aggregate result.
    pub fn push_boxed(&mut self, child: Box<RSIndexResult<'static>>) {
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
