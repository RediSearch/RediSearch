use std::{ffi::c_char, marker::PhantomData};

use enumflags2::{BitFlags, bitflags};
use ffi::{RSDocumentMetadata, RSQueryTerm, RSYieldableMetric, t_docId, t_fieldMask};
use low_memory_thin_vec::LowMemoryThinVec;

/// Represents a numeric value in an index record.
/// cbindgen:field-names=[value]
#[allow(rustdoc::broken_intra_doc_links)] // The field rename above breaks the intra-doc link
#[repr(C)]
pub struct RSNumericRecord(pub f64);

/// Represents the encoded offsets of a term in a document. You can read the offsets by iterating
/// over it with RSIndexResult_IterateOffsets
#[repr(C)]
pub struct RSOffsetVector<'index> {
    /// At this point the data ownership is still managed by the caller.
    // TODO: switch to a Cow once the caller code has been ported to Rust.
    pub data: *mut c_char,
    pub len: u32,
    /// data may be borrowed from the reader.
    /// The data pointer does not allow lifetime so use a PhantomData to carry the lifetime for it instead.
    _phantom: PhantomData<&'index ()>,
}

/// Represents a single record of a document inside a term in the inverted index
/// cbindgen:rename-all=CamelCase
#[repr(C)]
pub struct RSTermRecord<'index> {
    /// We mark copied terms so we can treat them a bit differently on delete.
    pub is_copy: bool,

    /// The term that brought up this record
    pub term: *mut RSQueryTerm,

    /// The encoded offsets in which the term appeared in the document
    pub offsets: RSOffsetVector<'index>,
}

/// Represents an aggregate array of values in an index record.
///
/// The C code should always use `AggregateResult_New` to construct a new instance of this type
/// using Rust since the internals cannot be constructed directly in C. The reason is because of
/// the `LowMemoryThinVec` which needs to exist in Rust's memory space to ensure its memory is
/// managed correctly.
/// cbindgen:rename-all=CamelCase
#[repr(C)]
pub struct RSAggregateResult<'index, 'children> {
    /// We mark copied aggregates so we can treat them a bit differently on delete.
    is_copy: bool,

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
            is_copy: false,
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
/// Represents a virtual result in an index record.
#[repr(C)]
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
#[derive(Copy, Clone)]
pub enum RSResultKind {
    Union = 1,
    Intersection = 2,
    Term = 4,
    Virtual = 8,
    Numeric = 16,
    Metric = 32,
    HybridMetric = 64,
}

pub type RSResultKindMask = BitFlags<RSResultKind, u8>;

/// Holds the actual data of an ['IndexResult']
///
/// These enum values should stay in sync with [`RSResultKind`], so that the C union generated matches
/// the bitflags on [`RSResultKindMask`]
///
/// The `'index` lifetime is linked to the [`IndexBlock`] when decoding borrows from the block.
/// While the `'aggregate_children` lifetime is linked to [`RSAggregateResult`] that is holding
/// raw pointers to results.
#[repr(u8)]
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
/// cbindgen:rename-all=CamelCase
#[repr(C)]
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

impl<'index, 'aggregate_children> RSIndexResult<'index, 'aggregate_children> {
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
}
