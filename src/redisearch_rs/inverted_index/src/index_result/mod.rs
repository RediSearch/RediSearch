use std::{ffi::c_char, fmt::Debug, marker::PhantomData, mem::ManuallyDrop, ptr};

use enumflags2::{BitFlags, bitflags};
use ffi::{
    FieldMask, RS_FIELDMASK_ALL, RSDocumentMetadata, RSQueryTerm, RSYieldableMetric, t_docId,
    t_fieldMask,
};
use low_memory_thin_vec::LowMemoryThinVec;

pub mod raw;

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
pub struct RSOffsetVector<'index> {
    /// At this point the data ownership is still managed by the caller.
    // TODO: switch to a Cow once the caller code has been ported to Rust.
    pub data: *mut c_char,
    pub len: u32,
    /// data may be borrowed from the reader.
    /// The data pointer does not allow lifetime so use a PhantomData to carry the lifetime for it instead.
    _phantom: PhantomData<&'index ()>,
}

impl Debug for RSOffsetVector<'_> {
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
/// cbindgen:rename-all=CamelCase
#[repr(C)]
#[derive(PartialEq)]
pub struct RSTermRecord<'index> {
    /// We mark copied terms so we can treat them a bit differently on deletion, and pool them if
    /// we want
    pub is_copy: bool,

    /// The term that brought up this record
    pub term: *mut RSQueryTerm,

    /// The encoded offsets in which the term appeared in the document
    pub offsets: RSOffsetVector<'index>,
}

impl<'index> RSTermRecord<'index> {
    /// Create a new term record without term pointer and offsets.
    pub fn new() -> Self {
        Self {
            is_copy: false,
            term: ptr::null_mut(),
            offsets: RSOffsetVector::empty(),
        }
    }

    /// Create a new term with the given term pointer and offsets.
    pub fn with_term(
        term: *mut RSQueryTerm,
        offsets: RSOffsetVector<'index>,
    ) -> RSTermRecord<'index> {
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

impl Debug for RSTermRecord<'_> {
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
pub struct RSAggregateResult<'index, 'children> {
    /// We mark copied aggregates so we can treat them a bit differently on deletion.
    is_copy: bool,

    /// The records making up this aggregate result
    ///
    /// The `RSAggregateResult` is part of a union in [`RSIndexResultData`], so it needs to have a
    /// known size. The std `Vec` won't have this since it is not `#[repr(C)]`, so we use our
    /// own `LowMemoryThinVec` type which is `#[repr(C)]` and has a known size instead.
    records: LowMemoryThinVec<*const RSIndexResult<'index, 'children>>,

    /// A map of the aggregate type of the underlying records
    type_mask: RSResultTypeMask,

    /// The lifetime is actually on `RsIndexResult` but it is stored as a pointer which does not
    /// support lifetimes. So use a PhantomData to carry the lifetime for it instead.
    _phantom: PhantomData<&'children ()>,
}

impl<'index, 'children> RSAggregateResult<'index, 'children> {
    /// Create a new empty aggregate result with the given capacity
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            is_copy: false,
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
#[derive(Debug, PartialEq)]
pub struct RSVirtualResult;

/// Holds the actual data of an ['IndexResult']
#[repr(C)]
pub union RSIndexResultData<'index, 'aggregate_children> {
    pub agg: ManuallyDrop<RSAggregateResult<'index, 'aggregate_children>>,
    pub term: ManuallyDrop<RSTermRecord<'index>>,
    pub num: ManuallyDrop<RSNumericRecord>,
    pub virt: ManuallyDrop<RSVirtualResult>,
}

/// The result of an inverted index
/// cbindgen:field-names=[docId, dmd, fieldMask, freq, offsetsSz, data, type, metrics, weight]
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

    data: RSIndexResultData<'index, 'aggregate_children>,

    /// The type of data stored at ['Self::data']
    pub result_type: RSResultType,

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
            data: RSIndexResultData {
                virt: ManuallyDrop::new(RSVirtualResult),
            },
            result_type: RSResultType::Virtual,
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
            data: RSIndexResultData {
                term: ManuallyDrop::new(RSTermRecord::with_term(term, offsets)),
            },
            result_type: RSResultType::Term,
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
    pub fn as_term(&self) -> Option<&RSTermRecord<'index>> {
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
    pub fn as_term_mut(&mut self) -> Option<&mut RSTermRecord<'index>> {
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
    pub fn as_aggregate(&self) -> Option<&RSAggregateResult<'index, 'aggregate_children>> {
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
    pub fn as_aggregate_mut(
        &mut self,
    ) -> Option<&mut RSAggregateResult<'index, 'aggregate_children>> {
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
            // unsafe {
            //     IndexResult_ConcatMetrics(self, child);
            // }
        }
    }

    /// Get a child at the given index if this is an aggregate record. Returns `None` if this is not
    /// an aggregate record or if the index is out-of-bounds.
    pub fn get(&self, index: usize) -> Option<&RSIndexResult<'index, 'aggregate_children>> {
        if self.is_aggregate() {
            // SAFETY: we know the data will be an aggregate because we just checked the type
            let agg = unsafe { &self.data.agg };

            agg.get(index)
        } else {
            None
        }
    }
}

impl Drop for RSIndexResult<'_, '_> {
    fn drop(&mut self) {
        // SAFETY: we know `self` still exists because we are in `drop`. We also know the C type is
        // the same since it was autogenerated from the Rust type
        // unsafe {
        //     ResultMetrics_Free(self);
        // }

        match self.result_type {
            RSResultType::Union | RSResultType::Intersection | RSResultType::HybridMetric => {
                // SAFETY: we just checked the type to ensure the union has aggregated data
                let agg = unsafe { &mut self.data.agg };

                // SAFETY: we are in `drop` so nothing else will have access to this union type
                let agg = unsafe { ManuallyDrop::take(agg) };

                if agg.is_copy {
                    for child in agg.into_iter() {
                        drop(child);
                    }
                }
            }
            RSResultType::Term => {
                // SAFETY: we just checked the type to ensure the unior has term data
                let term = unsafe { &mut self.data.term };

                if term.is_copy {
                    // SAFETY: we know the C type is the same because it was autogenerated from the Rust type
                    // unsafe {
                    //     Term_Offset_Data_Free(term.deref_mut() as *mut _);
                    // }
                } else {
                    // SAFETY: we know the C type is the same because it was autogenerated from the Rust type
                    // unsafe {
                    //     Term_Free(term.term);
                    // }
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

impl Debug for RSIndexResult<'_, '_> {
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
            .field("metrics", &self.metrics)
            .field("weight", &self.weight)
            .finish()
    }
}

impl PartialEq for RSIndexResult<'_, '_> {
    fn eq(&self, other: &Self) -> bool {
        if !(self.doc_id == other.doc_id
            && self.dmd == other.dmd
            && self.field_mask == other.field_mask
            && self.freq == other.freq
            && self.offsets_sz == other.offsets_sz
            && self.result_type == other.result_type
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
