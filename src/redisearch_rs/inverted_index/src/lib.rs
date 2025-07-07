/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{
    alloc::{Layout, alloc_zeroed},
    ffi::{c_char, c_int},
    fmt::Debug,
    io::{Read, Seek, Write},
    mem::ManuallyDrop,
    ptr,
};

use enumflags2::{BitFlags, bitflags};
use ffi::RS_FIELDMASK_ALL;
pub use ffi::{
    FieldMask, RSDocumentMetadata, RSQueryTerm, RSYieldableMetric, t_docId, t_fieldMask,
};

pub mod numeric;

/// A delta is the difference between document IDs. It is mostly used to save space in the index
/// because document IDs are usually sequential and the difference between them are small. With the
/// help of encoding, we can optionally store the difference (delta) efficiently instead of the full document
/// ID.
pub struct Delta(usize);

impl Delta {
    /// Make a new delta value
    pub fn new(delta: usize) -> Self {
        Delta(delta)
    }
}

impl From<Delta> for usize {
    fn from(delta: Delta) -> Self {
        delta.0
    }
}

/// Represents a numeric value in an index record.
/// cbindgen:field-names=[value]
#[allow(rustdoc::broken_intra_doc_links)] // The field rename above breaks the intra-doc link
#[repr(C)]
#[derive(Debug, PartialEq)]
pub struct RSNumericRecord(pub f64);

/// Represents the encoded offsets of a term in a document. You can read the offsets by iterating
/// over it with RSOffsetVector_Iterator
#[repr(C)]
#[derive(Debug, PartialEq)]
pub struct RSOffsetVector {
    pub data: *mut c_char,
    pub len: u32,
}

impl RSOffsetVector {
    /// Create a new, empty offset vector ready to receive data
    pub fn empty() -> Self {
        Self {
            data: ptr::null_mut(),
            len: 0,
        }
    }
}

/// Represents a single record of a document inside a term in the inverted index
#[repr(C)]
#[derive(Debug, PartialEq)]
pub struct RSTermRecord {
    /// The term that brought up this record
    pub term: *mut RSQueryTerm,

    /// The encoded offsets in which the term appeared in the document
    pub offsets: RSOffsetVector,
}

impl RSTermRecord {
    /// Create a new term record with the given term pointer
    pub fn new(term: *mut RSQueryTerm) -> Self {
        Self {
            term,
            offsets: RSOffsetVector::empty(),
        }
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
/// cbindgen:rename-all=CamelCase
#[repr(C)]
#[derive(Debug, PartialEq)]
pub struct RSAggregateResult {
    /// The number of child records
    pub num_children: c_int,

    /// The capacity of the records array. Has no use for extensions
    pub children_cap: c_int,

    /// An array of records
    pub children: *mut *mut RSIndexResult,

    /// A map of the aggregate type of the underlying records
    pub type_mask: RSResultTypeMask,
}

impl RSAggregateResult {
    /// Create a new aggregate result with th given capacity
    pub fn new(cap: usize) -> Self {
        let children = if cap > 0 {
            // Calculate the layout for an array of pointers
            let layout = Layout::array::<*mut RSIndexResult>(cap as usize)
                .expect("Failed to create layout for children array");

            // Allocate zero-initialized memory (equivalent to calloc)
            let ptr = unsafe { alloc_zeroed(layout) };

            if ptr.is_null() {
                panic!("Failed to allocate memory for children array");
            }

            ptr as *mut *mut RSIndexResult
        } else {
            ptr::null_mut()
        };

        Self {
            num_children: 0,
            children_cap: cap as c_int,
            children,
            type_mask: RSResultTypeMask::empty(),
        }
    }
}

/// Represents a virtual result in an index record.
#[repr(C)]
#[derive(Debug, PartialEq)]
pub struct RSVirtualResult;

/// Holds the actual data of an ['IndexResult']
#[repr(C)]
pub union RSIndexResultData {
    pub agg: ManuallyDrop<RSAggregateResult>,
    pub term: ManuallyDrop<RSTermRecord>,
    pub num: ManuallyDrop<RSNumericRecord>,
    pub virt: ManuallyDrop<RSVirtualResult>,
}

/// The result of an inverted index
/// cbindgen:field-names=[docId, dmd, fieldMask, freq, offsetsSz, data, type, isCopy, metrics, weight]
#[repr(C)]
pub struct RSIndexResult {
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

    data: RSIndexResultData,

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

impl Default for RSIndexResult {
    fn default() -> Self {
        Self::virt(0, 0, 0.0)
    }
}

impl RSIndexResult {
    /// Create a new virtual index result
    pub fn virt(doc_id: t_docId, field_mask: FieldMask, weight: f64) -> Self {
        Self {
            doc_id,
            dmd: ptr::null(),
            field_mask,
            freq: 0,
            offsets_sz: 0,
            data: RSIndexResultData {
                virt: ManuallyDrop::new(RSVirtualResult),
            },
            result_type: RSResultType::Virtual,
            is_copy: false,
            metrics: ptr::null_mut(),
            weight,
        }
    }

    /// Create a new numeric index result with the given numeric value
    pub fn numeric(doc_id: t_docId, num: f64) -> Self {
        Self {
            doc_id,
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

    pub fn metric(doc_id: t_docId) -> Self {
        Self {
            doc_id,
            field_mask: RS_FIELDMASK_ALL,
            data: RSIndexResultData {
                num: ManuallyDrop::new(RSNumericRecord(0.0)),
            },
            result_type: RSResultType::Metric,
            weight: 1.0,
            ..Default::default()
        }
    }

    /// Create a new intersection index result with the given document ID, capacity, and weight
    pub fn intersect(doc_id: t_docId, cap: usize, weight: f64) -> Self {
        Self {
            doc_id,
            data: RSIndexResultData {
                agg: ManuallyDrop::new(RSAggregateResult::new(cap)),
            },
            result_type: RSResultType::Intersection,
            weight,
            ..Default::default()
        }
    }

    /// Create a new union index result with the given document ID, capacity, and weight
    pub fn union(doc_id: t_docId, cap: usize, weight: f64) -> Self {
        Self {
            doc_id,
            data: RSIndexResultData {
                agg: ManuallyDrop::new(RSAggregateResult::new(cap)),
            },
            result_type: RSResultType::Union,
            weight,
            ..Default::default()
        }
    }

    /// Create a new hybrid metric index result with the given document ID
    pub fn hybrid_metric(doc_id: t_docId) -> Self {
        Self {
            doc_id,
            data: RSIndexResultData {
                agg: ManuallyDrop::new(RSAggregateResult::new(2)),
            },
            result_type: RSResultType::HybridMetric,
            weight: 1.0,
            ..Default::default()
        }
    }

    /// Create a new term index result with the given document ID, term pointer, and weight
    pub fn term(doc_id: t_docId, term: *mut RSQueryTerm, weight: f64) -> Self {
        Self {
            doc_id,
            data: RSIndexResultData {
                term: ManuallyDrop::new(RSTermRecord::new(term)),
            },
            result_type: RSResultType::Term,
            weight,
            ..Default::default()
        }
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
}

impl Debug for RSIndexResult {
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

impl PartialEq for RSIndexResult {
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
    /// Write the record to the writer and return the number of bytes written. The delta is the
    /// pre-computed difference between the current document ID and the last document ID written.
    fn encode<W: Write + Seek>(
        &self,
        writer: W,
        delta: Delta,
        record: &RSIndexResult,
    ) -> std::io::Result<usize>;
}

#[derive(Debug, PartialEq)]
pub enum DecoderResult {
    /// The record was successfully decoded.
    Record(RSIndexResult),
    /// The record was filtered out and should not be returned.
    FilteredOut,
}

/// Decoder to read records from an index
pub trait Decoder {
    /// Decode the next record from the reader. If any delta values are decoded, then they should
    /// add to the `base` document ID to get the actual document ID.
    ///
    /// Returns `Ok(None)` if there is nothing left on the reader to decode.
    fn decode<R: Read>(
        &self,
        reader: &mut R,
        base: t_docId,
    ) -> std::io::Result<Option<DecoderResult>>;

    /// Like `[Decoder::decode]`, but it skips all entries whose document ID is lower than `target`.
    ///
    /// Returns `None` if no record has a document ID greater than or equal to `target`.
    fn seek<R: Read + Seek>(
        &self,
        reader: &mut R,
        base: t_docId,
        target: t_docId,
    ) -> std::io::Result<Option<RSIndexResult>> {
        loop {
            match self.decode(reader, base)? {
                Some(DecoderResult::Record(record)) if record.doc_id >= target => {
                    return Ok(Some(record));
                }
                Some(DecoderResult::Record(_)) | Some(DecoderResult::FilteredOut) => continue,
                None => return Ok(None),
            }
        }
    }
}
