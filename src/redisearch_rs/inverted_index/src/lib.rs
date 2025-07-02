/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{
    ffi::{c_char, c_int},
    fmt::Debug,
    io::{Cursor, Read, Seek, Write},
    marker::PhantomData,
    mem::ManuallyDrop,
};

use enumflags2::{BitFlags, bitflags};
pub use ffi::{RSDocumentMetadata, RSQueryTerm, RSYieldableMetric, t_docId, t_fieldMask};

pub mod freqs_only;
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

/// Represents a single record of a document inside a term in the inverted index
#[repr(C)]
#[derive(Debug, PartialEq)]
pub struct RSTermRecord {
    /// The term that brought up this record
    pub term: *mut RSQueryTerm,

    /// The encoded offsets in which the term appeared in the document
    pub offsets: RSOffsetVector,
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

impl RSIndexResult {
    /// Create a new numeric index result with the given numeric value
    pub fn numeric(doc_id: t_docId, num: f64) -> Self {
        Self {
            doc_id,
            dmd: std::ptr::null(),
            field_mask: 0,
            freq: 0,
            offsets_sz: 0,
            data: RSIndexResultData {
                num: ManuallyDrop::new(RSNumericRecord(num)),
            },
            result_type: RSResultType::Numeric,
            is_copy: false,
            metrics: std::ptr::null_mut(),
            weight: 0.0,
        }
    }

    /// Create a new virtual index result
    pub fn virt(doc_id: t_docId) -> Self {
        Self {
            doc_id,
            dmd: std::ptr::null(),
            field_mask: 0,
            freq: 0,
            offsets_sz: 0,
            data: RSIndexResultData {
                virt: ManuallyDrop::new(RSVirtualResult),
            },
            result_type: RSResultType::Virtual,
            is_copy: false,
            metrics: std::ptr::null_mut(),
            weight: 0.0,
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

    /// Create a new freqs only index result with the given frequency.
    pub fn freqs_only(doc_id: t_docId, freq: u32) -> Self {
        Self {
            doc_id,
            dmd: std::ptr::null(),
            field_mask: 0,
            freq,
            offsets_sz: 0,
            data: RSIndexResultData {
                virt: ManuallyDrop::new(RSVirtualResult),
            },
            result_type: RSResultType::Virtual,
            is_copy: false,
            metrics: std::ptr::null_mut(),
            weight: 0.0,
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
    /// Does this encoder allow the same document to appear in the index multiple times. Defaults
    /// to `false`.
    const ALLOW_DUPLICATES: bool = false;

    /// The number of entries that can be written in a single block. Defaults to 100.
    const BLOCK_ENTRIES: usize = 100;

    /// Write the record to the writer and return the number of bytes written. The delta is the
    /// pre-computed difference between the current document ID and the last document ID written.
    ///
    /// # Panics
    /// Non-numeric encoders only accept deltas that fit in a `u32`, and so will panic if the delta
    /// is larger than `u32::MAX`.
    /// When using such encoders the inverted index should create new blocks if the delta exceeds `u32::MAX`.
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
    fn decode<R: Read>(&self, reader: &mut R, base: t_docId) -> std::io::Result<DecoderResult>;

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
            match self.decode(reader, base) {
                Ok(DecoderResult::Record(record)) if record.doc_id >= target => {
                    return Ok(Some(record));
                }
                Ok(DecoderResult::Record(_)) | Ok(DecoderResult::FilteredOut) => continue,
                Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
                Err(err) => return Err(err),
            }
        }
    }
}

pub struct InvertedIndex<E> {
    blocks: Vec<IndexBlock>,
    num_docs: usize,
    _encoder: PhantomData<E>,
}

struct IndexBlock {
    first_doc_id: t_docId,
    last_doc_id: t_docId,
    num_entries: usize,
    buffer: Vec<u8>,
}

impl IndexBlock {
    fn new(doc_id: t_docId) -> Self {
        Self {
            first_doc_id: doc_id,
            last_doc_id: doc_id,
            num_entries: 0,
            buffer: Vec::new(),
        }
    }

    fn writer(&mut self) -> Cursor<&mut Vec<u8>> {
        let pos = self.buffer.len();
        let mut buffer = Cursor::new(&mut self.buffer);

        buffer.set_position(pos as u64);

        buffer
    }
}

impl<E: Encoder> InvertedIndex<E> {
    pub fn new() -> Self {
        Self {
            blocks: Vec::new(),
            num_docs: 0,
            _encoder: Default::default(),
        }
    }

    pub fn add_record(&mut self, record: &RSIndexResult) -> std::io::Result<usize> {
        let doc_id = record.doc_id;
        let last_doc_id = self.last_doc_id();

        let same_doc = match (
            E::ALLOW_DUPLICATES,
            last_doc_id.map(|d| d == doc_id).unwrap_or_default(),
        ) {
            (true, true) => true,
            (false, true) => {
                // The encoder does not allow writting the same document to the same index twice. This
                // can happen when the index is created with duplicate tags for example.
                return Ok(0);
            }
            (_, false) => false,
        };

        let delta = doc_id - last_doc_id.unwrap_or_default();
        let block = self.get_last_block(doc_id);
        let buffer = block.writer();

        let bytes_written = E::encode(buffer, Delta::new(delta as _), record)?;

        block.num_entries += 1;
        block.last_doc_id = doc_id;

        if !same_doc {
            self.num_docs += 1;
        }

        Ok(bytes_written)
    }

    fn last_doc_id(&self) -> Option<t_docId> {
        self.blocks.last().map(|b| b.last_doc_id)
    }

    fn get_last_block(&mut self, doc_id: t_docId) -> &mut IndexBlock {
        if self.blocks.is_empty() {
            let block = IndexBlock::new(doc_id);
            self.blocks.push(block);
        } else if self
            .blocks
            .last()
            .expect("we just confirmed there are blocks")
            .num_entries
            >= E::BLOCK_ENTRIES
        {
            // We need to create a new block since the last one is full
            let block = IndexBlock::new(doc_id);
            self.blocks.push(block);
        }

        self.blocks
            .last_mut()
            .expect("to get the last block or the one we just added")
    }
}

#[cfg(test)]
mod tests;
