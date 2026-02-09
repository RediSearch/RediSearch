/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Numeric index types for the numeric range tree.
//!
//! This module contains the index and reader enums that abstract over
//! compressed and uncompressed numeric storage in inverted indexes.

use ffi::IndexFlags_Index_StoreNumeric;
use inverted_index::{
    EntriesTrackingIndex, IndexReader, IndexReaderCore, RSIndexResult,
    numeric::{Numeric, NumericFloatCompression},
};

/// Enum to hold either compressed or uncompressed numeric index.
#[derive(Debug)]
pub enum NumericIndex {
    /// Uncompressed: stores f64 values at full precision (8 bytes).
    Uncompressed(EntriesTrackingIndex<Numeric>),
    /// Compressed: attempts to compress f64 → f32 (4 bytes) when precision loss is deemed acceptable.
    Compressed(EntriesTrackingIndex<NumericFloatCompression>),
}

impl NumericIndex {
    /// Create a new numeric index.
    ///
    /// If `compress_floats` is true, the index will use float compression which
    /// attempts to store f64 values as f32 when precision loss is acceptable (< 0.01).
    pub fn new(compress_floats: bool) -> Self {
        if compress_floats {
            NumericIndex::Compressed(EntriesTrackingIndex::new(IndexFlags_Index_StoreNumeric))
        } else {
            NumericIndex::Uncompressed(EntriesTrackingIndex::new(IndexFlags_Index_StoreNumeric))
        }
    }

    /// Add a record to the index, returning bytes written.
    ///
    /// # Panics
    ///
    /// Panics if the underlying write fails. This should never happen with
    /// in-memory inverted indexes, so a panic indicates a bug.
    pub fn add_record(&mut self, record: &RSIndexResult<'_>) -> usize {
        let result = match self {
            NumericIndex::Uncompressed(idx) => idx.add_record(record),
            NumericIndex::Compressed(idx) => idx.add_record(record),
        };
        result.expect("in-memory inverted index write cannot fail")
    }

    /// Get the number of unique documents in this index.
    pub fn num_docs(&self) -> u32 {
        match self {
            NumericIndex::Uncompressed(idx) => idx.summary().number_of_docs,
            NumericIndex::Compressed(idx) => idx.summary().number_of_docs,
        }
    }

    /// Get the number of blocks in this index.
    pub fn num_blocks(&self) -> usize {
        match self {
            NumericIndex::Uncompressed(idx) => idx.summary().number_of_blocks,
            NumericIndex::Compressed(idx) => idx.summary().number_of_blocks,
        }
    }

    /// Get a reader for iterating over the entries.
    ///
    /// Returns an enum that can be either uncompressed or compressed reader.
    pub fn reader(&self) -> NumericIndexReader<'_> {
        match self {
            NumericIndex::Uncompressed(idx) => NumericIndexReader::Uncompressed(idx.reader()),
            NumericIndex::Compressed(idx) => NumericIndexReader::Compressed(idx.reader()),
        }
    }

    /// Get the number of entries in this index.
    pub const fn number_of_entries(&self) -> usize {
        match self {
            NumericIndex::Uncompressed(idx) => idx.number_of_entries(),
            NumericIndex::Compressed(idx) => idx.number_of_entries(),
        }
    }

    /// Get the number of unique documents.
    pub const fn unique_docs(&self) -> u32 {
        match self {
            NumericIndex::Uncompressed(idx) => idx.unique_docs(),
            NumericIndex::Compressed(idx) => idx.unique_docs(),
        }
    }

    /// Get the memory usage of this index in bytes.
    pub fn memory_usage(&self) -> usize {
        match self {
            NumericIndex::Uncompressed(idx) => idx.memory_usage(),
            NumericIndex::Compressed(idx) => idx.memory_usage(),
        }
    }
}

/// Iterate over the entries stored in a numeric index.
///
/// This abstracts over whether the underlying index is compressed or uncompressed.
pub enum NumericIndexReader<'a> {
    /// Reader over uncompressed entries.
    Uncompressed(IndexReaderCore<'a, Numeric>),
    /// Reader over compressed entries.
    Compressed(IndexReaderCore<'a, NumericFloatCompression>),
}

impl<'a> IndexReader<'a> for NumericIndexReader<'a> {
    fn next_record(&mut self, result: &mut RSIndexResult<'a>) -> std::io::Result<bool> {
        match self {
            Self::Uncompressed(r) => r.next_record(result),
            Self::Compressed(r) => r.next_record(result),
        }
    }

    fn seek_record(
        &mut self,
        doc_id: ffi::t_docId,
        result: &mut RSIndexResult<'a>,
    ) -> std::io::Result<bool> {
        match self {
            Self::Uncompressed(r) => r.seek_record(doc_id, result),
            Self::Compressed(r) => r.seek_record(doc_id, result),
        }
    }

    fn skip_to(&mut self, doc_id: ffi::t_docId) -> bool {
        match self {
            Self::Uncompressed(r) => r.skip_to(doc_id),
            Self::Compressed(r) => r.skip_to(doc_id),
        }
    }

    fn reset(&mut self) {
        match self {
            Self::Uncompressed(r) => r.reset(),
            Self::Compressed(r) => r.reset(),
        }
    }

    fn unique_docs(&self) -> u64 {
        match self {
            Self::Uncompressed(r) => r.unique_docs(),
            Self::Compressed(r) => r.unique_docs(),
        }
    }

    fn has_duplicates(&self) -> bool {
        match self {
            Self::Uncompressed(r) => r.has_duplicates(),
            Self::Compressed(r) => r.has_duplicates(),
        }
    }

    fn flags(&self) -> ffi::IndexFlags {
        match self {
            Self::Uncompressed(r) => r.flags(),
            Self::Compressed(r) => r.flags(),
        }
    }

    fn needs_revalidation(&self) -> bool {
        match self {
            Self::Uncompressed(r) => r.needs_revalidation(),
            Self::Compressed(r) => r.needs_revalidation(),
        }
    }

    fn refresh_buffer_pointers(&mut self) {
        match self {
            Self::Uncompressed(r) => r.refresh_buffer_pointers(),
            Self::Compressed(r) => r.refresh_buffer_pointers(),
        }
    }
}
