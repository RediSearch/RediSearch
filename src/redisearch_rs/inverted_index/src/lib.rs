/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{
    ffi::c_void,
    io::{BufRead, Cursor, Seek, Write},
};

use ffi::FieldSpec;
pub use ffi::{t_docId, t_fieldMask};
pub use index_result::{
    RSAggregateResult, RSAggregateResultIter, RSIndexResult, RSOffsetVector, RSQueryTerm,
    RSResultData, RSResultKind, RSResultKindMask, RSTermRecord,
};

pub mod debug;
pub mod doc_ids_only;
pub mod fields_offsets;
pub mod fields_only;
pub mod freqs_fields;
pub mod freqs_offsets;
pub mod freqs_only;
pub mod full;
mod index_result;
pub mod numeric;
pub mod offsets_only;
pub mod raw_doc_ids_only;
#[doc(hidden)]
pub mod test_utils;

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

/// Filter details to apply to numeric values
/// cbindgen:rename-all=CamelCase
#[repr(C)]
pub struct NumericFilter {
    /// The field specification which this filter is acting on
    field_spec: *const FieldSpec,

    /// Beginning of the range
    min: f64,

    /// End of the range
    max: f64,

    /// Geo filter, if any
    geo_filter: *const c_void,

    /// Range includes the min value
    min_inclusive: bool,

    /// Range includes the max value
    max_inclusive: bool,

    /// Order of SORTBY (ascending/descending)
    ascending: bool,

    /// Minimum number of results needed
    limit: usize,

    /// Number of results to skip
    offset: usize,
}

impl NumericFilter {
    /// Check if this is a numeric filter (and not a geo filter)
    pub fn is_numeric_filter(&self) -> bool {
        self.geo_filter.is_null()
    }

    /// Check if the given value is in the range specified by this filter
    pub fn value_in_range(&self, value: f64) -> bool {
        let min_ok = value > self.min || (self.min_inclusive && value == self.min);
        let max_ok = value < self.max || (self.max_inclusive && value == self.max);

        min_ok && max_ok
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
        &self,
        writer: W,
        delta: Self::Delta,
        record: &RSIndexResult,
    ) -> std::io::Result<usize>;

    /// Returns the base value that should be used for any delta calculations
    fn delta_base(block: &IndexBlock) -> t_docId {
        block.last_doc_id
    }
}

/// Trait to model that an encoder can be decoded by a decoder.
pub trait DecodedBy: Encoder {
    type Decoder: Decoder;

    /// Create a new decoder that can be used to decode records encoded by this encoder.
    fn decoder() -> Self::Decoder;
}

/// Decoder to read records from an index
pub trait Decoder {
    /// Decode the next record from the reader. If any delta values are decoded, then they should
    /// add to the `base` document ID to get the actual document ID.
    fn decode<'index>(
        &self,
        cursor: &mut Cursor<&'index [u8]>,
        base: t_docId,
    ) -> std::io::Result<RSIndexResult<'index>>;

    /// Like `[Decoder::decode]`, but it skips all entries whose document ID is lower than `target`.
    ///
    /// Returns `None` if no record has a document ID greater than or equal to `target`.
    fn seek<'index>(
        &self,
        cursor: &mut Cursor<&'index [u8]>,
        base: t_docId,
        target: t_docId,
    ) -> std::io::Result<Option<RSIndexResult<'index>>> {
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

    /// The memory size of the index in bytes.
    pub fn memory_usage(&self) -> usize {
        let blocks_size: usize = self
            .blocks
            .iter()
            .map(|b| IndexBlock::SIZE + b.buffer.capacity())
            .sum();

        std::mem::size_of::<Self>() + blocks_size
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

    /// Returns the number of unique documents in the index.
    pub fn unique_docs(&self) -> usize {
        self.n_unique_docs
    }
}

impl<E: Encoder + DecodedBy> InvertedIndex<E> {
    /// Create a new [`IndexReader`] for this inverted index.
    pub fn reader(&self) -> IndexReader<'_, E::Decoder> {
        let decoder = E::decoder();
        IndexReader::new(&self.blocks, decoder)
    }
}

/// A wrapper around the inverted index to track the total number of entries in the index.
/// Unlike [`InvertedIndex::unique_docs()`], this counts all entries, including duplicates.
pub struct EntriesTrackingIndex<E> {
    /// The underlying inverted index that stores the entries.
    index: InvertedIndex<E>,

    /// The total number of entries in the index. This is not the number of unique documents, but
    /// rather the total number of entries added to the index.
    number_of_entries: usize,
}

impl<E: Encoder> EntriesTrackingIndex<E> {
    /// Create a new entries tracking index with the given encoder.
    pub fn new(encoder: E) -> Self {
        Self {
            index: InvertedIndex::new(encoder),
            number_of_entries: 0,
        }
    }

    /// Add a new record to the index and return by how much memory grew. It is expected that
    /// the document ID of the record is greater than or equal the last document ID in the index.
    ///
    /// The total number of entries in the index is incremented by one.
    pub fn add_record(&mut self, record: &RSIndexResult) -> std::io::Result<usize> {
        let mem_growth = self.index.add_record(record)?;

        self.number_of_entries += 1;

        Ok(mem_growth)
    }

    /// The memory size of the index in bytes.
    pub fn memory_usage(&self) -> usize {
        self.index.memory_usage() + std::mem::size_of::<usize>()
    }

    /// The total number of entries in the index. This is not the number of unique documents, but
    /// rather the total number of entries added to the index.
    pub fn number_of_entries(&self) -> usize {
        self.number_of_entries
    }
}

impl<E: Encoder + DecodedBy> EntriesTrackingIndex<E> {
    /// Create a new [`IndexReader`] for this inverted index.
    pub fn reader(&self) -> IndexReader<'_, impl Decoder> {
        self.index.reader()
    }
}

/// A wrapper around the inverted index which tracks the fields for all the records in the index
/// using a mask. This makes is easy to know if the index has any records for a specific field.
pub struct FieldMaskTrackingIndex<E> {
    /// The underlying inverted index that stores the records.
    index: InvertedIndex<E>,

    /// A field mask of all the entries in the index. This is used to quickly determine if a
    /// record with a specific field mask exists in the index.
    field_mask: t_fieldMask,
}

impl<E: Encoder> FieldMaskTrackingIndex<E> {
    /// Create a new field mask tracking index with the given encoder.
    pub fn new(encoder: E) -> Self {
        Self {
            index: InvertedIndex::new(encoder),
            field_mask: 0,
        }
    }

    /// Add a new record to the index and return by how much memory grew. It is expected that
    /// the document ID of the record is greater than or equal the last document ID in the index.
    pub fn add_record(&mut self, record: &RSIndexResult) -> std::io::Result<usize> {
        let mem_growth = self.index.add_record(record)?;

        self.field_mask |= record.field_mask;

        Ok(mem_growth)
    }

    /// The memory size of the index in bytes.
    pub fn memory_usage(&self) -> usize {
        self.index.memory_usage() + std::mem::size_of::<t_fieldMask>()
    }

    /// Get the combined field mask of all records in the index.
    pub fn field_mask(&self) -> t_fieldMask {
        self.field_mask
    }
}

impl<E: Encoder + DecodedBy> FieldMaskTrackingIndex<E> {
    /// Create a new [`IndexReader`] for this inverted index.
    pub fn reader(&self) -> IndexReader<'_, impl Decoder> {
        self.index.reader()
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
    pub fn next_record(&mut self) -> std::io::Result<Option<RSIndexResult<'index>>> {
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

impl<'index, I: Iterator<Item = RSIndexResult<'index>>> SkipDuplicatesReader<I> {
    /// Create a new skip duplicates reader over the given inner iterator.
    pub fn new(inner: I) -> Self {
        Self {
            last_doc_id: 0,
            inner,
        }
    }
}

impl<'index, I: Iterator<Item = RSIndexResult<'index>>> Iterator for SkipDuplicatesReader<I> {
    type Item = RSIndexResult<'index>;

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

impl<'index, I: Iterator<Item = RSIndexResult<'index>>> FilterMaskReader<I> {
    /// Create a new filter mask reader with the given mask and inner iterator
    pub fn new(mask: t_fieldMask, inner: I) -> Self {
        Self { mask, inner }
    }
}

impl<'index, I: Iterator<Item = RSIndexResult<'index>>> Iterator for FilterMaskReader<I> {
    type Item = RSIndexResult<'index>;

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
