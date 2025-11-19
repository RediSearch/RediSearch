/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use serde::{Deserialize, Serialize};
use std::{
    ffi::c_void,
    io::{Cursor, Seek, Write},
    marker::PhantomData,
    sync::atomic::{self, AtomicU32, AtomicUsize},
};

use controlled_cursor::ControlledCursor;
use debug::{BlockSummary, Summary};
use ffi::{
    FieldSpec, GeoFilter, IndexFlags, IndexFlags_Index_DocIdsOnly, IndexFlags_Index_HasMultiValue,
    IndexFlags_Index_StoreFieldFlags,
};
pub use ffi::{t_docId, t_fieldMask};
pub use index_result::{
    FieldMaskOrIndex, RSAggregateResult, RSAggregateResultIter, RSIndexResult, RSOffsetVector,
    RSQueryTerm, RSResultData, RSResultKind, RSResultKindMask, RSTermRecord,
    ResultMetrics_Reset_func,
};
use smallvec::SmallVec;

pub mod controlled_cursor;
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

// Manually define some C functions, because we'll create a circular dependency if we use the FFI
// crate to make them automatically.
unsafe extern "C" {
    /// Checks if a value (distance) is within the given geo filter.
    ///
    /// # Safety
    /// The [`GeoFilter`] should not be null and a valid instance
    #[allow(improper_ctypes)] // The doc_id in `RSIndexResult` might be a u128
    unsafe fn isWithinRadius(gf: *const GeoFilter, d: f64, distance: *mut f64) -> bool;
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

/// Filter details to apply to numeric values
/// cbindgen:rename-all=CamelCase
#[derive(Debug)]
#[repr(C)]
pub struct NumericFilter {
    /// The field specification which this filter is acting on
    pub field_spec: *const FieldSpec,

    /// Beginning of the range
    pub min: f64,

    /// End of the range
    pub max: f64,

    /// Geo filter, if any
    pub geo_filter: *const c_void,

    /// Range includes the min value
    pub min_inclusive: bool,

    /// Range includes the max value
    pub max_inclusive: bool,

    /// Order of SORTBY (ascending/descending)
    pub ascending: bool,

    /// Minimum number of results needed
    pub limit: usize,

    /// Number of results to skip
    pub offset: usize,
}

impl Default for NumericFilter {
    fn default() -> Self {
        Self {
            min: 0.0,
            max: f64::MAX,
            min_inclusive: true,
            max_inclusive: true,
            field_spec: std::ptr::null(),
            geo_filter: std::ptr::null(),
            ascending: true,
            limit: 0,
            offset: 0,
        }
    }
}

impl NumericFilter {
    /// Check if this is a numeric filter (and not a geo filter)
    pub const fn is_numeric_filter(&self) -> bool {
        self.geo_filter.is_null()
    }

    /// Check if the given value is in the range specified by this filter
    #[inline(always)]
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
    const RECOMMENDED_BLOCK_ENTRIES: u16 = 100;

    /// Write the record to the writer and return the number of bytes written. The delta is the
    /// pre-computed difference between the current document ID and the last document ID written.
    fn encode<W: Write + Seek>(
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
}

impl<E: Encoder + Decoder> DecodedBy for E {
    type Decoder = E;
}

/// Decoder to read records from an index
pub trait Decoder {
    /// Decode the next record from the reader. If any delta values are decoded, then they should
    /// add to the `base` document ID to get the actual document ID.
    fn decode<'index>(
        cursor: &mut Cursor<&'index [u8]>,
        base: t_docId,
        result: &mut RSIndexResult<'index>,
    ) -> std::io::Result<()>;

    /// Creates a new instance of [`RSIndexResult`] which this decoder can decode into.
    fn base_result<'index>() -> RSIndexResult<'index>;

    /// Like `[Decoder::decode]`, but it returns a new instance of the result instead of filling
    /// an existing one.
    fn decode_new<'index>(
        cursor: &mut Cursor<&'index [u8]>,
        base: t_docId,
    ) -> std::io::Result<RSIndexResult<'index>> {
        let mut result = Self::base_result();
        Self::decode(cursor, base, &mut result)?;

        Ok(result)
    }

    /// Like `[Decoder::decode]`, but it skips all entries whose document ID is lower than `target`.
    ///
    /// Decoders can override the default implementation to provide a more efficient one by reading the
    /// document ID first and skipping ahead if the ID does not match the target, saving decoding
    /// the rest of the record.
    ///
    /// Returns `false` if end of the cursor was reached before finding a document equal, or bigger,
    /// than the target.
    fn seek<'index>(
        cursor: &mut Cursor<&'index [u8]>,
        mut base: t_docId,
        target: t_docId,
        result: &mut RSIndexResult<'index>,
    ) -> std::io::Result<bool> {
        loop {
            match Self::decode(cursor, base, result) {
                Ok(_) if result.doc_id >= target => {
                    return Ok(true);
                }
                Ok(_) => {
                    base = result.doc_id;
                    continue;
                }
                Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(false),
                Err(err) => return Err(err),
            }
        }
    }

    /// Returns the base value to use for any delta calculations
    fn base_id(_block: &IndexBlock, last_doc_id: t_docId) -> t_docId {
        last_doc_id
    }
}

/// Marker trait for decoders producing numeric results.
pub trait NumericDecoder: Decoder {}
/// Marker trait for decoders producing term results.
pub trait TermDecoder: Decoder {}

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
    n_unique_docs: u32,

    /// The flags of this index. This is used to determine the type of index and how it should be
    /// handled.
    flags: IndexFlags,

    /// A marker used by the garbage collector to determine if the index has been modified since
    /// the last GC pass. This is used to reset a reader if the index has been modified.
    gc_marker: AtomicU32,

    /// The encoder to use when adding new entries to the index
    _encoder: PhantomData<E>,
}

/// Each `IndexBlock` contains a set of entries for a specific range of document IDs. The entries
/// are ordered by document ID, so the first entry in the block has the lowest document ID, and the
/// last entry has the highest document ID. The block also contains a buffer that is used to
/// store the encoded entries. The buffer is dynamically resized as needed when new entries are
/// added to the block.
#[derive(Debug, Eq, PartialEq, Serialize)]
pub struct IndexBlock {
    /// The first document ID in this block. This is used to determine the range of document IDs
    /// that this block covers.
    first_doc_id: t_docId,

    /// The last document ID in this block. This is used to determine the range of document IDs
    /// that this block covers.
    last_doc_id: t_docId,

    /// The total number of non-unique entries in this block
    num_entries: u16,

    /// The encoded entries in this block
    buffer: Vec<u8>,
}

static TOTAL_BLOCKS: AtomicUsize = AtomicUsize::new(0);

/// Custom deserialization for `IndexBlock` to track the total number of blocks correctly.
impl<'de> Deserialize<'de> for IndexBlock {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct IB {
            first_doc_id: t_docId,
            last_doc_id: t_docId,
            num_entries: u16,
            buffer: Vec<u8>,
        }

        let ib = IB::deserialize(deserializer)?;

        // We are about to create a new `IndexBlock` object, so be sure to increment the global
        // counter correctly. Without this the `Drop` implementation will eventually cause an
        // underflow of the counter. This correctly counter balances the decrement in the `Drop`.
        TOTAL_BLOCKS.fetch_add(1, atomic::Ordering::Relaxed);

        Ok(IndexBlock {
            first_doc_id: ib.first_doc_id,
            last_doc_id: ib.last_doc_id,
            num_entries: ib.num_entries,
            buffer: ib.buffer,
        })
    }
}

/// The type of repair needed for a block after a garbage collection scan.
#[derive(Debug, Eq, PartialEq, Deserialize, Serialize)]
enum RepairType {
    /// This block can be deleted completely.
    Delete {
        /// Number of unique records this will remove
        n_unique_docs_removed: u32,
    },

    /// The block contains GCed entries, and should be replaced with the following blocks.
    Replace {
        /// The new blocks to replace this block with
        blocks: SmallVec<[IndexBlock; 3]>,

        /// How many unique documents were removed from the block being replaced.
        n_unique_docs_removed: u32,
    },
}

impl IndexBlock {
    const SIZE: usize = std::mem::size_of::<Self>();

    /// Make a new index block with primed with the initial doc ID. The next entry written into
    /// the block should be for this doc ID else the block will contain incoherent data.
    ///
    /// This returns the block and how much memory grew by.
    fn new(doc_id: t_docId) -> (Self, usize) {
        let this = Self {
            first_doc_id: doc_id,
            last_doc_id: doc_id,
            num_entries: 0,
            buffer: Vec::new(),
        };
        let buf_cap = this.buffer.capacity();

        TOTAL_BLOCKS.fetch_add(1, atomic::Ordering::Relaxed);

        (this, Self::SIZE + buf_cap)
    }

    /// Get the first document ID in this block. This is only needed for some C tests.
    pub const fn first_block_id(&self) -> t_docId {
        self.first_doc_id
    }

    /// Get the last document ID in the block. This is only needed for some C tests.
    pub const fn last_block_id(&self) -> t_docId {
        self.last_doc_id
    }

    /// Get the number of entries in this block. This is only needed for some C tests.
    pub const fn num_entries(&self) -> u16 {
        self.num_entries
    }

    /// Get a reference to the encoded data in this block. This is only needed for some C tests.
    pub fn data(&self) -> &[u8] {
        &self.buffer
    }

    const fn writer(&mut self) -> ControlledCursor<'_> {
        ControlledCursor::new(&mut self.buffer)
    }

    /// Returns the total number of index blocks in existence.
    pub fn total_blocks() -> usize {
        TOTAL_BLOCKS.load(atomic::Ordering::Relaxed)
    }

    /// Repair a block by removing records which no longer exists according to `doc_exists`. If a
    /// record does exist, then `repair` is called with it.
    ///
    /// `None` is returned when there is nothing to repair in this block.
    fn repair<'index, E: Encoder + DecodedBy<Decoder = D>, D: Decoder>(
        &'index self,
        doc_exist: impl Fn(t_docId) -> bool,
        mut repair: Option<impl FnMut(&RSIndexResult<'index>, &IndexBlock)>,
        _encoder: PhantomData<E>,
    ) -> std::io::Result<Option<RepairType>> {
        let mut cursor: Cursor<&'index [u8]> = Cursor::new(&self.buffer);
        let mut last_read_doc_id = None;
        let mut result = D::base_result();
        let mut unique_read = 0;
        let mut unique_write = 0;
        let mut block_changed = false;

        let mut tmp_inverted_index = InvertedIndex::<E>::new(IndexFlags_Index_DocIdsOnly);

        while self.buffer.len() as u64 > cursor.position() {
            let base = D::base_id(self, last_read_doc_id.unwrap_or(self.first_doc_id));
            D::decode(&mut cursor, base, &mut result)?;

            if doc_exist(result.doc_id) {
                if let Some(repair) = repair.as_mut() {
                    repair(&result, self);
                }

                tmp_inverted_index.add_record(&result)?;

                if last_read_doc_id.is_none_or(|id| id != result.doc_id) {
                    unique_write += 1;
                }
            } else {
                block_changed = true;
            }

            if last_read_doc_id.is_none_or(|id| id != result.doc_id) {
                unique_read += 1;
            }

            last_read_doc_id = Some(result.doc_id);
        }

        if tmp_inverted_index.blocks.is_empty() {
            Ok(Some(RepairType::Delete {
                n_unique_docs_removed: unique_read,
            }))
        } else if block_changed {
            Ok(Some(RepairType::Replace {
                blocks: tmp_inverted_index.blocks.into(),
                n_unique_docs_removed: unique_read - unique_write,
            }))
        } else {
            Ok(None)
        }
    }
}

impl Drop for IndexBlock {
    fn drop(&mut self) {
        TOTAL_BLOCKS.fetch_sub(1, atomic::Ordering::Relaxed);
    }
}

impl<E: Encoder> InvertedIndex<E> {
    /// Create a new inverted index with the given encoder. The encoder is used to write new
    /// entries to the index.
    pub fn new(flags: IndexFlags) -> Self {
        Self {
            blocks: Vec::new(),
            n_unique_docs: 0,
            flags,
            gc_marker: AtomicU32::new(0),
            _encoder: Default::default(),
        }
    }

    /// Create a new inverted index from the given blocks and encoder. The blocks are expected to not
    /// contain duplicate entries and be ordered by document ID.
    #[cfg(test)]
    fn from_blocks(flags: IndexFlags, blocks: Vec<IndexBlock>) -> Self {
        debug_assert!(!blocks.is_empty());
        debug_assert!(
            blocks.is_sorted_by(|a, b| a.last_doc_id < b.first_doc_id),
            "blocks must be sorted and not overlap"
        );
        debug_assert!(
            blocks.iter().all(|b| b.first_doc_id <= b.last_doc_id),
            "blocks must have valid ranges"
        );

        let n_unique_docs = blocks.iter().map(|b| b.num_entries as u32).sum();

        Self {
            blocks,
            n_unique_docs,
            flags,
            gc_marker: AtomicU32::new(0),
            _encoder: Default::default(),
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
                self.add_block(block);
                block = new_block;
                mem_growth += block_size;

                E::Delta::zero()
            }
        };

        let buf_cap = block.buffer.capacity();
        let writer = block.writer();
        let _bytes_written = E::encode(writer, delta, record)?;

        // We don't use `_bytes_written` returned by the encoder to determine by how much memory
        // grew because the buffer might have had enough capacity for the bytes in the encoding.
        // Instead we took the capacity of the buffer before the write and now check by how much it
        // has increased (if any).
        let buf_growth = block.buffer.capacity() - buf_cap;

        debug_assert!(block.num_entries.saturating_add(1) < u16::MAX);
        block.num_entries += 1;
        block.last_doc_id = doc_id;

        // We took ownership of the block so put it back
        self.add_block(block);

        if !same_doc {
            self.n_unique_docs += 1;
        } else {
            self.flags |= IndexFlags_Index_HasMultiValue;
        }

        Ok(buf_growth + mem_growth)
    }

    /// Returns the last document ID in the index, if any.
    pub fn last_doc_id(&self) -> Option<t_docId> {
        self.blocks.last().map(|b| b.last_doc_id)
    }

    /// Take a block that can be written to and report by how much memory grew
    fn take_block(&mut self, doc_id: t_docId, same_doc: bool) -> (IndexBlock, usize) {
        if self.blocks.is_empty()
            || (
                // If the block is full
                !same_doc
                    && self
                        .blocks
                        .last()
                        .expect("we just confirmed there are blocks")
                        .num_entries
                        >= E::RECOMMENDED_BLOCK_ENTRIES
            )
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

    /// Add a block back to the index. This allows us to control the growth strategy used by the
    /// `blocks` vector.
    fn add_block(&mut self, block: IndexBlock) {
        if self.blocks.len() == self.blocks.capacity() {
            self.blocks.reserve_exact(1);
        }

        self.blocks.push(block);
    }

    /// Returns the number of unique documents in the index.
    pub const fn unique_docs(&self) -> u32 {
        self.n_unique_docs
    }

    /// Returns the flags of this index.
    pub const fn flags(&self) -> IndexFlags {
        self.flags
    }

    /// Return the debug summary for this inverted index.
    pub fn summary(&self) -> Summary {
        Summary {
            number_of_docs: self.n_unique_docs,
            number_of_entries: self.n_unique_docs as usize,
            last_doc_id: self.last_doc_id().unwrap_or(0),
            flags: self.flags as _,
            number_of_blocks: self.blocks.len(),
            block_efficiency: 0.0,
            has_efficiency: false,
        }
    }

    /// Return basic information about the blocks in this inverted index.
    pub fn blocks_summary(&self) -> Vec<BlockSummary> {
        self.blocks
            .iter()
            .map(|b| BlockSummary {
                first_doc_id: b.first_doc_id,
                last_doc_id: b.last_doc_id,
                number_of_entries: b.num_entries,
            })
            .collect()
    }

    /// Returns the number of blocks in this index.
    pub const fn number_of_blocks(&self) -> usize {
        self.blocks.len()
    }

    /// Get a reference to the block at the given index, if it exists. This is only used by some C tests.
    pub fn block_ref(&self, index: usize) -> Option<&IndexBlock> {
        self.blocks.get(index)
    }

    /// Get the current GC marker of this index. This is only used by the some C tests.
    pub fn gc_marker(&self) -> u32 {
        self.gc_marker.load(atomic::Ordering::Relaxed)
    }

    /// Increment the GC marker of this index. This is only used by the some C tests.
    pub fn gc_marker_inc(&self) {
        self.gc_marker.fetch_add(1, atomic::Ordering::Relaxed);
    }
}

/// Result of scanning the index for garbage collection
#[derive(Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct GcScanDelta {
    /// The index of the last block in the index at the time of the scan. This is used to ensure
    /// that the index has not changed since the scan was performed.
    last_block_idx: usize,

    /// The number of entries in the last block at the time of the scan. This is used to ensure
    /// that the index has not changed since the scan was performed.
    last_block_num_entries: u16,

    /// The results of the scan for each block that needs to be repaired or deleted.
    deltas: Vec<BlockGcScanResult>,
}

impl GcScanDelta {
    /// Returns the index of the last block in the index at the time of the scan.
    pub const fn last_block_idx(&self) -> usize {
        self.last_block_idx
    }
}

/// Result of scanning a block for garbage collection
#[derive(Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct BlockGcScanResult {
    /// The index of the block in the inverted index
    index: usize,

    /// The type of repair needed for this block
    repair: RepairType,
}

/// Information about the result of applying a garbage collection scan to the index
#[derive(Debug, Eq, PartialEq)]
#[repr(C)]
pub struct GcApplyInfo {
    /// The number of bytes that were freed
    pub bytes_freed: usize,

    /// The number of bytes that were allocated
    pub bytes_allocated: usize,

    /// The number of entries that were removed from the index including duplicates
    pub entries_removed: usize,

    /// The number of blocks that were ignored because the index changed since the scan was performed
    pub blocks_ignored: usize,
}

impl<E: Encoder + DecodedBy> InvertedIndex<E> {
    /// Create a new [`IndexReader`] for this inverted index.
    pub fn reader(&self) -> IndexReaderCore<'_, E> {
        IndexReaderCore::new(self)
    }

    /// Scan the index for blocks that can be garbage collected. A block can be garbage collected
    /// if any of its records point to documents that no longer exist. The `doc_exist`
    /// callback is used to check if a document exists. It should return `true` if the document
    /// exists and `false` otherwise.
    ///
    /// If a doc does exist, then `repair` is called with it to run any repair calculations needed.
    ///
    /// This function returns a delta if GC is needed, or `None` if no GC is needed.
    pub fn scan_gc<'index>(
        &'index self,
        doc_exist: impl Fn(t_docId) -> bool,
        mut repair: Option<impl FnMut(&RSIndexResult<'index>, &IndexBlock)>,
    ) -> std::io::Result<Option<GcScanDelta>> {
        let mut results = Vec::new();

        for (i, block) in self.blocks.iter().enumerate() {
            let repair = block.repair(&doc_exist, repair.as_mut(), PhantomData::<E>)?;

            if let Some(repair) = repair {
                results.push(BlockGcScanResult { index: i, repair });
            }
        }

        if results.is_empty() {
            Ok(None)
        } else {
            Ok(Some(GcScanDelta {
                last_block_idx: self.blocks.len() - 1,
                last_block_num_entries: self.blocks.last().map(|b| b.num_entries).unwrap_or(0),
                deltas: results,
            }))
        }
    }

    /// Apply the deltas of a garbage collection scan to the index. This will modify the index
    /// by deleting or repairing blocks as needed.
    pub fn apply_gc(&mut self, delta: GcScanDelta) -> GcApplyInfo {
        let GcScanDelta {
            last_block_idx,
            last_block_num_entries,
            deltas,
        } = delta;

        let mut info = GcApplyInfo {
            bytes_freed: 0,
            bytes_allocated: 0,
            entries_removed: 0,
            blocks_ignored: 0,
        };

        // Check if the last block has changed since the scan was performed
        let last_block_changed = self
            .blocks
            .get(last_block_idx)
            .is_some_and(|b| b.num_entries != last_block_num_entries);

        // If the last block has changed, then we need to ignore any deltas that refer to it
        let deltas = if last_block_changed {
            deltas
                .into_iter()
                .filter(|d| {
                    if d.index == last_block_idx {
                        // The last block has changed since the scan, so we ignore this delta
                        info.blocks_ignored += 1;
                        false
                    } else {
                        true
                    }
                })
                .collect()
        } else {
            deltas
        };

        // There is no point in moving everything to a new vector if there are no deltas
        if deltas.is_empty() {
            return info;
        }

        let mut tmp_blocks = Vec::with_capacity(self.blocks.len());
        std::mem::swap(&mut self.blocks, &mut tmp_blocks);

        let mut deltas = deltas.into_iter().peekable();

        for (block_index, block) in tmp_blocks.into_iter().enumerate() {
            match deltas.peek() {
                Some(delta) if delta.index == block_index => {
                    // This block needs to be repaired
                    let Some(delta) = deltas.next() else {
                        unreachable!(
                            "we are in the `Some` case and therefore know the next value exists"
                        )
                    };

                    match delta.repair {
                        RepairType::Delete {
                            n_unique_docs_removed,
                        } => {
                            info.entries_removed += block.num_entries as usize;
                            info.bytes_freed += IndexBlock::SIZE + block.buffer.capacity();
                            self.n_unique_docs -= n_unique_docs_removed;
                        }
                        RepairType::Replace {
                            blocks,
                            n_unique_docs_removed,
                        } => {
                            info.entries_removed += block.num_entries as usize;
                            info.bytes_freed += IndexBlock::SIZE + block.buffer.capacity();
                            self.n_unique_docs -= n_unique_docs_removed;

                            for block in blocks {
                                info.entries_removed -= block.num_entries as usize;
                                info.bytes_allocated += IndexBlock::SIZE + block.buffer.capacity();
                                self.blocks.push(block);
                            }
                        }
                    }
                }
                _ => {
                    // This block does not need to be repaired, so just put it back
                    self.blocks.push(block);
                }
            }
        }

        self.blocks.shrink_to_fit();
        self.gc_marker_inc();

        info
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
    pub fn new(flags: IndexFlags) -> Self {
        Self {
            index: InvertedIndex::new(flags),
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
    pub const fn number_of_entries(&self) -> usize {
        self.number_of_entries
    }

    /// Returns the last document ID in the index, if any.
    pub fn last_doc_id(&self) -> Option<t_docId> {
        self.index.last_doc_id()
    }

    /// Returns the number of unique documents in the index.
    pub const fn unique_docs(&self) -> u32 {
        self.index.unique_docs()
    }

    /// Returns the flags of this index.
    pub const fn flags(&self) -> IndexFlags {
        self.index.flags()
    }

    /// Return the debug summary for this inverted index.
    pub fn summary(&self) -> Summary {
        let mut summary = self.index.summary();

        summary.number_of_entries = self.number_of_entries;
        summary.has_efficiency = true;

        if summary.number_of_blocks > 0 {
            summary.block_efficiency =
                summary.number_of_entries as f64 / summary.number_of_blocks as f64
        }

        summary
    }

    /// Return basic information about the blocks in this inverted index.
    pub fn blocks_summary(&self) -> Vec<BlockSummary> {
        self.index.blocks_summary()
    }

    /// Returns the number of blocks in this index.
    pub const fn number_of_blocks(&self) -> usize {
        self.index.number_of_blocks()
    }

    /// Get a reference to the block at the given index, if it exists. This is only used by some C tests.
    pub fn block_ref(&self, index: usize) -> Option<&IndexBlock> {
        self.index.block_ref(index)
    }

    /// Get the current GC marker of this index. This is only used by the some C tests.
    pub fn gc_marker(&self) -> u32 {
        self.index.gc_marker()
    }

    /// Increment the GC marker of this index. This is only used by the some C tests.
    pub fn gc_marker_inc(&self) {
        self.index.gc_marker_inc();
    }

    /// Get a reference to the inner inverted index.
    pub const fn inner(&self) -> &InvertedIndex<E> {
        &self.index
    }
}

impl<E: Encoder + DecodedBy> EntriesTrackingIndex<E> {
    /// Create a new [`IndexReader`] for this inverted index.
    pub fn reader(&self) -> IndexReaderCore<'_, E> {
        self.index.reader()
    }

    /// Scan the index for blocks that can be garbage collected. A block can be garbage collected
    /// if any of its records point to documents that no longer exist. The `doc_exist`
    /// callback is used to check if a document exists. It should return `true` if the document
    /// exists and `false` otherwise.
    ///
    /// If a doc does exist, then `repair` is called with it to run any repair calculations needed.
    ///
    /// This function returns a delta if GC is needed, or `None` if no GC is needed.
    pub fn scan_gc<'index>(
        &'index self,
        doc_exist: impl Fn(t_docId) -> bool,
        repair: Option<impl FnMut(&RSIndexResult<'index>, &IndexBlock)>,
    ) -> std::io::Result<Option<GcScanDelta>> {
        self.index.scan_gc(doc_exist, repair)
    }

    /// Apply the deltas of a garbage collection scan to the index. This will modify the index
    /// by deleting or repairing blocks as needed.
    pub fn apply_gc(&mut self, delta: GcScanDelta) -> GcApplyInfo {
        let info = self.index.apply_gc(delta);

        self.number_of_entries -= info.entries_removed;

        info
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
    pub fn new(flags: IndexFlags) -> Self {
        debug_assert!(
            flags & IndexFlags_Index_StoreFieldFlags > 1,
            "FieldMaskTrackingIndex should only be used with indices that store field flags"
        );

        Self {
            index: InvertedIndex::new(flags),
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

    /// Returns the last document ID in the index, if any.
    pub fn last_doc_id(&self) -> Option<t_docId> {
        self.index.last_doc_id()
    }

    /// Returns the number of unique documents in the index.
    pub const fn unique_docs(&self) -> u32 {
        self.index.unique_docs()
    }

    /// Returns the flags of this index.
    pub const fn flags(&self) -> IndexFlags {
        self.index.flags()
    }

    /// Get the combined field mask of all records in the index.
    pub const fn field_mask(&self) -> t_fieldMask {
        self.field_mask
    }

    /// Return the debug summary for this inverted index.
    pub fn summary(&self) -> Summary {
        self.index.summary()
    }

    /// Return basic information about the blocks in this inverted index.
    pub fn blocks_summary(&self) -> Vec<BlockSummary> {
        self.index.blocks_summary()
    }

    /// Returns the number of blocks in this index.
    pub const fn number_of_blocks(&self) -> usize {
        self.index.number_of_blocks()
    }

    /// Get a reference to the block at the given index, if it exists. This is only used by some C tests.
    pub fn block_ref(&self, index: usize) -> Option<&IndexBlock> {
        self.index.block_ref(index)
    }

    /// Get the current GC marker of this index. This is only used by the some C tests.
    pub fn gc_marker(&self) -> u32 {
        self.index.gc_marker()
    }

    /// Increment the GC marker of this index. This is only used by the some C tests.
    pub fn gc_marker_inc(&self) {
        self.index.gc_marker_inc();
    }

    /// Get a reference to the inner inverted index.
    pub const fn inner(&self) -> &InvertedIndex<E> {
        &self.index
    }
}

impl<E: Encoder + DecodedBy> FieldMaskTrackingIndex<E> {
    /// Create a new [`IndexReader`] for this inverted index.
    pub fn reader(&self, mask: t_fieldMask) -> FilterMaskReader<IndexReaderCore<'_, E>> {
        FilterMaskReader::new(mask, self.index.reader())
    }

    /// Scan the index for blocks that can be garbage collected. A block can be garbage collected
    /// if any of its records point to documents that no longer exist. The `doc_exist`
    /// callback is used to check if a document exists. It should return `true` if the document
    /// exists and `false` otherwise.
    ///
    /// If a doc does exist, then `repair` is called with it to run any repair calculations needed.
    ///
    /// This function returns a delta if GC is needed, or `None` if no GC is needed.
    pub fn scan_gc<'index>(
        &'index self,
        doc_exist: impl Fn(t_docId) -> bool,
        repair: Option<impl FnMut(&RSIndexResult<'index>, &IndexBlock)>,
    ) -> std::io::Result<Option<GcScanDelta>> {
        self.index.scan_gc(doc_exist, repair)
    }

    /// Apply the deltas of a garbage collection scan to the index. This will modify the index
    /// by deleting or repairing blocks as needed.
    pub fn apply_gc(&mut self, delta: GcScanDelta) -> GcApplyInfo {
        self.index.apply_gc(delta)
    }
}

/// Reader that is able to read the records from an [`InvertedIndex`]
pub struct IndexReaderCore<'index, E> {
    /// The inverted index that is being read from.
    ii: &'index InvertedIndex<E>,

    /// The current position in the block that is being read from.
    current_buffer: Cursor<&'index [u8]>,

    /// The index of the current block in the `blocks` vector. This is used to keep track of
    /// which block we are currently reading from, especially when the current buffer is empty and we
    /// need to move to the next block.
    current_block_idx: usize,

    /// The last document ID that was read from the index. This is used to determine the base
    /// document ID for delta calculations.
    last_doc_id: t_docId,

    /// The marker of the inverted index when this reader last read from it. This is used to
    /// detect if the index has been modified since the last read, in which case the reader
    /// should be reset.
    gc_marker: u32,
}

/// A reader is something which knows how to read / decode the records from an `[InvertedIndex]`.
pub trait IndexReader<'index> {
    /// Read the next record from the index into `result`. If there are no more records to read,
    /// then `false` is returned.
    fn next_record(&mut self, result: &mut RSIndexResult<'index>) -> std::io::Result<bool>;

    /// Seek to the first record whose ID is higher or equal to the given document ID and put it
    /// on `recult`. If the end of the index is reached before finding the document ID, then `false`
    /// is returned.
    fn seek_record(
        &mut self,
        doc_id: t_docId,
        result: &mut RSIndexResult<'index>,
    ) -> std::io::Result<bool>;

    /// Skip forward to the block containing the given document ID. Returns false if the end of the
    /// index was reached and true otherwise.
    fn skip_to(&mut self, doc_id: t_docId) -> bool;

    /// Reset the reader to the beginning of the index.
    fn reset(&mut self);

    /// Return the number of unique documents in the underlying index.
    fn unique_docs(&self) -> u32;

    /// Returns true if the underlying index has duplicate document IDs.
    fn has_duplicates(&self) -> bool;

    /// Get the flags of the underlying index
    fn flags(&self) -> IndexFlags;

    /// Check if the underlying index has been modified since the last time this reader read from it.
    /// If it has, then the reader should be reset before reading from it again.
    fn needs_revalidation(&self) -> bool;
}

/// Marker trait for readers producing numeric values.
pub trait NumericReader<'index>: IndexReader<'index> {}

/// Marker trait for readers producing term values.
pub trait TermReader<'index>: IndexReader<'index> {}

// Automatically implemented if the IndexReaderCore uses a NumericDecoder.
impl<'index, E: DecodedBy<Decoder = D>, D: Decoder + NumericDecoder> NumericReader<'index>
    for IndexReaderCore<'index, E>
{
}

/// Automatically implemented if the IndexReaderCore uses a TermDecoder.
impl<'index, E: DecodedBy<Decoder = D>, D: Decoder + TermDecoder> TermReader<'index>
    for IndexReaderCore<'index, E>
{
}

impl<'index, E: DecodedBy<Decoder = D>, D: Decoder> IndexReader<'index>
    for IndexReaderCore<'index, E>
{
    #[inline(always)]
    fn next_record(&mut self, result: &mut RSIndexResult<'index>) -> std::io::Result<bool> {
        // Check if the current buffer is empty or the end of the buffer has been reached
        if self.current_buffer.get_ref().len() as u64 <= self.current_buffer.position() {
            if self.current_block_idx + 1 >= self.ii.blocks.len() {
                // No more blocks to read from
                return Ok(false);
            };

            self.set_current_block(self.current_block_idx + 1);
        }

        let base = D::base_id(&self.ii.blocks[self.current_block_idx], self.last_doc_id);
        D::decode(&mut self.current_buffer, base, result)?;

        self.last_doc_id = result.doc_id;

        Ok(true)
    }

    #[inline(always)]
    fn seek_record(
        &mut self,
        doc_id: t_docId,
        result: &mut RSIndexResult<'index>,
    ) -> std::io::Result<bool> {
        if !self.skip_to(doc_id) {
            return Ok(false);
        }

        let base = D::base_id(&self.ii.blocks[self.current_block_idx], self.last_doc_id);
        let success = D::seek(&mut self.current_buffer, base, doc_id, result)?;

        if success {
            self.last_doc_id = result.doc_id;
        }

        Ok(success)
    }

    fn skip_to(&mut self, doc_id: t_docId) -> bool {
        if self.ii.blocks.is_empty() {
            return false;
        }

        if self.ii.blocks[self.current_block_idx].last_doc_id >= doc_id {
            // We are already in the correct block
            return true;
        }

        // SAFETY: it is safe to unwrap because we checked that the blocks are not empty when
        // creating the reader.
        if self.ii.blocks.last().unwrap().last_doc_id < doc_id {
            // The document ID is greater than the last document ID in the index
            return false;
        }

        // Check if the very next block is correct before doing a binary search. This is a small
        // optimization for the common case where we are skipping to the next block.
        let search_start = self.current_block_idx + 1;
        if let Some(next_block) = self.ii.blocks.get(search_start)
            && next_block.last_doc_id >= doc_id
        {
            self.set_current_block(search_start);
            return true;
        }

        // Binary search to find the correct block index
        let relative_idx = self.ii.blocks[search_start..]
            .binary_search_by_key(&doc_id, |b| b.last_doc_id)
            .unwrap_or_else(|insertion_point| insertion_point);

        self.set_current_block(search_start + relative_idx);

        true
    }

    fn reset(&mut self) {
        if !self.ii.blocks.is_empty() {
            self.set_current_block(0);
        } else {
            self.current_buffer = Cursor::new(&[]);
            self.last_doc_id = 0;
        }

        self.gc_marker = self.ii.gc_marker.load(atomic::Ordering::Relaxed);
    }

    fn unique_docs(&self) -> u32 {
        self.ii.unique_docs()
    }

    fn has_duplicates(&self) -> bool {
        self.ii.flags() & IndexFlags_Index_HasMultiValue > 0
    }

    fn flags(&self) -> IndexFlags {
        self.ii.flags()
    }

    fn needs_revalidation(&self) -> bool {
        self.gc_marker != self.ii.gc_marker.load(atomic::Ordering::Relaxed)
    }
}

impl<'index, E: DecodedBy<Decoder = D>, D: Decoder> IndexReaderCore<'index, E> {
    /// Create a new index reader that reads from the given [`InvertedIndex`].
    ///
    /// # Panic
    /// This function will panic if the inverted index is empty.
    fn new(ii: &'index InvertedIndex<E>) -> Self {
        let (current_buffer, last_doc_id) = if let Some(first_block) = ii.blocks.first() {
            (
                Cursor::new(first_block.buffer.as_ref()),
                first_block.first_doc_id,
            )
        } else {
            (Cursor::new(&[] as &[u8]), 0)
        };

        Self {
            ii,
            current_buffer,
            current_block_idx: 0,
            last_doc_id,
            gc_marker: ii.gc_marker.load(atomic::Ordering::Relaxed),
        }
    }

    /// Check if this reader is reading from the given index
    pub fn is_index(&self, index: &InvertedIndex<E>) -> bool {
        std::ptr::eq(self.ii, index)
    }

    /// Swap the inverted index of the reader with the supplied index. This is only used by the C
    /// tests to trigger a revalidation.
    pub const fn swap_index(&mut self, index: &mut &'index InvertedIndex<E>) {
        std::mem::swap(&mut self.ii, index);
    }

    /// Get the internal index of the reader. This is only used by some C tests.
    pub const fn internal_index(&self) -> &InvertedIndex<E> {
        self.ii
    }

    /// Set the current active block to the given index
    fn set_current_block(&mut self, index: usize) {
        debug_assert!(
            index < self.ii.blocks.len(),
            "block index should stay in bounds"
        );

        self.current_block_idx = index;
        let current_block = &self.ii.blocks[self.current_block_idx];
        self.last_doc_id = current_block.first_doc_id;
        self.current_buffer = Cursor::new(&current_block.buffer);
    }
}

/// Filter to apply when reading from an index. Entries which don't match the filter will not be
/// returned by the reader.
/// cbindgen:prefix-with-name=true
#[repr(u8)]
#[derive(Debug)]
pub enum ReadFilter<'numeric_filter> {
    /// No filter, all entries are accepted
    None,

    /// Accepts entries matching this field mask
    FieldMask(t_fieldMask),

    /// Accepts entries matching this numeric filter
    Numeric(&'numeric_filter NumericFilter),
}

/// A reader that filters out records that do not match a given field mask. It is used to
/// filter records in an index based on their field mask, allowing only those that match the
/// specified mask to be returned.
pub struct FilterMaskReader<IR> {
    /// Mask which a record needs to match to be valid
    mask: t_fieldMask,

    /// The inner reader that will be used to read the records from the index.
    inner: IR,
}

impl<'index, IR: IndexReader<'index>> FilterMaskReader<IR> {
    /// Create a new filter mask reader with the given mask and inner iterator
    pub const fn new(mask: t_fieldMask, inner: IR) -> Self {
        Self { mask, inner }
    }
}

impl<'index, IR: IndexReader<'index>> IndexReader<'index> for FilterMaskReader<IR> {
    #[inline(always)]
    fn next_record(&mut self, result: &mut RSIndexResult<'index>) -> std::io::Result<bool> {
        loop {
            let success = self.inner.next_record(result)?;

            if !success {
                return Ok(false);
            }

            if result.field_mask & self.mask > 0 {
                return Ok(true);
            }
        }
    }

    #[inline(always)]
    fn seek_record(
        &mut self,
        doc_id: t_docId,
        result: &mut RSIndexResult<'index>,
    ) -> std::io::Result<bool> {
        let success = self.inner.seek_record(doc_id, result)?;

        if !success {
            return Ok(false);
        }

        if result.field_mask & self.mask > 0 {
            Ok(true)
        } else {
            self.next_record(result)
        }
    }

    fn skip_to(&mut self, doc_id: t_docId) -> bool {
        self.inner.skip_to(doc_id)
    }

    fn reset(&mut self) {
        self.inner.reset();
    }

    fn unique_docs(&self) -> u32 {
        self.inner.unique_docs()
    }

    fn has_duplicates(&self) -> bool {
        self.inner.has_duplicates()
    }

    fn flags(&self) -> IndexFlags {
        self.inner.flags()
    }

    fn needs_revalidation(&self) -> bool {
        self.inner.needs_revalidation()
    }
}

impl<'index, E: DecodedBy<Decoder = D>, D: Decoder> FilterMaskReader<IndexReaderCore<'index, E>> {
    /// Check if the underlying index has been modified since the last time this reader read from it.
    /// If it has, then the reader should be reset before reading from it again.
    pub fn needs_revalidation(&self) -> bool {
        self.inner.needs_revalidation()
    }

    /// Check if this reader is reading from the given index
    pub fn is_index(&self, index: &InvertedIndex<E>) -> bool {
        self.inner.is_index(index)
    }

    /// Swap the inverted index of the reader with the supplied index. This is only used by the C
    /// tests to trigger a revalidation.
    pub const fn swap_index(&mut self, index: &mut &'index InvertedIndex<E>) {
        self.inner.swap_index(index);
    }

    /// Get the internal index of the reader. This is only used by some C tests.
    pub const fn internal_index(&self) -> &InvertedIndex<E> {
        self.inner.internal_index()
    }
}

/// Automatically implemented if the IndexReaderCore uses a TermDecoder.
impl<'index, E: DecodedBy<Decoder = D>, D: Decoder + TermDecoder> TermReader<'index>
    for FilterMaskReader<IndexReaderCore<'index, E>>
{
}

/// A reader that filters out records that do not match a given numeric filter. It is used to
/// filter records in an index based on their numeric value, allowing only those that match the
/// specified filter to be returned.
///
/// This should only be wrapped around readers that return numeric records.
pub struct FilterNumericReader<'filter, IR> {
    /// The numeric filter that is used to filter the records.
    filter: &'filter NumericFilter,

    /// The inner reader that will be used to read the records from the index.
    inner: IR,
}

impl<'filter, 'index, IR: NumericReader<'index>> FilterNumericReader<'filter, IR> {
    /// Create a new filter numeric reader with the given filter and inner iterator.
    pub const fn new(filter: &'filter NumericFilter, inner: IR) -> Self {
        Self { filter, inner }
    }
}

impl<'filter, 'index, E> FilterNumericReader<'filter, IndexReaderCore<'index, E>> {
    /// Get the numeric filter used by this reader.
    pub const fn filter(&self) -> &NumericFilter {
        self.filter
    }
}

impl<'index, IR: NumericReader<'index>> IndexReader<'index> for FilterNumericReader<'index, IR> {
    /// Get the next record from the inner reader that matches the numeric filter.
    ///
    /// # Safety
    ///
    /// 1. `result.is_numeric()` must be true when this function is called.
    #[inline(always)]
    fn next_record(&mut self, result: &mut RSIndexResult<'index>) -> std::io::Result<bool> {
        loop {
            let success = self.inner.next_record(result)?;

            if !success {
                return Ok(false);
            }

            // SAFETY: the caller must ensure the result is numeric
            let value = unsafe { result.as_numeric_unchecked() };

            if self.filter.value_in_range(value) {
                return Ok(true);
            }
        }
    }

    /// Seek to the record with the given document ID in the inner reader that matches the numeric filter.
    ///
    /// # Safety
    ///
    /// 1. `result.is_numeric()` must be true when this function is called.
    #[inline(always)]
    fn seek_record(
        &mut self,
        doc_id: t_docId,
        result: &mut RSIndexResult<'index>,
    ) -> std::io::Result<bool> {
        let success = self.inner.seek_record(doc_id, result)?;

        if !success {
            return Ok(false);
        }

        // SAFETY: the caller must ensure the result is numeric
        let value = unsafe { result.as_numeric_unchecked() };

        if self.filter.value_in_range(value) {
            Ok(true)
        } else {
            self.next_record(result)
        }
    }

    fn skip_to(&mut self, doc_id: t_docId) -> bool {
        self.inner.skip_to(doc_id)
    }

    fn reset(&mut self) {
        self.inner.reset();
    }

    fn unique_docs(&self) -> u32 {
        self.inner.unique_docs()
    }

    fn has_duplicates(&self) -> bool {
        self.inner.has_duplicates()
    }

    fn flags(&self) -> ffi::IndexFlags {
        self.inner.flags()
    }

    fn needs_revalidation(&self) -> bool {
        self.inner.needs_revalidation()
    }
}

impl<'filter, 'index, E: DecodedBy<Decoder = D>, D: Decoder>
    FilterNumericReader<'filter, IndexReaderCore<'index, E>>
{
    /// Check if the underlying index has been modified since the last time this reader read from it.
    /// If it has, then the reader should be reset before reading from it again.
    pub fn needs_revalidation(&self) -> bool {
        self.inner.needs_revalidation()
    }

    /// Check if this reader is reading from the given index
    pub fn is_index(&self, index: &InvertedIndex<E>) -> bool {
        self.inner.is_index(index)
    }

    /// Swap the inverted index of the reader with the supplied index. This is only used by the C
    /// tests to trigger a revalidation.
    pub const fn swap_index(&mut self, index: &mut &'index InvertedIndex<E>) {
        self.inner.swap_index(index);
    }

    /// Get the internal index of the reader. This is only used by some C tests.
    pub const fn internal_index(&self) -> &InvertedIndex<E> {
        self.inner.internal_index()
    }
}

/// A [`FilterNumericReader`] wrapping a [`NumericReader'] is also a [`NumericReader`].
impl<'index, IR: NumericReader<'index>> NumericReader<'index> for FilterNumericReader<'index, IR> {}

/// A reader that filters out records that do not match a given geo filter. It is used to
/// filter records in an index based on their geo location, allowing only those that match the
/// specified geo filter to be returned.
///
/// This should only be wrapped around readers that return numeric records.
pub struct FilterGeoReader<'filter, IR> {
    /// Numeric filter with a geo filter set to which a record needs to match to be valid.
    /// This is only needed because the reader needs to be able to return the original numeric
    /// filter.
    filter: &'filter NumericFilter,

    /// Geo filter which a record needs to match to be valid
    geo_filter: &'filter GeoFilter,

    /// The inner reader that will be used to read the records from the index.
    inner: IR,
}

impl<'filter, 'index, IR: NumericReader<'index>> FilterGeoReader<'filter, IR> {
    /// Create a new filter geo reader with the given numeric filter and inner iterator
    ///
    /// # Safety
    /// The caller should ensure the `geo_filter` pointer in the numeric filter is set and a valid
    /// pointer to a `GeoFilter` struct for the lifetime of this reader.
    pub fn new(filter: &'filter NumericFilter, inner: IR) -> Self {
        debug_assert!(
            !filter.geo_filter.is_null(),
            "FilterGeoReader needs the geo filter to be set on the numeric filter"
        );

        // SAFETY: we just asserted the filter is set and the caller is to ensure it is a valid
        // `GeoFilter` instance
        let geo_filter = unsafe { &*(filter.geo_filter as *const GeoFilter) };

        Self {
            filter,
            geo_filter,
            inner,
        }
    }
}

impl<'filter, 'index, E> FilterGeoReader<'filter, IndexReaderCore<'index, E>> {
    /// Get the numeric filter used by this reader.
    pub const fn filter(&self) -> &NumericFilter {
        self.filter
    }
}

impl<'index, IR: NumericReader<'index>> IndexReader<'index> for FilterGeoReader<'index, IR> {
    /// Get the next record from the inner reader that matches the geo filter.
    ///
    /// # Safety
    ///
    /// 1. `result.is_numeric()` must be true when this function is called.
    #[inline(always)]
    fn next_record(&mut self, result: &mut RSIndexResult<'index>) -> std::io::Result<bool> {
        loop {
            let success = self.inner.next_record(result)?;

            if !success {
                return Ok(false);
            }

            // SAFETY: the caller must ensure the result is numeric
            let value = unsafe { result.as_numeric_unchecked_mut() };

            // SAFETY: we know the filter is not a null pointer since we hold a reference to it
            let in_radius = unsafe { isWithinRadius(self.geo_filter, *value, value) };

            if in_radius {
                return Ok(true);
            }
        }
    }

    /// Seek to the record with the given document ID in the inner reader that matches the geo filter.
    ///
    /// # Safety
    ///
    /// 1. `result.is_numeric()` must be true when this function is called.
    #[inline(always)]
    fn seek_record(
        &mut self,
        doc_id: t_docId,
        result: &mut RSIndexResult<'index>,
    ) -> std::io::Result<bool> {
        let success = self.inner.seek_record(doc_id, result)?;

        if !success {
            return Ok(false);
        }

        // SAFETY: the caller must ensure the result is numeric
        let value = unsafe { result.as_numeric_unchecked_mut() };

        // SAFETY: we know the filter is not a null pointer since we hold a reference to it
        let in_radius = unsafe { isWithinRadius(self.geo_filter, *value, value) };

        if in_radius {
            Ok(true)
        } else {
            self.next_record(result)
        }
    }

    fn skip_to(&mut self, doc_id: t_docId) -> bool {
        self.inner.skip_to(doc_id)
    }

    fn reset(&mut self) {
        self.inner.reset();
    }

    fn unique_docs(&self) -> u32 {
        self.inner.unique_docs()
    }

    fn has_duplicates(&self) -> bool {
        self.inner.has_duplicates()
    }

    fn flags(&self) -> IndexFlags {
        self.inner.flags()
    }

    fn needs_revalidation(&self) -> bool {
        self.inner.needs_revalidation()
    }
}

impl<'filter, 'index, E: DecodedBy<Decoder = D>, D: Decoder>
    FilterGeoReader<'filter, IndexReaderCore<'index, E>>
{
    /// Check if the underlying index has been modified since the last time this reader read from it.
    /// If it has, then the reader should be reset before reading from it again.
    pub fn needs_revalidation(&self) -> bool {
        self.inner.needs_revalidation()
    }

    /// Check if this reader is reading from the given index
    pub fn is_index(&self, index: &InvertedIndex<E>) -> bool {
        self.inner.is_index(index)
    }

    /// Swap the inverted index of the reader with the supplied index. This is only used by the C
    /// tests to trigger a revalidation.
    pub const fn swap_index(&mut self, index: &mut &'index InvertedIndex<E>) {
        self.inner.swap_index(index);
    }

    /// Get the internal index of the reader. This is only used by some C tests.
    pub const fn internal_index(&self) -> &InvertedIndex<E> {
        self.inner.internal_index()
    }
}

/// A [`FilterGeoReader`] wrapping a [`NumericReader'] is also a [`NumericReader`].
impl<'index, IR: NumericReader<'index>> NumericReader<'index> for FilterGeoReader<'index, IR> {}

#[cfg(test)]
mod tests;
