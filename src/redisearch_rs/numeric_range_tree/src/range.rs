/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Numeric range storage for the numeric range tree.
//!
//! A numeric range is the leaf-level storage unit that holds the actual
//! document-value entries in an inverted index format. Ranges track their
//! value bounds and estimate cardinality using HyperLogLog.

use ffi::{IndexFlags_Index_StoreNumeric, t_docId};
use hyperloglog::{Hasher32 as _, HyperLogLog6, WyHasher};
use inverted_index::{
    EntriesTrackingIndex, IndexReader, IndexReaderCore, RSIndexResult,
    numeric::{Numeric, NumericFloatCompression},
};

/// HyperLogLog type used for cardinality estimation.
///
/// See the [crate-level documentation](crate#cardinality-estimation) for details
/// on precision, error rate, and memory usage.
pub type Hll = HyperLogLog6<WyHasher>;

/// Enum to hold either compressed or uncompressed numeric index.
#[derive(Debug)]
pub enum NumericIndex {
    /// Uncompressed: stores f64 values at full precision (8 bytes).
    Uncompressed(EntriesTrackingIndex<Numeric>),
    /// Compressed: attempts to compress f64 → f32 (4 bytes) when precision loss is deemed acceptable.
    Compressed(EntriesTrackingIndex<NumericFloatCompression>),
}

impl NumericIndex {
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

    /// Get the first document ID in a specific block.
    ///
    /// Returns `None` if the block index is out of bounds.
    pub(crate) fn block_first_id(&self, block_idx: usize) -> Option<ffi::t_docId> {
        match self {
            NumericIndex::Uncompressed(idx) => idx.block_ref(block_idx).map(|b| b.first_block_id()),
            NumericIndex::Compressed(idx) => idx.block_ref(block_idx).map(|b| b.first_block_id()),
        }
    }

    /// Apply garbage collection deltas to this index.
    ///
    /// Consumes the `delta` and returns information about what changed.
    pub fn apply_gc(&mut self, delta: inverted_index::GcScanDelta) -> inverted_index::GcApplyInfo {
        match self {
            NumericIndex::Uncompressed(idx) => idx.apply_gc(delta),
            NumericIndex::Compressed(idx) => idx.apply_gc(delta),
        }
    }

    /// Scan the index for blocks that can be garbage collected.
    ///
    /// The `doc_exist` callback returns `true` if the document still exists.
    /// Returns `Ok(Some(delta))` if GC is needed, `Ok(None)` otherwise.
    pub fn scan_gc(
        &self,
        doc_exist: impl Fn(ffi::t_docId) -> bool,
    ) -> std::io::Result<Option<inverted_index::GcScanDelta>> {
        match self {
            NumericIndex::Uncompressed(idx) => idx.scan_gc(
                doc_exist,
                None::<fn(&inverted_index::RSIndexResult<'_>, &inverted_index::IndexBlock)>,
            ),
            NumericIndex::Compressed(idx) => idx.scan_gc(
                doc_exist,
                None::<fn(&inverted_index::RSIndexResult<'_>, &inverted_index::IndexBlock)>,
            ),
        }
    }
}

/// A numeric range is a leaf-level storage unit in the numeric range tree.
///
/// It stores document IDs and their associated numeric values in an inverted index,
/// along with metadata for range queries and cardinality estimation.
///
/// # Structure
///
/// - **Bounds** (`min_val`, `max_val`): Track the actual value range for overlap
///   and containment tests during queries.
/// - **Cardinality** (`hll`): HyperLogLog estimator for the number of distinct
///   values, used to decide when to split.
/// - **Entries** (`entries`): Inverted index storing (docId, value) pairs.
///
/// # Initialization
///
/// New ranges start with inverted bounds (`min_val = +∞`, `max_val = -∞`) so
/// the first added value correctly sets both bounds.
#[derive(Debug)]
pub struct NumericRange {
    /// The minimum value stored in this range.
    /// Initialized to `f64::INFINITY` so any value will be smaller.
    min_val: f64,
    /// The maximum value stored in this range.
    /// Initialized to `f64::NEG_INFINITY` so any value will be larger.
    max_val: f64,
    /// HyperLogLog for estimating the number of distinct values (cardinality).
    /// Used to decide when to split the range.
    hll: Hll,
    /// The inverted index storing (docId, value) entries.
    /// Can be either uncompressed (full f64 precision) or compressed (f64→f32).
    entries: NumericIndex,
}

impl NumericRange {
    /// Create a new empty numeric range.
    ///
    /// If `compress_floats` is true, the range will use float compression which
    /// attempts to store f64 values as f32 when precision loss is acceptable (< 0.01).
    pub fn new(compress_floats: bool) -> Self {
        let entries = if compress_floats {
            NumericIndex::Compressed(EntriesTrackingIndex::new(IndexFlags_Index_StoreNumeric))
        } else {
            NumericIndex::Uncompressed(EntriesTrackingIndex::new(IndexFlags_Index_StoreNumeric))
        };
        Self {
            min_val: f64::INFINITY,
            max_val: f64::NEG_INFINITY,
            hll: Hll::new(),
            entries,
        }
    }

    /// Add a (docId, value) entry to this range.
    ///
    /// Updates min/max bounds and cardinality estimation.
    /// Returns the number of bytes the inverted index grew by.
    pub fn add(&mut self, doc_id: t_docId, value: f64) -> usize {
        // Update bounds
        if value < self.min_val {
            self.min_val = value;
        }
        if value > self.max_val {
            self.max_val = value;
        }

        // Update cardinality estimate
        self.update_cardinality(value);

        // Add to inverted index
        let record = RSIndexResult::numeric(value).doc_id(doc_id);
        self.entries.add_record(&record)
    }

    /// Add a (docId, value) entry without updating cardinality.
    ///
    /// This function DOES NOT update the cardinality of the range. It is the
    /// caller's responsibility to update cardinality if needed by calling
    /// [`update_cardinality`](Self::update_cardinality) separately.
    ///
    /// # Use Cases
    ///
    /// - **Internal node ranges**: When adding to a retained range in an internal
    ///   node, cardinality is already tracked at the leaf level.
    /// - **Splitting**: When redistributing entries during a split, the caller
    ///   explicitly calls `update_cardinality` for each destination range.
    pub fn add_without_cardinality(&mut self, doc_id: t_docId, value: f64) -> usize {
        // Update bounds
        if value < self.min_val {
            self.min_val = value;
        }
        if value > self.max_val {
            self.max_val = value;
        }

        // Add to inverted index
        let record = RSIndexResult::numeric(value).doc_id(doc_id);
        self.entries.add_record(&record)
    }

    /// Update the cardinality estimate with a new value.
    ///
    /// # Implementation
    ///
    /// We hash the raw bytes (bit representation) of the f64 value rather than
    /// its numeric value. This ensures:
    /// - Bit-level uniqueness: Different bit patterns are always distinct.
    /// - No floating-point comparison issues: -0.0 and +0.0 have different bits.
    /// - Deterministic: Native-endian bytes produce consistent hashes.
    ///
    /// The hash is computed using WyHash for speed, then fed to the HyperLogLog.
    pub fn update_cardinality(&mut self, value: f64) {
        use std::hash::Hasher as _;

        let mut hasher = WyHasher::default();
        // Hash the raw bytes of the f64 for bit-level uniqueness
        hasher.write(&value.to_ne_bytes());
        self.hll.add_precomputed_hash(hasher.finish32());
    }

    /// Get the estimated cardinality (number of distinct values).
    pub fn cardinality(&self) -> usize {
        self.hll.count()
    }

    /// Returns true if this range is completely contained within [min, max].
    pub const fn contained_in(&self, min: f64, max: f64) -> bool {
        self.min_val >= min && self.max_val <= max
    }

    /// Returns true if this range overlaps with [min, max].
    pub const fn overlaps(&self, min: f64, max: f64) -> bool {
        !(min > self.max_val || max < self.min_val)
    }

    /// Get the minimum value in this range.
    pub const fn min_val(&self) -> f64 {
        self.min_val
    }

    /// Get the maximum value in this range.
    pub const fn max_val(&self) -> f64 {
        self.max_val
    }

    /// Get the number of entries in this range.
    pub const fn num_entries(&self) -> usize {
        match &self.entries {
            NumericIndex::Uncompressed(idx) => idx.number_of_entries(),
            NumericIndex::Compressed(idx) => idx.number_of_entries(),
        }
    }

    /// Get the number of unique documents in this range.
    pub const fn num_docs(&self) -> u32 {
        match &self.entries {
            NumericIndex::Uncompressed(idx) => idx.unique_docs(),
            NumericIndex::Compressed(idx) => idx.unique_docs(),
        }
    }

    /// Get the memory usage of the inverted index in bytes.
    pub fn memory_usage(&self) -> usize {
        match &self.entries {
            NumericIndex::Uncompressed(idx) => idx.memory_usage(),
            NumericIndex::Compressed(idx) => idx.memory_usage(),
        }
    }

    /// Get a reference to the numeric index entries.
    pub const fn entries(&self) -> &NumericIndex {
        &self.entries
    }

    /// Get a mutable reference to the numeric index entries.
    pub const fn entries_mut(&mut self) -> &mut NumericIndex {
        &mut self.entries
    }

    /// Get a reader for iterating over the entries.
    ///
    /// Returns an enum that can be either uncompressed or compressed reader.
    pub fn reader(&self) -> NumericRangeReader<'_> {
        match &self.entries {
            NumericIndex::Uncompressed(idx) => NumericRangeReader::Uncompressed(idx.reader()),
            NumericIndex::Compressed(idx) => NumericRangeReader::Compressed(idx.reader()),
        }
    }

    /// Get a reference to the HyperLogLog.
    pub const fn hll(&self) -> &Hll {
        &self.hll
    }

    /// Reset the HLL cardinality after garbage collection.
    ///
    /// This sets the HLL registers from GC scan results and re-adds entries
    /// from blocks that were added since the fork.
    ///
    /// # Arguments
    ///
    /// * `blocks_ignored` - Number of blocks ignored during GC scan (from `GcApplyInfo`)
    /// * `blocks_since_fork` - Number of new blocks added since the fork
    /// * `registers_with_last_block` - HLL registers including the last block's cardinality
    /// * `registers_without_last_block` - HLL registers excluding the last block's cardinality
    pub(crate) fn reset_cardinality_after_gc(
        &mut self,
        blocks_ignored: usize,
        blocks_since_fork: usize,
        registers_with_last_block: &[u8; 64],
        registers_without_last_block: &[u8; 64],
    ) {
        let mut blocks_to_rescan = blocks_since_fork;

        if blocks_ignored == 0 {
            self.hll.set_registers(*registers_with_last_block);
            if blocks_to_rescan == 0 {
                return; // No new blocks since fork, we're done
            }
        } else {
            self.hll.set_registers(*registers_without_last_block);
            blocks_to_rescan += 1; // The last block was ignored, so re-add it too
        }

        // Get the starting point for HLL update - iterate entries added since fork
        let num_blocks = self.entries.num_blocks();
        if blocks_to_rescan > num_blocks {
            return; // Safety check
        }
        let start_idx = num_blocks - blocks_to_rescan;
        let Some(start_id) = self.entries.block_first_id(start_idx) else {
            return;
        };

        // Collect values from entries added since fork, then update HLL.
        // We collect first to avoid borrowing `self.entries` and `self.hll` simultaneously.
        let values: Vec<f64> = {
            let mut result = RSIndexResult::numeric(0.0);
            let mut vals = Vec::new();
            let mut reader = self.reader();
            reader.skip_to(start_id);
            while reader.next_record(&mut result).unwrap_or(false) {
                // SAFETY: We know the result contains numeric data
                vals.push(unsafe { result.as_numeric_unchecked() });
            }
            vals
        };

        for value in values {
            self.update_cardinality(value);
        }
    }
}

/// Reader enum for iterating over numeric range entries.
///
/// This abstracts over whether the underlying index is compressed or uncompressed.
pub enum NumericRangeReader<'a> {
    /// Reader over uncompressed entries.
    Uncompressed(IndexReaderCore<'a, Numeric>),
    /// Reader over compressed entries.
    Compressed(IndexReaderCore<'a, NumericFloatCompression>),
}

impl<'a> IndexReader<'a> for NumericRangeReader<'a> {
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

impl Default for NumericRange {
    fn default() -> Self {
        Self::new(false)
    }
}
