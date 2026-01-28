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
    EntriesTrackingIndex, IndexReaderCore, RSIndexResult,
    numeric::{Numeric, NumericFloatCompression},
};
use tracing::debug;

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

    /// Returns whether this range uses float compression.
    pub const fn is_compressed(&self) -> bool {
        matches!(self.entries, NumericIndex::Compressed(_))
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
        let old_min = self.min_val;
        let old_max = self.max_val;

        // Update bounds
        if value < self.min_val {
            self.min_val = value;
        }
        if value > self.max_val {
            self.max_val = value;
        }

        debug!(
            doc_id,
            value,
            old_min,
            old_max,
            new_min = self.min_val,
            new_max = self.max_val,
            "NumericRange::add_without_cardinality"
        );

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

    /// Get a reference to the HLL registers.
    /// This is used for serialization during fork GC.
    pub const fn hll_registers(&self) -> &[u8; 64] {
        self.hll.registers()
    }

    /// Set the HLL registers.
    /// This is used for deserialization during fork GC.
    pub fn set_hll_registers(&mut self, registers: [u8; 64]) {
        self.hll.set_registers(registers);
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

impl Default for NumericRange {
    fn default() -> Self {
        Self::new(false)
    }
}
