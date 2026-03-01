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

use ffi::t_docId;
use hyperloglog::{HyperLogLog6, WyHasher};
use inverted_index::{IndexReader as _, RSIndexResult};

use crate::index::{NumericIndex, NumericIndexReader};

/// HyperLogLog type used for cardinality estimation.
///
/// See the [crate-level documentation](crate#cardinality-estimation) for details
/// on precision, error rate, and memory usage.
pub type Hll = HyperLogLog6<[u8; 8], WyHasher>;

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

/// Update the cardinality estimate of an HLL with a new float value.
///
/// This is the single source of truth for how numeric values are
/// hashed into HyperLogLog registers. We hash the raw bytes (bit
/// representation) rather than the numeric value — see
/// [`NumericRange::update_cardinality`] for rationale.
pub(crate) fn update_cardinality(hll: &mut Hll, value: f64) {
    hll.add(&value.to_ne_bytes());
}

impl NumericRange {
    /// Create a new empty numeric range.
    ///
    /// If `compress_floats` is true, the range will use float compression which
    /// attempts to store f64 values as f32 when precision loss is acceptable (< 0.01).
    pub fn new(compress_floats: bool) -> Self {
        Self {
            min_val: f64::INFINITY,
            max_val: f64::NEG_INFINITY,
            hll: Hll::new(),
            entries: NumericIndex::new(compress_floats),
        }
    }

    /// Add a (docId, value) entry to this range.
    ///
    /// Updates min/max bounds and cardinality estimation.
    /// Returns the number of bytes the inverted index grew by.
    pub fn add(&mut self, doc_id: t_docId, value: f64) -> usize {
        self.update_cardinality(value);
        self.add_without_cardinality(doc_id, value)
    }

    /// Add a (docId, value) entry without updating cardinality.
    ///
    /// This function DOES NOT update the cardinality of the range.
    /// Use [`add`][Self::add] to add an entry _and_ update cardinality of the range.
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
    pub(crate) fn update_cardinality(&mut self, value: f64) {
        update_cardinality(&mut self.hll, value);
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
        self.entries.number_of_entries()
    }

    /// Get the number of unique documents in this range.
    pub const fn num_docs(&self) -> u32 {
        self.entries.unique_docs()
    }

    /// Get the memory usage of the inverted index in bytes.
    pub fn memory_usage(&self) -> usize {
        self.entries.memory_usage()
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
    pub fn reader(&self) -> NumericIndexReader<'_> {
        self.entries.reader()
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
    /// * `ignored_last_block` - Whether the last block was ignored during GC scan (from `GcApplyInfo`)
    /// * `blocks_since_fork` - Number of new blocks added since the fork
    /// * `registers_with_last_block` - HLL registers including the last block's cardinality
    /// * `registers_without_last_block` - HLL registers excluding the last block's cardinality
    pub(crate) fn reset_cardinality_after_gc(
        &mut self,
        ignored_last_block: bool,
        blocks_since_fork: usize,
        registers_with_last_block: &[u8; Hll::size()],
        registers_without_last_block: &[u8; Hll::size()],
    ) {
        let mut blocks_to_rescan = blocks_since_fork;

        if ignored_last_block {
            self.hll.set_registers(*registers_without_last_block);
            blocks_to_rescan += 1; // The last block was ignored, so re-add it too
        } else {
            self.hll.set_registers(*registers_with_last_block);
            if blocks_to_rescan == 0 {
                return; // No new blocks since fork, we're done
            }
        }

        // Get the starting point for HLL update - iterate entries added since fork
        let num_blocks = self.entries.num_blocks();
        debug_assert!(
            blocks_to_rescan <= num_blocks,
            "The number of blocks should never decrease in between two GC runs, \
            therefore the number of blocks to rescan can never be greater than the current number of blocks"
        );
        let start_idx = num_blocks - blocks_to_rescan;
        let Some(start_id) = self.entries.block_first_id(start_idx) else {
            return;
        };

        // Iterate entries added since fork and update the cardinality estimation
        // via HLL.
        let mut reader = self.entries.reader();
        reader.skip_to(start_id);
        let mut result = RSIndexResult::numeric(0.0);
        while reader.next_record(&mut result).unwrap_or(false) {
            // SAFETY: We know the result contains numeric data
            let value = unsafe { result.as_numeric_unchecked() };
            update_cardinality(&mut self.hll, value);
        }
    }
}

impl Default for NumericRange {
    fn default() -> Self {
        Self::new(false)
    }
}
