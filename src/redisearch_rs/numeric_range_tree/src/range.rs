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

use hyperloglog::{HyperLogLog6, WyHasher};
use index_result::RSIndexResult;
use inverted_index::IndexReader as _;
use inverted_index::numeric::{PreparedValue, StoredValue};
use rqe_core::DocId;

use crate::index::{NumericIndex, NumericIndexReader};

/// Newtype around [`f64`] that hashes via native-endian bytes.
///
/// Ensures HLL cardinality estimation uses a consistent raw bit representation, so
/// no float comparison is involved.
///
/// Only constructible from a [`StoredValue`], so inputs the encoder maps onto the
/// same stored value count once — including `-0.0` and `+0.0`.
#[derive(Debug, Clone, Copy)]
pub struct NumericValue(f64);

impl From<StoredValue> for NumericValue {
    fn from(value: StoredValue) -> Self {
        Self(value.get())
    }
}

impl std::hash::Hash for NumericValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.to_ne_bytes().hash(state);
    }
}

/// The smallest and largest of a set of stored values.
///
/// [`Self::EMPTY`] describes the empty set; it is also the bounds of a range that
/// has never held an entry or that GC emptied.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ValueBounds {
    /// Smallest value in the set.
    pub min: f64,
    /// Largest value in the set.
    pub max: f64,
}

impl ValueBounds {
    /// Bounds of the empty set: any included value replaces both ends.
    pub const EMPTY: Self = Self {
        min: f64::INFINITY,
        max: f64::NEG_INFINITY,
    };

    /// Widen the bounds to include `value`.
    pub const fn include(&mut self, value: f64) {
        if value < self.min {
            self.min = value;
        }
        if value > self.max {
            self.max = value;
        }
    }

    /// Widen the bounds to include every value in `other`.
    pub const fn merge(&mut self, other: Self) {
        if other.min < self.min {
            self.min = other.min;
        }
        if other.max > self.max {
            self.max = other.max;
        }
    }
}

/// HyperLogLog type used for cardinality estimation.
///
/// See the [crate-level documentation](crate#cardinality-estimation) for details
/// on precision, error rate, and memory usage.
pub type Hll = HyperLogLog6<NumericValue, WyHasher>;

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
    ///
    /// A lower bound on the stored values, in [`StoredValue`] form. Adds lower it
    /// and GC resets it to the smallest surviving entry, so it is normally exact.
    min_val: f64,
    /// The maximum value stored in this range.
    /// Initialized to `f64::NEG_INFINITY` so any value will be larger.
    ///
    /// An upper bound, maintained like [`Self::min_val`].
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
        Self {
            min_val: f64::INFINITY,
            max_val: f64::NEG_INFINITY,
            hll: Hll::new(),
            entries: NumericIndex::new(compress_floats),
        }
    }

    /// Add a (docId, value) entry to this range.
    ///
    /// Updates min/max bounds and cardinality estimation. Returns an [`AddRecordOutcome`]
    /// reporting how many bytes the inverted index grew by and how many new index blocks the
    /// write created.
    ///
    /// Takes the encoder's decision from [`NumericIndex::prepare`], so both statistics
    /// describe what the index will return.
    ///
    /// [`AddRecordOutcome`]: inverted_index::AddRecordOutcome
    pub fn add(
        &mut self,
        doc_id: DocId,
        value: PreparedValue,
        has_field_expiration: bool,
    ) -> inverted_index::AddRecordOutcome {
        self.hll.add(&value.stored_value().into());
        self.add_without_cardinality(doc_id, value, has_field_expiration)
    }

    /// Add a (docId, value) entry without updating cardinality.
    ///
    /// This function DOES NOT update the cardinality of the range.
    /// Use [`add`][Self::add] to add an entry _and_ update cardinality of the range.
    /// Returns `(memory_growth, blocks_added)` — see [`Self::add`].
    ///
    /// # Use Cases
    ///
    /// - **Internal node ranges**: When adding to a retained range in an internal
    ///   node, cardinality is already tracked at the leaf level.
    /// - **Splitting**: When redistributing entries during a split, the caller
    ///   explicitly updates cardinality for each destination range.
    pub fn add_without_cardinality(
        &mut self,
        doc_id: DocId,
        value: PreparedValue,
        has_field_expiration: bool,
    ) -> inverted_index::AddRecordOutcome {
        let stored = value.stored_value().get();

        if stored < self.min_val {
            self.min_val = stored;
        }
        if stored > self.max_val {
            self.max_val = stored;
        }

        self.entries
            .add_prepared_record(doc_id, value, has_field_expiration)
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

    /// Reset the statistics derived from the entries after garbage collection.
    ///
    /// Takes the HLL registers and value bounds that the GC scan computed over the
    /// surviving entries, then folds in the entries added since the fork.
    ///
    /// If those entries cannot all be read back, the current bounds are kept: they
    /// still cover every stored entry, while the survivor bounds would miss the
    /// unread ones.
    ///
    /// # Arguments
    ///
    /// * `ignored_last_block` - Whether the last block was ignored during GC scan (from
    ///   [`GcApplyInfo`](inverted_index::GcApplyInfo))
    /// * `blocks_since_fork` - Number of new blocks added since the fork
    /// * `registers` - HLL registers `(with, without)` the last scanned block
    /// * `bounds` - Survivor bounds `(with, without)` the last scanned block
    pub(crate) fn reset_stats_after_gc(
        &mut self,
        ignored_last_block: bool,
        blocks_since_fork: usize,
        registers: (&[u8; Hll::size()], &[u8; Hll::size()]),
        bounds: (ValueBounds, ValueBounds),
    ) {
        let mut blocks_to_rescan = blocks_since_fork;
        let (registers, mut bounds) = if ignored_last_block {
            blocks_to_rescan += 1; // The last block was ignored, so re-add it too
            (registers.1, bounds.1)
        } else {
            (registers.0, bounds.0)
        };
        self.hll.set_registers(*registers);

        if blocks_to_rescan > 0 && !self.fold_entries_since_fork(blocks_to_rescan, &mut bounds) {
            return;
        }
        self.min_val = bounds.min;
        self.max_val = bounds.max;
    }

    /// Add the entries of the last `blocks_to_rescan` blocks to the HLL and to
    /// `bounds`. Returns `false` if they could not all be read.
    fn fold_entries_since_fork(
        &mut self,
        blocks_to_rescan: usize,
        bounds: &mut ValueBounds,
    ) -> bool {
        let num_blocks = self.entries.num_blocks();
        debug_assert!(
            blocks_to_rescan <= num_blocks,
            "The number of blocks should never decrease in between two GC runs, \
            therefore the number of blocks to rescan can never be greater than the current number of blocks"
        );
        let Some(start_id) = num_blocks
            .checked_sub(blocks_to_rescan)
            .and_then(|start_idx| self.entries.block_first_id(start_idx))
        else {
            return false;
        };

        let mut reader = self.entries.reader();
        reader.skip_to(start_id);
        let mut result = RSIndexResult::build_numeric(0.0).build();
        loop {
            match reader.next_record(&mut result) {
                Ok(true) => {}
                Ok(false) => return true,
                Err(_) => return false,
            }
            // SAFETY: We know the result contains numeric data
            let value = unsafe { result.as_numeric_unchecked() };
            // Read back out of the index, so already in stored form.
            self.hll.add(&StoredValue::from_decoded(value).into());
            bounds.include(value);
        }
    }
}

impl Default for NumericRange {
    fn default() -> Self {
        Self::new(false)
    }
}
