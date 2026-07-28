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

/// HyperLogLog type used for cardinality estimation.
///
/// See the [crate-level documentation](crate#cardinality-estimation) for details
/// on precision, error rate, and memory usage.
pub type Hll = HyperLogLog6<NumericValue, WyHasher>;

/// The smallest and largest value observed over a set of numeric entries.
///
/// Starts inverted (`min = +∞`, `max = -∞`) so the first observed value sets both
/// ends. An interval that never observed a value stays inverted, which is exactly
/// the sentinel [`NumericRange`] uses for "no entries".
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ValueBounds {
    min: f64,
    max: f64,
}

impl ValueBounds {
    /// Bounds that have observed no value at all.
    pub const EMPTY: Self = Self {
        min: f64::INFINITY,
        max: f64::NEG_INFINITY,
    };

    /// Build bounds from a previously extracted `(min, max)` pair — pass
    /// [`Self::EMPTY`]'s components to denote "no value observed".
    pub const fn from_min_max(min: f64, max: f64) -> Self {
        Self { min, max }
    }

    /// Widen the bounds to include `value`.
    ///
    /// `NaN` compares false against everything, so it never moves either end.
    pub const fn observe(&mut self, value: StoredValue) {
        let value = value.get();
        if value < self.min {
            self.min = value;
        }
        if value > self.max {
            self.max = value;
        }
    }

    /// Widen the bounds to include everything `other` observed.
    ///
    /// Merging [`Self::EMPTY`] is a no-op — its inverted ends must not be mistaken
    /// for observed values.
    pub const fn merge(&mut self, other: Self) {
        if other.min < self.min {
            self.min = other.min;
        }
        if other.max > self.max {
            self.max = other.max;
        }
    }

    /// The smallest observed value, or `f64::INFINITY` if none was observed.
    pub const fn min(&self) -> f64 {
        self.min
    }

    /// The largest observed value, or `f64::NEG_INFINITY` if none was observed.
    pub const fn max(&self) -> f64 {
        self.max
    }
}

impl Default for ValueBounds {
    fn default() -> Self {
        Self::EMPTY
    }
}

/// Per-range statistics recomputed over the surviving entries during a GC scan.
///
/// The GC child cannot mutate the parent's ranges, so it ships these numbers back
/// over the pipe and the parent installs them via
/// [`NumericRange::reset_stats_after_gc`]. Cardinality and bounds travel together
/// because they are derived from the same pass over the surviving entries and must
/// be applied together.
#[derive(Debug, Clone, Copy)]
pub struct GcSurvivorStats {
    /// HyperLogLog registers covering the surviving entries.
    pub registers: [u8; Hll::size()],
    /// Value bounds covering the surviving entries.
    pub bounds: ValueBounds,
}

impl GcSurvivorStats {
    /// Statistics for a scan that observed no surviving entry.
    pub const EMPTY: Self = Self {
        registers: [0; Hll::size()],
        bounds: ValueBounds::EMPTY,
    };

    /// Length of the byte string produced by [`Self::write_to`].
    pub const WIRE_SIZE: usize = Hll::size() + 2 * size_of::<f64>();

    /// Append the wire representation of these statistics to `buffer`.
    ///
    /// Bounds use native byte order: the only reader is the parent of the fork that
    /// produced them, so both ends always agree on endianness.
    pub fn write_to(&self, buffer: &mut Vec<u8>) {
        buffer.extend_from_slice(&self.registers);
        buffer.extend_from_slice(&self.bounds.min().to_ne_bytes());
        buffer.extend_from_slice(&self.bounds.max().to_ne_bytes());
    }

    /// Rebuild statistics from the [`Self::write_to`] representation.
    ///
    /// Returns `None` unless `bytes` is exactly [`Self::WIRE_SIZE`] long. The values
    /// themselves are trusted: they come from this process's own GC child.
    pub fn read_from(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != Self::WIRE_SIZE {
            return None;
        }

        let (registers, bounds) = bytes.split_at(Hll::size());
        let (min, max) = bounds.split_at(size_of::<f64>());

        let mut stats = Self {
            registers: [0; Hll::size()],
            bounds: ValueBounds::from_min_max(
                f64::from_ne_bytes(min.try_into().expect("min is 8 bytes long")),
                f64::from_ne_bytes(max.try_into().expect("max is 8 bytes long")),
            ),
        };
        stats.registers.copy_from_slice(registers);
        Some(stats)
    }
}

impl Default for GcSurvivorStats {
    fn default() -> Self {
        Self::EMPTY
    }
}

/// Whether a GC apply may tighten a range's value bounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GcBoundsUpdate {
    /// Shrink the bounds to what the surviving entries need. Correct for leaf
    /// ranges, whose bounds describe exactly their own entries.
    Tighten,
    /// Leave the bounds untouched. Used for the ranges retained on internal nodes:
    /// those must stay a superset of their whole subtree, and the descendants are
    /// applied *after* the ancestor, so tightening here would leave the tree
    /// temporarily claiming an ancestor range narrower than its children.
    Keep,
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
    ///
    /// A [`StoredValue`] lowered by [`Self::add_without_cardinality`] when a smaller
    /// value arrives and raised by [`Self::reset_stats_after_gc`] when GC removes the
    /// entries that were holding it. Always a valid *lower bound*; only exactly the
    /// smallest stored value if the last GC pass tightened it.
    min_val: f64,
    /// The maximum value stored in this range.
    /// Initialized to `f64::NEG_INFINITY` so any value will be larger.
    ///
    /// Upper bound, maintained symmetrically to [`Self::min_val`].
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
    pub fn add(&mut self, doc_id: DocId, value: PreparedValue) -> inverted_index::AddRecordOutcome {
        self.hll.add(&value.stored_value().into());
        self.add_without_cardinality(doc_id, value)
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
    ) -> inverted_index::AddRecordOutcome {
        let stored = value.stored_value().get();

        if stored < self.min_val {
            self.min_val = stored;
        }
        if stored > self.max_val {
            self.max_val = stored;
        }

        self.entries.add_prepared_record(doc_id, value)
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

    /// Reset the cardinality estimate and the value bounds after garbage collection.
    ///
    /// The GC child recomputed both over the entries that survived its scan. This
    /// installs those numbers, then rescans the blocks the scan could not account
    /// for — blocks the parent appended after the fork, plus the last block when the
    /// apply step had to ignore it — so that neither statistic misses an entry that
    /// is still in the index.
    ///
    /// `ignored_last_block` comes from the [`GcApplyInfo`] the apply step returned.
    /// Pass [`GcBoundsUpdate::Keep`] to skip the bounds update; see that variant.
    ///
    /// # Bounds only tighten
    ///
    /// Adds and scans both observe values in stored form, so the recomputed interval
    /// is always contained in the one already reported — asserted below, so a future
    /// divergence surfaces here rather than as a range that stops covering its own
    /// entries.
    ///
    /// [`GcApplyInfo`]: inverted_index::GcApplyInfo
    pub(crate) fn reset_stats_after_gc(
        &mut self,
        ignored_last_block: bool,
        blocks_since_fork: usize,
        with_last_block: &GcSurvivorStats,
        without_last_block: &GcSurvivorStats,
        bounds_update: GcBoundsUpdate,
    ) {
        let mut blocks_to_rescan = blocks_since_fork;

        let mut bounds = if ignored_last_block {
            self.hll.set_registers(without_last_block.registers);
            blocks_to_rescan += 1; // The last block was ignored, so re-add it too
            without_last_block.bounds
        } else {
            self.hll.set_registers(with_last_block.registers);
            with_last_block.bounds
        };

        if blocks_to_rescan > 0 {
            // Get the starting point for the update - iterate entries added since fork
            let num_blocks = self.entries.num_blocks();
            debug_assert!(
                blocks_to_rescan <= num_blocks,
                "The number of blocks should never decrease in between two GC runs, \
                therefore the number of blocks to rescan can never be greater than the current number of blocks"
            );
            let start_idx = num_blocks - blocks_to_rescan;

            if let Some(start_id) = self.entries.block_first_id(start_idx) {
                // Iterate entries added since fork and fold them into the cardinality
                // estimation and the bounds.
                let mut reader = self.entries.reader();
                reader.skip_to(start_id);
                let mut result = RSIndexResult::build_numeric(0.0).build();
                while reader.next_record(&mut result).unwrap_or(false) {
                    // SAFETY: We know the result contains numeric data
                    let value = unsafe { result.as_numeric_unchecked() };
                    // Read back out of the index, so already in stored form.
                    let value = StoredValue::from_decoded(value);
                    self.hll.add(&value.into());
                    bounds.observe(value);
                }
            }
        }

        if bounds_update == GcBoundsUpdate::Tighten {
            // See "Bounds only tighten" above. An empty interval (nothing survived) is
            // inverted, so it trivially satisfies both comparisons.
            debug_assert!(
                bounds.min() >= self.min_val && bounds.max() <= self.max_val,
                "recomputed bounds [{}, {}] must be contained in the reported bounds [{}, {}] — \
                 both are observed in stored form",
                bounds.min(),
                bounds.max(),
                self.min_val,
                self.max_val,
            );

            self.min_val = bounds.min();
            self.max_val = bounds.max();
        }
    }
}

impl Default for NumericRange {
    fn default() -> Self {
        Self::new(false)
    }
}
