/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! NumericRange - A leaf node containing numeric index data.

use ffi::{IndexFlags_Index_StoreNumeric, t_docId};
use hll::HyperLogLog;
use inverted_index::{EntriesTrackingIndex, RSIndexResult, numeric::Numeric};

/// Precision bits for HyperLogLog cardinality estimation.
/// This gives approximately 13% error rate (1.04 / sqrt(2^6)).
pub const NR_BIT_PRECISION: u8 = 6;

/// Number of HLL registers (2^6 = 64).
pub const NR_REG_SIZE: u32 = 1 << NR_BIT_PRECISION;

/// A numeric range is a leaf node in a numeric range tree, representing a range
/// of values bunched together. Since we do not know the distribution of scores
/// ahead, we use a splitting approach - we start with single value nodes, and
/// when a node passes some cardinality we split it.
///
/// We save the minimum and maximum values inside the node, and when we split we
/// split by finding the median value.
#[derive(Debug)]
pub struct NumericRange {
    /// Minimum value in this range
    pub min_val: f64,

    /// Maximum value in this range
    pub max_val: f64,

    /// HyperLogLog for cardinality estimation
    hll: HyperLogLog,

    /// Inverted index containing the numeric entries
    pub entries: EntriesTrackingIndex<Numeric>,
}

impl NumericRange {
    /// Creates a new empty numeric range.
    #[must_use]
    pub fn new() -> Self {
        Self {
            min_val: f64::INFINITY,
            max_val: f64::NEG_INFINITY,
            // SAFETY: NR_BIT_PRECISION (6) is within the valid range 4-20
            hll: HyperLogLog::new(NR_BIT_PRECISION).expect("NR_BIT_PRECISION is valid"),
            entries: EntriesTrackingIndex::<Numeric>::new(IndexFlags_Index_StoreNumeric),
        }
    }

    /// Returns the memory size of the inverted index.
    #[must_use]
    pub fn inverted_index_size(&self) -> usize {
        self.entries.memory_usage()
    }

    /// Returns the number of entries in this range.
    #[must_use]
    pub const fn num_entries(&self) -> usize {
        self.entries.number_of_entries()
    }

    /// Returns the number of unique documents in this range.
    #[must_use]
    pub const fn num_docs(&self) -> u32 {
        self.entries.unique_docs()
    }

    /// Returns the estimated cardinality (number of distinct values).
    pub fn cardinality(&mut self) -> usize {
        self.hll.count()
    }

    /// Returns the estimated cardinality without caching.
    #[must_use]
    pub fn cardinality_uncached(&self) -> usize {
        self.hll.count_uncached()
    }

    /// Updates the cardinality estimation with a new value.
    pub fn update_cardinality(&mut self, value: f64) {
        self.hll.add(&value.to_le_bytes());
    }

    /// Adds a numeric entry to the range without updating cardinality.
    ///
    /// Returns the additional memory used for this action. The caller is
    /// responsible for updating cardinality if needed by calling
    /// [`Self::update_cardinality`].
    pub fn add(&mut self, doc_id: t_docId, value: f64) -> std::io::Result<usize> {
        // Update min/max bounds
        if value < self.min_val {
            self.min_val = value;
        }
        if value > self.max_val {
            self.max_val = value;
        }

        // Create a numeric index result and add it to the inverted index
        let record = RSIndexResult::numeric(value).doc_id(doc_id);
        self.entries.add_record(&record)
    }

    /// Returns true if the entire numeric range is contained between min and max.
    #[must_use]
    pub fn is_contained(&self, min: f64, max: f64) -> bool {
        self.min_val >= min && self.max_val <= max
    }

    /// Returns true if there is any overlap between the range and min/max.
    #[must_use]
    pub fn overlaps(&self, min: f64, max: f64) -> bool {
        !(min > self.max_val || max < self.min_val)
    }

    /// Clears the HLL registers and resets cardinality estimation.
    pub fn clear_hll(&mut self) {
        self.hll.clear();
    }

    /// Returns a reference to the HLL for merging or inspection.
    #[must_use]
    pub const fn hll(&self) -> &HyperLogLog {
        &self.hll
    }

    /// Returns a mutable reference to the HLL.
    pub const fn hll_mut(&mut self) -> &mut HyperLogLog {
        &mut self.hll
    }
}

impl Default for NumericRange {
    fn default() -> Self {
        Self::new()
    }
}
