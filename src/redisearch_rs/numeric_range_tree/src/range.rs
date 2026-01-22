/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Numeric range storage for the numeric range tree.

use ffi::{IndexFlags_Index_StoreNumeric, t_docId};
use hyperloglog::{CFnvHasher, HyperLogLog6};
use inverted_index::{EntriesTrackingIndex, RSIndexResult, numeric::Numeric};

/// HyperLogLog precision used for cardinality estimation.
/// 6 bits = 64 registers, ~13% error rate.
/// This matches the C implementation's `NR_BIT_PRECISION`.
pub type Hll = HyperLogLog6<CFnvHasher>;

/// A numeric range is a leaf-level storage unit in the numeric range tree.
///
/// It stores document IDs and their associated numeric values in an inverted index,
/// along with metadata for range queries and cardinality estimation.
#[derive(Debug)]
pub struct NumericRange {
    /// The minimum value stored in this range.
    min_val: f64,
    /// The maximum value stored in this range.
    max_val: f64,
    /// HyperLogLog for estimating cardinality (number of distinct values).
    hll: Hll,
    /// The inverted index storing (docId, value) entries.
    entries: EntriesTrackingIndex<Numeric>,
}

impl NumericRange {
    /// Create a new empty numeric range.
    pub fn new() -> Self {
        Self {
            min_val: f64::INFINITY,
            max_val: f64::NEG_INFINITY,
            hll: Hll::new(),
            entries: EntriesTrackingIndex::new(IndexFlags_Index_StoreNumeric),
        }
    }

    /// Add a (docId, value) entry to this range.
    ///
    /// Updates min/max bounds and cardinality estimation.
    /// Returns the number of bytes the inverted index grew by.
    pub fn add(&mut self, doc_id: t_docId, value: f64) -> std::io::Result<usize> {
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
    /// This is used when splitting nodes, where cardinality is tracked
    /// separately for the destination ranges.
    pub fn add_without_cardinality(&mut self, doc_id: t_docId, value: f64) -> std::io::Result<usize> {
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
    pub fn update_cardinality(&mut self, value: f64) {
        self.hll.add(&value.to_le_bytes());
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
    pub fn inverted_index_size(&self) -> usize {
        self.entries.memory_usage()
    }

    /// Get a reference to the underlying inverted index.
    pub const fn entries(&self) -> &EntriesTrackingIndex<Numeric> {
        &self.entries
    }

    /// Get a reference to the HyperLogLog.
    pub const fn hll(&self) -> &Hll {
        &self.hll
    }
}

impl Default for NumericRange {
    fn default() -> Self {
        Self::new()
    }
}
