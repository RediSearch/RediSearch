/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Sparse Vector Index for RediSearch.
//!
//! This crate provides the core data structure for sparse vector indexing,
//! mapping dimension IDs to inverted indexes with numeric encoding.
//!
//! # Architecture
//!
//! A sparse vector is represented as a set of `(dimension_id, weight)` pairs,
//! where most dimensions have zero weight and are not stored. This crate
//! stores these sparse vectors in an inverted index structure:
//!
//! ```text
//! HashMap<dimension_id, InvertedIndex<Numeric>>
//!
//! dimension_0  ──→ [(doc_1, 0.5), (doc_3, 0.7), (doc_8, 0.2), ...]
//! dimension_42 ──→ [(doc_1, 0.3), (doc_5, 0.9), ...]
//! dimension_99 ──→ [(doc_2, 0.1), (doc_3, 0.4), (doc_8, 0.6), ...]
//! ```
//!
//! This structure enables efficient similarity queries by:
//! 1. Looking up posting lists for each query dimension (O(1) per dimension)
//! 2. Merging posting lists to accumulate dot product scores
//! 3. Returning top-K results by score
//!
//! # Design Rationale
//!
//! - **HashMap over Trie**: Dimension IDs are integers (u32), not strings.
//!   No prefix matching is needed, so O(1) HashMap lookup is preferred.
//! - **Numeric encoder reuse**: The existing [`inverted_index::numeric::Numeric`]
//!   encoder efficiently stores `(doc_id, float)` pairs with delta encoding
//!   and float compression.
//! - **EntriesTrackingIndex**: Wraps InvertedIndex to track entry count,
//!   matching the pattern used by NumericRangeTree.
//! - **Metric-agnostic storage**: The index stores raw weights only. Similarity
//!   metrics (dot product, cosine) are applied at query time, not index time.

mod query;

use std::ptr;

use ffi::{IndexFlags_Index_StoreNumeric, t_docId};
use inverted_index::{
    EntriesTrackingIndex, RSIndexResult, RSResultData, numeric::Numeric, t_fieldMask,
};
use rustc_hash::FxHashMap;

pub use query::SparseVectorQueryIterator;

/// All fields mask constant for sparse vector entries.
const RS_FIELDMASK_ALL: t_fieldMask = !0;

/// Sparse vector index storing dimension_id -> posting list mappings.
///
/// Each posting list is an [`EntriesTrackingIndex`] with [`Numeric`] encoding,
/// storing `(doc_id, weight)` pairs. This structure is analogous to how
/// [`TagIndex`] maps tag values to InvertedIndex instances, but uses a
/// HashMap for O(1) dimension lookup instead of a Trie.
///
/// # Similarity Metrics
///
/// The index itself is metric-agnostic - it stores raw dimension weights.
/// Similarity metrics (dot product, cosine similarity) are applied at query
/// time by the query iterator, not at index time. This design allows:
/// - The same index to be queried with different metrics
/// - No redundant storage of norms or normalized weights
/// - Query-time flexibility without re-indexing
#[derive(Debug, Default)]
pub struct SparseVectorIndex {
    /// Mapping from dimension ID to its posting list.
    /// Uses u32 for dimension IDs (supports vocabularies up to 4B dimensions).
    dimensions: FxHashMap<u32, EntriesTrackingIndex<Numeric>>,

    /// Total number of unique documents indexed.
    num_docs: u64,

    /// Total memory used by all posting lists (bytes).
    memory_usage: usize,
}

impl SparseVectorIndex {
    /// Create a new empty sparse vector index.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a sparse vector for a document.
    ///
    /// # Arguments
    /// * `doc_id` - The document ID
    /// * `entries` - Slice of `(dimension_id, weight)` pairs
    ///
    /// # Returns
    /// The number of bytes the index grew by.
    ///
    /// # Errors
    /// Returns an error if writing to the inverted index fails.
    ///
    /// # Note on Cosine Similarity
    /// For cosine similarity queries, the L2 norm of each document's sparse
    /// vector should be computed at indexing time and stored in the
    /// DocumentMetadata (DocTable). This keeps the index metric-agnostic
    /// while still enabling efficient cosine similarity computation at
    /// query time.
    pub fn add(&mut self, doc_id: t_docId, entries: &[(u32, f64)]) -> std::io::Result<usize> {
        let mut bytes_added = 0;

        // Add each dimension entry to its posting list
        for &(dim_id, weight) in entries {
            let posting_list = self.dimensions.entry(dim_id).or_insert_with(|| {
                EntriesTrackingIndex::new(IndexFlags_Index_StoreNumeric)
            });

            let record = RSIndexResult {
                doc_id,
                dmd: ptr::null(),
                field_mask: RS_FIELDMASK_ALL,
                freq: 1,
                data: RSResultData::Numeric(weight),
                metrics: ptr::null_mut(),
                weight: 1.0,
            };
            bytes_added += posting_list.add_record(&record)?;
        }

        self.num_docs += 1;
        self.memory_usage += bytes_added;
        Ok(bytes_added)
    }

    /// Get the posting list for a specific dimension.
    ///
    /// Returns `None` if the dimension has no indexed documents.
    pub fn get_dimension(&self, dim_id: u32) -> Option<&EntriesTrackingIndex<Numeric>> {
        self.dimensions.get(&dim_id)
    }

    /// Get the number of unique dimensions in the index.
    pub fn num_dimensions(&self) -> usize {
        self.dimensions.len()
    }

    /// Get the total number of indexed documents.
    pub const fn num_docs(&self) -> u64 {
        self.num_docs
    }

    /// Get total memory usage in bytes.
    ///
    /// This includes the memory used by all posting lists but not
    /// the overhead of the HashMap structure itself.
    pub const fn memory_usage(&self) -> usize {
        self.memory_usage
    }

    /// Check if the index is empty.
    pub const fn is_empty(&self) -> bool {
        self.num_docs == 0
    }

    /// Get an iterator over all dimension IDs in the index.
    pub fn dimension_ids(&self) -> impl Iterator<Item = u32> + '_ {
        self.dimensions.keys().copied()
    }

    /// Get the total number of entries across all dimensions.
    ///
    /// This counts all `(doc_id, weight)` pairs stored in the index.
    pub fn total_entries(&self) -> usize {
        self.dimensions
            .values()
            .map(|pl| pl.number_of_entries())
            .sum()
    }
}
