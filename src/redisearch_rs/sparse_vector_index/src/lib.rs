/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Sparse vector index for RediSearch.
//!
//! A sparse vector is a set of `(dimension_id, weight)` pairs. Each field keeps
//! one posting list per dimension in use, so a query is answered the way a
//! full-text OR query is: the union of its dimensions' posting lists, with each
//! matching document scored by a dot product.
//!
//! ```text
//! HashMap<dimension_id, InvertedIndex<NumericFloatCompression>>
//!
//! dimension_0  ──→ [(doc_1, 0.5), (doc_3, 0.7), (doc_8, 0.2), ...]
//! dimension_42 ──→ [(doc_1, 0.3), (doc_5, 0.9), ...]
//! ```
//!
//! - Dimension IDs are integers with no prefix queries, so a hashmap replaces
//!   the trie text terms use, and memory grows with the dimensions in use, not
//!   with the field's [`dim`](SparseVectorIndex::dim).
//! - Weights are `f32`. [`NumericFloatCompression`] stores a value that fits an
//!   `f32` exactly in 4 bytes, so they round-trip losslessly.
//! - The [`SparseMetric`] is fixed when the field is created. Under
//!   [`SparseMetric::Cosine`] every vector is L2-normalized before it is
//!   written, so scoring is always a plain dot product.

mod query;
mod vector;

use ffi::{IndexFlags_Index_StoreNumeric, t_docId};
use index_result::RSIndexResult;
use inverted_index::{EntriesTrackingIndex, numeric::NumericFloatCompression};
use rustc_hash::FxHashMap;

pub use query::{DimensionIterator, SparseVectorIterator};
pub use vector::{SparseVectorError, parse_blob, prepare};

/// The posting list of one dimension: `(doc_id, weight)` pairs in doc-ID order.
pub type PostingList = EntriesTrackingIndex<NumericFloatCompression>;

/// The similarity a sparse vector field ranks by, fixed at field creation.
///
/// Crosses the C boundary as its `u32` discriminant (see [`TryFrom<u32>`]).
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SparseMetric {
    /// Dot product of the raw weights.
    InnerProduct = 0,
    /// Dot product of L2-normalized weights; every vector is normalized before
    /// it is written or queried.
    Cosine = 1,
}

impl TryFrom<u32> for SparseMetric {
    type Error = u32;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::InnerProduct),
            1 => Ok(Self::Cosine),
            other => Err(other),
        }
    }
}

/// One field's sparse vector index.
#[derive(Debug)]
pub struct SparseVectorIndex {
    /// Dimension ID to its posting list.
    ///
    /// Each posting list is boxed so its address survives a rehash: a query
    /// iterator keeps borrowing it while the spec lock is released, and a
    /// concurrent write that adds a dimension may grow the map.
    dimensions: FxHashMap<u32, Box<PostingList>>,
    metric: SparseMetric,
    /// Size of the dimension space; valid dimension IDs are `0..dim`.
    dim: u64,
    /// Bytes used by all posting lists.
    memory_usage: usize,
    /// `(doc_id, weight)` pairs across all posting lists.
    num_entries: usize,
}

impl SparseVectorIndex {
    /// Create an empty index for a field of `dim` dimensions ranked by `metric`.
    pub fn new(dim: u64, metric: SparseMetric) -> Self {
        Self {
            dimensions: FxHashMap::default(),
            metric,
            dim,
            memory_usage: 0,
            num_entries: 0,
        }
    }

    /// The field's metric.
    pub const fn metric(&self) -> SparseMetric {
        self.metric
    }

    /// The size of the field's dimension space.
    pub const fn dim(&self) -> u64 {
        self.dim
    }

    /// Add a document's vector, already checked and normalized by [`prepare`].
    ///
    /// `doc_id` must be greater than every doc ID added before, as for any
    /// inverted index.
    ///
    /// Returns the number of bytes the index grew by.
    pub fn add(&mut self, doc_id: t_docId, entries: &[(u32, f32)]) -> std::io::Result<usize> {
        let mut bytes_added = 0;
        for &(dim_id, weight) in entries {
            debug_assert!(u64::from(dim_id) < self.dim, "entries must be prepared");
            let posting_list = self.dimensions.entry(dim_id).or_insert_with(|| {
                let list = Box::new(PostingList::new(IndexFlags_Index_StoreNumeric));
                bytes_added += list.memory_usage();
                list
            });
            let record = RSIndexResult::build_numeric(f64::from(weight))
                .doc_id(doc_id)
                .build();
            bytes_added += posting_list.add_record(&record)?.mem_growth as usize;
            self.num_entries += 1;
        }
        self.memory_usage += bytes_added;
        Ok(bytes_added)
    }

    /// The posting list of `dim_id`, or `None` when no document has it.
    pub fn get_dimension(&self, dim_id: u32) -> Option<&PostingList> {
        self.dimensions.get(&dim_id).map(Box::as_ref)
    }

    /// The number of dimensions that have a posting list.
    pub fn num_dimensions(&self) -> usize {
        self.dimensions.len()
    }

    /// The number of `(doc_id, weight)` pairs across all posting lists.
    pub const fn num_entries(&self) -> usize {
        self.num_entries
    }

    /// Bytes used by all posting lists, not counting the map itself.
    pub const fn memory_usage(&self) -> usize {
        self.memory_usage
    }

    /// Whether no document has been added.
    pub const fn is_empty(&self) -> bool {
        self.num_entries == 0
    }

    /// The dimension IDs that have a posting list, in no particular order.
    pub fn dimension_ids(&self) -> impl Iterator<Item = u32> + '_ {
        self.dimensions.keys().copied()
    }
}
