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

pub mod debug;
mod query;
mod vector;

use std::sync::atomic::{AtomicU32, Ordering};

use ffi::{IndexFlags_Index_StoreNumeric, t_docId};
use index_result::RSIndexResult;
use inverted_index::{
    DocLiveness, EntriesTrackingIndex, GcApplyInfo, GcScanDelta, IndexReader,
    numeric::NumericFloatCompression,
};
use rustc_hash::FxHashMap;

pub use query::{DimensionIterator, SparseVectorIterator};
pub use vector::{SparseVectorError, parse_blob, prepare};

/// The posting list of one dimension: `(doc_id, weight)` pairs in doc-ID order.
pub type PostingList = EntriesTrackingIndex<NumericFloatCompression>;

/// The similarity a sparse vector field ranks by, fixed at field creation.
///
/// The single source of its discriminants: cheadergen exports it to C as
/// `SparseMetric_InnerProduct` and `SparseMetric_Cosine`, which the field
/// stores. It crosses the FFI as a `u32`, converted back with [`TryFrom<u32>`].
#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cheadergen::config(export, prefix_with_name)]
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
        [Self::InnerProduct, Self::Cosine]
            .into_iter()
            .find(|metric| *metric as u32 == value)
            .ok_or(value)
    }
}

/// Source of [`SparseVectorIndex::unique_id`]. Wraps, like the tag index's
/// counter: it only has to tell a replaced index apart from the one a GC scan
/// saw at the same field.
static NEXT_UNIQUE_ID: AtomicU32 = AtomicU32::new(0);

/// What one [`SparseVectorIndex::add`] changed, for the spec's statistics.
#[repr(C)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SparseVectorAddOutcome {
    /// Bytes the posting lists grew by.
    pub bytes: usize,
    /// `(doc_id, weight)` pairs written.
    pub records: usize,
    /// Index blocks created.
    pub blocks: usize,
}

/// A field's size, as `FT.INFO` and `INFO MODULES` report it.
#[repr(C)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SparseVectorIndexStats {
    /// Bytes used by all posting lists.
    pub memory: usize,
    /// Dimensions that have a posting list.
    pub num_dimensions: usize,
    /// `(doc_id, weight)` pairs across all posting lists.
    pub num_records: usize,
}

/// One dimension's posting list size, for `FT.DEBUG`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DimensionStats {
    /// The dimension ID.
    pub dim_id: u32,
    /// Documents in its posting list.
    pub num_entries: usize,
    /// Index blocks of its posting list.
    pub num_blocks: usize,
    /// Bytes its posting list uses.
    pub memory: usize,
}

/// What one [`SparseVectorIndex::apply_gc`] changed.
#[derive(Debug)]
pub struct SparseVectorGcOutcome {
    /// What the posting list's own GC did.
    pub info: GcApplyInfo,
    /// Set when the posting list was left empty and the dimension removed.
    pub removed: Option<RemovedDimension>,
}

/// What removing an emptied dimension freed, beyond [`GcApplyInfo`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RemovedDimension {
    /// The bytes the empty posting list still held.
    pub bytes: usize,
    /// The blocks it still held.
    pub blocks: usize,
}

/// One field's sparse vector index.
#[derive(Debug)]
pub struct SparseVectorIndex {
    /// Dimension ID to its posting list.
    ///
    /// Each posting list is boxed so its address survives a rehash: a query
    /// iterator keeps reading it while the spec lock is released, and a
    /// concurrent write that adds a dimension may grow the map. GC may still
    /// remove a dimension, so a parked iterator re-resolves its list on
    /// revalidation (see [`DimensionIterator`]).
    dimensions: FxHashMap<u32, Box<PostingList>>,
    metric: SparseMetric,
    /// Size of the dimension space; valid dimension IDs are `0..dim`.
    dim: u64,
    /// Bytes used by all posting lists.
    memory_usage: usize,
    /// `(doc_id, weight)` pairs across all posting lists.
    num_records: usize,
    unique_id: u32,
}

impl SparseVectorIndex {
    /// Create an empty index for a field of `dim` dimensions ranked by `metric`.
    pub fn new(dim: u64, metric: SparseMetric) -> Self {
        Self {
            dimensions: FxHashMap::default(),
            metric,
            dim,
            memory_usage: 0,
            num_records: 0,
            unique_id: NEXT_UNIQUE_ID.fetch_add(1, Ordering::Relaxed),
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

    /// An ID that tells this index apart from one that replaced it at the same
    /// field, for a GC delta computed against an earlier state.
    pub const fn unique_id(&self) -> u32 {
        self.unique_id
    }

    /// Add a document's vector, already checked and normalized by [`prepare`].
    ///
    /// `doc_id` must be greater than every doc ID added before, as for any
    /// inverted index.
    pub fn add(
        &mut self,
        doc_id: t_docId,
        entries: &[(u32, f32)],
    ) -> std::io::Result<SparseVectorAddOutcome> {
        let mut outcome = SparseVectorAddOutcome::default();
        for &(dim_id, weight) in entries {
            debug_assert!(u64::from(dim_id) < self.dim, "entries must be prepared");
            let posting_list = self.dimensions.entry(dim_id).or_insert_with(|| {
                let list = Box::new(PostingList::new(IndexFlags_Index_StoreNumeric));
                outcome.bytes += list.memory_usage();
                list
            });
            let record = RSIndexResult::build_numeric(f64::from(weight))
                .doc_id(doc_id)
                .build();
            let added = posting_list.add_record(&record)?;
            outcome.bytes += added.mem_growth as usize;
            outcome.blocks += added.blocks_added as usize;
            outcome.records += 1;
        }
        self.memory_usage += outcome.bytes;
        self.num_records += outcome.records;
        Ok(outcome)
    }

    /// The posting list of `dim_id`, or `None` when no document has it.
    pub fn get_dimension(&self, dim_id: u32) -> Option<&PostingList> {
        self.dimensions.get(&dim_id).map(Box::as_ref)
    }

    /// The number of dimensions that have a posting list.
    pub fn num_dimensions(&self) -> usize {
        self.dimensions.len()
    }

    /// Bytes used by all posting lists, not counting the map itself.
    pub const fn memory_usage(&self) -> usize {
        self.memory_usage
    }

    /// The number of `(doc_id, weight)` pairs across all posting lists.
    pub const fn num_records(&self) -> usize {
        self.num_records
    }

    /// The index's size, for `FT.INFO`.
    pub fn stats(&self) -> SparseVectorIndexStats {
        SparseVectorIndexStats {
            memory: self.memory_usage,
            num_dimensions: self.num_dimensions(),
            num_records: self.num_records,
        }
    }

    /// The `n` dimensions with the most entries, longest first and, among
    /// equally long ones, by increasing ID.
    ///
    /// Long posting lists are what make a sparse query slow, so this is what a
    /// pruning decision needs to see.
    pub fn longest_dimensions(&self, n: usize) -> Vec<DimensionStats> {
        let mut dims: Vec<DimensionStats> = self
            .dimensions
            .iter()
            .map(|(&dim_id, list)| DimensionStats {
                dim_id,
                num_entries: list.number_of_entries(),
                num_blocks: list.number_of_blocks(),
                memory: list.memory_usage(),
            })
            .collect();
        dims.sort_unstable_by(|a, b| {
            b.num_entries
                .cmp(&a.num_entries)
                .then(a.dim_id.cmp(&b.dim_id))
        });
        dims.truncate(n);
        dims
    }

    /// The `(doc_id, weight)` pairs of `dim_id`'s posting list, in doc-ID
    /// order, or `None` when no document has it.
    pub fn dimension_entries(&self, dim_id: u32) -> Option<Vec<(t_docId, f32)>> {
        let list = self.get_dimension(dim_id)?;
        let mut reader = list.reader();
        let mut result = RSIndexResult::build_numeric(0.0).build();
        let mut entries = Vec::with_capacity(list.number_of_entries());
        while reader
            .next_record(&mut result)
            .expect("a posting list in memory decodes")
        {
            let weight = result
                .as_numeric()
                .expect("a posting list holds numeric records");
            // Weights are written from an `f32`, so this is exact.
            entries.push((result.doc_id, weight as f32));
        }
        Some(entries)
    }

    /// The dimension IDs that have a posting list, in no particular order.
    pub fn dimension_ids(&self) -> impl Iterator<Item = u32> + '_ {
        self.dimensions.keys().copied()
    }

    /// The dimension IDs that have a posting list, in increasing order.
    ///
    /// This copies and sorts every ID, so a caller that walks the dimensions
    /// in pieces should take it once and resume by position.
    pub fn sorted_dimension_ids(&self) -> Vec<u32> {
        let mut ids: Vec<u32> = self.dimension_ids().collect();
        ids.sort_unstable();
        ids
    }

    /// Apply a GC delta to `dim_id`'s posting list, and remove the dimension if
    /// that leaves it empty.
    ///
    /// Returns `None` without touching anything when the dimension is gone or
    /// its posting list is not the one `list_unique_id` names, so a delta
    /// computed against a replaced list is never applied.
    ///
    /// `live` is as for [`EntriesTrackingIndex::apply_gc_with`].
    pub fn apply_gc(
        &mut self,
        dim_id: u32,
        list_unique_id: u32,
        delta: GcScanDelta,
        live: Option<&dyn DocLiveness>,
    ) -> Option<SparseVectorGcOutcome> {
        let list = self.dimensions.get_mut(&dim_id)?;
        if u32::from(list.unique_id()) != list_unique_id {
            return None;
        }
        let info = list.apply_gc_with(delta, live);
        self.memory_usage =
            (self.memory_usage + info.bytes_allocated).saturating_sub(info.bytes_freed);
        self.num_records = self.num_records.saturating_sub(info.entries_removed);

        let emptied = (list.unique_docs() == 0).then(|| RemovedDimension {
            bytes: list.memory_usage(),
            blocks: list.number_of_blocks(),
        });
        if let Some(removed) = emptied {
            self.dimensions.remove(&dim_id);
            self.memory_usage = self.memory_usage.saturating_sub(removed.bytes);
        }
        Some(SparseVectorGcOutcome {
            info,
            removed: emptied,
        })
    }
}
