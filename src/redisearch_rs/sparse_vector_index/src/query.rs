/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Query processing for sparse vector search.
//!
//! This module provides the [`SparseVectorQueryIterator`] which uses an
//! intersection-based approach to find documents matching all query dimensions.

use inverted_index::{IndexReaderCore, RSIndexResult, numeric::Numeric};
use rqe_iterators::{
    Intersection, NoOpChecker, RQEIterator, RQEIteratorError, inverted_index::InvIndIterator,
};

use crate::SparseVectorIndex;

/// Type alias for a dimension posting list iterator.
///
/// Uses `InvIndIterator` directly with `IndexReaderCore<Numeric>` to iterate
/// over `(doc_id, weight)` entries in a dimension's posting list.
type DimensionIterator<'index> =
    InvIndIterator<'index, IndexReaderCore<'index, Numeric>, NoOpChecker>;

/// Iterator for sparse vector queries.
///
/// Uses an intersection of per-dimension inverted index iterators to find
/// documents that contain ALL query dimensions.
///
/// # Query Semantics
///
/// This iterator only returns documents that have ALL query dimensions present.
/// Documents missing any query dimension are not returned.
///
/// # Example
///
/// ```ignore
/// let mut index = SparseVectorIndex::new();
/// index.add(1, &[(10, 0.5), (20, 0.3)])?;
/// index.add(2, &[(10, 0.2), (20, 0.8)])?;
///
/// let query_dims = [10, 20];
/// let mut iter = SparseVectorQueryIterator::new(&index, &query_dims)?;
///
/// while let Some(result) = iter.read()? {
///     println!("Doc {}", result.doc_id);
/// }
/// ```
pub struct SparseVectorQueryIterator<'index> {
    /// The intersection iterator over dimension posting lists.
    intersection: Intersection<'index, DimensionIterator<'index>>,
}

impl<'index> SparseVectorQueryIterator<'index> {
    /// Create a query iterator from a sparse vector index and query.
    ///
    /// # Arguments
    /// * `index` - The sparse vector index to query
    /// * `query` - Query as `(dimension_id, weight)` pairs. Weights are stored
    ///   in the query but not used by the iterator (scoring is a separate concern).
    ///
    /// # Returns
    /// A query iterator, or `None` if any query dimension doesn't exist in the
    /// index (since intersection would be empty anyway).
    pub fn new(index: &'index SparseVectorIndex, query: &[(u32, f64)]) -> Option<Self> {
        if query.is_empty() {
            return None;
        }

        // Build iterators for each query dimension (weights are ignored by the iterator)
        let mut iterators: Vec<DimensionIterator<'index>> = Vec::with_capacity(query.len());

        for &(dim_id, _weight) in query {
            let posting_list = index.get_dimension(dim_id)?;
            let reader = posting_list.reader();
            let result = RSIndexResult::numeric(0.0);
            let iterator = InvIndIterator::new(reader, result, NoOpChecker);
            iterators.push(iterator);
        }

        // Sort by estimated count (smallest first) for efficient intersection
        iterators.sort_by_cached_key(|it| it.num_estimated());

        let intersection = Intersection::new(iterators);

        Some(Self { intersection })
    }

    /// Read the next matching document.
    ///
    /// Returns the intersection result (containing doc_id and aggregate children
    /// with per-dimension weights from the index).
    pub fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        self.intersection.read()
    }
}
