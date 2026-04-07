/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Mock implementations of [`ScoreBatch`] and [`ScoreSource`] for use in unit
//! tests and benchmarks.
//!
//! Gated behind the `test-utils` feature.

use ffi::t_docId;
use inverted_index::RSIndexResult;
use rqe_iterators::RQEIteratorError;

use crate::traits::{ScoreBatch, ScoreSource};

// ── MockScoreBatch ────────────────────────────────────────────────────────────

/// A [`ScoreBatch`] backed by a pre-sorted `Vec<(t_docId, f64)>`.
///
/// Doc IDs must be strictly increasing; this is not validated at runtime.
pub struct MockScoreBatch {
    items: Vec<(t_docId, f64)>,
    pos: usize,
}

impl MockScoreBatch {
    /// Creates a batch from a vector of `(doc_id, score)` pairs.
    ///
    /// The pairs must be sorted by `doc_id` in strictly ascending order.
    pub fn new(items: Vec<(t_docId, f64)>) -> Self {
        Self { items, pos: 0 }
    }
}

impl ScoreBatch for MockScoreBatch {
    fn next(&mut self) -> Option<(t_docId, f64)> {
        let item = self.items.get(self.pos).copied();
        if item.is_some() {
            self.pos += 1;
        }
        item
    }
}

// ── MockScoreSource ───────────────────────────────────────────────────────────

/// A [`ScoreSource`] backed by fixed data for use in tests and benchmarks.
///
/// # Usage
///
/// ```rust
/// # use top_k::mock::MockScoreSource;
/// let source = MockScoreSource::new(
///     // Two batches: each is a Vec<(doc_id, score)>
///     vec![
///         vec![(1, 0.5), (3, 0.8)],
///         vec![(5, 0.2), (7, 0.9)],
///     ],
/// );
/// ```
pub struct MockScoreSource {
    batches: Vec<Vec<(t_docId, f64)>>,
    batch_pos: usize,
    num_estimated: usize,
}

impl MockScoreSource {
    /// Creates a new `MockScoreSource`.
    ///
    /// - `batches` — sequence of batches; each inner `Vec` is one [`MockScoreBatch`].
    /// - `strategy` — function called after each batch.
    ///
    /// `num_estimated` defaults to the total number of entries across all batches.
    pub fn new(batches: Vec<Vec<(t_docId, f64)>>) -> Self {
        let num_estimated = batches.iter().map(Vec::len).sum();
        Self {
            batches,
            batch_pos: 0,
            num_estimated,
        }
    }

    /// Override the `num_estimated` value.
    pub fn with_num_estimated(mut self, n: usize) -> Self {
        self.num_estimated = n;
        self
    }
}

impl<'index> ScoreSource<'index> for MockScoreSource {
    type Batch = MockScoreBatch;

    fn next_batch(&mut self) -> Result<Option<Self::Batch>, RQEIteratorError> {
        let batch = self
            .batches
            .get(self.batch_pos)
            .cloned()
            .map(MockScoreBatch::new);
        if batch.is_some() {
            self.batch_pos += 1;
        }
        Ok(batch)
    }

    fn num_estimated(&self) -> usize {
        self.num_estimated
    }

    fn rewind(&mut self) {
        self.batch_pos = 0;
    }

    fn build_result(&self, doc_id: t_docId, _score: f64) -> RSIndexResult<'index> {
        RSIndexResult::build_virt().doc_id(doc_id).build()
    }

    fn iterator_type(&self) -> rqe_iterator_type::IteratorType {
        rqe_iterator_type::IteratorType::Mock
    }
}
