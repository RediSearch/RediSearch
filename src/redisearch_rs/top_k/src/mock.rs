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

use std::collections::HashMap;

use ffi::t_docId;
use inverted_index::RSIndexResult;
use rqe_iterators::RQEIteratorError;

use crate::traits::{CollectionStrategy, ScoreBatch, ScoreSource};

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

    fn skip_to(&mut self, target: t_docId) -> Option<(t_docId, f64)> {
        while self.pos < self.items.len() && self.items[self.pos].0 < target {
            self.pos += 1;
        }
        self.next()
    }
}

// ── MockScoreSource ───────────────────────────────────────────────────────────

/// A [`ScoreSource`] backed by fixed data for use in tests and benchmarks.
///
/// # Usage
///
/// ```rust
/// # use top_k::mock::MockScoreSource;
/// # use top_k::traits::CollectionStrategy;
/// let source = MockScoreSource::new(
///     // Two batches: each is a Vec<(doc_id, score)>
///     vec![
///         vec![(1, 0.5), (3, 0.8)],
///         vec![(5, 0.2), (7, 0.9)],
///     ],
///     // Per-doc scores for adhoc-BF lookup
///     vec![(1, 0.5), (3, 0.8), (5, 0.2), (7, 0.9)],
///     // Strategy: always Continue
///     |_, _| CollectionStrategy::Continue,
/// );
/// ```
pub struct MockScoreSource {
    batches: Vec<Vec<(t_docId, f64)>>,
    batch_pos: usize,
    scores: HashMap<t_docId, f64>,
    strategy: Box<dyn FnMut(usize, usize) -> CollectionStrategy>,
    num_estimated: usize,
}

impl MockScoreSource {
    /// Creates a new `MockScoreSource`.
    ///
    /// - `batches` — sequence of batches; each inner `Vec` is one [`MockScoreBatch`].
    /// - `scores`  — per-document scores returned by [`lookup_score`].
    /// - `strategy` — function called after each batch / adhoc step.
    ///
    /// `num_estimated` defaults to the total number of entries across all batches.
    ///
    /// [`lookup_score`]: ScoreSource::lookup_score
    pub fn new(
        batches: Vec<Vec<(t_docId, f64)>>,
        scores: Vec<(t_docId, f64)>,
        strategy: impl FnMut(usize, usize) -> CollectionStrategy + 'static,
    ) -> Self {
        let num_estimated = batches.iter().map(Vec::len).sum();
        Self {
            batches,
            batch_pos: 0,
            scores: scores.into_iter().collect(),
            strategy: Box::new(strategy),
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

    fn lookup_score(&mut self, doc_id: t_docId) -> Option<f64> {
        self.scores.get(&doc_id).copied()
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

    fn collection_strategy(&mut self, heap_count: usize, k: usize) -> CollectionStrategy {
        (self.strategy)(heap_count, k)
    }

    fn iterator_type(&self) -> rqe_iterator_type::IteratorType {
        rqe_iterator_type::IteratorType::Mock
    }
}
