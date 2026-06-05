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

use index_result::RSIndexResult;
use rqe_core::DocId;
use rqe_iterators::RQEIteratorError;

use crate::{
    ScoredResult,
    traits::{BatchStrategy, ScoreBatch, ScoreSource},
};

/// A [`ScoreBatch`] backed by a pre-sorted `Vec<(DocId, f64)>`.
///
/// Doc IDs must be strictly increasing; this is validated in debug builds.
pub struct MockScoreBatch {
    items: Vec<(DocId, f64)>,
    pos: usize,
}

impl MockScoreBatch {
    /// Creates a batch from a vector of `(doc_id, score)` pairs.
    ///
    /// The pairs must be sorted by `doc_id` in strictly ascending order (asserted in debug builds).
    pub fn new(items: Vec<(DocId, f64)>) -> Self {
        debug_assert!(
            items.windows(2).all(|w| w[0].0 < w[1].0),
            "MockScoreBatch: doc IDs must be strictly increasing"
        );
        Self { items, pos: 0 }
    }
}

impl ScoreBatch for MockScoreBatch {
    fn next(&mut self) -> Option<(DocId, f64)> {
        let item = self.items.get(self.pos).copied();
        if item.is_some() {
            self.pos += 1;
        }
        item
    }

    fn skip_to(&mut self, target: DocId) -> Option<(DocId, f64)> {
        self.pos += self.items[self.pos..].partition_point(|(id, _)| *id < target);
        self.next()
    }
}

/// A [`ScoreSource`] backed by fixed data for use in tests and benchmarks.
///
/// # Usage
///
/// ```rust
/// # use top_k::mock::MockScoreSource;
/// # use top_k::traits::BatchStrategy;
/// let source = MockScoreSource::new(
///     // Two batches: each is a Vec<(doc_id, score)>
///     vec![
///         vec![(1, 0.5), (3, 0.8)],
///         vec![(5, 0.2), (7, 0.9)],
///     ],
///     // Per-doc scores for adhoc-BF lookup
///     vec![(1, 0.5), (3, 0.8), (5, 0.2), (7, 0.9)],
///     // Batch strategy: always Continue
///     |_, _| BatchStrategy::Continue,
/// );
/// ```
pub struct MockScoreSource {
    batches: Vec<Vec<(DocId, f64)>>,
    batch_pos: usize,
    scores: HashMap<DocId, f64>,
    batch_strategy: Box<dyn FnMut(usize, usize) -> BatchStrategy>,
    num_estimated: usize,
    /// Exact scores applied by [`ScoreSource::rerank`]. `None` disables
    /// reranking (the default); `Some` map rescores any retained doc it
    /// contains and leaves the rest untouched.
    rerank_scores: Option<HashMap<DocId, f64>>,
}

impl MockScoreSource {
    /// Creates a new `MockScoreSource`.
    ///
    /// - `batches` — sequence of batches; each inner `Vec` is one [`MockScoreBatch`].
    /// - `scores`  — per-document scores returned by [`lookup_score`].
    /// - `batch_strategy` — function called after each batch (Batches mode only).
    ///
    /// `num_estimated` defaults to the total number of entries across all batches.
    ///
    /// [`lookup_score`]: ScoreSource::lookup_score
    pub fn new(
        batches: Vec<Vec<(DocId, f64)>>,
        scores: Vec<(DocId, f64)>,
        batch_strategy: impl FnMut(usize, usize) -> BatchStrategy + 'static,
    ) -> Self {
        let num_estimated = batches.iter().map(Vec::len).sum();
        Self {
            batches,
            batch_pos: 0,
            scores: scores.into_iter().collect(),
            batch_strategy: Box::new(batch_strategy),
            num_estimated,
            rerank_scores: None,
        }
    }

    /// Override the `num_estimated` value.
    pub fn with_num_estimated(mut self, n: usize) -> Self {
        self.num_estimated = n;
        self
    }

    /// Enable reranking with the given exact scores. After an adhoc scan, each
    /// retained doc present in `scores` is rescored to that value; docs absent
    /// from the map keep their adhoc score, mirroring the disk path's handling
    /// of labels with no exact distance.
    pub fn with_rerank(mut self, scores: Vec<(DocId, f64)>) -> Self {
        self.rerank_scores = Some(scores.into_iter().collect());
        self
    }
}

impl ScoreSource for MockScoreSource {
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

    fn lookup_score(&mut self, doc_id: DocId) -> Option<f64> {
        self.scores.get(&doc_id).copied()
    }

    fn num_estimated(&self) -> usize {
        self.num_estimated
    }

    fn rewind(&mut self) {
        self.batch_pos = 0;
    }

    fn build_result<'r>(&self, doc_id: DocId, _score: f64) -> RSIndexResult<'r>
    where
        Self: 'r,
    {
        RSIndexResult::build_virt().doc_id(doc_id).build()
    }

    fn batch_strategy(&mut self, heap_count: usize, k: usize) -> BatchStrategy {
        (self.batch_strategy)(heap_count, k)
    }

    fn adhoc_check_timeout(&mut self) -> bool {
        return false;
    }

    fn should_rerank(&self) -> bool {
        self.rerank_scores.is_some()
    }

    fn rerank(&mut self, results: &mut [ScoredResult]) {
        let Some(scores) = &self.rerank_scores else {
            return;
        };
        for result in results.iter_mut() {
            if let Some(&exact) = scores.get(&result.doc_id) {
                result.score = exact;
            }
        }
    }

    fn iterator_type(&self) -> rqe_iterator_type::IteratorType {
        rqe_iterator_type::IteratorType::Mock
    }
}
