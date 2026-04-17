/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Core traits that score sources must implement to plug into [`TopKIterator`].
//!
//! [`TopKIterator`]: crate::TopKIterator

use ffi::t_docId;
use inverted_index::RSIndexResult;
use rqe_iterator_type::IteratorType;
use rqe_iterators::RQEIteratorError;

// ── ScoreBatch ────────────────────────────────────────────────────────────────

/// A cursor over a single score-ordered batch of `(doc_id, score)` pairs.
///
/// Batches are produced by [`ScoreSource::next_batch`] and consumed by the
/// [`TopKIterator`]'s intersection engine.  Doc IDs within a batch must be
/// **strictly increasing**.
///
/// [`TopKIterator`]: crate::TopKIterator
pub trait ScoreBatch {
    /// Advance to the next `(doc_id, score)` pair.
    ///
    /// Returns `None` when the batch is exhausted.
    fn next(&mut self) -> Option<(t_docId, f64)>;

    /// Skip forward to the first pair whose `doc_id >= target`.
    ///
    /// Returns `None` if no such pair exists in this batch.
    fn skip_to(&mut self, target: t_docId) -> Option<(t_docId, f64)>;
}

// ── CollectionStrategy ────────────────────────────────────────────────────────

/// Decision returned by [`ScoreSource::collection_strategy`] after each batch,
/// telling [`TopKIterator`] how to proceed.
///
/// [`TopKIterator`]: crate::TopKIterator
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CollectionStrategy {
    /// Keep iterating — fetch the next batch.
    Continue,

    /// Restart batch collection (e.g. after the source has expanded its range).
    ///
    /// The iterator rewinds both the source and the child, then re-enters
    /// Batches mode from the beginning.
    SwitchToBatches,

    /// Collection is complete — stop and yield whatever is in the heap.
    Stop,
}

// ── ScoreSource ───────────────────────────────────────────────────────────────

/// The score-producing half of a [`TopKIterator`].
///
/// A [`ScoreSource`] knows how to:
/// 1. Produce score-ordered batches ([`next_batch`]).
/// 2. Build the final [`RSIndexResult`] for a `(doc_id, score)` pair ([`build_result`]).
/// 3. Decide, after each batch, whether to continue or switch strategy
///    ([`collection_strategy`]).
///
/// [`TopKIterator`]: crate::TopKIterator
/// [`next_batch`]: ScoreSource::next_batch
/// [`build_result`]: ScoreSource::build_result
/// [`collection_strategy`]: ScoreSource::collection_strategy
pub trait ScoreSource<'index> {
    /// The type of batch cursor this source produces.
    type Batch: ScoreBatch;

    /// Fetch the next score-ordered batch.
    ///
    /// Returns:
    /// - `Ok(Some(batch))` — a new batch is available.
    /// - `Ok(None)` — the source is exhausted; no more batches.
    /// - `Err(RQEIteratorError::TimedOut)` — the query time limit was reached.
    fn next_batch(&mut self) -> Result<Option<Self::Batch>, RQEIteratorError>;

    /// Return an upper-bound estimate for the number of documents this source
    /// can produce.
    fn num_estimated(&self) -> usize;

    /// Rewind the source to its initial state so batch iteration can restart.
    fn rewind(&mut self);

    /// Build the [`RSIndexResult`] that the [`TopKIterator`] will yield for a
    /// given `(doc_id, score)` pair.
    ///
    /// [`TopKIterator`]: crate::TopKIterator
    fn build_result(&self, doc_id: t_docId, score: f64) -> RSIndexResult<'index>;

    /// Called after each batch to decide how collection should proceed.
    ///
    /// - `heap_count` — number of results currently in the heap.
    /// - `k` — the target number of results.
    fn collection_strategy(&mut self, heap_count: usize, k: usize) -> CollectionStrategy;

    /// The [`IteratorType`] that the wrapping [`TopKIterator`] should report.
    ///
    /// [`TopKIterator`]: crate::TopKIterator
    fn iterator_type(&self) -> IteratorType;
}
