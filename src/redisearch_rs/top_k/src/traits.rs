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

use index_result::RSIndexResult;
use rqe_core::DocId;
use rqe_iterator_type::IteratorType;
use rqe_iterators::RQEIteratorError;

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
    fn next(&mut self) -> Option<(DocId, f64)>;

    /// Skip forward to the first pair whose `doc_id >= target`.
    ///
    /// Returns `None` if no such pair exists in this batch.
    fn skip_to(&mut self, target: DocId) -> Option<(DocId, f64)>;
}

/// Decision returned by [`ScoreSource::batch_strategy`] after each batch,
/// telling [`TopKIterator`] how to proceed in Batches mode.
///
/// [`TopKIterator`]: crate::TopKIterator
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchStrategy {
    /// Keep fetching batches.
    Continue,
    /// Switch from Batches mode to Adhoc-BF mode.
    ///
    /// The iterator will rewind the child and start calling
    /// [`ScoreSource::lookup_score`] for each document the child yields.
    SwitchToAdhoc,
    /// Restart batch collection (e.g. after the source has expanded its range).
    ///
    /// The iterator rewinds both the source and the child, then re-enters
    /// Batches mode from the beginning.
    SwitchToBatches,
    /// Collection is complete — stop and yield whatever is in the heap.
    Stop,
}

/// Decision returned by [`ScoreSource::adhoc_strategy`] after each adhoc
/// lookup, telling [`TopKIterator`] whether to keep walking the child iterator.
///
/// [`TopKIterator`]: crate::TopKIterator
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdhocStrategy {
    /// Keep walking the child iterator.
    Continue,

    /// Collection is complete — stop and yield whatever is in the heap.
    Stop,
}

/// The score-producing half of a [`TopKIterator`].
///
/// A [`ScoreSource`] knows how to:
/// 1. Produce score-ordered batches ([`next_batch`]).
/// 2. Produce all results in a single shot ([`next_batch_unfiltered`]), used in Unfiltered mode.
/// 3. Look up the score for an individual document ([`lookup_score`]), used in Adhoc-BF mode.
/// 4. Build the final [`RSIndexResult`] for a `(doc_id, score)` pair ([`build_result`]).
/// 5. Decide, after each batch, whether to continue or switch strategy ([`batch_strategy`]).
/// 6. Decide, after each adhoc lookup, whether to keep walking the child ([`adhoc_strategy`]).
///
/// # Result lifetime
///
/// The trait is intentionally unparameterized. [`build_result`] uses an
/// HRTB-on-method with `where Self: 'r` so the caller picks the result
/// lifetime, bounded by the source's own lifetime: a `'static` source admits
/// any `'r`, while a hypothetical source borrowing into the index (e.g.
/// `VectorScoreSource<'index>`) constrains `'r ⊆ 'index`. That source would
/// declare its own lifetime parameter on the concrete type and handle refresh
/// internally — none of that leaks into this trait.
///
/// [`TopKIterator`]: crate::TopKIterator
/// [`next_batch`]: ScoreSource::next_batch
/// [`next_batch_unfiltered`]: ScoreSource::next_batch_unfiltered
/// [`lookup_score`]: ScoreSource::lookup_score
/// [`build_result`]: ScoreSource::build_result
/// [`batch_strategy`]: ScoreSource::batch_strategy
/// [`adhoc_strategy`]: ScoreSource::adhoc_strategy
pub trait ScoreSource {
    /// The type of batch cursor this source produces.
    type Batch: ScoreBatch;

    /// Fetch the next score-ordered batch.
    ///
    /// Called repeatedly by [`TopKIterator`] in Batches mode.
    ///
    /// Returns:
    /// - `Ok(Some(batch))` — a new batch is available.
    /// - `Ok(None)` — the source is exhausted; no more batches.
    /// - `Err(RQEIteratorError::TimedOut)` — the query time limit was reached.
    ///
    /// [`TopKMode::Unfiltered`]: crate::TopKMode::Unfiltered
    /// [`TopKIterator`]: crate::TopKIterator
    fn next_batch(&mut self) -> Result<Option<Self::Batch>, RQEIteratorError>;

    /// Single-shot query returning all results directly, without a heap.
    ///
    /// Called exactly once by [`TopKIterator`] in [`Unfiltered`](crate::TopKMode::Unfiltered)
    /// mode. Implementations that can answer the full query in one call (e.g.
    /// `VecSimIndex_TopKQuery`) should override this; the default delegates to
    /// [`next_batch`](Self::next_batch).
    ///
    /// [`TopKIterator`]: crate::TopKIterator
    fn next_batch_unfiltered(&mut self) -> Result<Option<Self::Batch>, RQEIteratorError> {
        self.next_batch()
    }

    /// Return the score for `doc_id`, or `None` if the document is not in
    /// the source's index.
    ///
    /// Used in Adhoc-BF mode where the child iterator drives traversal.
    fn lookup_score(&mut self, doc_id: DocId) -> Option<f64>;

    /// Return an upper-bound estimate for the number of documents this source
    /// can produce.
    fn num_estimated(&self) -> usize;

    /// Rewind the source to its initial state so batch iteration can restart.
    fn rewind(&mut self);

    /// Build the [`RSIndexResult`] that the [`TopKIterator`] will yield for a
    /// given `(doc_id, score)` pair.
    ///
    /// `'r` is the caller-chosen lifetime of the returned result. The
    /// `where Self: 'r` bound forces `'r` to be no longer than the source
    /// itself, so any borrows the source might embed in the result stay
    /// valid. For `'static` sources, `'r` is unconstrained.
    ///
    /// [`TopKIterator`]: crate::TopKIterator
    fn build_result<'r>(&self, doc_id: DocId, score: f64) -> RSIndexResult<'r>
    where
        Self: 'r;

    /// Called after each batch (Batches mode only) to decide how collection
    /// should proceed.
    ///
    /// May update internal estimates as a side effect (e.g., refine child
    /// selectivity).
    ///
    /// Must **not** be called from Adhoc-BF mode.
    ///
    /// # Arguments
    ///
    /// - `heap_count` — number of results currently in the heap.
    /// - `k` — the target number of results.
    fn batch_strategy(&mut self, heap_count: usize, k: usize) -> BatchStrategy;

    /// Called after each adhoc lookup (Adhoc-BF mode only) to decide whether
    /// collection should continue.
    ///
    /// The default implementation always returns [`AdhocStrategy::Continue`]
    /// because child iterators yield documents in doc-ID order, not score
    /// order — early termination would produce an incorrect top-k.  Override
    /// to implement early-stop heuristics when the caller can guarantee
    /// correctness (e.g., after accumulating enough high-scoring results).
    ///
    /// Must **not** be called from Batches mode.
    ///
    /// # Arguments
    /// - `heap_count` — number of results currently in the heap.
    /// - `k` — the target number of results.
    fn adhoc_strategy(&mut self, _heap_count: usize, _k: usize) -> AdhocStrategy {
        // The child yields documents in doc-ID order, not score order, so we
        // must scan every match to guarantee a correct top-k — stopping when
        // the heap fills would freeze the answer at the first k child docs.
        // The bounded `TopKHeap` keeps the result set at k entries regardless.
        AdhocStrategy::Continue
    }

    /// The [`IteratorType`] that the wrapping [`TopKIterator`] should report.
    ///
    /// [`TopKIterator`]: crate::TopKIterator
    fn iterator_type(&self) -> IteratorType;
}
