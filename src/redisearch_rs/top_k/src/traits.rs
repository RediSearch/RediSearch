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

use crate::heap::ScoredResult;

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

/// The score-producing half of a [`TopKIterator`].
///
/// A [`ScoreSource`] knows how to:
/// 1. Produce score-ordered batches ([`next_batch`]).
/// 2. Produce all results in a single shot ([`all_results_unfiltered_batch`]), used in Unfiltered mode.
/// 3. Look up the score for an individual document ([`lookup_score`]), used in Adhoc-BF mode.
/// 4. Build the final [`RSIndexResult`] for a `(doc_id, score)` pair ([`build_result`]).
/// 5. Decide, after each batch, whether to continue or switch strategy ([`batch_strategy`]).
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
/// [`all_results_unfiltered_batch`]: ScoreSource::all_results_unfiltered_batch
/// [`lookup_score`]: ScoreSource::lookup_score
/// [`build_result`]: ScoreSource::build_result
/// [`batch_strategy`]: ScoreSource::batch_strategy
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
    fn all_results_unfiltered_batch(&mut self) -> Result<Option<Self::Batch>, RQEIteratorError> {
        self.next_batch()
    }

    /// Return the score for `doc_id`, or `None` if the document is not in
    /// the source's index.
    ///
    /// Used in Adhoc-BF mode where the child iterator drives traversal.
    fn lookup_score(&mut self, doc_id: DocId) -> Option<f64>;

    /// Called by [`TopKIterator`] exactly once, before the first
    /// [`lookup_score`](Self::lookup_score) call in an adhoc scan.
    ///
    /// Implementations may acquire scan-scoped resources here (locks,
    /// preprocessed query contexts). The default is a no-op, so sources that
    /// have no scan-scoped state are unaffected.
    ///
    /// Pairs with [`end_adhoc`](Self::end_adhoc); the iterator guarantees
    /// `end_adhoc` runs on every exit path out of the adhoc scan loop,
    /// including error paths.
    ///
    /// [`TopKIterator`]: crate::TopKIterator
    fn begin_adhoc(&mut self) {}

    /// Called by [`TopKIterator`] exactly once, after the last
    /// [`lookup_score`](Self::lookup_score) call in an adhoc scan — including
    /// on error paths out of the scan loop.
    ///
    /// Implementations must release everything they acquired in
    /// [`begin_adhoc`](Self::begin_adhoc). The default is a no-op.
    ///
    /// [`TopKIterator`]: crate::TopKIterator
    fn end_adhoc(&mut self) {}

    /// Whether [`rerank`](Self::rerank) should run after an adhoc scan
    /// completes. The default is `false`, so sources with no rerank step pay
    /// nothing — [`TopKIterator`] skips draining and rebuilding the heap.
    ///
    /// [`TopKIterator`]: crate::TopKIterator
    fn should_rerank(&self) -> bool {
        false
    }

    /// Recompute the scores of the collected top-k results in place.
    ///
    /// [`TopKIterator`] calls this once, only when
    /// [`should_rerank`](Self::should_rerank) returns `true` and the heap is
    /// non-empty, after the adhoc scan finishes and before
    /// [`end_adhoc`](Self::end_adhoc) releases the scan resources. It is never
    /// called on a timed-out or otherwise aborted scan.
    ///
    /// The iterator rebuilds heap order from the updated scores afterward, so
    /// an implementation may overwrite scores freely without preserving the
    /// prior ordering. An implementation must leave the original score in place
    /// for any entry it cannot rescore; the default no-op is therefore always
    /// safe.
    ///
    /// [`TopKIterator`]: crate::TopKIterator
    fn rerank(&mut self, _results: &mut [ScoredResult]) {}

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

    /// Checked only in Adhoc-BF mode, returns `true` if the iteration needs to abort
    /// completely due to a timeout.
    fn adhoc_check_timeout(&mut self) -> bool;

    /// The [`IteratorType`] that the wrapping [`TopKIterator`] should report.
    ///
    /// [`TopKIterator`]: crate::TopKIterator
    fn iterator_type(&self) -> IteratorType;
}
