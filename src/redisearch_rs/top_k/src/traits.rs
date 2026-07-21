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
/// A forward iterator over a single score-ordered batch of `(doc_id, score)` pairs.
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
    /// Advance to the next, disjoint window of the source.
    ///
    /// The source has already re-resolved itself to a new window whose
    /// documents do not overlap any previously emitted window, so the running
    /// heap stays valid and keeps accumulating the global top `k`. The iterator
    /// rewinds **only the child** and keeps collecting; it does **not** clear
    /// the heap or rewind the source.
    ExpandWindow,
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
    /// The type of batch iterator this source produces.
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

    /// Whether `result` is no longer valid and must not be surfaced
    /// (e.g. its field has expired).
    ///
    /// Queried on the result about to be yielded, so the answer must reflect
    /// the document's current state rather than state captured during
    /// collection. The default never expires.
    fn is_expired(&self, _result: &RSIndexResult) -> bool {
        false
    }

    /// Called at the start of an adhoc scan, before any
    /// [`lookup_score`](Self::lookup_score) call in that scan.
    ///
    /// Implementations may acquire scan-scoped resources here (locks,
    /// preprocessed query contexts). The default is a no-op, so sources that
    /// have no scan-scoped state are unaffected.
    ///
    /// Brackets the scan with [`end_adhoc`](Self::end_adhoc): each
    /// [`begin_adhoc`](Self::begin_adhoc) is followed by one
    /// [`end_adhoc`](Self::end_adhoc), which the iterator guarantees runs on
    /// every exit path out of the scan loop, including error paths.
    /// Implementations should rely on this pairing rather than on how many
    /// scans the iterator performs.
    fn begin_adhoc(&mut self) {}

    /// Called by [`TopKIterator`] after the last
    /// [`lookup_score`](Self::lookup_score) call in an adhoc scan — including
    /// on error paths out of the scan loop.
    ///
    /// Implementations must release everything they acquired in
    /// [`begin_adhoc`](Self::begin_adhoc). The default is a no-op.
    ///
    /// Must be idempotent: calling it more than once, or without a preceding
    /// [`begin_adhoc`](Self::begin_adhoc), must be safe and have no effect
    /// beyond the first release.
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
    /// Used in [`Unfiltered`](crate::TopKMode::Unfiltered) mode where there is
    /// no child iterator: the source must produce a complete result on its own.
    ///
    /// In filtered modes ([`Batches`](crate::TopKMode::Batches),
    /// [`AdhocBF`](crate::TopKMode::AdhocBF)) the iterator yields the **child's**
    /// `RSIndexResult` directly so its scoring inputs (frequency, field mask,
    /// term records) are preserved, and calls
    /// [`attach_score_metric`](Self::attach_score_metric) instead.
    ///
    /// [`TopKIterator`]: crate::TopKIterator
    fn build_result<'r>(&self, doc_id: DocId, score: f64) -> RSIndexResult<'r>
    where
        Self: 'r;

    /// Attach this source's score to the child's result as a metric entry.
    ///
    /// Called by [`TopKIterator`] in the filtered yield path: the child's
    /// `RSIndexResult` is what the relevance scorer will see (so BM25/TFIDF
    /// can recurse into the term records), and the source's score (e.g. a
    /// vector distance) is exposed via the metrics channel for output fields
    /// like `__v_score`.
    ///
    /// Implementations that maintain a stable score key should overwrite an
    /// existing entry with the same key rather than appending, so repeated
    /// yields of the same child storage don't leak metrics across docs.
    ///
    /// [`TopKIterator`]: crate::TopKIterator
    fn attach_score_metric<'r>(&self, _result: &mut RSIndexResult<'r>, _score: f64)
    where
        Self: 'r;

    /// Whether the filtered yield path should hand back the child's captured
    /// `RSIndexResult` (with [`attach_score_metric`](Self::attach_score_metric)
    /// applied) rather than a source-built one.
    ///
    /// `true` (the default) preserves the child's scoring inputs for relevance
    /// scoring — the hybrid/vector case. A source whose score *is* the ordering
    /// key and needs no child scoring inputs (e.g. a numeric `SORTBY`) returns
    /// `false`, so the iterator yields [`build_result`](Self::build_result) even
    /// when a filter child is present.
    fn yields_child_record(&self) -> bool {
        true
    }

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

    /// Poll the query deadline, returning [`RQEIteratorError::TimedOut`] once
    /// it has been reached.
    fn check_timeout(&mut self) -> Result<(), RQEIteratorError>;

    /// The [`IteratorType`] that the wrapping [`TopKIterator`] should report.
    ///
    /// [`TopKIterator`]: crate::TopKIterator
    fn iterator_type(&self) -> IteratorType;
}
