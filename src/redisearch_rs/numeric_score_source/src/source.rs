/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! [`NumericScoreSource`] — [`ScoreSource`] implementation backed by a numeric
//! [`RQEIterator`].

use std::marker::PhantomData;

use index_result::{RSIndexResult, RSResultKind};
use rqe_core::DocId;
use rqe_iterator_type::IteratorType;
use rqe_iterators::{RQEIterator, RQEIteratorError, inverted_index::Numeric, metric::Metric};
use top_k::{BatchStrategy, ScoreSource};

use crate::score_batch::NumericScoreBatch;

/// An [`RQEIterator`] whose records are guaranteed to be numeric.
///
/// [`NumericScoreSource`] is generic over its source iterator, but reads each
/// record's score via [`as_numeric_unchecked`](RSIndexResult::as_numeric_unchecked),
/// which is undefined behavior on a non-numeric record. This trait restricts the
/// source to iterators that uphold that guarantee.
///
/// # Safety
///
/// Implementers must guarantee that every record produced by
/// [`read`](RQEIterator::read) is numeric — its [`kind`](RSIndexResult::kind) is
/// either [`RSResultKind::Numeric`] or [`RSResultKind::Metric`].
pub unsafe trait NumericRecords<'index>: RQEIterator<'index> {}

// SAFETY: every record is built via `RSIndexResult::build_metric`, so its kind is
// always `Metric`.
unsafe impl<'index, const SORTED_BY_ID: bool> NumericRecords<'index>
    for Metric<'index, SORTED_BY_ID>
{
}

// SAFETY: the wrapped reader decodes into a result seeded with
// `RSIndexResult::build_numeric`, so its kind is always `Numeric`.
unsafe impl<'index, R, E> NumericRecords<'index> for Numeric<'index, R, E> where
    Self: RQEIterator<'index>
{
}

/// Default per-batch record cap for [`next_batch`](ScoreSource::next_batch).
///
/// At 16 bytes per `(DocId, f64)` pair this caps a batch at ~64 KiB. It is large
/// enough to amortize the per-batch child rewind in `intersect_batch_with_child`
/// (the child is rewound and merge-joined once per batch) yet small enough that a
/// filtered top-k query over a huge numeric field no longer allocates one entry
/// per record.
const MATERIALIZE_BATCH_SIZE: usize = 4096;

/// Scores each document by a numeric field, for top-k queries like
/// `SORTBY <numeric field>`.
///
/// A document's score is the numeric value read from the wrapped `source`
/// iterator, so taking the top `k` here means taking the `k` highest values of
/// that field.
///
/// The wrapped source yields records in doc-id order, not score order, so the
/// surrounding [`TopKIterator`] does the top-k selection with its heap. To keep
/// peak memory bounded, [`next_batch`](ScoreSource::next_batch) drains the source
/// in chunks of at most `batch_size` records rather than materializing the whole
/// index at once; the iterator pushes each chunk through the heap (intersecting
/// with the filter child first, when present) and keeps the running top `k`.
///
/// Both the filtered and unfiltered numeric top-k iterators run in
/// [`TopKMode::Batches`], so every query path flows through
/// [`next_batch`](ScoreSource::next_batch) and the heap — there is no
/// heap-bypassing single-batch path here, and bounded batches keep memory
/// bounded in both cases.
///
/// [`TopKMode::Batches`]: top_k::TopKMode::Batches
///
/// Adhoc-BF strategy has not been implemented
///
/// Not implemented yet:
/// - TODO: MOD-14207 Proper batching logic through numeric filter
/// - TODO: MOD-14945 Timeout handling
/// - TODO: MOD-14946 Profile metrics
/// - TODO: MOD-14947 Parity tests
/// - TODO: MOD-14948 Performance validation
///
/// [`TopKIterator`]: top_k::TopKIterator
pub struct NumericScoreSource<'index, S: NumericRecords<'index>> {
    /// Numeric iterator whose per-document value is used as the score.
    source: S,
    /// Maximum number of records [`next_batch`](ScoreSource::next_batch) reads
    /// into a single batch. Always `>= 1`.
    batch_size: usize,
    /// Upper-bound result estimate captured at construction, so it stays stable
    /// after the source is drained.
    num_estimated: usize,
    _index: PhantomData<&'index ()>,
}

impl<'index, S: NumericRecords<'index>> NumericScoreSource<'index, S> {
    /// Wrap a numeric `source` iterator as a score source, using the default
    /// `MATERIALIZE_BATCH_SIZE` per-batch cap.
    pub fn new(source: S) -> Self {
        Self::with_batch_size(source, MATERIALIZE_BATCH_SIZE)
    }

    /// Like [`new`](Self::new) but with an explicit per-batch record cap, so
    /// callers (chiefly tests) can force multi-batch behavior on small inputs.
    /// `batch_size` is clamped to at least `1`.
    pub fn with_batch_size(source: S, batch_size: usize) -> Self {
        let num_estimated = source.num_estimated();
        Self {
            source,
            batch_size: batch_size.max(1),
            num_estimated,
            _index: PhantomData,
        }
    }

    /// Drain up to [`batch_size`](Self::batch_size) records from the source into a
    /// doc-id-ordered batch, so a top-k query never materializes the whole numeric
    /// index at once. Returns `Ok(None)` once the source is exhausted.
    fn read_batch(&mut self) -> Result<Option<NumericScoreBatch>, RQEIteratorError> {
        let mut items = Vec::with_capacity(self.batch_size.min(self.num_estimated));
        while items.len() < self.batch_size {
            let Some(result) = self.source.read()? else {
                break;
            };
            debug_assert!(
                matches!(result.kind(), RSResultKind::Numeric | RSResultKind::Metric),
                "NumericScoreSource: wrapped iterator yielded a non-numeric record ({})",
                result.kind()
            );
            // SAFETY: the `S: NumericRecords` bound guarantees every record read
            // from `source` is numeric.
            let score = unsafe { result.as_numeric_unchecked() };
            items.push((result.doc_id, score));
        }

        if items.is_empty() {
            return Ok(None);
        }
        Ok(Some(NumericScoreBatch::new(items)))
    }
}

impl<'index, S: NumericRecords<'index>> ScoreSource for NumericScoreSource<'index, S> {
    type Batch = NumericScoreBatch;

    fn next_batch(&mut self) -> Result<Option<Self::Batch>, RQEIteratorError> {
        self.read_batch()
    }

    fn lookup_score(&mut self, _doc_id: DocId) -> Option<f64> {
        // Adhoc-BF is not used by the numeric path.
        None
    }

    fn num_estimated(&self) -> usize {
        self.num_estimated
    }

    fn rewind(&mut self) {
        self.source.rewind();
    }

    fn build_result<'r>(&self, doc_id: DocId, score: f64) -> RSIndexResult<'r>
    where
        Self: 'r,
    {
        RSIndexResult::build_numeric(score).doc_id(doc_id).build()
    }

    fn batch_strategy(&mut self, _heap_count: usize, _k: usize) -> BatchStrategy {
        // Records arrive in doc-id order, so a batch is an arbitrary doc-id slice,
        // not a score bracket: a higher score may still appear in a later batch.
        // The heap can therefore only know the true top-k once every record has
        // been offered, so always keep pulling until the source reaches EOF
        // (`next_batch` returns `Ok(None)`). Unlike the vector source — whose
        // batches are successive score brackets — we must never `Stop` early on a
        // full heap, or we would drop higher-scored documents from later batches.
        BatchStrategy::Continue
    }

    fn iterator_type(&self) -> IteratorType {
        IteratorType::Optimus
    }

    fn check_timeout(&mut self) -> Result<(), RQEIteratorError> {
        // TODO: MOD-14945 — real timeout handling. The numeric source never
        // times out yet.
        Ok(())
    }
}
