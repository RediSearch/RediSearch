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

/// Scores each document by a numeric field, for top-k queries like
/// `SORTBY <numeric field>`.
///
/// A document's score is the numeric value read from the wrapped `source`
/// iterator, so taking the top `k` here means taking the `k` highest values of
/// that field.
///
/// [`next_batch`](ScoreSource::next_batch) reads the whole source into one
/// batch, ordered by doc id; the surrounding [`TopKIterator`] then picks the top
/// `k` with its heap.
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
    /// Whether [`next_batch`](ScoreSource::next_batch) has already emitted its
    /// single batch. Reset by [`rewind`](ScoreSource::rewind).
    drained: bool,
    /// Upper-bound result estimate captured at construction, so it stays stable
    /// after the source is drained.
    num_estimated: usize,
    _index: PhantomData<&'index ()>,
}

impl<'index, S: NumericRecords<'index>> NumericScoreSource<'index, S> {
    /// Wrap a numeric `source` iterator as a score source.
    pub fn new(source: S) -> Self {
        let num_estimated = source.num_estimated();
        Self {
            source,
            drained: false,
            num_estimated,
            _index: PhantomData,
        }
    }
}

impl<'index, S: NumericRecords<'index>> ScoreSource for NumericScoreSource<'index, S> {
    type Batch = NumericScoreBatch;

    fn next_batch(&mut self) -> Result<Option<Self::Batch>, RQEIteratorError> {
        if self.drained {
            return Ok(None);
        }

        let mut items = Vec::new();
        while let Some(result) = self.source.read()? {
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
        self.drained = true;

        if items.is_empty() {
            return Ok(None);
        }
        Ok(Some(NumericScoreBatch::new(items)))
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
        self.drained = false;
    }

    fn build_result<'r>(&self, doc_id: DocId, score: f64) -> RSIndexResult<'r>
    where
        Self: 'r,
    {
        RSIndexResult::build_numeric(score).doc_id(doc_id).build()
    }

    fn batch_strategy(&mut self, heap_count: usize, k: usize) -> BatchStrategy {
        if heap_count >= k {
            BatchStrategy::Stop
        } else {
            BatchStrategy::Continue
        }
    }

    fn iterator_type(&self) -> IteratorType {
        IteratorType::Optimus
    }
}
