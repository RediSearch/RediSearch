/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! [`TopKIterator`] — the generic top-k state machine.

use std::{cmp::Ordering, num::NonZeroUsize};

use ffi::t_docId;
use inverted_index::RSIndexResult;
use rqe_iterator_type::IteratorType;
use rqe_iterators::{RQEIterator, RQEIteratorError, RQEValidateStatus, SkipToOutcome};

use crate::{
    heap::{ScoredResult, TopKHeap},
    traits::{ScoreBatch, ScoreSource},
};

// ── Public enums ─────────────────────────────────────────────────────────────

/// Determines which collection algorithm [`TopKIterator`] uses.
///
/// Selected at construction based on whether a child filter is present,
/// and may be switched mid-execution when the source decides a different
/// strategy is more efficient.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TopKMode {
    /// No child filter — stream directly from the source's single batch.
    /// The heap is bypassed entirely.
    Unfiltered,
}

/// Diagnostic counters collected during a single evaluation.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct TopKMetrics {
    /// Number of batches fetched from the source (Batches mode only).
    pub num_batches: usize,
    /// Number of times the collection strategy was switched.
    pub strategy_switches: usize,
    /// Number of (batch_doc, child_doc) comparisons performed during
    /// merge-join intersection (Batches mode only).
    pub total_comparisons: usize,
}

// ── Private phase enum ────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    /// Collection has not started; the first call to [`read`](TopKIterator::read)
    /// will trigger it.
    NotStarted,
    /// Actively collecting results (transient; only visible inside collection methods).
    Collecting,
    /// Collection is done; yielding results from `results` in order.
    #[expect(
        dead_code,
        reason = "TopKMode::Unfiltered is without child so no yielding necessary"
    )]
    Yielding,
    /// Unfiltered path: yielding directly from `direct_batch` without a heap.
    YieldingDirect,
}

// ── TopKIterator ──────────────────────────────────────────────────────────────

/// A generic top-k iterator parameterized over a [`ScoreSource`].
///
/// Implements the execution mode described in the design doc:
/// [`Unfiltered`](TopKMode::Unfiltered).
pub struct TopKIterator<'index, S: ScoreSource<'index>> {
    source: S,
    child: Option<Box<dyn RQEIterator<'index> + 'index>>,
    mode: TopKMode,
    /// Preserved so [`rewind`](Self::rewind) can restore the original mode.
    initial_mode: TopKMode,
    heap: TopKHeap,
    /// Holds the in-progress batch for the Unfiltered path.
    direct_batch: Option<S::Batch>,
    k: NonZeroUsize,
    compare: fn(f64, f64) -> Ordering,
    phase: Phase,
    /// Heap contents drained into score order for yielding.
    results: Vec<ScoredResult>,
    yield_pos: usize,
    current: Option<RSIndexResult<'index>>,
    last_doc_id: t_docId,
    at_eof: bool,
    /// Diagnostic counters — not reset on [`rewind`](Self::rewind).
    pub metrics: TopKMetrics,
}

impl<'index, S: ScoreSource<'index>> TopKIterator<'index, S> {
    /// Create a new [`TopKIterator`].
    ///
    /// The execution mode is inferred from `child`.
    ///
    /// For now, only [`TopKMode::Unfiltered`] exists.
    pub fn new(
        source: S,
        child: Option<Box<dyn RQEIterator<'index> + 'index>>,
        k: NonZeroUsize,
        compare: fn(f64, f64) -> Ordering,
    ) -> Self {
        let mode = TopKMode::Unfiltered;
        Self::new_with_mode(source, child, k, compare, mode)
    }

    /// Create a new [`TopKIterator`] with an explicit initial mode.
    pub fn new_with_mode(
        source: S,
        child: Option<Box<dyn RQEIterator<'index> + 'index>>,
        k: NonZeroUsize,
        compare: fn(f64, f64) -> Ordering,
        mode: TopKMode,
    ) -> Self {
        Self {
            heap: TopKHeap::new(k, compare),
            source,
            child,
            mode,
            initial_mode: mode,
            direct_batch: None,
            k,
            compare,
            phase: Phase::NotStarted,
            results: Vec::new(),
            yield_pos: 0,
            current: None,
            last_doc_id: 0,
            at_eof: false,
            metrics: TopKMetrics::default(),
        }
    }

    /// Returns a reference to the diagnostic counters accumulated so far.
    pub const fn metrics(&self) -> &TopKMetrics {
        &self.metrics
    }

    // ── Collection dispatch ───────────────────────────────────────────────────

    /// Drive collection based on the current mode.
    fn collect(&mut self) -> Result<(), RQEIteratorError> {
        self.phase = Phase::Collecting;
        match self.mode {
            TopKMode::Unfiltered => self.prepare_unfiltered_direct(),
        }
    }

    // ── Unfiltered path ───────────────────────────────────────────────────────

    /// Set up the unfiltered direct-yield path.
    ///
    /// Calls [`ScoreSource::next_batch`] exactly once.  Results are streamed
    /// directly from the batch cursor — no heap is involved.
    fn prepare_unfiltered_direct(&mut self) -> Result<(), RQEIteratorError> {
        self.direct_batch = self.source.next_batch()?;
        if self.direct_batch.is_none() {
            self.at_eof = true;
        }
        self.phase = Phase::YieldingDirect;
        Ok(())
    }

    // ── Yielding helpers ─────────────────────────────────────────────────────

    /// Yield the next result from the unfiltered direct batch.
    fn advance_unfiltered_direct(
        &mut self,
    ) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        let item = if let Some(batch) = &mut self.direct_batch {
            batch.next()
        } else {
            None
        };

        match item {
            Some((doc_id, score)) => {
                let result = self.source.build_result(doc_id, score);
                self.current = Some(result);
                self.last_doc_id = doc_id;
                Ok(self.current.as_mut())
            }
            None => {
                self.at_eof = true;
                Ok(None)
            }
        }
    }

    /// Yield the next result from the pre-sorted `results` vec.
    fn advance_from_results(
        &mut self,
    ) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        if self.yield_pos >= self.results.len() {
            self.at_eof = true;
            return Ok(None);
        }
        let ScoredResult { doc_id, score } = self.results[self.yield_pos];
        self.yield_pos += 1;
        let result = self.source.build_result(doc_id, score);
        self.current = Some(result);
        self.last_doc_id = doc_id;
        Ok(self.current.as_mut())
    }
}

// ── RQEIterator impl ──────────────────────────────────────────────────────────

impl<'index, S: ScoreSource<'index>> RQEIterator<'index> for TopKIterator<'index, S> {
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        self.current.as_mut()
    }

    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        if self.at_eof {
            return Ok(None);
        }

        if self.phase == Phase::NotStarted {
            self.collect()?;
        }

        match self.phase {
            Phase::YieldingDirect => self.advance_unfiltered_direct(),
            Phase::Yielding => self.advance_from_results(),
            Phase::NotStarted | Phase::Collecting => {
                unreachable!("collect() must set phase to YieldingDirect or Yielding")
            }
        }
    }

    fn skip_to(
        &mut self,
        _doc_id: t_docId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        // TopKIterator is a root-only iterator that yields results sorted by
        // score, not by doc_id.  It cannot be used as a child in a larger
        // iterator tree, so skip_to is unsupported.
        unimplemented!("TopKIterator is a root-only iterator; skip_to is not supported")
    }

    fn revalidate(&mut self) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        if let Some(child) = &mut self.child {
            return child.revalidate();
        }
        Ok(RQEValidateStatus::Ok)
    }

    fn rewind(&mut self) {
        self.source.rewind();
        if let Some(child) = &mut self.child {
            child.rewind();
        }
        self.mode = self.initial_mode;
        self.heap = TopKHeap::new(self.k, self.compare);
        self.direct_batch = None;
        self.results.clear();
        self.yield_pos = 0;
        self.current = None;
        self.last_doc_id = 0;
        self.at_eof = false;
        self.phase = Phase::NotStarted;
        // We explicitly do NOT reset self.metrics because they're used for diagnostics.
    }

    fn num_estimated(&self) -> usize {
        self.k.get().min(self.source.num_estimated())
    }

    fn last_doc_id(&self) -> t_docId {
        self.last_doc_id
    }

    fn at_eof(&self) -> bool {
        self.at_eof
    }

    fn type_(&self) -> IteratorType {
        self.source.iterator_type()
    }

    fn intersection_sort_weight(&self, _: bool) -> f64 {
        1.0
    }
}
