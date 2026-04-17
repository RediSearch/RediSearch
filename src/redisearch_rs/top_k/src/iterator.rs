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
    traits::{CollectionStrategy, ScoreBatch, ScoreSource},
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
    /// Fetch score-ordered batches from the source and intersect each one
    /// with the child filter iterator.
    Batches,
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
    Yielding,
    /// Unfiltered path: yielding directly from `direct_batch` without a heap.
    YieldingDirect,
}

// ── TopKIterator ──────────────────────────────────────────────────────────────

/// A generic top-k iterator parameterized over a [`ScoreSource`].
///
/// Implements the two execution modes described in the design doc:
/// [`Unfiltered`](TopKMode::Unfiltered) and [`Batches`](TopKMode::Batches).
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
    /// The execution mode is inferred from `child`:
    /// - `None` → [`TopKMode::Unfiltered`]
    /// - `Some(_)` → [`TopKMode::Batches`]
    ///
    /// Use [`new_with_mode`](Self::new_with_mode) to override the initial mode.
    pub fn new(
        source: S,
        child: Option<Box<dyn RQEIterator<'index> + 'index>>,
        k: NonZeroUsize,
        compare: fn(f64, f64) -> Ordering,
    ) -> Self {
        let mode = if child.is_some() {
            TopKMode::Batches
        } else {
            TopKMode::Unfiltered
        };
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
            TopKMode::Batches => self.collect_batches(),
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

    // ── T4: Batches intersection engine ──────────────────────────────────────

    /// Collect results by intersecting score-ordered batches with the child filter.
    fn collect_batches(&mut self) -> Result<(), RQEIteratorError> {
        loop {
            let Some(mut batch) = self.source.next_batch()? else {
                break;
            };
            self.metrics.num_batches += 1;

            // Borrow-checker split: we can't hold `&mut self.child` and call
            // `self.heap.push` at the same time.  Pass fields explicitly.
            if let Some(child) = &mut self.child {
                intersect_batch_with_child(child, &mut batch, &mut self.heap, &mut self.metrics)?;
            }

            match self
                .source
                .collection_strategy(self.heap.len(), self.k.get())
            {
                CollectionStrategy::Continue => continue,
                CollectionStrategy::Stop => break,
                CollectionStrategy::SwitchToBatches => {
                    self.metrics.strategy_switches += 1;
                    // Clear the heap: the source restarts with new parameters
                    // (e.g. expanded numeric range) and will re-emit previously
                    // collected docs. Keeping stale entries would cause duplicates.
                    self.heap = TopKHeap::new(self.k, self.compare);
                    self.source.rewind();
                    if let Some(child) = &mut self.child {
                        child.rewind();
                    }
                    continue;
                }
            }
        }
        self.finalize_collection();
        Ok(())
    }

    // ── Shared finalization ───────────────────────────────────────────────────

    /// Drain the heap into `self.results` in best-first order and transition to
    /// the [`Yielding`](Phase::Yielding) phase.
    fn finalize_collection(&mut self) {
        // Replace heap with a fresh one; drain_sorted consumes the old one.
        let old_heap = std::mem::replace(&mut self.heap, TopKHeap::new(self.k, self.compare));
        self.results = old_heap.drain_sorted();
        self.yield_pos = 0;
        self.phase = Phase::Yielding;
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

// ── Free function: merge-join intersection ────────────────────────────────────

/// Intersect one score-ordered batch with a child filter iterator,
/// pushing matches into `heap`.
///
/// Uses a merge-join (alternating `skip_to` calls) to find matching doc IDs
/// in O((n + m) log k) time where n = batch size, m = child size, k = heap capacity.
///
/// The child is **rewound** at the start of each call.
fn intersect_batch_with_child<'index>(
    child: &mut Box<dyn RQEIterator<'index> + 'index>,
    batch: &mut impl ScoreBatch,
    heap: &mut TopKHeap,
    metrics: &mut TopKMetrics,
) -> Result<(), RQEIteratorError> {
    child.rewind();

    // Prime both cursors.
    let Some((mut batch_doc, mut batch_score)) = batch.next() else {
        return Ok(());
    };
    let Some(first) = child.read()? else {
        return Ok(());
    };
    let mut child_doc = first.doc_id;

    loop {
        metrics.total_comparisons += 1;
        match batch_doc.cmp(&child_doc) {
            Ordering::Equal => {
                heap.push(batch_doc, batch_score);
                // Advance both cursors.
                match batch.next() {
                    Some((d, s)) => {
                        batch_doc = d;
                        batch_score = s;
                    }
                    None => break,
                }
                match child.read()? {
                    Some(r) => child_doc = r.doc_id,
                    None => break,
                }
            }
            Ordering::Less => {
                // batch is behind child — skip batch forward to child_doc.
                match batch.skip_to(child_doc) {
                    Some((d, s)) => {
                        batch_doc = d;
                        batch_score = s;
                    }
                    None => break,
                }
            }
            Ordering::Greater => {
                // child is behind batch — skip child forward to batch_doc.
                match child.skip_to(batch_doc)? {
                    Some(SkipToOutcome::Found(r) | SkipToOutcome::NotFound(r)) => {
                        child_doc = r.doc_id;
                    }
                    None => break,
                }
            }
        }
    }
    Ok(())
}
