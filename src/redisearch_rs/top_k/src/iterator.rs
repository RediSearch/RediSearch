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
    traits::{AdhocStrategy, BatchStrategy, ScoreBatch, ScoreSource},
};

/// Determines which collection algorithm [`TopKIterator`] uses.
///
/// Selected at construction based on whether a child filter is present,
/// and may be switched mid-execution when the source decides a different
/// strategy is more efficient.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TopKMode {
    /// No child filter — stream directly from the source's single batch.
    /// The heap is bypassed entirely.
    ///
    /// # Invariants
    ///
    /// The [`ScoreSource`] used with this mode **must** produce at most one
    /// batch (i.e. the first [`ScoreSource::next_batch`] call returns the
    /// complete result set, and a second call would return `Ok(None)`).
    /// Any additional batches are not consumed and their results are silently
    /// lost.  In debug builds, [`TopKIterator`] asserts this invariant.
    Unfiltered,
    /// Fetch score-ordered batches from the source and intersect each one
    /// with the child filter iterator.
    Batches,
    /// Walk the child iterator and call [`ScoreSource::lookup_score`] for
    /// each document it yields.
    AdhocBF,
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

/// A generic top-k iterator parameterized over a [`ScoreSource`].
///
/// Implements the execution mode described in the design doc:
/// [`Unfiltered`](TopKMode::Unfiltered), [`Batches`](TopKMode::Batches),
/// and [`AdhocBF`](TopKMode::AdhocBF).
pub struct TopKIterator<
    'index,
    S: ScoreSource<'index>,
    C: RQEIterator<'index> + 'index = Box<dyn RQEIterator<'index> + 'index>,
> {
    source: S,
    child: Option<C>,
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
    /// Create a new unfiltered [`TopKIterator`] (no child filter).
    ///
    /// Results are streamed directly from the source's batch — the heap is bypassed.
    /// Use [`new`](Self::new) when a filter child is present.
    pub fn new_unfiltered(source: S, k: NonZeroUsize, compare: fn(f64, f64) -> Ordering) -> Self {
        Self::new_with_mode(source, None, k, compare, TopKMode::Unfiltered)
    }
}

impl<'index, S: ScoreSource<'index>, C: RQEIterator<'index> + 'index> TopKIterator<'index, S, C> {
    /// Create a new [`TopKIterator`] with a filter child.
    ///
    /// The initial mode defaults to [`TopKMode::Batches`].
    /// Use [`new_with_mode`](Self::new_with_mode) to start in [`TopKMode::AdhocBF`].
    pub fn new(source: S, child: C, k: NonZeroUsize, compare: fn(f64, f64) -> Ordering) -> Self {
        Self::new_with_mode(source, Some(child), k, compare, TopKMode::Batches)
    }

    /// Create a new [`TopKIterator`] with an explicit initial mode.
    ///
    /// Useful for tests and for constructors that want to start in
    /// [`AdhocBF`](TopKMode::AdhocBF) immediately.
    pub fn new_with_mode(
        source: S,
        child: Option<C>,
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

    /// Returns the current execution mode.
    pub fn mode(&self) -> TopKMode {
        self.mode
    }

    /// Returns a shared reference to the score source.
    pub fn source(&self) -> &S {
        &self.source
    }

    /// Returns a mutable reference to the score source.
    pub fn source_mut(&mut self) -> &mut S {
        &mut self.source
    }

    /// Returns a reference to the filter child iterator, if present.
    pub fn child(&self) -> Option<&C> {
        self.child.as_ref()
    }

    /// Returns a reference to the diagnostic counters accumulated so far.
    pub const fn metrics(&self) -> &TopKMetrics {
        &self.metrics
    }

    /// Drive collection based on the current mode.
    fn collect(&mut self) -> Result<(), RQEIteratorError> {
        self.phase = Phase::Collecting;
        let result = match self.mode {
            TopKMode::Unfiltered => self.prepare_unfiltered_direct(),
            TopKMode::Batches => self.collect_batches(),
            TopKMode::AdhocBF => self.collect_adhoc(),
        };
        if result.is_err() {
            // Reset so a retry via read() works: Phase::Collecting has no handler there.
            // TODO: MOD-14209: bubble up errors
            self.phase = Phase::NotStarted;
        }
        result
    }

    /// Set up the unfiltered direct-yield path.
    ///
    /// Calls [`ScoreSource::next_batch_unfiltered`] exactly once. Results are streamed
    /// directly from the batch cursor — no heap is involved.
    ///
    /// # Invariants
    ///
    /// [`TopKMode::Unfiltered`] requires the source to produce at most one
    /// batch.  In debug builds this method calls [`ScoreSource::next_batch_unfiltered`] a
    /// second time and panics if another batch is returned, catching
    /// misbehaving implementations early.
    fn prepare_unfiltered_direct(&mut self) -> Result<(), RQEIteratorError> {
        self.direct_batch = self.source.next_batch_unfiltered()?;
        if self.direct_batch.is_none() {
            self.at_eof = true;
        }
        debug_assert!(
            matches!(self.source.next_batch_unfiltered(), Ok(None)),
            "ScoreSource did not return Ok(None) in TopKMode::Unfiltered \
             (extra batch or error); use a batched mode instead"
        );
        self.phase = Phase::YieldingDirect;
        Ok(())
    }

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
            match self.source.batch_strategy(self.heap.len(), self.k.get()) {
                BatchStrategy::Continue => continue,
                BatchStrategy::Stop => break,
                BatchStrategy::SwitchToAdhoc => {
                    self.metrics.strategy_switches += 1;
                    self.mode = TopKMode::AdhocBF;
                    // Fall through to adhoc collection; heap is preserved.
                    self.collect_adhoc()?;
                    return Ok(());
                }
                BatchStrategy::SwitchToBatches => {
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

    /// Collect results by walking the child iterator and calling
    /// [`ScoreSource::lookup_score`] for each document.
    fn collect_adhoc(&mut self) -> Result<(), RQEIteratorError> {
        let child = self
            .child
            .as_mut()
            .expect("AdhocBF mode requires a child iterator");
        child.rewind();

        loop {
            let Some(result) = child.read()? else {
                break;
            };
            let doc_id = result.doc_id;

            // `self.child` is now free (we only needed `doc_id`).
            if let Some(score) = self.source.lookup_score(doc_id) {
                self.heap.push(doc_id, score);
            }
            if self.source.adhoc_strategy(self.heap.len(), self.k.get()) == AdhocStrategy::Stop {
                break;
            }
        }
        self.finalize_collection();
        Ok(())
    }

    /// Drain the heap into `self.results` in best-first order and transition to
    /// the [`Yielding`](Phase::Yielding) phase.
    fn finalize_collection(&mut self) {
        // Replace heap with a fresh one; drain_sorted consumes the old one.
        let old_heap = std::mem::replace(&mut self.heap, TopKHeap::new(self.k, self.compare));
        self.results = old_heap.drain_sorted();
        self.yield_pos = 0;
        self.phase = Phase::Yielding;
    }

    /// Yield the next result from the unfiltered direct batch.
    fn advance_unfiltered_direct(
        &mut self,
    ) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        let item = self.direct_batch.as_mut().and_then(S::Batch::next);

        match item {
            Some((doc_id, score)) => {
                let result = self.source.build_result(doc_id, score);
                self.current = Some(result);
                self.last_doc_id = doc_id;
                Ok(self.current.as_mut())
            }
            None => {
                self.at_eof = true;
                self.current = None;
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

impl<'index, S: ScoreSource<'index>, C: RQEIterator<'index> + 'index> RQEIterator<'index>
    for TopKIterator<'index, S, C>
{
    #[inline(always)]
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        self.current.as_mut()
    }

    #[inline(always)]
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

    #[inline(always)]
    unsafe fn revalidate(
        &mut self,
        spec: std::ptr::NonNull<ffi::IndexSpec>,
    ) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        if let Some(child) = &mut self.child {
            // SAFETY: Delegating to child with the same `spec` passed by our caller.
            return unsafe { child.revalidate(spec) };
        }
        Ok(RQEValidateStatus::Ok)
    }

    #[inline(always)]
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
    }

    #[inline(always)]
    fn num_estimated(&self) -> usize {
        self.k.get().min(self.source.num_estimated())
    }

    #[inline(always)]
    fn last_doc_id(&self) -> t_docId {
        self.last_doc_id
    }

    #[inline(always)]
    fn at_eof(&self) -> bool {
        self.at_eof
    }

    #[inline(always)]
    fn type_(&self) -> IteratorType {
        self.source.iterator_type()
    }

    #[inline(always)]
    fn intersection_sort_weight(&self, _: bool) -> f64 {
        1.0
    }
}

/// Intersect one score-ordered batch with a child filter iterator,
/// pushing matches into `heap`.
///
/// Uses a merge-join (alternating `skip_to` calls) to find matching doc IDs
/// in O((n + m) log k) time where n = batch size, m = child size, k = heap capacity.
///
/// The child is **rewound** at the start of each call.
fn intersect_batch_with_child<'index, C: RQEIterator<'index>>(
    child: &mut C,
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

impl<'index, S: ScoreSource<'index> + 'index> rqe_iterators::interop::ProfileChildren<'index>
    for TopKIterator<'index, S, rqe_iterators::c2rust::CRQEIterator>
{
    fn profile_children(self) -> Self {
        TopKIterator {
            child: self
                .child
                .map(rqe_iterators::c2rust::CRQEIterator::into_profiled),
            source: self.source,
            mode: self.mode,
            initial_mode: self.initial_mode,
            heap: self.heap,
            direct_batch: self.direct_batch,
            k: self.k,
            compare: self.compare,
            phase: self.phase,
            results: self.results,
            yield_pos: self.yield_pos,
            current: self.current,
            last_doc_id: self.last_doc_id,
            at_eof: self.at_eof,
            metrics: self.metrics,
        }
    }
}
