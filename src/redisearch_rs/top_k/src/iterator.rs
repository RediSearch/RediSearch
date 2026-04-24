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

use index_result::RSIndexResult;
use index_spec::IndexSpecReadGuard;
use rqe_core::DocId;
use rqe_iterator_type::IteratorType;
use rqe_iterators::{RQEIterator, RQEIteratorError, RQEValidateStatus, SkipToOutcome};

use crate::{
    heap::{ScoredResult, TopKHeap},
    traits::{BatchStrategy, ScoreBatch, ScoreSource},
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
    ///
    /// The source's [`ScoreSource::batch_strategy`] may return
    /// [`BatchStrategy::SwitchToAdhoc`] mid-run to switch to
    /// [`AdhocBF`](TopKMode::AdhocBF) when the source considers it more
    /// efficient.  Use [`ForcedBatches`](TopKMode::ForcedBatches) to suppress
    /// that switch.
    Batches,
    /// Like [`Batches`](TopKMode::Batches), but [`BatchStrategy::SwitchToAdhoc`]
    /// from the source is ignored and treated as [`BatchStrategy::Continue`].
    ForcedBatches,
    /// Walk the child iterator and call [`ScoreSource::lookup_score`] for
    /// each document it yields.
    ///
    /// `BF` stands for "Brute Force": for the lookup, opposed to score-ordered batches.
    AdhocBF,
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
pub struct TopKIterator<'index, S: ScoreSource> {
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
    last_doc_id: DocId,
    at_eof: bool,
}

impl<'index, S: ScoreSource + 'index> TopKIterator<'index, S> {
    /// Create a new [`TopKIterator`].
    ///
    /// The execution mode is inferred from `child`:
    /// - `None` → [`TopKMode::Unfiltered`]
    /// - `Some(_)` → [`TopKMode::Batches`]
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
        Self::_new_with_mode(source, child, k, compare, mode)
    }

    /// Create a new [`TopKIterator`] with an explicit initial mode.
    ///
    /// Useful for tests and for constructors that want to start in
    /// [`AdhocBF`](TopKMode::AdhocBF) immediately.
    #[cfg(feature = "test-utils")]
    pub fn new_with_mode(
        source: S,
        child: Option<Box<dyn RQEIterator<'index> + 'index>>,
        k: NonZeroUsize,
        compare: fn(f64, f64) -> Ordering,
        mode: TopKMode,
    ) -> Self {
        Self::_new_with_mode(source, child, k, compare, mode)
    }

    /// Create a new [`TopKIterator`] with an explicit initial mode.
    fn _new_with_mode(
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

    /// Drive collection based on the current mode.
    fn collect(&mut self) -> Result<(), RQEIteratorError> {
        self.phase = Phase::Collecting;
        let result = match self.mode {
            TopKMode::Unfiltered => self.prepare_unfiltered_direct(),
            TopKMode::Batches | TopKMode::ForcedBatches => self.collect_batches(),
            TopKMode::AdhocBF => self.collect_adhoc(),
        };
        if result.is_err() {
            // Reset so a retry via read() works: Phase::Collecting has no handler there.
            // TODO: MOD-14209: bubble up errors
            self.phase = Phase::NotStarted;
            self.mode = self.initial_mode;
        }
        result
    }

    /// Set up the unfiltered direct-yield path.
    ///
    /// Calls [`ScoreSource::all_results_unfiltered_batch`] exactly once. Results are streamed
    /// directly from the batch cursor — no heap is involved.
    ///
    /// # Invariants
    ///
    /// [`TopKMode::Unfiltered`] requires the source to produce at most one
    /// batch.  In debug builds this method calls [`ScoreSource::all_results_unfiltered_batch`] a
    /// second time and panics if another batch is returned, catching
    /// misbehaving implementations early.
    fn prepare_unfiltered_direct(&mut self) -> Result<(), RQEIteratorError> {
        self.direct_batch = self.source.all_results_unfiltered_batch()?;
        if self.direct_batch.is_none() {
            self.at_eof = true;
        }
        debug_assert!(
            matches!(self.source.all_results_unfiltered_batch(), Ok(None)),
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

            // Borrow-checker split: we can't hold `&mut self.child` and call
            // `self.heap.push` at the same time.  Pass fields explicitly.
            if let Some(child) = &mut self.child {
                intersect_batch_with_child(child, &mut batch, &mut self.heap)?;
            }
            match self.source.batch_strategy(self.heap.len(), self.k.get()) {
                BatchStrategy::Continue => continue,
                BatchStrategy::Stop => break,
                BatchStrategy::SwitchToAdhoc => {
                    if self.mode == TopKMode::ForcedBatches {
                        // Honour the forced-batches contract: never switch
                        // mid-run.
                        continue;
                    }
                    self.mode = TopKMode::AdhocBF;
                    // Clear the heap: collect_adhoc rewinds the child and
                    // rescans every match from scratch, so batch-phase entries
                    // are redundant. Keeping them would re-admit the same doc id
                    // (TopKHeap::push only de-dups against the worst element).
                    self.heap = TopKHeap::new(self.k, self.compare);
                    self.collect_adhoc()?;
                    return Ok(());
                }
                BatchStrategy::SwitchToBatches => {
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

            if let Some(score) = self.source.lookup_score(doc_id) {
                self.heap.push(doc_id, score);
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

impl<'index, S: ScoreSource + 'index> RQEIterator<'index> for TopKIterator<'index, S> {
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
        _doc_id: DocId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        // TopKIterator is a root-only iterator that yields results sorted by
        // score, not by doc_id.  It cannot be used as a child in a larger
        // iterator tree, so skip_to is unsupported.
        unimplemented!("TopKIterator is a root-only iterator; skip_to is not supported")
    }

    #[inline(always)]
    fn revalidate(
        &mut self,
        spec: &IndexSpecReadGuard,
    ) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        if let Some(child) = &mut self.child {
            return child.revalidate(spec);
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
    fn last_doc_id(&self) -> DocId {
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
/// Uses a merge-join (alternating `skip_to` calls) to find matching doc IDs.
///
/// The child is **rewound** at the start of each call.
fn intersect_batch_with_child<'index>(
    child: &mut Box<dyn RQEIterator<'index> + 'index>,
    batch: &mut impl ScoreBatch,
    heap: &mut TopKHeap,
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
