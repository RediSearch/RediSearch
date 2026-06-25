/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! [`TopKIterator`] — the generic top-k state machine.

use std::{cmp::Ordering, mem::ManuallyDrop, num::NonZeroUsize};

use index_result::RSIndexResult;
use index_spec::IndexSpecReadGuard;
use redis_reply::MapBuilder;
use rqe_core::DocId;
use rqe_iterator_type::IteratorType;
use rqe_iterators::{
    RQEIterator, RQEIteratorError, RQEValidateStatus, SkipToOutcome,
    profile_print::{ProfilePrint, ProfilePrintCtx},
};

use crate::{
    heap::{HeapResult, ScoredResult, TopKHeap},
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
    /// lost.
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
    S: ScoreSource,
    C: RQEIterator<'index> + 'index = Box<dyn RQEIterator<'index> + 'index>,
> {
    source: S,
    mode: TopKMode,
    /// Preserved so [`rewind`](Self::rewind) can restore the original mode.
    initial_mode: TopKMode,
    /// Captured child records alias `child`, so `heap`, `results`, and `current`
    /// are [`ManuallyDrop`] and freed by the [`Drop`] impl before `child`.
    heap: ManuallyDrop<TopKHeap<'index>>,
    /// Holds the in-progress batch for the Unfiltered path.
    direct_batch: Option<S::Batch>,
    k: NonZeroUsize,
    compare: fn(f64, f64) -> Ordering,
    phase: Phase,
    /// Heap contents drained into score order for yielding. In filtered modes
    /// each entry carries the child's record captured at match time.
    results: ManuallyDrop<Vec<HeapResult<'index>>>,
    yield_pos: usize,
    current: ManuallyDrop<Option<RSIndexResult<'index>>>,
    child: Option<C>,
    last_doc_id: DocId,
    at_eof: bool,
    /// Diagnostic counters — not reset on [`rewind`](Self::rewind).
    pub metrics: TopKMetrics,
}

impl<'index, S: ScoreSource, C: RQEIterator<'index> + 'index> Drop for TopKIterator<'index, S, C> {
    fn drop(&mut self) {
        // `heap`, `results`, and `current` hold child records captured via
        // `capture_child_record`, whose term borrows alias data owned by `child`.
        // Freeing them here — before the compiler's field glue drops `child` —
        // keeps those borrows valid regardless of field declaration order.
        // SAFETY: each buffer is dropped exactly once and never touched again.
        unsafe {
            ManuallyDrop::drop(&mut self.heap);
            ManuallyDrop::drop(&mut self.results);
            ManuallyDrop::drop(&mut self.current);
        }
    }
}

impl<'index, S: ScoreSource + 'index> TopKIterator<'index, S> {
    /// Create a new unfiltered [`TopKIterator`] (no child filter).
    ///
    /// Results are streamed directly from the source's batch — the heap is bypassed.
    /// Use [`new`](Self::new) when a filter child is present.
    pub fn new_unfiltered(source: S, k: NonZeroUsize, compare: fn(f64, f64) -> Ordering) -> Self {
        Self::new_with_mode(source, None, k, compare, TopKMode::Unfiltered)
    }
}

impl<'index, S: ScoreSource + 'index, C: RQEIterator<'index> + 'index> TopKIterator<'index, S, C> {
    /// Create a new [`TopKIterator`] with a filter child.
    ///
    /// The initial mode defaults to [`TopKMode::Batches`].
    pub fn new(source: S, child: C, k: NonZeroUsize, compare: fn(f64, f64) -> Ordering) -> Self {
        Self::new_with_mode(source, Some(child), k, compare, TopKMode::Batches)
    }

    /// Create a new [`TopKIterator`] with an explicit initial mode.
    pub fn new_with_mode(
        source: S,
        child: Option<C>,
        k: NonZeroUsize,
        compare: fn(f64, f64) -> Ordering,
        mode: TopKMode,
    ) -> Self {
        Self {
            heap: ManuallyDrop::new(TopKHeap::new(k, compare)),
            source,
            child,
            mode,
            initial_mode: mode,
            direct_batch: None,
            k,
            compare,
            phase: Phase::NotStarted,
            results: ManuallyDrop::new(Vec::new()),
            yield_pos: 0,
            current: ManuallyDrop::new(None),
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
            TopKMode::Batches | TopKMode::ForcedBatches => self.collect_batches(),
            TopKMode::AdhocBF => self.collect_adhoc(),
        };
        if result.is_err() {
            // Reset so a retry via read() works: Phase::Collecting has no handler there.
            // TODO: MOD-14209: bubble up errors
            self.phase = Phase::NotStarted;
            self.mode = self.initial_mode;
            // Discard whatever the aborted scan accumulated. A retry re-collects
            // from scratch, and the collection paths append to the heap without
            // de-duping against it, so leftover hits would duplicate doc ids and
            // skew the top-k set. Rewind the source too: collect_batches/
            // prepare_unfiltered_direct resume from its cursor rather than the start.
            *self.heap = TopKHeap::new(self.k, self.compare);
            self.source.rewind();
            if let Some(child) = &mut self.child {
                child.rewind();
            }
        }
        result
    }

    /// Set up the unfiltered direct-yield path.
    ///
    /// Calls [`ScoreSource::all_results_unfiltered_batch`] exactly once. Results are streamed
    /// directly from the batch iterator — no heap is involved.
    ///
    /// # Invariants
    ///
    /// [`TopKMode::Unfiltered`] requires the source to produce at most one
    /// batch.
    fn prepare_unfiltered_direct(&mut self) -> Result<(), RQEIteratorError> {
        self.direct_batch = self.source.all_results_unfiltered_batch()?;
        if self.direct_batch.is_none() {
            self.at_eof = true;
        }
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
                    if self.mode == TopKMode::ForcedBatches {
                        // Honour the forced-batches contract: never switch
                        // mid-run.
                        continue;
                    }
                    self.metrics.strategy_switches += 1;
                    self.mode = TopKMode::AdhocBF;
                    // Clear the heap: collect_adhoc rewinds the child and
                    // rescans every match from scratch, so batch-phase entries
                    // are redundant. Keeping them would re-admit the same doc id
                    // (TopKHeap::push only de-dups against the worst element).
                    *self.heap = TopKHeap::new(self.k, self.compare);
                    self.collect_adhoc()?;
                    return Ok(());
                }
                BatchStrategy::SwitchToBatches => {
                    self.metrics.strategy_switches += 1;
                    // Clear the heap: the source restarts with new parameters
                    // (e.g. expanded numeric range) and will re-emit previously
                    // collected docs. Keeping stale entries would cause duplicates.
                    *self.heap = TopKHeap::new(self.k, self.compare);
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
    ///
    /// Wraps the scan loop in an [`AdhocScope`] RAII guard so that
    /// [`ScoreSource::begin_adhoc`] and [`ScoreSource::end_adhoc`]
    /// wrap the adhoc code. This allows [`ScoreSource::lookup_score`]
    /// to reuse expensive resources.
    fn collect_adhoc(&mut self) -> Result<(), RQEIteratorError> {
        let child = self
            .child
            .as_mut()
            .expect("AdhocBF mode requires a child iterator");
        child.rewind();

        let scope = AdhocScope::new(&mut self.source);

        loop {
            let Some(result) = child.read()? else {
                break;
            };
            let doc_id = result.doc_id;

            // Poll before the lookup so an expired deadline skips the expensive
            // VecSim distance lookup.
            scope.0.check_timeout()?;
            if let Some(score) = scope.0.lookup_score(doc_id) {
                // Capture the child's record only if the heap retains this entry,
                // before the next `child.read()` reuses its storage, so the yield
                // phase needn't re-walk the child. A discarded candidate skips the
                // copy entirely.
                self.heap
                    .push_with_record_lazy(doc_id, score, || Some(capture_child_record(result)));
            }
        }

        // Reached only on a clean scan exit (EOF or `Stop`); a timed-out scan
        // propagates earlier.
        if scope.0.should_rerank() && !self.heap.is_empty() {
            let entries = self.heap.drain_unsorted().collect::<Vec<_>>();
            // `rerank` only rewrites scores in place (doc ids stay put), so the
            // scored slice stays index-aligned with `entries` and each stored
            // record can be re-paired with its updated score below.
            let mut scored: Vec<ScoredResult> = entries.iter().map(|e| e.scored).collect();
            scope.0.rerank(&mut scored);
            // Restore heap order under the (possibly) new scores, carrying each
            // entry's captured record along. The count never exceeded k, so
            // every entry is retained and a bulk rebuild needs no eviction.
            let reranked = entries
                .into_iter()
                .zip(scored)
                .map(|(entry, scored)| HeapResult {
                    scored,
                    record: entry.record,
                });
            self.heap.rebuild_from(reranked);
        }

        drop(scope);
        self.finalize_collection();
        Ok(())
    }

    /// Drain the heap into `self.results` in best-first order and transition to
    /// the [`Yielding`](Phase::Yielding) phase.
    fn finalize_collection(&mut self) {
        // Replace heap with a fresh one; drain_sorted consumes the old one.
        let old_heap = std::mem::replace(&mut *self.heap, TopKHeap::new(self.k, self.compare));
        *self.results = old_heap.drain_sorted();
        self.yield_pos = 0;
        self.phase = Phase::Yielding;
    }

    /// Yield the next result from the unfiltered direct batch.
    ///
    /// Results whose document expired ([`ScoreSource::is_expired`]) are dropped
    /// without replacement: the batch holds at most k entries, so skipping
    /// shrinks the yielded count.
    fn advance_unfiltered_direct(
        &mut self,
    ) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        loop {
            let item = self.direct_batch.as_mut().and_then(S::Batch::next);

            // Poll once per step, after classifying the entry and before yielding
            // it — gates valid results, EOF, and expired skips alike.
            self.source.check_timeout()?;

            match item {
                Some((doc_id, score)) => {
                    let result = self.source.build_result(doc_id, score);
                    if self.source.is_expired(&result) {
                        continue;
                    }
                    self.last_doc_id = doc_id;
                    *self.current = Some(result);
                    return Ok(self.current.as_mut());
                }
                None => {
                    self.at_eof = true;
                    *self.current = None;
                    return Ok(None);
                }
            }
        }
    }

    /// Yield the next result from the pre-sorted `results` vec.
    ///
    /// Results whose document expired ([`ScoreSource::is_expired`]) since
    /// collection are dropped without replacement: they occupied their top-k
    /// slots during collection, so they shrink the yielded count rather than
    /// being refilled from lower-scored candidates.
    fn advance_from_results(
        &mut self,
    ) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        loop {
            if self.yield_pos >= self.results.len() {
                self.at_eof = true;
                return Ok(None);
            }
            let entry = &mut self.results[self.yield_pos];
            let ScoredResult { doc_id, score } = entry.scored;
            let record = entry.record.take();
            self.yield_pos += 1;

            // Poll once per step, before yielding or skipping an entry.
            self.source.check_timeout()?;

            // Filtered mode: yield the stored child record so BM25 inputs survive.
            if self.child.is_some() {
                let Some(mut record) = record else {
                    // A filtered-mode entry must always carry its captured record;
                    // a missing one would indicate a collection-side bug. Treat as
                    // EOF rather than panicking.
                    self.at_eof = true;
                    return Ok(None);
                };
                if self.source.is_expired(&record) {
                    continue;
                }
                self.last_doc_id = doc_id;
                self.source.attach_score_metric(&mut record, score);
                *self.current = Some(record);
                return Ok(self.current.as_mut());
            }

            // Unfiltered (no child): build a fresh result from the source.
            // Unfiltered fallback (no child): build a fresh result from the source.
            let result = self.source.build_result(doc_id, score);
            if self.source.is_expired(&result) {
                continue;
            }
            self.last_doc_id = doc_id;
            *self.current = Some(result);
            return Ok(self.current.as_mut());
        }
    }
}

impl<'index, S: ScoreSource + 'index, C: RQEIterator<'index> + 'index> RQEIterator<'index>
    for TopKIterator<'index, S, C>
{
    #[inline(always)]
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        // Every yield path — filtered (stored record) and unfiltered (source-built)
        // — stashes the most recent record in `self.current`, so callers always
        // see the same `RSIndexResult` they got back from `read()`.
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
        // Only a child abort aborts us. Results come from our own score-ordered
        // buffer, so a moved child does not move our cursor: collapse it to Ok.
        if let Some(child) = &mut self.child {
            match child.revalidate(spec)? {
                RQEValidateStatus::Aborted => return Ok(RQEValidateStatus::Aborted),
                RQEValidateStatus::Ok | RQEValidateStatus::Moved { .. } => {}
            }
        }
        Ok(RQEValidateStatus::Ok)
    }

    #[inline(always)]
    fn rewind(&mut self) {
        *self.heap = TopKHeap::new(self.k, self.compare);
        self.results.clear();
        *self.current = None;
        self.source.rewind();
        if let Some(child) = &mut self.child {
            child.rewind();
        }
        self.mode = self.initial_mode;
        self.direct_batch = None;
        self.yield_pos = 0;
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

/// Deep-copy the child's current record, widening the borrowed query-term
/// lifetime to `'index` so the copy can outlive subsequent child reads.
///
/// Used at match time (during batch intersection and adhoc-BF scans) to stash
/// the child's full `RSIndexResult` in the heap, so the yield phase can return
/// it directly instead of re-walking the child — which would otherwise inflate
/// the child's profiled read counts.
fn capture_child_record<'index>(record: &RSIndexResult<'index>) -> RSIndexResult<'index> {
    // `to_owned` copies the offset bytes into fresh allocations but leaves the
    // term records borrowing the child iterator's `RSQueryTerm`s (and copies the
    // doc-metadata pointer). It therefore borrows from `record` for a lifetime
    // shorter than `'index`.
    let owned: RSIndexResult<'_> = record.to_owned();
    // SAFETY: the only borrows `owned` retains are the `RSQueryTerm`s and the
    // `dmd` pointer — all owned by the child iterator and the index read guard,
    // never by `record`'s transient per-read storage. The `TopKIterator` owns
    // the child, and its `Drop` impl frees the `heap`, `results`, and `current`
    // buffers before `child`, so every stored record is dropped before its
    // borrowed terms; the index guard outlives the iterator. So those borrows
    // stay valid for `'index`; widening the lifetime is therefore sound.
    // The offsets are owned copies, so they do not dangle when the child
    // advances, and dropping the copy frees only those offsets — it never
    // dereferences the borrowed term.
    unsafe { std::mem::transmute::<RSIndexResult<'_>, RSIndexResult<'index>>(owned) }
}

/// Intersect one score-ordered batch with a child filter iterator,
/// pushing matches into `heap`.
///
/// Uses a merge-join (alternating `skip_to` calls) to find matching doc IDs.
///
/// The child is **rewound** at the start of each call.
fn intersect_batch_with_child<'index, C: RQEIterator<'index>>(
    child: &mut C,
    batch: &mut impl ScoreBatch,
    heap: &mut TopKHeap<'index>,
    metrics: &mut TopKMetrics,
) -> Result<(), RQEIteratorError> {
    child.rewind();

    // Prime both iterators.
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
                // Capture the matching child record only if the heap retains it,
                // before advancing past it, so the yield phase can return it
                // without re-reading the child. A discarded match skips the copy.
                heap.push_with_record_lazy(batch_doc, batch_score, || {
                    child.current().map(|r| capture_child_record(r))
                });
                // Advance the batch first; only read the child when another
                // batch doc remains. Reading the child past an exhausted batch
                // is needless work that inflates its profile counters and could
                // turn a completed batch into a spurious TimedOut.
                let Some((d, s)) = batch.next() else { break };
                batch_doc = d;
                batch_score = s;
                match child.read()?.map(|r| r.doc_id) {
                    Some(doc_id) => child_doc = doc_id,
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

/// RAII guard bracketing an adhoc scan: calls [`ScoreSource::begin_adhoc`] on
/// construction and [`ScoreSource::end_adhoc`] when dropped.
struct AdhocScope<'a, S: ScoreSource>(&'a mut S);

impl<'a, S: ScoreSource> AdhocScope<'a, S> {
    /// Opens the adhoc scope on `source`, returning a guard that closes it on drop.
    fn new(source: &'a mut S) -> Self {
        source.begin_adhoc();
        Self(source)
    }
}

impl<S: ScoreSource> Drop for AdhocScope<'_, S> {
    fn drop(&mut self) {
        self.0.end_adhoc();
    }
}

/// Source-side profile rendering. The blanket [`ProfilePrint`] impl below
/// forwards [`TopKIterator`] profile output to the source.
pub trait TopKSourceProfile {
    /// Render this source's profile entry.
    ///
    /// `child` is the filter child's profile renderer, when present. The
    /// [`TopKIterator`] passes its own (already profile-wrapped) child here so
    /// the source renders the same iterator it read through — and thus the
    /// child's real read counts — rather than an unprofiled side handle.
    fn print_profile(
        &self,
        mode: TopKMode,
        switches: usize,
        map: &mut MapBuilder<'_>,
        ctx: &mut ProfilePrintCtx<'_>,
        child: Option<&dyn ProfilePrint>,
    );
}

impl<'index, S, C> ProfilePrint for TopKIterator<'index, S, C>
where
    S: ScoreSource + TopKSourceProfile + 'index,
    C: RQEIterator<'index> + ProfilePrint + 'index,
{
    fn print_profile(&self, map: &mut MapBuilder<'_>, ctx: &mut ProfilePrintCtx<'_>) {
        let child = self.child.as_ref().map(|c| c as &dyn ProfilePrint);
        self.source
            .print_profile(self.mode, self.metrics.strategy_switches, map, ctx, child);
    }
}

impl<'index, S: ScoreSource + 'index> rqe_iterators::interop::ProfileChildren<'index>
    for TopKIterator<'index, S, rqe_iterators::c2rust::CRQEIterator>
{
    fn profile_children(mut self) -> Self {
        self.child = self
            .child
            .take()
            .map(rqe_iterators::c2rust::CRQEIterator::into_profiled);
        self
    }
}
