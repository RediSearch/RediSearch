/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! [`TopKIterator`] — the generic top-k state machine.

use std::{cmp::Ordering, marker::PhantomData, mem::ManuallyDrop, num::NonZeroUsize};

use index_result::RSIndexResult;
use index_spec::IndexSpecReadGuard;
use redis_reply::MapBuilder;
use ref_mode::{Active, Ref, Suspended};
use rqe_core::DocId;
use rqe_iterator_type::IteratorType;
use rqe_iterators::{
    RQEIterator, RQEIteratorBoxed, RQEIteratorError, RQESuspendedIterator, RQEValidateStatus,
    ResumeOutcome, SkipToOutcome,
    boxed::{
        ResumeSlotOutcome, assert_layout_compatible, resume_child_slot_in_place,
        suspend_child_slot_in_place,
    },
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
    /// with the child filter iterator, keeping the top `k` in the heap.
    ///
    /// With no child filter every source record is a candidate: the whole
    /// batch is fed through the heap, which still retains the top `k`. This is
    /// the mode for a source that has no native top-k and must rely on the heap
    /// for selection (e.g. a numeric `SORTBY` with no query filter).
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
///
/// Parameterised over a [`Ref`] mode: [`TopKIterator`] is the [`Active`]
/// instantiation that implements [`RQEIterator`], and
/// `RawTopK<'query, Suspended, S, I::Suspended>` its suspended counterpart (see
/// [`RQEIteratorBoxed::suspend`]).
///
/// # Invariants
///
/// 1. **Layout compatibility across modes.** `RawTopK` is `#[repr(C)]`, and the
///    only field whose type varies with the mode is the `child` slot — a
///    `#[repr(C)]` `TopKChild`, layout-compatible between `I` and
///    `I::Suspended` (enforced at monomorphization by
///    [`suspend_child_slot_in_place`]). Every other field has the *same* type in
///    both instantiations, and `Rf` is carried by the zero-sized `_marker`
///    alone. The `const` block below proves this for a representative
///    instantiation, which is what lets [`suspend`](RQEIteratorBoxed::suspend)
///    and [`resume`](RQESuspendedIterator::resume) reinterpret the owning `Box`
///    in place rather than rebuilding it.
/// 2. **The result buffers hold no index-backed payload.** `heap`, `results` and
///    `current` are typed [`Active`] in *both* modes, which is only sound
///    because the records parked in them borrow nothing from the index: they are
///    produced exclusively by `capture_child_record` (whose
///    [`to_owned`](RSIndexResult::to_owned) copies the offset bytes and
///    deep-copies aggregate children), by `capture_child_metrics` (a
///    metric-only result), or by [`ScoreSource::build_result`], whose contract
///    obliges every source to return such a record. What survives is
///    the child's borrowed `RSQueryTerm`s, the query's `RLookupKey`-backed
///    metrics and the `dmd` pointer — none of which a mode transition weakens or
///    re-narrows. `carries_no_index_borrow` states the property in code;
///    because neither that trait contract nor the `&mut` handout from
///    [`current`](RQEIterator::current) can be enforced statically, it is
///    *checked* — not assumed — at the boundary that re-narrows
///    ([`resume`](RQESuspendedIterator::resume)), which aborts on violation.
/// 3. **The heap is empty outside collection.** `collect` runs to completion
///    within a single [`read`](RQEIterator::read) — draining the heap into
///    `results` on success, clearing it on error — so every externally
///    observable state, suspension included, has an empty heap.
#[repr(C)]
pub struct RawTopK<'query, Rf: Ref, S: ScoreSource, I> {
    source: S,
    mode: TopKMode,
    /// Preserved so [`rewind`](RQEIterator::rewind) can restore the original mode.
    initial_mode: TopKMode,
    /// Captured child records alias `child`, so `heap`, `results`, and `current`
    /// are [`ManuallyDrop`] and freed by the [`Drop`] impl before `child`.
    heap: ManuallyDrop<TopKHeap<'query>>,
    /// Holds the in-progress batch for the Unfiltered path.
    direct_batch: Option<S::Batch>,
    k: NonZeroUsize,
    compare: fn(&f64, &f64) -> Ordering,
    /// When `true`, filtered modes skip deep-copying the child's rich result
    /// subtree and yield its metrics with the source's score attached.
    /// Set when the downstream pipeline needs no rich results (no relevance
    /// scorer or highlighter that reads the child's term records).
    can_trim_deep_results: bool,
    phase: Phase,
    /// Heap contents drained into score order for yielding. In filtered modes
    /// each entry carries the child's record captured at match time.
    results: ManuallyDrop<Vec<HeapResult<'query>>>,
    yield_pos: usize,
    current: ManuallyDrop<Option<RSIndexResult<'query>>>,
    child: TopKChild<I>,
    last_doc_id: DocId,
    at_eof: bool,
    /// Diagnostic counters — not reset on [`rewind`](RQEIterator::rewind).
    pub metrics: TopKMetrics,
    _marker: PhantomData<Rf>,
}

/// Alias for an [`Active`] [`RawTopK`] — the only instantiation with an
/// [`RQEIterator`] impl.
pub type TopKIterator<'index, S, C = Box<dyn RQEIterator<'index> + 'index>> =
    RawTopK<'index, Active<'index>, S, C>;

/// Child-filter slot for [`RawTopK`].
///
/// A dedicated `#[repr(C)]` enum rather than `Option<I>`, whose niche layout
/// depends on `I` and is therefore not transmute-stable across the
/// `I` → `I::Suspended` swap that the whole-box cast performs. The `I`-free
/// [`Absent`](TopKChild::Absent) variant doubles as the teardown target when a
/// child is consumed by an aborted resume. Mirrors
/// [`rqe_iterators::optional`]'s `OptionalChild` for the same reasons.
#[repr(C)]
enum TopKChild<I> {
    /// No child filter — the [`Unfiltered`](TopKMode::Unfiltered) path, or a
    /// child consumed during teardown.
    Absent,
    /// The filter child provided at construction.
    Present(I),
}

impl<I> TopKChild<I> {
    /// Shared reference to the child, if present.
    #[inline(always)]
    const fn as_ref(&self) -> Option<&I> {
        match self {
            Self::Absent => None,
            Self::Present(i) => Some(i),
        }
    }

    /// Mutable reference to the child, if present.
    #[inline(always)]
    const fn as_mut(&mut self) -> Option<&mut I> {
        match self {
            Self::Absent => None,
            Self::Present(i) => Some(i),
        }
    }

    /// Whether the slot holds no child.
    #[inline(always)]
    const fn is_none(&self) -> bool {
        matches!(self, Self::Absent)
    }

    /// Whether the slot holds a child.
    #[inline(always)]
    const fn is_some(&self) -> bool {
        matches!(self, Self::Present(_))
    }

    /// Move the child out, leaving the slot [`Absent`](TopKChild::Absent).
    #[inline(always)]
    fn take(&mut self) -> Option<I> {
        match std::mem::replace(self, Self::Absent) {
            Self::Absent => None,
            Self::Present(i) => Some(i),
        }
    }
}

impl<I> From<Option<I>> for TopKChild<I> {
    fn from(child: Option<I>) -> Self {
        match child {
            None => Self::Absent,
            Some(i) => Self::Present(i),
        }
    }
}

// Compile-time proof of invariant 1 on `RawTopK`: for a representative
// instantiation, the `Active` and `Suspended` forms agree on every field offset,
// on size and on alignment. Module consts cannot be generic, so the proof fixes
// one source and one child; that is enough because `S`, `S::Batch` and every
// buffer type are *the same types* in both modes (only `child` and the ZST
// `_marker` vary), and the child slot's own compatibility is enforced for every
// implementer by `suspend_child_slot_in_place`. `Wildcard` is a representative
// child whose `Active`/`Suspended` forms differ but stay layout-compatible.
const _: () = {
    use rqe_iterators::Wildcard;
    use std::mem::{align_of, offset_of, size_of};

    /// A do-nothing [`ScoreSource`] standing in for every source: the proof
    /// only needs `S` to be *some* concrete type, since it is identical across
    /// modes.
    struct ProofSource;

    impl ScoreBatch for ProofSource {
        fn next(&mut self) -> Option<(DocId, f64)> {
            None
        }

        fn skip_to(&mut self, _target: DocId) -> Option<(DocId, f64)> {
            None
        }
    }

    impl ScoreSource for ProofSource {
        type Batch = Self;

        fn next_batch(&mut self) -> Result<Option<Self::Batch>, RQEIteratorError> {
            Ok(None)
        }

        fn lookup_score(&mut self, _doc_id: DocId) -> Option<f64> {
            None
        }

        fn num_estimated(&self) -> usize {
            0
        }

        fn rewind(&mut self) {}

        fn build_result<'r>(&self, doc_id: DocId, _score: f64) -> RSIndexResult<'r>
        where
            Self: 'r,
        {
            RSIndexResult::build_virt().doc_id(doc_id).build()
        }

        fn attach_score_metric<'r>(&self, _result: &mut RSIndexResult<'r>, _score: f64)
        where
            Self: 'r,
        {
        }

        fn yields_child_record(&self) -> bool {
            false
        }

        fn batch_strategy(&mut self, _heap_count: usize, _k: usize) -> BatchStrategy {
            BatchStrategy::Stop
        }

        fn check_timeout(&mut self) -> Result<(), RQEIteratorError> {
            Ok(())
        }

        fn iterator_type(&self) -> IteratorType {
            IteratorType::Mock
        }
    }

    type AChild = Wildcard<'static>;
    type SChild = <Wildcard<'static> as RQEIteratorBoxed<'static>>::Suspended;
    type A = RawTopK<'static, Active<'static>, ProofSource, AChild>;
    type S = RawTopK<'static, Suspended, ProofSource, SChild>;

    assert!(offset_of!(A, source) == offset_of!(S, source));
    assert!(offset_of!(A, mode) == offset_of!(S, mode));
    assert!(offset_of!(A, initial_mode) == offset_of!(S, initial_mode));
    assert!(offset_of!(A, heap) == offset_of!(S, heap));
    assert!(offset_of!(A, direct_batch) == offset_of!(S, direct_batch));
    assert!(offset_of!(A, k) == offset_of!(S, k));
    assert!(offset_of!(A, compare) == offset_of!(S, compare));
    assert!(offset_of!(A, can_trim_deep_results) == offset_of!(S, can_trim_deep_results));
    assert!(offset_of!(A, phase) == offset_of!(S, phase));
    assert!(offset_of!(A, results) == offset_of!(S, results));
    assert!(offset_of!(A, yield_pos) == offset_of!(S, yield_pos));
    assert!(offset_of!(A, current) == offset_of!(S, current));
    assert!(offset_of!(A, child) == offset_of!(S, child));
    assert!(offset_of!(A, last_doc_id) == offset_of!(S, last_doc_id));
    assert!(offset_of!(A, at_eof) == offset_of!(S, at_eof));
    assert!(offset_of!(A, metrics) == offset_of!(S, metrics));
    assert!(size_of::<A>() == size_of::<S>());
    assert!(align_of::<A>() == align_of::<S>());
};

impl<'query, Rf: Ref, S: ScoreSource, I> Drop for RawTopK<'query, Rf, S, I> {
    fn drop(&mut self) {
        // `heap`, `results`, and `current` hold child records captured via
        // `capture_child_record`, whose term borrows alias data owned by `child`.
        // Freeing them here — before the compiler's field glue drops `child` —
        // keeps those borrows valid regardless of field declaration order.
        //
        // This also covers the *suspended* instantiation, including the teardown
        // path where an aborted child resume already consumed `child` and left
        // the slot `Absent`: dropping a captured record frees its owned offset
        // copies and its boxed aggregate children, and never dereferences the
        // borrowed query term, so a record whose term has already gone away is
        // still safe to drop.
        //
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
    pub fn new_unfiltered(source: S, k: NonZeroUsize, compare: fn(&f64, &f64) -> Ordering) -> Self {
        Self::new_with_mode(source, None, k, compare, TopKMode::Unfiltered)
    }
}

impl<'index, S: ScoreSource + 'index, C: RQEIterator<'index> + 'index> TopKIterator<'index, S, C> {
    /// Create a new [`TopKIterator`] with a filter child.
    ///
    /// The initial mode defaults to [`TopKMode::Batches`].
    pub fn new(source: S, child: C, k: NonZeroUsize, compare: fn(&f64, &f64) -> Ordering) -> Self {
        Self::new_with_mode(source, Some(child), k, compare, TopKMode::Batches)
    }

    /// Create a new [`TopKIterator`] with an explicit initial mode.
    pub fn new_with_mode(
        source: S,
        child: Option<C>,
        k: NonZeroUsize,
        compare: fn(&f64, &f64) -> Ordering,
        mode: TopKMode,
    ) -> Self {
        Self {
            heap: ManuallyDrop::new(TopKHeap::new(k, compare)),
            source,
            child: child.into(),
            mode,
            initial_mode: mode,
            direct_batch: None,
            k,
            compare,
            can_trim_deep_results: false,
            phase: Phase::NotStarted,
            results: ManuallyDrop::new(Vec::new()),
            yield_pos: 0,
            current: ManuallyDrop::new(None),
            last_doc_id: 0,
            at_eof: false,
            metrics: TopKMetrics::default(),
            _marker: PhantomData,
        }
    }

    /// Set whether filtered modes may yield metric-only results.
    ///
    /// When `true`, the collection phase captures only each matching child's
    /// yielded metrics instead of deep-copying its whole record, and the yield
    /// phase hands those back with the source's score attached via
    /// [`ScoreSource::attach_score_metric`].
    /// Leave `false` (the default) whenever a downstream scorer or highlighter
    /// reads the child's term records.
    pub fn with_trim_deep_results(mut self, trim: bool) -> Self {
        self.can_trim_deep_results = trim;
        self
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

    /// Returns a reference to the metrics accumulated so far.
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
            if let Some(child) = self.child.as_mut() {
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
            let can_trim_deep_results = self.can_trim_deep_results;
            if let Some(child) = self.child.as_mut() {
                intersect_batch_with_child(
                    child,
                    &mut batch,
                    &mut self.heap,
                    &mut self.metrics,
                    can_trim_deep_results,
                )?;
            } else {
                // No filter child: every source batch record is a candidate, so feed
                // the whole batch through the heap, which retains the top k.
                while let Some((doc_id, score)) = batch.next() {
                    self.heap.push(doc_id, score);
                }
            }
            // Batch consumption is unpolled; check once at the boundary.
            self.source.check_timeout()?;
            match self.source.batch_strategy(self.heap.len(), self.k.get()) {
                BatchStrategy::Continue => continue,
                BatchStrategy::Stop => break,
                BatchStrategy::SwitchToAdhoc => {
                    if self.mode == TopKMode::ForcedBatches {
                        // Honour the forced-batches contract: never switch
                        // mid-run.
                        continue;
                    }
                    if self.child.is_none() {
                        // Adhoc-BF requires a child filter iterator:
                        // ignore the strategy switch.
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
                BatchStrategy::ExpandWindow => {
                    self.metrics.strategy_switches += 1;
                    // The source already re-resolved itself to the next disjoint
                    // window, so the heap stays valid and keeps accumulating.
                    // Only the child is rewound for re-intersection.
                    if let Some(child) = self.child.as_mut() {
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
        let can_trim_deep_results = self.can_trim_deep_results;
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
                // Capture the child's data only if the heap retains this entry,
                // before the next `child.read()` reuses its storage, so the yield
                // phase needn't re-walk the child. A discarded candidate skips the
                // copy entirely. When rich results can be trimmed, copy only the
                // child's yielded metrics rather than its full scoring subtree.
                self.heap.push_with_record_lazy(doc_id, score, || {
                    Some(if can_trim_deep_results {
                        capture_child_metrics(result)
                    } else {
                        capture_child_record(result)
                    })
                });
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
                // Cleared alongside the flag, as `advance_unfiltered_direct` does:
                // `current()` is the negation of `at_eof()`, so leaving the last
                // yielded record here would hand a caller a document it has
                // already consumed.
                *self.current = None;
                return Ok(None);
            }
            let entry = &mut self.results[self.yield_pos];
            let ScoredResult { doc_id, score } = entry.scored;
            let record = entry.record.take();
            self.yield_pos += 1;

            // Poll once per step, before yielding or skipping an entry.
            self.source.check_timeout()?;

            // Filtered mode carries the child's captured data. Without trimming,
            // the stored record is the child's full subtree, kept so BM25 inputs
            // survive; when trimming it holds only the child's yielded metrics.
            // A source that builds its own result (e.g. numeric `SORTBY`) opts out
            // and takes the source-built path below.
            if self.child.is_some() && self.source.yields_child_record() {
                if self.can_trim_deep_results {
                    // Carry the child's captured metrics and attach our score
                    // last, so a lookup key shared with the child (e.g. nested
                    // KNN reusing an `AS` alias) keeps the outer vector score.
                    let mut result = match record {
                        Some(record) => record,
                        None => self.source.build_result(doc_id, score),
                    };
                    if self.source.is_expired(&result) {
                        continue;
                    }
                    self.last_doc_id = doc_id;
                    // A carried record holds only the child's metrics, with a zeroed
                    // payload. The score is this record's own value, so it has to reach
                    // the payload too — that is what `ScoreSource::build_result` puts
                    // there, and what consumers reading the record numerically expect.
                    if let Some(value) = result.as_numeric_mut() {
                        *value = score;
                    }
                    self.source.attach_score_metric(&mut result, score);
                    *self.current = Some(result);
                    return Ok(self.current.as_mut());
                }
                let Some(mut record) = record else {
                    // A rich filtered-mode entry must always carry its captured
                    // record; a missing one would indicate a collection-side bug.
                    // Treat as EOF rather than panicking — and report it as EOF
                    // fully, with no stale current left behind.
                    self.at_eof = true;
                    *self.current = None;
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

            // No child record to yield: build a fresh result from the source.
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
        if let Some(child) = self.child.as_mut() {
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
        if let Some(child) = self.child.as_mut() {
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
        let estimate = self.k.get().min(self.source.num_estimated());
        // A filtered query yields the intersection, so the child's own upper
        // bound caps ours: a selective filter must not be masked by a large `k`.
        match self.child.as_ref() {
            Some(child) => estimate.min(child.num_estimated()),
            None => estimate,
        }
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

impl<'index, S: ScoreSource + 'index, I: RQEIteratorBoxed<'index>> RQEIteratorBoxed<'index>
    for TopKIterator<'index, S, I>
{
    type Suspended = RawTopK<'index, Suspended, S, I::Suspended>;

    fn suspend(mut self: Box<Self>) -> Box<Self::Suspended> {
        // Invariant 1 covers every field but the child slot, whose `I` is only
        // known per instantiation. `suspend_child_slot_in_place` checks it — but
        // only when a child is *there*, and an unfiltered top-k has none, so on
        // that path nothing would bind `I` to `I::Suspended` before the whole
        // box is cast. `TopKChild<I>` sizes with `I` whether the variant is
        // `Present` or `Absent`, so a mismatch is just as fatal either way.
        assert_layout_compatible::<I, I::Suspended>();

        // The source is the one part of us the `Ref` split does not describe: it
        // is carried across identically, so anything index-backed inside it
        // would outlive the read guard it was made under. Hand it the chance to
        // let go before the lock drops.
        self.source.release_index_handles();

        // Nothing else is touched: the yield position (`phase`, `results`,
        // `yield_pos`, `direct_batch`) is what `resume` must preserve, and the
        // buffered records carry no index-backed payload (invariant 2), so they
        // survive the lock release untouched — exactly as they do across the
        // legacy `revalidate`, which also leaves them alone.
        // Weakening a borrow is sound whatever the buffers hold, so this is an
        // early warning, not a guard: the release-mode guarantee is the same
        // check at the resume boundary, which is where the borrows are
        // re-narrowed and where a violation would actually be unsound.
        debug_assert!(
            self.buffers_carry_no_index_borrows(),
            "a record parked for yielding borrows index data, or the heap \
             survived collection (invariants 2/3): it cannot survive the lock \
             release this suspend is preparing for",
        );

        let raw = Box::into_raw(self);
        // SAFETY: `raw` came from `Box::into_raw` (non-null, aligned,
        // initialised, exclusively owned), so `&raw mut` can form a pointer to
        // the child slot without creating a reference to the whole value.
        let child_slot = unsafe { &raw mut (*raw).child };
        // SAFETY: `child_slot` is valid and exclusively owned, so `&mut` is a
        // sound unique borrow of the slot.
        if let Some(child) = unsafe { &mut *child_slot }.as_mut() {
            // Route the transition through the trait: a dyn-erased child swaps
            // its vtable, which a byte reinterpretation of the slot would not.
            //
            // SAFETY: `child` is a valid, exclusively-owned `I`; the helper
            // leaves the slot holding a valid `I::Suspended`.
            unsafe { suspend_child_slot_in_place(child as *mut I) };
        }
        // SAFETY: the child slot (if occupied) now holds `I::Suspended` and
        // `TopKChild::Absent` is `I`-free, so the allocation is a valid
        // `RawTopK<'index, Suspended, S, I::Suspended>` — layout-identical to the
        // active form by invariant 1 (const proof above), with the child slot's
        // `I`/`I::Suspended` size and alignment match statically enforced by
        // `suspend_child_slot_in_place`. `Box::from_raw` reuses the same
        // allocation, so every interior address survives the cycle.
        unsafe { Box::from_raw(raw.cast::<RawTopK<'index, Suspended, S, I::Suspended>>()) }
    }
}

impl<'query, S: ScoreSource + 'query, IS: RQESuspendedIterator<'query>> RQESuspendedIterator<'query>
    for RawTopK<'query, Suspended, S, IS>
{
    type Resumed<'index>
        = RawTopK<'index, Active<'index>, S, IS::Resumed<'index>>
    where
        'query: 'index;

    fn resume<'index>(
        self: Box<Self>,
        guard: &IndexSpecReadGuard<'index>,
    ) -> Result<ResumeOutcome<Box<Self::Resumed<'index>>>, RQEIteratorError>
    where
        'query: 'index,
    {
        /// Outcome of resuming the child, captured so the `&mut` borrow of the
        /// child slot is released before the slot is touched as a raw pointer.
        enum ChildResume {
            /// No child to resume (unfiltered mode).
            NoChild,
            /// Child resumed in place.
            Active,
            /// Child aborted and was consumed; forward `Aborted`.
            Aborted,
            /// Child resume failed; forward the error.
            Failed(RQEIteratorError),
        }

        // As on suspend: the child slot's layout is the one part invariant 1
        // cannot state, and the slot helper only checks it when a child is
        // present.
        assert_layout_compatible::<IS::Resumed<'index>, IS>();

        // Resume is the direction that *re-narrows*: the cast below asserts the
        // parked records' payloads are live `&'index` borrows again. Invariant 2
        // says they hold none — but neither half of that invariant is statically
        // enforceable (`ScoreSource::build_result` only promises to return an
        // index-free record, and `current()` hands the parked record out `&mut`),
        // so it is checked here rather than assumed. Unlike a leaf, we have
        // nothing to re-validate a foreign payload against: these records are our
        // own collected output, not a position in the index we could re-read. So
        // on violation we refuse to reinterpret and abort — `Ok(Aborted)`, not
        // `Err`, since the state is recoverable and `RQEIteratorError` is for
        // genuine `TimedOut`/`IoError` failures. Checked on `&self`, before
        // `Box::into_raw` opens the raw-pointer critical section, so a violation
        // simply drops the box.
        //
        // The cost is `O(k)` in the collected set, plus each record's subtree.
        // That is real, but resume only runs on a concurrent index change and `k`
        // is the top-k bound, so it is worth paying for a release-mode guarantee.
        // Suspend keeps the same check as a `debug_assert!` early warning
        // instead: weakening a borrow is sound whatever the buffers hold.
        if !self.buffers_carry_no_index_borrows() {
            return Ok(ResumeOutcome::Aborted);
        }

        let raw = Box::into_raw(self);
        // SAFETY: `raw` came from `Box::into_raw` (non-null, aligned,
        // initialised, exclusively owned); `&raw mut` forms a field pointer to
        // the child slot without creating a reference to the whole value.
        let child_slot = unsafe { &raw mut (*raw).child };

        // Resume the child in place. The `&mut` borrow of the slot is confined
        // to this block so the raw `child_slot` write below never aliases it.
        // SAFETY: `child_slot` is valid and exclusively owned, so `&mut` is a
        // sound unique borrow of the slot.
        let step = match unsafe { &mut *child_slot }.as_mut() {
            None => ChildResume::NoChild,
            Some(child) => {
                // SAFETY: `child` is the valid, exclusively-owned suspended
                // child. On Unchanged/Moved the helper rewrites the slot as a
                // valid `IS::Resumed<'index>`; on Aborted/Err it consumes the
                // child, leaving the slot uninitialised (handled below).
                match unsafe { resume_child_slot_in_place(child as *mut IS, guard) } {
                    // A moved child does not move *our* cursor — see the outcome
                    // note below — so `Unchanged` and `Moved` are the same to us.
                    Ok(ResumeSlotOutcome::Unchanged | ResumeSlotOutcome::Moved) => {
                        ChildResume::Active
                    }
                    Ok(ResumeSlotOutcome::Aborted) => ChildResume::Aborted,
                    Err(e) => ChildResume::Failed(e),
                }
            }
        };

        // An aborted child aborts the whole top-k: the filter it applied is
        // unrecoverable, so the collected set can no longer be trusted. This
        // mirrors `RQEIterator::revalidate`, which reports `Aborted` for exactly
        // this case.
        match step {
            ChildResume::NoChild | ChildResume::Active => {}
            ChildResume::Aborted | ChildResume::Failed(_) => {
                // The child was consumed → its slot is uninitialised. Overwrite
                // it with the `IS`-free `Absent` variant so the box is a valid
                // owned value again, then drop it (see the `Drop` impl for why
                // the buffered records outliving their terms is fine).
                //
                // SAFETY: `child_slot` is valid and exclusively owned;
                // `ptr::write` does not drop the moved-from payload.
                unsafe { child_slot.write(TopKChild::Absent) };
                // SAFETY: `raw` is again a valid, exclusively-owned
                // `RawTopK<'query, Suspended, S, IS>`; reclaim and drop it,
                // freeing the allocation.
                drop(unsafe { Box::from_raw(raw) });
                return match step {
                    ChildResume::Failed(e) => Err(e),
                    _ => Ok(ResumeOutcome::Aborted),
                };
            }
        }

        // Reinterpret the owning box, reusing the allocation so the inline child
        // — which `read()` delegates into, and whose own `resume` preserved its
        // interior addresses — is not moved, and so the pointer the FFI wrapper
        // caches into this iterator stays valid.
        //
        // SAFETY: `RawTopK<'query, Suspended, S, IS>` and
        // `RawTopK<'index, Active<'index>, S, IS::Resumed<'index>>` are
        // layout-identical by invariant 1 (const proof above): the child slot's
        // `IS`/`IS::Resumed` size and alignment match is enforced by
        // `resume_child_slot_in_place`, and every other field has the same type
        // in both forms. Re-narrowing the buffered records' `'query` to the
        // shorter `'index` is sound because they hold no index-backed payload —
        // invariant 2, just verified above — and their query-pipeline borrows
        // outlive `'index` by the `'query: 'index` bound. `Box::from_raw` reuses
        // the same allocation.
        let active = unsafe {
            Box::from_raw(raw.cast::<RawTopK<'index, Active<'index>, S, IS::Resumed<'index>>>())
        };

        // Always `Ok`, never `Moved`: our position is `yield_pos` into our own
        // score-ordered buffer, which the suspension left untouched, so the
        // document `current()` reports is still the one the caller last read.
        // `RQEIterator::revalidate` collapses a moved child the same way.
        Ok(ResumeOutcome::Ok(active))
    }

    fn last_doc_id(&self) -> DocId {
        self.last_doc_id
    }

    fn num_estimated(&self) -> usize {
        // Mode-independent — mirrors the active `num_estimated`; neither `k` nor
        // `source` carries mode-dependent state.
        self.k.get().min(self.source.num_estimated())
    }
}

impl<'query, Rf: Ref, S: ScoreSource, I> RawTopK<'query, Rf, S, I> {
    /// Whether every record parked for yielding satisfies invariant 2.
    ///
    /// Gates [`resume`](RQESuspendedIterator::resume) in every build, and backs
    /// the `debug_assert!` early warning at the suspend boundary.
    fn buffers_carry_no_index_borrows(&self) -> bool {
        // The heap is drained into `results` by the end of `collect`, so it is
        // empty at every point a caller can suspend us (invariant 3). Its
        // emptiness *is* how it is covered here: there is nothing to walk, and a
        // heap that somehow survived collection is a state this check must not
        // wave through either.
        self.heap.is_empty()
            && self
                .results
                .iter()
                .filter_map(|entry| entry.record.as_ref())
                .chain(self.current.as_ref())
                .all(carries_no_index_borrow)
    }
}

/// Whether `record` carries no index-backed payload, recursively — the property
/// invariant 2 on [`RawTopK`] rests on.
///
/// The index-backed payloads an [`Active`] result can hold are a term record's
/// borrowed offset slice and an aggregate's borrowed entries; a captured record
/// has neither, since [`RSIndexResult::to_owned`] copies the offsets and
/// deep-copies the children.
fn carries_no_index_borrow(record: &RSIndexResult<'_>) -> bool {
    if let Some(term) = record.as_term()
        && !term.is_copy()
    {
        return false;
    }
    if let Some(aggregate) = record.as_aggregate() {
        // Borrowed entries point at a child's live result, which the release
        // invalidates; only the owned representation can be carried across, and
        // then only if its boxed children are index-free too.
        let Some(owned) = aggregate.as_owned() else {
            return false;
        };
        return (0..owned.len()).all(|i| owned.get(i).is_some_and(carries_no_index_borrow));
    }
    true
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

/// Capture only the child's yielded metrics (e.g. `AS`-yielded vector
/// distances) into a fresh metric-only result.
///
/// Used on the trim path, where the child's deep scoring subtree is skipped but
/// its yielded metrics remain explicit output/sort fields that must still be
/// carried. Cloning copies each metric entry by value; the `RLookupKey`s they
/// reference are owned by the query and stay valid for `'index`, so the copy
/// outlives subsequent child reads.
fn capture_child_metrics<'index>(record: &RSIndexResult<'index>) -> RSIndexResult<'index> {
    let mut owned = RSIndexResult::build_metric(0.0)
        .doc_id(record.doc_id)
        .build();
    owned.metrics = record.metrics.clone();
    owned
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
    can_trim_deep_results: bool,
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
                // Capture the matching child data only if the heap retains it,
                // before advancing past it, so the yield phase can return it
                // without re-reading the child. A discarded match skips the copy.
                // When rich results can be trimmed, copy only the child's yielded
                // metrics rather than its full scoring subtree.
                heap.push_with_record_lazy(batch_doc, batch_score, || {
                    child.current().map(|r| {
                        if can_trim_deep_results {
                            capture_child_metrics(r)
                        } else {
                            capture_child_record(r)
                        }
                    })
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
            .map(rqe_iterators::c2rust::CRQEIterator::into_profiled)
            .into();
        self
    }
}

#[cfg(test)]
mod tests {
    use index_result::RSOffsetSlice;
    use rqe_iterators::Empty;
    use rqe_iterators_test_utils::MockContext;

    use super::*;

    /// Encoded offset bytes standing in for a run owned by the inverted index —
    /// the kind of buffer that a lock release can free underneath a parked
    /// record.
    static INDEX_OFFSETS: [u8; 3] = [1, 2, 3];

    /// A single-entry [`ScoreBatch`].
    struct OneDocBatch(Option<(DocId, f64)>);

    impl ScoreBatch for OneDocBatch {
        fn next(&mut self) -> Option<(DocId, f64)> {
            self.0.take()
        }

        fn skip_to(&mut self, target: DocId) -> Option<(DocId, f64)> {
            self.0.take_if(|(doc_id, _)| *doc_id >= target)
        }
    }

    /// A [`ScoreSource`] that breaks [`ScoreSource::build_result`]'s obligation:
    /// its record borrows its offsets from [`INDEX_OFFSETS`] instead of owning a
    /// copy, so `carries_no_index_borrow` rejects it.
    struct IndexBorrowingSource;

    impl ScoreSource for IndexBorrowingSource {
        type Batch = OneDocBatch;

        fn next_batch(&mut self) -> Result<Option<Self::Batch>, RQEIteratorError> {
            Ok(Some(OneDocBatch(Some((1, 1.0)))))
        }

        fn lookup_score(&mut self, _doc_id: DocId) -> Option<f64> {
            None
        }

        fn num_estimated(&self) -> usize {
            1
        }

        fn rewind(&mut self) {}

        fn build_result<'r>(&self, doc_id: DocId, _score: f64) -> RSIndexResult<'r>
        where
            Self: 'r,
        {
            RSIndexResult::build_term()
                .doc_id(doc_id)
                .borrowed_record(None, RSOffsetSlice::from_slice(&INDEX_OFFSETS))
                .build()
        }

        fn attach_score_metric<'r>(&self, _result: &mut RSIndexResult<'r>, _score: f64)
        where
            Self: 'r,
        {
        }

        fn yields_child_record(&self) -> bool {
            false
        }

        fn batch_strategy(&mut self, _heap_count: usize, _k: usize) -> BatchStrategy {
            BatchStrategy::Stop
        }

        fn check_timeout(&mut self) -> Result<(), RQEIteratorError> {
            Ok(())
        }

        fn iterator_type(&self) -> IteratorType {
            IteratorType::Mock
        }
    }

    /// A [`ScoreSource`] that records whether the suspend boundary asked it to
    /// let go of its index-backed state.
    #[derive(Default)]
    struct HandleTrackingSource {
        released: std::rc::Rc<std::cell::Cell<bool>>,
    }

    impl ScoreSource for HandleTrackingSource {
        type Batch = OneDocBatch;

        fn next_batch(&mut self) -> Result<Option<Self::Batch>, RQEIteratorError> {
            Ok(Some(OneDocBatch(Some((1, 1.0)))))
        }

        fn lookup_score(&mut self, _doc_id: DocId) -> Option<f64> {
            None
        }

        fn num_estimated(&self) -> usize {
            1
        }

        fn rewind(&mut self) {}

        fn build_result<'r>(&self, doc_id: DocId, _score: f64) -> RSIndexResult<'r>
        where
            Self: 'r,
        {
            RSIndexResult::build_virt().doc_id(doc_id).build()
        }

        fn attach_score_metric<'r>(&self, _result: &mut RSIndexResult<'r>, _score: f64)
        where
            Self: 'r,
        {
        }

        fn yields_child_record(&self) -> bool {
            false
        }

        fn batch_strategy(&mut self, _heap_count: usize, _k: usize) -> BatchStrategy {
            BatchStrategy::Stop
        }

        fn check_timeout(&mut self) -> Result<(), RQEIteratorError> {
            Ok(())
        }

        fn iterator_type(&self) -> IteratorType {
            IteratorType::Mock
        }

        fn release_index_handles(&mut self) {
            self.released.set(true);
        }
    }

    /// The source rides across the lock release untouched by the `Ref` split, so
    /// whatever index state it holds is only released because `suspend` asks.
    /// The case that makes this load-bearing is a VecSim batch iterator, which
    /// dereferences its index pointer when *freed* — so a suspended top-k that
    /// still owned one would fault on the drop path alone, never mind a read.
    #[test]
    fn suspend_asks_the_source_to_release_its_index_handles() {
        let released = std::rc::Rc::new(std::cell::Cell::new(false));
        let source = HandleTrackingSource {
            released: std::rc::Rc::clone(&released),
        };
        let it = Box::new(TopKIterator::<_, Empty>::new_with_mode(
            source,
            None,
            NonZeroUsize::new(1).unwrap(),
            f64::total_cmp,
            TopKMode::Unfiltered,
        ));

        assert!(!released.get(), "nothing released before the suspend");
        let _suspended = it.suspend();
        assert!(
            released.get(),
            "suspend must give the source the chance to drop index-backed state",
        );
    }

    /// A parked record that borrows index data must abort the resume.
    ///
    /// Invariant 2 rests on two things no compiler checks: that
    /// [`ScoreSource::build_result`] honours its obligation, and that no
    /// consumer installs an index-backed payload through the `&mut` handed out
    /// by [`current`](RQEIterator::current). Either can put a violating record
    /// in front of the resume boundary in a release build, where the
    /// suspend-side `debug_assert!` is compiled out — and re-narrowing it would
    /// assert `&'index` borrows into memory the lock release may have freed.
    #[test]
    fn resume_aborts_when_a_parked_record_borrows_index_data() {
        let mock_ctx = MockContext::new(0, 0);
        let guard = mock_ctx.spec_read();
        let mut it = Box::new(TopKIterator::<_, Empty>::new_with_mode(
            IndexBorrowingSource,
            None,
            NonZeroUsize::new(1).unwrap(),
            f64::total_cmp,
            TopKMode::Unfiltered,
        ));
        assert_eq!(it.read().unwrap().expect("expected a doc").doc_id, 1);

        // The offending record now sits in `current`, exactly where the yield
        // path put it. Park it aside across the suspend boundary: this is a
        // debug build, so the `debug_assert!` there would fire first, and what
        // this test pins is the *release-mode* guarantee — which only the
        // resume-side check provides.
        let offending = it.current.take();
        let mut suspended = it.suspend();
        *suspended.current = offending;

        let outcome = suspended
            .resume(&guard)
            .expect("an unsafe-to-resume state is recoverable, not an error");
        assert!(
            matches!(outcome, ResumeOutcome::Aborted),
            "a record borrowing index data must abort the resume",
        );
    }
}
