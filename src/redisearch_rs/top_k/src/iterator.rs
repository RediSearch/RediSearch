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

use index_result::{RSIndexResult, RawTermRecord};
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
    order::{Ascending, ScoreOrdering},
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
}

/// What collection keeps of each matching child record for the yield phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChildCapture {
    /// The child's full record, so relevance scoring sees its term records.
    Full,
    /// Only the child's yielded metrics (see
    /// [`with_trim_deep_results`](TopKIterator::with_trim_deep_results)).
    Metrics,
    /// Nothing: the source builds every yielded result itself (see
    /// [`ScoreSource::yields_child_record`]). Keeping no record also keeps no
    /// borrowed query term that a resume would have to refuse.
    Nothing,
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
/// `RawTopK<'query, Suspended, S, I::Suspended, O>` its suspended counterpart (see
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
///    *checked* — not assumed — by `survives_lock_release`, at both boundaries
///    that hand the records back after a lock release
///    ([`resume`](RQESuspendedIterator::resume) and the legacy
///    [`revalidate`](RQEIterator::revalidate)), which abort on violation.
///
///    Keeping the buffers [`Active`]-typed, instead of weakening them to the
///    `Rf` mode like the rest of the iterator family, is deliberate: the records
///    are our own collected output, so there is no index position to re-read
///    them from on resume, only the check above.
///
///    The borrowed `RSQueryTerm`s cannot be relied on across a suspension
///    either, for two reasons: the child's own resume may drop the leaf that
///    owns one (a union discards a child that aborted) while still reporting
///    itself unchanged, and suspending the child moves it by value, which
///    retags the leaf's `Box<RSQueryTerm>` and invalidates every reference
///    derived from it. [`resume`](RQESuspendedIterator::resume) therefore
///    aborts whenever a parked record borrows a query term, as detected by
///    `borrows_query_term`.
///
///    The legacy [`revalidate`](RQEIterator::revalidate) deliberately does
///    not: it runs on every re-acquisition of the lock, every cursor
///    continuation included, so aborting there would cut short every filtered
///    query that keeps rich results. It still shares the first hazard (a union
///    dropping an aborted child) until it is retired in favour of `resume`.
/// 3. **The heap is empty outside collection.** `collect` runs to completion
///    within a single [`read`](RQEIterator::read) — draining the heap into
///    `results` on success, clearing it on error — so every externally
///    observable state, suspension included, has an empty heap.
/// 4. **The source is not rebound across a suspension.** It is carried over
///    unchanged and may still hold index-backed state (a numeric range stream,
///    a VecSim index handle) that a concurrent writer or GC can invalidate.
///    Collection is the only phase that reads that state, so unless the source
///    opts in through [`ScoreSource::can_collect_after_lock_release`], a top-k
///    that has not collected yet aborts at both boundaries, and `source_stale`
///    makes a collection that a later [`rewind`](RQEIterator::rewind) would
///    start report EOF instead. The suspended form never asks the source anything:
///    its estimate comes from `suspended_estimate`.
#[repr(C)]
pub struct RawTopK<'query, Rf: Ref, S: ScoreSource, I, O: ScoreOrdering = Ascending> {
    source: S,
    mode: TopKMode,
    /// Preserved so [`rewind`](RQEIterator::rewind) can restore the original mode.
    initial_mode: TopKMode,
    /// Captured child records alias `child`, so `heap`, `results`, and `current`
    /// are [`ManuallyDrop`] and freed by the [`Drop`] impl before `child`.
    heap: ManuallyDrop<TopKHeap<'query, O>>,
    /// Holds the in-progress batch for the Unfiltered path.
    direct_batch: Option<S::Batch>,
    k: NonZeroUsize,
    order: O,
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
    /// Set once the iterator has crossed a lock release; survives
    /// [`rewind`](RQEIterator::rewind). See invariant 4.
    source_stale: bool,
    /// The source's estimate capped by `k`, recorded by
    /// [`suspend`](RQEIteratorBoxed::suspend) while the lock is still held, so
    /// the suspended [`num_estimated`](RQESuspendedIterator::num_estimated)
    /// never asks the source (invariant 4).
    suspended_estimate: usize,
    /// Diagnostic counters for the current evaluation. Cleared by
    /// [`rewind`](RQEIterator::rewind), but preserved when collection aborts on
    /// timeout so a timed-out profile still reports the work done.
    pub metrics: TopKMetrics,
    _marker: PhantomData<Rf>,
}

/// Alias for an [`Active`] [`RawTopK`] — the only instantiation with an
/// [`RQEIterator`] impl.
pub type TopKIterator<'index, S, C = Box<dyn RQEIterator<'index> + 'index>, O = Ascending> =
    RawTopK<'index, Active<'index>, S, C, O>;

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
    assert!(offset_of!(A, order) == offset_of!(S, order));
    assert!(offset_of!(A, can_trim_deep_results) == offset_of!(S, can_trim_deep_results));
    assert!(offset_of!(A, phase) == offset_of!(S, phase));
    assert!(offset_of!(A, results) == offset_of!(S, results));
    assert!(offset_of!(A, yield_pos) == offset_of!(S, yield_pos));
    assert!(offset_of!(A, current) == offset_of!(S, current));
    assert!(offset_of!(A, child) == offset_of!(S, child));
    assert!(offset_of!(A, last_doc_id) == offset_of!(S, last_doc_id));
    assert!(offset_of!(A, at_eof) == offset_of!(S, at_eof));
    assert!(offset_of!(A, source_stale) == offset_of!(S, source_stale));
    assert!(offset_of!(A, suspended_estimate) == offset_of!(S, suspended_estimate));
    assert!(offset_of!(A, metrics) == offset_of!(S, metrics));
    assert!(size_of::<A>() == size_of::<S>());
    assert!(align_of::<A>() == align_of::<S>());
};

impl<'query, Rf: Ref, S: ScoreSource, I, O: ScoreOrdering> Drop for RawTopK<'query, Rf, S, I, O> {
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

impl<'index, S: ScoreSource + 'index, O: ScoreOrdering>
    TopKIterator<'index, S, Box<dyn RQEIterator<'index> + 'index>, O>
{
    /// Create a new unfiltered [`TopKIterator`] (no child filter).
    ///
    /// Results are streamed directly from the source's batch — the heap is bypassed.
    /// Use [`new`](Self::new) when a filter child is present.
    pub fn new_unfiltered(source: S, k: NonZeroUsize, order: O) -> Self {
        Self::new_with_mode(source, None, k, order, TopKMode::Unfiltered)
    }
}

impl<'index, S: ScoreSource + 'index, C: RQEIterator<'index> + 'index, O: ScoreOrdering>
    TopKIterator<'index, S, C, O>
{
    /// Create a new [`TopKIterator`] with a filter child.
    ///
    /// The initial mode defaults to [`TopKMode::Batches`].
    pub fn new(source: S, child: C, k: NonZeroUsize, order: O) -> Self {
        Self::new_with_mode(source, Some(child), k, order, TopKMode::Batches)
    }

    /// Create a new [`TopKIterator`] with an explicit initial mode.
    pub fn new_with_mode(
        source: S,
        child: Option<C>,
        k: NonZeroUsize,
        order: O,
        mode: TopKMode,
    ) -> Self {
        Self {
            heap: ManuallyDrop::new(TopKHeap::new(k, order)),
            source,
            child: child.into(),
            mode,
            initial_mode: mode,
            direct_batch: None,
            k,
            order,
            can_trim_deep_results: false,
            phase: Phase::NotStarted,
            results: ManuallyDrop::new(Vec::new()),
            yield_pos: 0,
            current: ManuallyDrop::new(None),
            last_doc_id: 0,
            at_eof: false,
            source_stale: false,
            suspended_estimate: 0,
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

    /// What collection keeps of each matching child record.
    fn child_capture(&self) -> ChildCapture {
        if !self.source.yields_child_record() {
            ChildCapture::Nothing
        } else if self.can_trim_deep_results {
            ChildCapture::Metrics
        } else {
            ChildCapture::Full
        }
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
            // An aborted scan is not resumable: the collection paths append to the
            // heap without de-duping against it, so a second pass would re-admit
            // doc ids already held. Move to the yield phase instead, so what was
            // collected is still served and the iterator reaches EOF rather than
            // re-collecting on every subsequent read. Only `rewind` starts a new
            // scan. The source is rewound to release scan-scoped resources; the
            // yield path needs none of them.
            self.finalize_collection();
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
            let capture = self.child_capture();
            if let Some(child) = self.child.as_mut() {
                intersect_batch_with_child(child, &mut batch, &mut self.heap, capture)?;
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
                    *self.heap = TopKHeap::new(self.k, self.order);
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
        let capture = self.child_capture();
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
                // copy entirely.
                self.heap
                    .push_with_record_lazy(doc_id, score, || capture.apply(result));
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
        let old_heap = std::mem::replace(&mut *self.heap, TopKHeap::new(self.k, self.order));
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

impl<'index, S: ScoreSource + 'index, C: RQEIterator<'index> + 'index, O: ScoreOrdering>
    RQEIterator<'index> for TopKIterator<'index, S, C, O>
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
            if self.source_stale && !self.source.can_collect_after_lock_release() {
                // Invariant 4: collecting would read the source's view of the
                // index from before the lock release.
                self.at_eof = true;
                return Ok(None);
            }
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
        // Same decisions as `resume`; see `survives_lock_release`.
        if !self.survives_lock_release() {
            return Ok(RQEValidateStatus::Aborted);
        }
        // Otherwise only a child abort aborts us. Results come from our own
        // score-ordered buffer, so a moved child does not move our cursor:
        // collapse it to Ok.
        if let Some(child) = self.child.as_mut() {
            match child.revalidate(spec)? {
                RQEValidateStatus::Aborted => return Ok(RQEValidateStatus::Aborted),
                RQEValidateStatus::Ok | RQEValidateStatus::Moved { .. } => {}
            }
        }
        self.source_stale = true;
        Ok(RQEValidateStatus::Ok)
    }

    #[inline(always)]
    fn rewind(&mut self) {
        *self.heap = TopKHeap::new(self.k, self.order);
        self.results.clear();
        *self.current = None;
        self.metrics = TopKMetrics::default();
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

impl<'index, S: ScoreSource + 'index, I: RQEIteratorBoxed<'index>, O: ScoreOrdering + 'index>
    RQEIteratorBoxed<'index> for TopKIterator<'index, S, I, O>
{
    type Suspended = RawTopK<'index, Suspended, S, I::Suspended, O>;

    fn suspend(mut self: Box<Self>) -> Box<Self::Suspended> {
        // The `const _` proof of invariant 1 covers only a representative
        // instantiation; these fire at the generics actually used. The child
        // check is not left to `suspend_child_slot_in_place` because an
        // unfiltered top-k never calls it, yet `TopKChild<I>` still sizes with
        // `I`.
        const { assert_layout_compatible::<Self, Self::Suspended>() };
        const { assert_layout_compatible::<I, I::Suspended>() };

        // The source is the one part of us the `Ref` split does not describe: it
        // is carried across identically, so anything index-backed inside it
        // would outlive the read guard it was made under. Hand it the chance to
        // let go before the lock drops.
        self.suspended_estimate = self.k.get().min(self.source.num_estimated());
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
        // `RawTopK<'index, Suspended, S, I::Suspended, O>` — layout-identical to the
        // active form by invariant 1, checked for this instantiation by the
        // `const` assertions at the top of this function. `Box::from_raw` reuses
        // the same allocation, so every interior address survives the cycle.
        unsafe { Box::from_raw(raw.cast::<RawTopK<'index, Suspended, S, I::Suspended, O>>()) }
    }
}

impl<'query, S: ScoreSource + 'query, IS: RQESuspendedIterator<'query>, O: ScoreOrdering + 'query>
    RQESuspendedIterator<'query> for RawTopK<'query, Suspended, S, IS, O>
{
    type Resumed<'index>
        = RawTopK<'index, Active<'index>, S, IS::Resumed<'index>, O>
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

        // As on suspend.
        const { assert_layout_compatible::<Self, Self::Resumed<'index>>() };
        const { assert_layout_compatible::<IS::Resumed<'index>, IS>() };

        // Resume is the direction that *re-narrows*: the cast below asserts the
        // parked records are live `&'index` borrows again, so the invariants that
        // make that sound are checked here rather than assumed. These records
        // are our own collected output, not an index position we could re-read,
        // so on violation we refuse to reinterpret and abort — `Ok(Aborted)`,
        // not `Err`, since `RQEIteratorError` is for genuine `TimedOut`/`IoError`
        // failures. Checked on `&self`, before `Box::into_raw`, so a violation
        // simply drops the box.
        if !self.survives_lock_release() {
            return Ok(ResumeOutcome::Aborted);
        }
        // Resume only; see invariant 2 for why `revalidate` does not check it.
        if self.child.is_some() && self.buffers_borrow_query_terms() {
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
                // `RawTopK<'query, Suspended, S, IS, O>`; reclaim and drop it,
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
        // SAFETY: `RawTopK<'query, Suspended, S, IS, O>` and
        // `RawTopK<'index, Active<'index>, S, IS::Resumed<'index>, O>` are
        // layout-identical by invariant 1, checked for this instantiation by the
        // `const` assertions at the top of this function. Re-narrowing the
        // buffered records' `'query` to the shorter `'index` is sound because
        // invariant 2 was verified above: they hold no index-backed payload and
        // borrow no query term, and their remaining
        // query-pipeline borrows outlive `'index` by the `'query: 'index` bound.
        // `Box::from_raw` reuses the same allocation.
        let mut active = unsafe {
            Box::from_raw(raw.cast::<RawTopK<'index, Active<'index>, S, IS::Resumed<'index>, O>>())
        };

        // Always `Ok`, never `Moved`: our position is `yield_pos` into our own
        // score-ordered buffer, which the suspension left untouched, so the
        // document `current()` reports is still the one the caller last read.
        // `RQEIterator::revalidate` collapses a moved child the same way.
        active.source_stale = true;
        Ok(ResumeOutcome::Ok(active))
    }

    fn last_doc_id(&self) -> DocId {
        self.last_doc_id
    }

    fn num_estimated(&self) -> usize {
        let estimate = self.suspended_estimate;
        match self.child.as_ref() {
            Some(child) => estimate.min(child.num_estimated()),
            None => estimate,
        }
    }
}

impl<'query, Rf: Ref, S: ScoreSource, I, O: ScoreOrdering> RawTopK<'query, Rf, S, I, O> {
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

    /// Whether the state carried across a lock release can be handed back:
    /// invariants 2 and 4 hold.
    ///
    /// The shared decision point for both [`resume`](RQESuspendedIterator::resume)
    /// and the legacy [`revalidate`](RQEIterator::revalidate), so the two paths
    /// cannot drift; the one condition only `resume` adds is explained under
    /// invariant 2. The cost is `O(k)` plus each record's subtree, paid only
    /// when the lock was released.
    fn survives_lock_release(&self) -> bool {
        (self.phase != Phase::NotStarted || self.source.can_collect_after_lock_release())
            && self.buffers_carry_no_index_borrows()
    }

    /// Whether any record parked for yielding borrows a query term from the
    /// child subtree (see invariant 2).
    fn buffers_borrow_query_terms(&self) -> bool {
        self.results
            .iter()
            .filter_map(|entry| entry.record.as_ref())
            .chain(self.current.as_ref())
            .any(borrows_query_term)
    }
}

/// Whether `record`, or any record nested in it, borrows its query term from
/// another record rather than owning it.
///
/// A borrowed aggregate counts as borrowing: its entries cannot be inspected
/// without trusting the children they point at.
fn borrows_query_term(record: &RSIndexResult<'_>) -> bool {
    if let Some(term) = record.as_term() {
        return matches!(term, RawTermRecord::Owned { term: Some(_), .. });
    }
    record
        .as_aggregate()
        .is_some_and(|aggregate| match aggregate.as_owned() {
            Some(owned) => owned
                .records()
                .iter()
                .any(|child| borrows_query_term(child)),
            None => true,
        })
}

/// Whether `record` carries no index-backed payload, recursively — the property
/// invariant 2 on [`RawTopK`] rests on.
///
/// The index-backed payloads an [`Active`] result can hold are a term record's
/// borrowed offset slice and an aggregate's borrowed entries; a captured record
/// has neither, since [`RSIndexResult::to_owned`] copies the offsets and
/// deep-copies the children.
///
/// A non-null [`dmd`](RSIndexResult::dmd) also counts: [`RSIndexResult::to_owned`]
/// copies the pointer without taking a reference, so the document table may
/// free it once the lock is released. Other iterators leave `dmd` to the
/// consumer; top-k cannot, because its records outlive the read that produced
/// them.
fn carries_no_index_borrow(record: &RSIndexResult<'_>) -> bool {
    if !record.dmd.is_null() {
        return false;
    }
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

impl ChildCapture {
    /// The record to park for `record`, if any.
    fn apply<'index>(self, record: &RSIndexResult<'index>) -> Option<RSIndexResult<'index>> {
        match self {
            Self::Full => Some(capture_child_record(record)),
            Self::Metrics => Some(capture_child_metrics(record)),
            Self::Nothing => None,
        }
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
    // stay valid for `'index` while the iterator stays active; widening the
    // lifetime is therefore sound. A suspension ends that guarantee (see
    // invariant 2 on `RawTopK`), which is why `resume` refuses to hand such
    // records back.
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
fn intersect_batch_with_child<'index, C: RQEIterator<'index>, O: ScoreOrdering>(
    child: &mut C,
    batch: &mut impl ScoreBatch,
    heap: &mut TopKHeap<'index, O>,
    capture: ChildCapture,
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
        match batch_doc.cmp(&child_doc) {
            Ordering::Equal => {
                // Capture the matching child data only if the heap retains it,
                // before advancing past it, so the yield phase can return it
                // without re-reading the child. A discarded match skips the copy.
                heap.push_with_record_lazy(batch_doc, batch_score, || {
                    child.current().and_then(|r| capture.apply(r))
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
    ///
    /// Prefer `metrics` over any source-local equivalent: it spans the whole
    /// evaluation, whereas a source counter is cleared by every mid-evaluation
    /// source reset, including the one on the timeout path.
    fn print_profile(
        &self,
        mode: TopKMode,
        metrics: &TopKMetrics,
        map: &mut MapBuilder<'_>,
        ctx: &mut ProfilePrintCtx<'_>,
        child: Option<&dyn ProfilePrint>,
    );
}

impl<'index, S, C, O> ProfilePrint for TopKIterator<'index, S, C, O>
where
    S: ScoreSource + TopKSourceProfile + 'index,
    C: RQEIterator<'index> + ProfilePrint + 'index,
    O: ScoreOrdering,
{
    fn print_profile(&self, map: &mut MapBuilder<'_>, ctx: &mut ProfilePrintCtx<'_>) {
        let child = self.child.as_ref().map(|c| c as &dyn ProfilePrint);
        self.source
            .print_profile(self.mode, &self.metrics, map, ctx, child);
    }
}

impl<'index, S: ScoreSource + 'index, O: ScoreOrdering + 'index>
    rqe_iterators::interop::ProfileChildren<'index>
    for TopKIterator<'index, S, rqe_iterators::c2rust::CRQEIterator, O>
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
    use index_result::{MetricsVec, RSOffsetSlice, RSQueryTerm};
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
            assert!(
                !self.released.get(),
                "the estimate must not be asked of a released source",
            );
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
            Ascending,
            TopKMode::Unfiltered,
        ));

        assert!(!released.get(), "nothing released before the suspend");
        let _suspended = it.suspend();
        assert!(
            released.get(),
            "suspend must give the source the chance to drop index-backed state",
        );
    }

    /// The suspended estimate is read while the lock is released, when the
    /// source's index handles may dangle, so it must come from a value cached
    /// at suspension rather than from the source.
    #[test]
    fn suspended_estimate_does_not_ask_the_source() {
        let it = Box::new(TopKIterator::<_, Empty>::new_with_mode(
            HandleTrackingSource::default(),
            None,
            NonZeroUsize::new(3).unwrap(),
            Ascending,
            TopKMode::Unfiltered,
        ));
        let suspended = it.suspend();
        assert_eq!(RQESuspendedIterator::num_estimated(&*suspended), 1);
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
            Ascending,
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

    /// A term record as a leaf iterator yields it: owning its query term,
    /// borrowing its offsets from the index.
    fn leaf_term_record() -> RSIndexResult<'static> {
        RSIndexResult::build_term()
            .doc_id(1)
            .borrowed_record(
                Some(RSQueryTerm::new("term", 1, 0)),
                RSOffsetSlice::from_slice(&INDEX_OFFSETS),
            )
            .build()
    }

    #[test]
    fn term_records_are_classified_by_what_they_borrow() {
        let leaf = leaf_term_record();
        assert!(!carries_no_index_borrow(&leaf), "leaf borrows its offsets");
        assert!(!borrows_query_term(&leaf), "leaf owns its query term");

        // What `capture_child_record` parks: offsets copied, term borrowed.
        let captured = leaf.to_owned();
        assert!(carries_no_index_borrow(&captured));
        assert!(borrows_query_term(&captured));
    }

    #[test]
    fn borrowed_aggregate_is_rejected_by_both_checks() {
        let leaf = leaf_term_record();
        let mut union = RSIndexResult::build_union(1).doc_id(1).build();
        union.push_borrowed(&leaf, MetricsVec::new());

        assert!(!carries_no_index_borrow(&union));
        assert!(borrows_query_term(&union));
    }

    #[test]
    fn owned_aggregate_is_checked_recursively() {
        let leaf = leaf_term_record();
        let virt = RSIndexResult::build_virt().doc_id(1).build();

        let mut term_union = RSIndexResult::build_union(1).doc_id(1).build();
        term_union.push_borrowed(&leaf, MetricsVec::new());
        let term_union = term_union.to_owned();
        assert!(carries_no_index_borrow(&term_union));
        assert!(
            borrows_query_term(&term_union),
            "a nested captured term borrows its query term",
        );

        let mut virt_union = RSIndexResult::build_union(1).doc_id(1).build();
        virt_union.push_borrowed(&virt, MetricsVec::new());
        let mut virt_union = virt_union.to_owned();
        assert!(carries_no_index_borrow(&virt_union));
        assert!(!borrows_query_term(&virt_union));

        // An owned aggregate is only as index-free as its children.
        virt_union.push_boxed(Box::new(leaf_term_record()));
        assert!(!carries_no_index_borrow(&virt_union));
    }

    /// The legacy path hands the parked records back after the same lock
    /// release, so it applies the same index-borrow check as `resume`.
    #[test]
    fn revalidate_aborts_when_a_parked_record_borrows_index_data() {
        let mock_ctx = MockContext::new(0, 0);
        let mut it = TopKIterator::<_, Empty>::new_with_mode(
            IndexBorrowingSource,
            None,
            NonZeroUsize::new(1).unwrap(),
            Ascending,
            TopKMode::Unfiltered,
        );
        assert_eq!(it.read().unwrap().expect("expected a doc").doc_id, 1);

        let status = it.revalidate(&mock_ctx.spec_read()).unwrap();
        assert_eq!(status, RQEValidateStatus::Aborted);
    }

    #[test]
    fn document_metadata_pointer_is_an_index_borrow() {
        let mut record = RSIndexResult::build_virt().doc_id(1).build();
        assert!(carries_no_index_borrow(&record));

        // Never dereferenced: the check only looks at nullness.
        record.dmd = std::ptr::NonNull::dangling().as_ptr();
        assert!(!carries_no_index_borrow(&record));
    }
}
