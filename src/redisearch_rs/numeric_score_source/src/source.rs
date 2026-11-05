/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! [`NumericScoreSource`] — a [`ScoreSource`] that scores documents by a
//! numeric field, reading the field's value-ordered ranges directly from a
//! [`NumericRangeTree`].

use index_result::RSIndexResult;
use inverted_index::NumericFilter;

#[cfg(doc)]
use numeric_range_tree::NumericRangeTree;
use numeric_range_tree::RangeWindow;
use rqe_core::DocId;
use rqe_iterator_type::IteratorType;
use rqe_iterators::{
    ExpirationChecker, NoOpChecker, RQEIteratorError,
    utils::{NoTimeoutChecker, TimeoutContext},
};
use top_k::{BatchStrategy, ScoreSource};

use crate::range_iterator::{NumericRangeIterator, NumericTreeRef};
use crate::score_batch::NumericScoreBatch;

/// Reports whether a doc id still resolves to a live result document — one that
/// has not been deleted.
///
/// Deletion only; TTL expiry is a separate concern carried by an
/// [`ExpirationChecker`]. The numeric index keeps entries for deleted documents
/// until GC reclaims them, so the source drops them before they reach the top-k
/// heap: the bounded heap must hold `k` *valid* survivors, and a downstream drop
/// cannot retroactively admit the live document a stale entry displaced.
pub trait DocValidity {
    /// Returns `true` if `doc_id` still resolves to a valid result document.
    fn is_valid(&self, doc_id: DocId) -> bool;

    /// Fast-path gate: `false` lets the source skip filtering entirely.
    fn may_filter(&self) -> bool;
}

impl<F: Fn(DocId) -> bool> DocValidity for F {
    fn is_valid(&self, doc_id: DocId) -> bool {
        self(doc_id)
    }

    fn may_filter(&self) -> bool {
        true
    }
}

/// A [`DocValidity`] that treats every document as valid, for when no doc table
/// is available (the default) or validity filtering is not wanted.
#[derive(Clone, Copy, Default)]
pub struct AllValid;

impl DocValidity for AllValid {
    #[inline(always)]
    fn is_valid(&self, _doc_id: DocId) -> bool {
        true
    }

    #[inline(always)]
    fn may_filter(&self) -> bool {
        false
    }
}

/// Default number of value-ordered ranges materialized into a single batch.
///
/// Larger batches amortize the per-batch child rewind in the surrounding
/// [`TopKIterator`]'s `intersect_batch_with_child`; smaller batches tighten the
/// early-`Stop` granularity, since a batch boundary is also a score-bracket
/// boundary. Callers (chiefly tests) override it to force multi-batch behavior.
///
/// [`TopKIterator`]: top_k::TopKIterator
pub(crate) const DEFAULT_RANGE_BATCH_SIZE: usize = 8;

/// Maximum number of expand-and-retry iterations before the next retry reads
/// every remaining document.
const MAX_ITERATIONS: usize = 3;

/// Below this hit ratio for a consumed window, the next retry abandons
/// estimation and reads every remaining document.
const MIN_SUCCESS_RATIO: f64 = 0.01;

/// Scores each document by a numeric field, for top-k queries like
/// `SORTBY <numeric field>`.
///
/// A document's score is its value in the numeric field. The source asks the
/// [`NumericRangeTree`] for the field's ranges in score order (per `ascending`),
/// restricted to a [`RangeWindow`], and hands them to the surrounding
/// [`TopKIterator`] as doc-id-ordered batches. Because batches arrive
/// best-score-first, a full heap (`heap_count >= k`) means no later batch can
/// improve the result, so [`batch_strategy`](ScoreSource::batch_strategy) can
/// `Stop` early — the value-ordering is what makes that sound.
///
/// # Window retry
///
/// The initial window may be too small to fill the heap. When a window is
/// drained with `heap_count < k`,
/// [`batch_strategy`](ScoreSource::batch_strategy) advances the window to the
/// next disjoint one and returns [`BatchStrategy::ExpandWindow`]. The
/// windows are disjoint in value space, so the heap keeps accumulating the
/// global top `k` across them.
///
/// [`BatchStrategy::ExpandWindow`]: top_k::BatchStrategy::ExpandWindow
///
/// [`TopKIterator`]: top_k::TopKIterator
///
/// # Revalidation
///
/// Suspension releases borrowed ranges. Collection can resume only if the tree
/// revision is unchanged; the next batch resolves fresh ranges and refreshes
/// multivalue tracking. Already collected results need no tree access.
///
/// # Rewind after timeout
///
/// A timeout aborts collection with the heap and range stream left partway;
/// [`rewind`](ScoreSource::rewind) is what restores a clean, re-runnable state
/// (fresh heap, ranges re-resolved to the initial window). The query deadline is
/// absolute and is *not* reset by rewind.
///
/// Not implemented yet:
/// - TODO: MOD-14946 Profile metrics
/// - TODO: MOD-14947 Parity tests
/// - TODO: MOD-14948 Performance validation
pub struct NumericScoreSource<
    'index,
    V: DocValidity = AllValid,
    E: ExpirationChecker = NoOpChecker,
    T: TimeoutContext = NoTimeoutChecker,
> {
    scan: NumericScan<'index>,
    /// Drops deleted documents before they can occupy a top-k slot.
    validity: V,
    /// Checks sort-field TTL before heap insertion.
    expiration: E,
    /// Polls the query deadline during collection and yielding.
    timeout: T,
}

/// Scan state is independent of the query's validity, expiration and timeout
/// policies, so replacing a policy transfers this state unchanged.
struct NumericScan<'index> {
    /// Value-ordered range stream over the numeric index.
    ranges: NumericRangeIterator<'index>,
    /// The window the source started on, restored by [`rewind`](ScoreSource::rewind).
    initial_window: RangeWindow,
    /// Number of value-ordered ranges materialized per batch. Always `>= 1`.
    range_batch_size: usize,
    /// Window document estimate captured at construction, kept stable after the
    /// window is drained or expanded.
    num_estimated: usize,
    /// Whether the filtered expand-and-retry path is active.
    retry_enabled: bool,
    /// Total documents in the index, the selectivity denominator that sizes
    /// each retry window's estimated limit.
    num_docs: usize,
    /// Filter child's selectivity estimate, used to size the next window.
    child_estimate: usize,
    /// Limit of the window currently being consumed, the denominator of the
    /// success ratio.
    last_limit_estimate: usize,
    /// Heap count at the start of the current window, the baseline for counting
    /// hits collected from it.
    heap_old_size: usize,
    /// Number of expand-and-retry iterations performed so far.
    num_iterations: usize,
    /// Number of value-ordered range batches materialized so far, surfaced as
    /// the `Batches number` profile metric. Reset on rewind.
    num_batches: usize,
}

impl<'index> NumericScoreSource<'index> {
    /// Build a source for the unfiltered case (no filter child), with no retry.
    /// `filter` is the numeric range over the sort field that picks which ranges
    /// to read; its `offset`/`limit` window is ignored here and sized from `k`
    /// instead, by [`narrow_window_to_top_k`](Self::narrow_window_to_top_k).
    pub fn unfiltered(
        tree: impl Into<NumericTreeRef<'index>>,
        filter: NumericFilter,
        ascending: bool,
    ) -> Self {
        Self::with_range_batch_size(tree, filter, ascending, DEFAULT_RANGE_BATCH_SIZE)
    }

    /// Like [`unfiltered`](Self::unfiltered) but with an explicit per-batch range
    /// count, so tests can force multi-batch behavior on small inputs.
    pub fn with_range_batch_size(
        tree: impl Into<NumericTreeRef<'index>>,
        mut filter: NumericFilter,
        ascending: bool,
        range_batch_size: usize,
    ) -> Self {
        filter.ascending = ascending;
        Self::build(
            tree,
            filter,
            RangeWindow::UNBOUNDED,
            range_batch_size,
            0,
            0,
            false,
        )
    }

    /// Build a filtered source with the expand-and-retry path enabled.
    ///
    /// `window` is the initial slice of the value-ordered stream to read.
    /// `num_docs` is the total document count and `child_estimate` the filter
    /// child's selectivity estimate; both size the retry windows.
    pub fn filtered(
        tree: impl Into<NumericTreeRef<'index>>,
        mut filter: NumericFilter,
        window: RangeWindow,
        ascending: bool,
        range_batch_size: usize,
        num_docs: usize,
        child_estimate: usize,
    ) -> Self {
        filter.ascending = ascending;
        Self::build(
            tree,
            filter,
            window,
            range_batch_size,
            num_docs,
            child_estimate,
            true,
        )
    }

    fn build(
        tree: impl Into<NumericTreeRef<'index>>,
        filter: NumericFilter,
        window: RangeWindow,
        range_batch_size: usize,
        num_docs: usize,
        child_estimate: usize,
        retry_enabled: bool,
    ) -> Self {
        let mut ranges = NumericRangeIterator::new(tree, &filter, window);
        let num_estimated = ranges.total_docs_estimate();
        Self {
            scan: NumericScan {
                ranges,
                last_limit_estimate: window.limit,
                initial_window: window,
                range_batch_size: range_batch_size.max(1),
                num_estimated,
                retry_enabled,
                num_docs,
                child_estimate,
                heap_old_size: 0,
                num_iterations: 0,
                num_batches: 0,
            },
            validity: AllValid,
            expiration: NoOpChecker,
            timeout: NoTimeoutChecker,
        }
    }
}

impl<'index, V: DocValidity, E: ExpirationChecker, T: TimeoutContext>
    NumericScoreSource<'index, V, E, T>
{
    /// Attach a document-validity oracle. [`ScoreSource::accept_candidate`]
    /// rejects deleted documents after child intersection, before heap insertion.
    pub fn with_validity<V2: DocValidity>(
        self,
        validity: V2,
    ) -> NumericScoreSource<'index, V2, E, T> {
        NumericScoreSource {
            validity,
            expiration: self.expiration,
            timeout: self.timeout,
            scan: self.scan,
        }
    }

    /// Attach a field-level TTL checker. [`ScoreSource::accept_candidate`]
    /// rejects expired sort fields before they can occupy a top-k slot.
    pub fn with_expiration<E2: ExpirationChecker>(
        self,
        expiration: E2,
    ) -> NumericScoreSource<'index, V, E2, T> {
        NumericScoreSource {
            expiration,
            validity: self.validity,
            timeout: self.timeout,
            scan: self.scan,
        }
    }

    /// Attach a query-deadline poll, checked once per record during
    /// materialization and once per step during yielding. [`NoTimeoutChecker`] (the
    /// default) never times out.
    pub fn with_timeout<T2: TimeoutContext>(
        self,
        timeout: T2,
    ) -> NumericScoreSource<'index, V, E, T2> {
        NumericScoreSource {
            timeout,
            validity: self.validity,
            expiration: self.expiration,
            scan: self.scan,
        }
    }

    /// Sort direction the source reads in, for the heap comparator.
    pub fn ascending(&self) -> bool {
        self.scan.ranges.ascending()
    }

    /// Bound the initial window to the documents a top-`k` heap needs, and arm
    /// the retry path in case that window under-delivers.
    ///
    /// Only for a source with no filter child: every document read reaches the
    /// heap, so `k` of them fill it and the tree need only resolve the
    /// best-scored ranges covering `k` documents instead of the whole field.
    /// A validity or expiration drop can still leave the heap short, which the
    /// retry expansion then covers — with no child, the whole field is the
    /// candidate set, so that is also the selectivity denominator.
    pub fn narrow_window_to_top_k(&mut self, k: usize) {
        let scan = &mut self.scan;
        scan.initial_window = RangeWindow {
            offset: 0,
            limit: k,
        };
        scan.last_limit_estimate = k;
        scan.num_docs = scan.num_estimated;
        scan.child_estimate = scan.num_estimated;
        scan.retry_enabled = true;
        scan.ranges.refind(scan.initial_window);
    }

    /// Number of value-ordered range batches materialized so far, for the
    /// `Batches number` profile metric.
    pub fn num_batches(&self) -> usize {
        self.scan.num_batches
    }
}

impl NumericScan<'_> {
    /// Hit ratio of the window just consumed: results collected from it over the
    /// window's limit. Returns `0.0` when the limit is `0`, guarding the
    /// division.
    fn success_ratio(&self, heap_count: usize) -> f64 {
        if self.last_limit_estimate == 0 {
            return 0.0;
        }
        let collected = heap_count.saturating_sub(self.heap_old_size);
        collected as f64 / self.last_limit_estimate as f64
    }

    /// Advance the window past the drained one, size the next, and re-resolve
    /// the range stream onto it.
    ///
    /// Returns `true` if a new (disjoint) window was resolved, so the caller can
    /// return [`BatchStrategy::ExpandWindow`]; `false` if no expansion is
    /// possible (already unbounded, out of retries, or retry disabled), leaving
    /// the window untouched.
    ///
    /// [`BatchStrategy::ExpandWindow`]: top_k::BatchStrategy::ExpandWindow
    fn expand_window(&mut self, heap_count: usize, k: usize) -> bool {
        if !self.retry_enabled || self.num_iterations >= MAX_ITERATIONS {
            return false;
        }
        // A `limit == 0` window is unbounded and has already read every remaining
        // range, so there is nothing left to expand into.
        let mut window = self.ranges.window();
        if window.limit == 0 {
            return false;
        }

        window.offset += self.ranges.total_docs_estimate();
        let success_ratio = self.success_ratio(heap_count);

        if success_ratio < MIN_SUCCESS_RATIO || self.num_iterations + 1 >= MAX_ITERATIONS {
            // Hits are too sparse, or this is the last allowed retry: drop the
            // window limit so the retry reads every remaining range.
            window.limit = 0;
        } else {
            let results_missing = k.saturating_sub(heap_count);
            let estimate = estimate_limit(self.num_docs, self.child_estimate, results_missing);
            self.last_limit_estimate = ((estimate as f64) * success_ratio) as usize;
            window.limit = self.last_limit_estimate.max(1);
        }

        self.ranges.refind(window);

        // The advanced window resolved to no ranges: the tree is exhausted, so
        // report no expansion rather than a spurious empty one.
        if self.ranges.is_exhausted() {
            return false;
        }

        self.num_iterations += 1;
        self.heap_old_size = heap_count;
        true
    }
}

impl<'index, V: DocValidity, E: ExpirationChecker, T: TimeoutContext> ScoreSource
    for NumericScoreSource<'index, V, E, T>
{
    type Batch = NumericScoreBatch;

    fn next_batch(&mut self) -> Result<Option<Self::Batch>, RQEIteratorError> {
        // `ranges` and `timeout` are disjoint fields; the split borrow lets the
        // materialization loop poll the deadline once per record.
        let Some(batch) = self
            .scan
            .ranges
            .next_n(self.scan.range_batch_size, &mut self.timeout)?
        else {
            return Ok(None);
        };
        self.scan.num_batches += 1;
        Ok(Some(batch))
    }

    fn accept_candidate(&mut self, doc_id: DocId, score: f64) -> Result<bool, RQEIteratorError> {
        let filter_validity = self.validity.may_filter();
        let filter_expiration = self.expiration.has_expiration();
        if !filter_validity && !filter_expiration {
            return Ok(true);
        }
        self.timeout.check_timeout()?;
        Ok((!filter_validity || self.validity.is_valid(doc_id))
            && (!filter_expiration
                || !self
                    .expiration
                    .is_expired(&self.build_result(doc_id, score))))
    }

    fn lookup_score(&mut self, _doc_id: DocId) -> Option<f64> {
        // The numeric path never returns `SwitchToAdhoc`, so Adhoc-BF is never
        // entered and this is unreachable.
        unimplemented!("numeric top-k does not use Adhoc-BF")
    }

    fn num_estimated(&self) -> usize {
        self.scan.num_estimated
    }

    fn release_index_handles(&mut self) {
        self.scan.ranges.release_index_handles();
    }

    fn can_collect_after_lock_release(&self) -> bool {
        self.scan.ranges.can_collect_after_lock_release()
    }

    fn rewind(&mut self) {
        // Full reset to the initial window. The expand-and-retry path resolves
        // its own windows inside `expand_window`, so this is only reached for an
        // outer rewind that restarts the whole query.
        let scan = &mut self.scan;
        scan.last_limit_estimate = scan.initial_window.limit;
        scan.num_iterations = 0;
        scan.num_batches = 0;
        scan.heap_old_size = 0;
        scan.ranges.forget_emitted();
        scan.ranges.refind(scan.initial_window);
    }

    fn build_result<'r>(&self, doc_id: DocId, score: f64) -> RSIndexResult<'r>
    where
        Self: 'r,
    {
        RSIndexResult::build_numeric(score).doc_id(doc_id).build()
    }

    fn batch_strategy(&mut self, heap_count: usize, k: usize) -> BatchStrategy {
        // Batches arrive best-score-first, so a full heap can never be improved
        // by a later (worse-scored) batch.
        if heap_count >= k {
            return BatchStrategy::Stop;
        }
        // More ranges remain in this window: keep draining before deciding.
        if !self.scan.ranges.is_exhausted() {
            return BatchStrategy::Continue;
        }
        // Window drained with the heap still short of k: advance to the next
        // disjoint window if we can, otherwise `Continue` so `next_batch`
        // reports EOF and finalizes.
        if self.scan.expand_window(heap_count, k) {
            BatchStrategy::ExpandWindow
        } else {
            BatchStrategy::Continue
        }
    }

    fn iterator_type(&self) -> IteratorType {
        IteratorType::Optimus
    }

    fn check_timeout(&mut self) -> Result<(), RQEIteratorError> {
        // Materialization and candidate admission also poll during collection.
        self.timeout.check_timeout()
    }

    fn attach_score_metric<'r>(&self, _result: &mut RSIndexResult<'r>, _score: f64)
    where
        Self: 'r,
    {
        // Unused: the numeric field value is the ordering key and is carried by
        // the source-built result ([`yields_child_record`] is `false`), so the
        // iterator never takes the child-record yield path that would call this.
        // TODO: Production metric-key wiring is tracked by MOD-14946.
    }

    fn yields_child_record(&self) -> bool {
        // Numeric `SORTBY` orders purely by the field value the source supplies,
        // with no relevance scoring, so yield the source-built numeric result
        // rather than the child's record.
        false
    }
}

/// Estimate the window limit needed to collect `limit` more results, given the
/// child's selectivity (`estimate`/`num_docs`).
///
/// Returns `0` when `num_docs` or `estimate` is `0`, guarding the division.
pub(crate) fn estimate_limit(num_docs: usize, estimate: usize, limit: usize) -> usize {
    if num_docs == 0 || estimate == 0 {
        return 0;
    }
    let ratio = estimate as f64 / num_docs as f64;
    ((limit as f64 / ratio) as usize).saturating_add(1)
}
