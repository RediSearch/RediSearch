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

use std::ffi::CStr;

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
use top_k::{BatchStrategy, Offer, ScoreSource};

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
}

impl<F: Fn(DocId) -> bool> DocValidity for F {
    fn is_valid(&self, doc_id: DocId) -> bool {
        self(doc_id)
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
}

/// Which numeric-optimizer strategy the query plan runs, reported as the
/// `Optimizer mode` entry of an `FT.PROFILE` reply.
///
/// The strategy is fixed by the plan before the source exists, so the source only
/// reports it and never acts on it. Crosses the FFI boundary because no runtime
/// state recovers it: a filtered range whose window never widens is
/// [`PartialRange`](Self::PartialRange) despite having a child.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cheadergen::config(prefix_with_name, rename_all = "SCREAMING_SNAKE_CASE")]
pub enum NumericOptimizerMode {
    /// One bounded slice of the value-ordered stream is expected to satisfy the
    /// limit, so no window expansion is anticipated.
    PartialRange,
    /// The filter's selectivity is only estimated, so the window may have to
    /// expand before the limit is met.
    Hybrid,
}

impl NumericOptimizerMode {
    /// The string this mode is reported under in a profile reply.
    pub fn profile_name(self) -> &'static CStr {
        match self {
            Self::PartialRange => c"Query partial range",
            Self::Hybrid => c"Hybrid",
        }
    }
}

/// Default number of value-ordered ranges read as a single batch.
///
/// Larger batches amortize the per-batch child rewind in the surrounding
/// [`TopKIterator`]'s `intersect_batch_with_child`; smaller batches tighten the
/// early-`Stop` granularity, since a batch boundary is also a score-bracket
/// boundary. Tests override it via
/// [`with_range_batch_size`](NumericScoreSource::with_range_batch_size) to force
/// multi-batch behavior.
///
/// [`TopKIterator`]: top_k::TopKIterator
pub const DEFAULT_RANGE_BATCH_SIZE: usize = 8;

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
/// [`NumericRangeTree`] for the field's ranges in score order (per the filter's
/// `ascending`),
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
    /// Polls the query deadline once per candidate and once per step during
    /// yielding.
    timeout: T,
}

/// Callback streaming live document IDs for unfiltered missing-value
/// collection, stopping early as soon as the visitor returns `false`.
pub type UnscoredLiveDocsFn = Box<dyn Fn(usize, &mut dyn FnMut(DocId) -> bool)>;

/// Scan state is independent of the query's validity, expiration and timeout
/// policies, so replacing a policy transfers this state unchanged.
struct NumericScan<'index> {
    /// Value-ordered range stream over the numeric index.
    ranges: NumericRangeIterator<'index>,
    /// The window the source started on, restored by [`rewind`](ScoreSource::rewind).
    initial_window: RangeWindow,
    /// Number of value-ordered ranges read per batch. Always `>= 1`.
    range_batch_size: usize,
    /// Window document estimate captured at construction, kept stable after the
    /// window is drained or expanded.
    num_estimated: usize,
    /// Total documents in the index, the selectivity denominator that sizes
    /// each retry window's estimated limit.
    num_docs: usize,
    /// Heap count at the start of the current window, the baseline for counting
    /// hits collected from it.
    heap_old_size: usize,
    /// Number of expand-and-retry iterations performed so far.
    num_iterations: usize,
    /// Strategy reported as the profile's `Optimizer mode`.
    optimizer_mode: NumericOptimizerMode,
    /// When set, collect matches lacking the sort field once an unbounded walk
    /// finishes with `heap.len() < k`. For unfiltered (elided wildcard) walks,
    /// this enumerates live unscored document IDs directly from the doc table.
    unscored_live_docs: Option<UnscoredLiveDocsFn>,
}

impl<'index> NumericScoreSource<'index> {
    /// Build a source for the unfiltered case (no filter child), reading every
    /// range `filter` selects in its sort order.
    ///
    /// An unbounded window has nothing to expand into, so this source never
    /// retries; [`narrow_window_to_top_k`](Self::narrow_window_to_top_k) bounds it
    /// to `k` and arms the retry.
    pub fn unfiltered(tree: impl Into<NumericTreeRef<'index>>, filter: NumericFilter) -> Self {
        Self::filtered(tree, filter, RangeWindow::UNBOUNDED, 0)
    }

    /// Build a filtered source with the expand-and-retry path enabled.
    ///
    /// `window` is the initial slice of the value-ordered stream to read.
    /// `num_docs` is the total document count, which with each window's
    /// observed hit rate sizes the retry windows.
    pub fn filtered(
        tree: impl Into<NumericTreeRef<'index>>,
        filter: NumericFilter,
        window: RangeWindow,
        num_docs: usize,
    ) -> Self {
        let mut ranges = NumericRangeIterator::new(tree, &filter, window);
        let num_estimated = ranges.total_docs_estimate();
        Self {
            scan: NumericScan {
                ranges,
                initial_window: window,
                range_batch_size: DEFAULT_RANGE_BATCH_SIZE,
                num_estimated,
                num_docs,
                heap_old_size: 0,
                num_iterations: 0,
                optimizer_mode: NumericOptimizerMode::PartialRange,
                unscored_live_docs: None,
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
    /// Attach a document-validity oracle, dropping records for doc ids it reports
    /// invalid (deleted) from every batch. This is the
    /// source's equivalent of the numeric optimizer's per-document validity
    /// check, keeping entries that survive in the index until GC out of the
    /// top-k results.
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

    /// Attach a field-level TTL checker, dropping records whose sort field has
    /// expired from every batch. This mirrors the numeric optimizer feeding a
    /// field-expiration predicate to its numeric sub-iterator: the check is
    /// pre-heap so expired records never displace a live document from the
    /// bounded top-k.
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

    /// Attach a query-deadline poll, checked once per candidate and once per
    /// step during yielding. [`NoTimeoutChecker`] (the
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

    /// Set the strategy the profile reports.
    pub fn with_optimizer_mode(mut self, mode: NumericOptimizerMode) -> Self {
        self.scan.optimizer_mode = mode;
        self
    }

    /// Enable collecting matching documents that lack a value for the sort
    /// field when the walk finishes short of `k`, using `live_docs` to
    /// enumerate live unscored document IDs when no filter child is present.
    pub fn with_collect_missing(mut self, live_docs: UnscoredLiveDocsFn) -> Self {
        self.scan.unscored_live_docs = Some(live_docs);
        self
    }

    /// Read `n` value-ordered ranges per batch instead of
    /// [`DEFAULT_RANGE_BATCH_SIZE`]. A zero `n` reads one.
    pub fn with_range_batch_size(mut self, n: usize) -> Self {
        self.scan.range_batch_size = n.max(1);
        self
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
        scan.num_docs = scan.num_estimated;
        scan.ranges.refind(scan.initial_window);
    }

    /// Strategy reported as the profile's `Optimizer mode`.
    pub fn optimizer_mode(&self) -> NumericOptimizerMode {
        self.scan.optimizer_mode
    }
}

impl NumericScan<'_> {
    /// Hit ratio of the window just consumed: results collected from it over
    /// its non-zero `limit`.
    fn success_ratio(&self, heap_count: usize, limit: usize) -> f64 {
        let collected = heap_count.saturating_sub(self.heap_old_size);
        collected as f64 / limit as f64
    }

    /// Advance the window past the drained one, size the next, and re-resolve
    /// the range stream onto it.
    ///
    /// Returns `true` if a new (disjoint) window was resolved, so the caller can
    /// return [`BatchStrategy::ExpandWindow`]; `false` if no expansion is
    /// possible (already unbounded or out of retries), leaving the window
    /// untouched.
    ///
    /// [`BatchStrategy::ExpandWindow`]: top_k::BatchStrategy::ExpandWindow
    fn expand_window(&mut self, heap_count: usize, k: usize) -> bool {
        if self.num_iterations >= MAX_ITERATIONS {
            return false;
        }
        // A `limit == 0` window is unbounded and has already read every remaining
        // range, so there is nothing left to expand into.
        let mut window = self.ranges.window();
        if window.limit == 0 {
            return false;
        }

        let success_ratio = self.success_ratio(heap_count, window.limit);
        window.offset += self.ranges.total_docs_estimate();

        if success_ratio < MIN_SUCCESS_RATIO || self.num_iterations + 1 >= MAX_ITERATIONS {
            // Hits are too sparse, or this is the last allowed retry: drop the
            // window limit so the retry reads every remaining range.
            window.limit = 0;
        } else {
            // Feed the estimator the observed selectivity, as C does. A zero
            // estimate yields a zero, i.e. unbounded, window.
            let results_missing = k.saturating_sub(heap_count);
            let observed_estimate = (success_ratio * self.num_docs as f64) as usize;
            window.limit = estimate_limit(self.num_docs, observed_estimate, results_missing);
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
    type Batch = NumericScoreBatch<'index>;

    fn next_batch(&mut self) -> Result<Option<Self::Batch>, RQEIteratorError> {
        Ok(self.scan.ranges.next_n(self.scan.range_batch_size))
    }

    fn all_results_unfiltered_batch(&mut self) -> Result<Option<Self::Batch>, RQEIteratorError> {
        unimplemented!("numeric top-k batches borrow the index and only run in Batches mode")
    }

    fn accept_candidate(&mut self, doc_id: DocId, score: f64) -> Result<bool, RQEIteratorError> {
        self.timeout.check_timeout()?;
        // Drop stale entries pre-heap so they never displace a live document from
        // the bounded top-k: document deletion and field-level TTL, the two the
        // range tree only sheds at GC time. Missing-value candidates (`score.is_nan()`)
        // have no sort-field value (including documents whose sort field expired), so
        // field expiration does not exclude them.
        Ok(!self.scan.ranges.is_retained(doc_id)
            && self.validity.is_valid(doc_id)
            && (score.is_nan()
                || !(self.expiration.has_expiration()
                    && self
                        .expiration
                        .is_expired(&self.build_result(doc_id, score)))))
    }

    fn admitted(&mut self, doc_id: DocId, evicted: Option<DocId>) {
        self.scan.ranges.record_admission(doc_id, evicted);
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

    fn collects_unscored_on_shortfall(&self) -> bool {
        self.scan.unscored_live_docs.is_some()
    }

    fn for_each_unscored_live_doc(
        &mut self,
        heap_count: usize,
        is_valued: &dyn Fn(DocId) -> bool,
        on_accepted: &mut dyn FnMut(DocId) -> Offer,
    ) -> Result<(), RQEIteratorError> {
        let Some(streamer) = self.scan.unscored_live_docs.take() else {
            return Ok(());
        };
        let mut err: Option<RQEIteratorError> = None;
        streamer(heap_count, &mut |doc_id| {
            if is_valued(doc_id) {
                return true;
            }
            match self.accept_candidate(doc_id, f64::NAN) {
                Ok(true) => {
                    if let Offer::Inserted { evicted } = on_accepted(doc_id) {
                        self.admitted(doc_id, evicted);
                    }
                    true
                }
                Ok(false) => true,
                Err(e) => {
                    err = Some(e);
                    false
                }
            }
        });
        self.scan.unscored_live_docs = Some(streamer);
        match err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    fn rewind(&mut self) {
        // Full reset to the initial window. The expand-and-retry path resolves
        // its own windows inside `expand_window`, so this is only reached for an
        // outer rewind that restarts the whole query.
        let scan = &mut self.scan;
        scan.num_iterations = 0;
        scan.heap_old_size = 0;
        scan.ranges.forget_retained();
        scan.ranges.refind(scan.initial_window);
    }

    fn build_result<'r>(&self, doc_id: DocId, score: f64) -> RSIndexResult<'r>
    where
        Self: 'r,
    {
        let score = if score.is_nan() { 0.0 } else { score };
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

/// Estimate the window limit needed to collect `limit` more results at a
/// selectivity of `estimate`/`num_docs`.
///
/// Returns `0` when `num_docs` or `estimate` is `0`, guarding the division.
pub(crate) fn estimate_limit(num_docs: usize, estimate: usize, limit: usize) -> usize {
    if num_docs == 0 || estimate == 0 {
        return 0;
    }
    let ratio = estimate as f64 / num_docs as f64;
    ((limit as f64 / ratio) as usize).saturating_add(1)
}

#[cfg(test)]
mod tests {
    use inverted_index::NumericFilter;
    use numeric_range_tree::{NumericRangeTree, RangeWindow};
    use top_k::{BatchStrategy, ScoreSource};

    use super::NumericScoreSource;

    /// Drain a 100-doc first window over a 1000-doc tree, report `collected` of
    /// `k` results, and return the retry window's limit.
    fn retry_window_limit(num_docs: usize, collected: usize, k: usize) -> usize {
        let mut tree = NumericRangeTree::new(false);
        for id in 1..=1000 {
            tree.add(id, id as f64, false, false, 0);
        }
        let window = RangeWindow {
            offset: 0,
            limit: 100,
        };
        let mut source =
            NumericScoreSource::filtered(&tree, NumericFilter::default(), window, num_docs)
                .with_range_batch_size(usize::MAX);
        while source.next_batch().unwrap().is_some() {}
        assert_eq!(
            source.batch_strategy(collected, k),
            BatchStrategy::ExpandWindow
        );
        source.scan.ranges.window().limit
    }

    #[test]
    #[cfg_attr(miri, ignore = "Too slow to run under miri")]
    fn retry_window_holds_the_missing_results_at_the_observed_hit_rate() {
        // 20 missing results at a 10% hit rate.
        assert_eq!(retry_window_limit(1000, 10, 30), 201);
    }

    #[test]
    #[cfg_attr(miri, ignore = "Too slow to run under miri")]
    fn retry_window_is_unbounded_when_the_observed_estimate_truncates_to_zero() {
        assert_eq!(retry_window_limit(40, 2, 30), 0);
    }
}
