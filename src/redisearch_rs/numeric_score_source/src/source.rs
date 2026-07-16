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
use numeric_range_tree::NumericRangeTree;
use rqe_core::DocId;
use rqe_iterator_type::IteratorType;
use rqe_iterators::{
    ExpirationChecker, NoOpChecker, RQEIteratorError,
    utils::{NoTimeout, TimeoutContext},
};
use top_k::{BatchStrategy, ScoreSource};

use crate::range_iterator::NumericRangeIterator;
use crate::score_batch::NumericScoreBatch;

/// Reports whether a doc id still resolves to a live result document — one that
/// has not been deleted and whose whole-document TTL has not lapsed.
///
/// This is the document-level check (deletion + whole-doc expiry); *field*-level
/// TTL is a separate concern carried by an [`ExpirationChecker`]. The numeric
/// index keeps entries for stale documents until GC reclaims them, so the source
/// drops them before they reach the top-k heap. This mirrors the result
/// processor's per-document validity check: the numeric optimizer's bounded heap
/// must hold `k` *valid* survivors, and a downstream drop cannot retroactively
/// admit the live document a stale entry displaced.
pub trait DocValidity {
    /// Returns `true` if `doc_id` still resolves to a valid result document.
    fn is_valid(&self, doc_id: DocId) -> bool;

    /// Fast-path gate: `false` lets the source skip filtering entirely.
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
const DEFAULT_RANGE_BATCH_SIZE: usize = 8;

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
/// windowed by the filter's `offset`/`limit`, and hands them to the surrounding
/// [`TopKIterator`] as doc-id-ordered batches. Because batches arrive
/// best-score-first, a full heap (`heap_count >= k`) means no later batch can
/// improve the result, so [`batch_strategy`](ScoreSource::batch_strategy) can
/// `Stop` early — the value-ordering is what makes that sound.
///
/// # Filtered retry
///
/// With a filter child, the initial window may be too small to fill the heap.
/// When a window is drained with `heap_count < k`,
/// [`batch_strategy`](ScoreSource::batch_strategy) advances `offset`/`limit` to
/// the next disjoint window and returns [`BatchStrategy::ExpandWindow`]. The
/// windows are disjoint in value space, so the heap keeps accumulating the
/// global top `k` across them. The unfiltered path reads an unbounded window and
/// never retries.
///
/// [`BatchStrategy::ExpandWindow`]: top_k::BatchStrategy::ExpandWindow
///
/// [`TopKIterator`]: top_k::TopKIterator
///
/// # Revalidation
///
/// The source has no revalidate step of its own: the wrapping [`TopKIterator`]
/// collects eagerly and then yields from a materialized buffer, so once results
/// are collected the range stream is never read again. A revalidate therefore
/// only consults the filter child — a child abort aborts the query, and a child
/// reposition collapses to `Ok` because it cannot move a cursor that reads from
/// the buffer.
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
    T: TimeoutContext = NoTimeout,
> {
    /// Value-ordered range stream over the numeric index.
    ranges: NumericRangeIterator<'index>,
    /// Filter driving [`find`](NumericRangeTree::find); its `offset`/`limit`
    /// window is mutated in place by the retry expansion.
    filter: NumericFilter,
    /// Initial `offset`/`limit` of `filter`, restored by [`rewind`](ScoreSource::rewind).
    initial_offset: usize,
    initial_limit: usize,
    /// Sort direction (`SORTBY field ASC`/`DESC`), also written into `filter`.
    ascending: bool,
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
    /// Document-level validity oracle: drops records for doc ids it reports
    /// invalid (deleted or whole-doc expired) before they reach the top-k heap.
    /// [`AllValid`] keeps every record.
    validity: V,
    /// Field-level TTL checker: drops records whose sort field has expired before
    /// they reach the top-k heap, matching the optimizer's numeric field
    /// predicate. [`NoOpChecker`] keeps every record.
    expiration: E,
    /// Query-deadline poll, checked once per record during batch materialization
    /// and once per step during yielding. [`NoTimeout`] never times out.
    timeout: T,
}

impl<'index> NumericScoreSource<'index> {
    /// Build a source for the unfiltered case (no filter child): an unbounded
    /// value-ordered window with no retry. `filter` is the numeric range over
    /// the sort field that picks which ranges to read; its `offset`/`limit`
    /// window is ignored here, since `LIMIT k` is served by the heap plus the
    /// early `Stop`.
    pub fn unfiltered(
        tree: &'index NumericRangeTree,
        filter: NumericFilter,
        ascending: bool,
    ) -> Self {
        Self::with_range_batch_size(tree, filter, ascending, DEFAULT_RANGE_BATCH_SIZE)
    }

    /// Like [`unfiltered`](Self::unfiltered) but with an explicit per-batch range
    /// count, so tests can force multi-batch behavior on small inputs.
    pub fn with_range_batch_size(
        tree: &'index NumericRangeTree,
        mut filter: NumericFilter,
        ascending: bool,
        range_batch_size: usize,
    ) -> Self {
        filter.ascending = ascending;
        filter.limit = 0;
        filter.offset = 0;
        Self::build(tree, filter, ascending, range_batch_size, 0, 0, false)
    }

    /// Build a filtered source with the expand-and-retry path enabled.
    ///
    /// `num_docs` is the total document count and `child_estimate` the filter
    /// child's selectivity estimate; both size the retry windows. `filter.limit`
    /// is the initial window size.
    pub fn filtered(
        tree: &'index NumericRangeTree,
        mut filter: NumericFilter,
        ascending: bool,
        range_batch_size: usize,
        num_docs: usize,
        child_estimate: usize,
    ) -> Self {
        filter.ascending = ascending;
        Self::build(
            tree,
            filter,
            ascending,
            range_batch_size,
            num_docs,
            child_estimate,
            true,
        )
    }

    fn build(
        tree: &'index NumericRangeTree,
        filter: NumericFilter,
        ascending: bool,
        range_batch_size: usize,
        num_docs: usize,
        child_estimate: usize,
        retry_enabled: bool,
    ) -> Self {
        let ranges = NumericRangeIterator::new(tree, &filter);
        let num_estimated = ranges.total_docs_estimate();
        Self {
            ranges,
            last_limit_estimate: filter.limit,
            initial_offset: filter.offset,
            initial_limit: filter.limit,
            filter,
            ascending,
            range_batch_size: range_batch_size.max(1),
            num_estimated,
            retry_enabled,
            num_docs,
            child_estimate,
            heap_old_size: 0,
            num_iterations: 0,
            validity: AllValid,
            expiration: NoOpChecker,
            timeout: NoTimeout,
        }
    }
}

impl<'index, V: DocValidity, E: ExpirationChecker, T: TimeoutContext>
    NumericScoreSource<'index, V, E, T>
{
    /// Attach a document-validity oracle, dropping records for doc ids it reports
    /// invalid (deleted or whole-doc expired) from every batch. This is the
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
            ranges: self.ranges,
            filter: self.filter,
            initial_offset: self.initial_offset,
            initial_limit: self.initial_limit,
            ascending: self.ascending,
            range_batch_size: self.range_batch_size,
            num_estimated: self.num_estimated,
            retry_enabled: self.retry_enabled,
            num_docs: self.num_docs,
            child_estimate: self.child_estimate,
            last_limit_estimate: self.last_limit_estimate,
            heap_old_size: self.heap_old_size,
            num_iterations: self.num_iterations,
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
            ranges: self.ranges,
            filter: self.filter,
            initial_offset: self.initial_offset,
            initial_limit: self.initial_limit,
            ascending: self.ascending,
            range_batch_size: self.range_batch_size,
            num_estimated: self.num_estimated,
            retry_enabled: self.retry_enabled,
            num_docs: self.num_docs,
            child_estimate: self.child_estimate,
            last_limit_estimate: self.last_limit_estimate,
            heap_old_size: self.heap_old_size,
            num_iterations: self.num_iterations,
        }
    }

    /// Attach a query-deadline poll, checked once per record during
    /// materialization and once per step during yielding. [`NoTimeout`] (the
    /// default) never times out.
    pub fn with_timeout<T2: TimeoutContext>(
        self,
        timeout: T2,
    ) -> NumericScoreSource<'index, V, E, T2> {
        NumericScoreSource {
            timeout,
            validity: self.validity,
            expiration: self.expiration,
            ranges: self.ranges,
            filter: self.filter,
            initial_offset: self.initial_offset,
            initial_limit: self.initial_limit,
            ascending: self.ascending,
            range_batch_size: self.range_batch_size,
            num_estimated: self.num_estimated,
            retry_enabled: self.retry_enabled,
            num_docs: self.num_docs,
            child_estimate: self.child_estimate,
            last_limit_estimate: self.last_limit_estimate,
            heap_old_size: self.heap_old_size,
            num_iterations: self.num_iterations,
        }
    }

    /// Sort direction the source reads in, for the heap comparator.
    pub fn ascending(&self) -> bool {
        self.ascending
    }

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
        if self.filter.limit == 0 {
            return false;
        }

        self.filter.offset += self.ranges.total_docs_estimate();
        let success_ratio = self.success_ratio(heap_count);

        if success_ratio < MIN_SUCCESS_RATIO || self.num_iterations + 1 >= MAX_ITERATIONS {
            // Hits are too sparse, or this is the last allowed retry: drop the
            // window limit so the retry reads every remaining range.
            self.filter.limit = 0;
        } else {
            let results_missing = k.saturating_sub(heap_count);
            let estimate = estimate_limit(self.num_docs, self.child_estimate, results_missing);
            self.last_limit_estimate = ((estimate as f64) * success_ratio) as usize;
            self.filter.limit = self.last_limit_estimate.max(1);
        }

        self.ranges.refind(&self.filter);

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
            .ranges
            .next_n(self.range_batch_size, &mut self.timeout)?
        else {
            return Ok(None);
        };
        // Drop stale entries pre-heap so they never displace a live document from
        // the bounded top-k: document-level validity (deletion, whole-doc expiry)
        // and field-level TTL, the two the range tree only sheds at GC time. Each
        // gate keeps the common no-filtering case free of its per-record check.
        let filter_validity = self.validity.may_filter();
        let filter_expiration = self.expiration.has_expiration();
        if filter_validity || filter_expiration {
            batch.retain(|doc_id, score| {
                if filter_validity && !self.validity.is_valid(doc_id) {
                    return false;
                }
                if filter_expiration {
                    let record = RSIndexResult::build_numeric(score).doc_id(doc_id).build();
                    if self.expiration.is_expired(&record) {
                        return false;
                    }
                }
                true
            });
        }
        Ok(Some(batch))
    }

    fn lookup_score(&mut self, _doc_id: DocId) -> Option<f64> {
        // The numeric path never returns `SwitchToAdhoc`, so Adhoc-BF is never
        // entered and this is unreachable.
        unimplemented!("numeric top-k does not use Adhoc-BF")
    }

    fn num_estimated(&self) -> usize {
        self.num_estimated
    }

    fn rewind(&mut self) {
        // Full reset to the initial window. The expand-and-retry path resolves
        // its own windows inside `expand_window`, so this is only reached for an
        // outer rewind that restarts the whole query.
        self.filter.offset = self.initial_offset;
        self.filter.limit = self.initial_limit;
        self.last_limit_estimate = self.initial_limit;
        self.num_iterations = 0;
        self.heap_old_size = 0;
        self.ranges.forget_emitted();
        self.ranges.refind(&self.filter);
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
        if !self.ranges.is_exhausted() {
            return BatchStrategy::Continue;
        }
        // Window drained with the heap still short of k: advance to the next
        // disjoint window if we can, otherwise `Continue` so `next_batch`
        // reports EOF and finalizes.
        if self.expand_window(heap_count, k) {
            BatchStrategy::ExpandWindow
        } else {
            BatchStrategy::Continue
        }
    }

    fn iterator_type(&self) -> IteratorType {
        IteratorType::Optimus
    }

    fn check_timeout(&mut self) -> Result<(), RQEIteratorError> {
        // Yielding-phase hook. Collection self-checks per record via `next_batch`,
        // because the surrounding `TopKIterator` collects eagerly and only polls
        // this during yielding.
        self.timeout.check_timeout()
    }
}

/// Estimate the window limit needed to collect `limit` more results, given the
/// child's selectivity (`estimate`/`num_docs`).
///
/// Returns `0` when `num_docs` or `estimate` is `0`, guarding the division.
fn estimate_limit(num_docs: usize, estimate: usize, limit: usize) -> usize {
    if num_docs == 0 || estimate == 0 {
        return 0;
    }
    let ratio = estimate as f64 / num_docs as f64;
    (limit as f64 / ratio) as usize + 1
}
