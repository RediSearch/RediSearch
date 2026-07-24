/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Tests for [`NumericScoreSource`], driven by real [`NumericRangeTree`]
//! fixtures.

use std::cell::Cell;
use std::collections::HashSet;
use std::rc::Rc;
use std::time::Duration;
use std::{iter, num::NonZeroUsize};

use index_result::RSIndexResult;
use inverted_index::NumericFilter;
use numeric_range_tree::NumericRangeTree;
use numeric_range_tree::test_utils::build_tree;
use numeric_score_source::{
    DocValidity, NumericScoreSource, new_numeric_top_k_filtered, new_numeric_top_k_unfiltered,
};
use rqe_core::DocId;
use rqe_iterators::{
    ExpirationChecker, IdList, RQEIterator, RQEIteratorError,
    utils::{NoTimeout, TimeoutContext, TimeoutContextClock},
};
use top_k::{ScoreBatch, ScoreSource};

/// A validity oracle backed by an explicit set of deleted doc ids, standing in
/// for a doc table with entries flagged deleted but not yet reclaimed by GC.
struct DeletedDocs(HashSet<DocId>);

impl DocValidity for DeletedDocs {
    fn is_valid(&self, doc_id: DocId) -> bool {
        !self.0.contains(&doc_id)
    }

    fn may_filter(&self) -> bool {
        !self.0.is_empty()
    }
}

impl FromIterator<DocId> for DeletedDocs {
    fn from_iter<I: IntoIterator<Item = DocId>>(ids: I) -> Self {
        Self(ids.into_iter().collect())
    }
}

/// A field-TTL checker backed by an explicit set of doc ids, standing in for a
/// TTL table whose entries have lapsed but not yet been reclaimed by GC.
struct ExpiredDocs(HashSet<DocId>);

impl ExpirationChecker for ExpiredDocs {
    fn has_expiration(&self) -> bool {
        !self.0.is_empty()
    }

    fn is_expired(&self, result: &RSIndexResult) -> bool {
        self.0.contains(&result.doc_id)
    }
}

impl FromIterator<DocId> for ExpiredDocs {
    fn from_iter<I: IntoIterator<Item = DocId>>(ids: I) -> Self {
        Self(ids.into_iter().collect())
    }
}

/// Build a numeric tree from `(doc_id, value)` pairs (added in the given order).
///
/// Few distinct values stay a single leaf; use [`build_tree`] for a multi-leaf
/// tree with `doc_id == value == i`.
fn tree_from(pairs: &[(u64, f64)]) -> NumericRangeTree {
    let mut tree = NumericRangeTree::new(false);
    for &(id, value) in pairs {
        tree.add(id, value, false, 0);
    }
    tree
}

/// Whole-field filter: matches every value, ascending.
///
/// `NumericFilter::default()` bounds the window at `0.0..f64::MAX`, which drops
/// negatives and `+inf`; the `±inf` bounds here match the true whole-field span.
fn full_range() -> NumericFilter {
    NumericFilter {
        min: f64::NEG_INFINITY,
        max: f64::INFINITY,
        ..NumericFilter::default()
    }
}

/// Drain a batch into a `Vec`.
fn drain<B: ScoreBatch>(mut batch: B) -> Vec<(DocId, f64)> {
    iter::from_fn(|| batch.next()).collect()
}

/// Drive an unfiltered top-k iterator to exhaustion, collecting the yielded
/// `(doc_id, score)` pairs.
fn run_unfiltered(pairs: &[(u64, f64)], k: usize, ascending: bool) -> Vec<(DocId, f64)> {
    let tree = tree_from(pairs);
    let source = NumericScoreSource::unfiltered(&tree, full_range(), ascending);
    let mut it = new_numeric_top_k_unfiltered(source, NonZeroUsize::new(k).unwrap());
    let mut got = Vec::new();
    while let Some(result) = it.read().unwrap() {
        got.push((result.doc_id, result.as_numeric().expect("numeric result")));
    }
    got
}

/// Drive a filtered top-k iterator to exhaustion. The child filter passes
/// exactly `child_ids`.
fn run_filtered(
    pairs: &[(u64, f64)],
    child_ids: Vec<DocId>,
    range_batch_size: usize,
    k: usize,
    ascending: bool,
) -> Vec<(DocId, f64)> {
    let tree = tree_from(pairs);
    let child_estimate = child_ids.len();
    let source = NumericScoreSource::filtered(
        &tree,
        full_range(),
        ascending,
        range_batch_size,
        pairs.len(),
        child_estimate,
    );
    let mut it = new_numeric_top_k_filtered(
        source,
        IdList::<true>::new(child_ids),
        NonZeroUsize::new(k).unwrap(),
    );
    let mut got = Vec::new();
    while let Some(result) = it.read().unwrap() {
        got.push((result.doc_id, result.as_numeric().expect("numeric result")));
    }
    got
}

#[test]
fn unfiltered_caps_at_k_descending() {
    // More records than k: LIMIT k returns the k highest values (DESC),
    // best-first.
    let got = run_unfiltered(
        &[(1, 5.0), (2, 1.0), (3, 4.0), (4, 2.0), (5, 3.0)],
        3,
        false,
    );
    assert_eq!(got, vec![(1, 5.0), (3, 4.0), (5, 3.0)]);
}

#[test]
fn unfiltered_caps_at_k_ascending() {
    // Same records, ASC: the k lowest values, best-first.
    let got = run_unfiltered(&[(1, 5.0), (2, 1.0), (3, 4.0), (4, 2.0), (5, 3.0)], 3, true);
    assert_eq!(got, vec![(2, 1.0), (4, 2.0), (5, 3.0)]);
}

#[test]
fn unfiltered_includes_negative_values() {
    // A whole-field sort must score values below zero; the `0.0..f64::MAX`
    // default bounds would drop them before the heap.
    let got = run_unfiltered(&[(1, -5.0), (2, 2.0), (3, -1.0), (4, 0.0)], 4, true);
    assert_eq!(got, vec![(1, -5.0), (3, -1.0), (4, 0.0), (2, 2.0)]);
}

#[test]
fn unfiltered_fewer_than_k_yields_all() {
    let got = run_unfiltered(&[(1, 3.0), (2, 1.0)], 5, false);
    assert_eq!(got, vec![(1, 3.0), (2, 1.0)]);
}

#[test]
fn empty_source_yields_no_batch() {
    let tree = tree_from(&[]);
    let mut source = NumericScoreSource::unfiltered(&tree, full_range(), false);
    assert!(source.next_batch().unwrap().is_none());
}

#[test]
fn filtered_top_k_finds_high_score_among_matches() {
    // The best-scoring match (doc 4) must win even though lower-scored docs also
    // pass the filter. A premature stop would wrongly return doc 2.
    let pairs = [(1, 1.0), (2, 2.0), (3, 3.0), (4, 100.0), (5, 5.0)];
    let got = run_filtered(&pairs, vec![2, 4], 1, 1, false);
    assert_eq!(got, vec![(4, 100.0)]);
}

#[test]
fn next_batch_yields_ranges_best_score_first_descending() {
    // 20 distinct values produce a multi-leaf tree; doc_id == value == i.
    let tree = build_tree(20, false, 0);
    assert!(tree.num_leaves() > 1, "fixture must split into many ranges");

    // One range per batch, descending: each batch's scores must all be >= the
    // next batch's, and be doc-id-sorted within the batch.
    let mut source = NumericScoreSource::with_range_batch_size(&tree, full_range(), false, 1);
    let b1 = drain(source.next_batch().unwrap().expect("first batch"));
    let b2 = drain(source.next_batch().unwrap().expect("second batch"));

    let min1 = b1.iter().map(|&(_, s)| s).fold(f64::MAX, f64::min);
    let max2 = b2.iter().map(|&(_, s)| s).fold(f64::MIN, f64::max);
    assert!(
        min1 >= max2,
        "DESC: batch1 scores must all be >= batch2 scores"
    );
    assert!(
        b1.windows(2).all(|w| w[0].0 < w[1].0),
        "doc ids within a batch must be strictly increasing"
    );
}

#[test]
fn filtered_retry_expands_window_to_reach_low_scored_match() {
    // Multi-leaf tree, doc_id == value == i. DESC, k=1, and the child matches
    // only the lowest-valued doc — which sits in the last (worst-scored) range.
    // A tiny initial window can't reach it, so the source must expand and retry.
    let tree = build_tree(20, false, 0);
    assert!(tree.num_leaves() > 1, "fixture must split into many ranges");

    let mut filter = full_range();
    filter.limit = 1;
    let source = NumericScoreSource::filtered(&tree, filter, false, 1, 20, 1);
    let mut it = new_numeric_top_k_filtered(
        source,
        IdList::<true>::new(vec![1u64]),
        NonZeroUsize::new(1).unwrap(),
    );

    let mut got = Vec::new();
    while let Some(result) = it.read().unwrap() {
        got.push((result.doc_id, result.as_numeric().expect("numeric result")));
    }
    assert_eq!(got, vec![(1, 1.0)]);
}

#[test]
fn filtered_retry_reaches_match_past_multivalue_inflated_ranges() {
    // Numeric top-k over a multivalue field keeps a low-valued match when a
    // selective filter forces the window to expand into low ranges.
    //
    // A multivalue field indexes one entry per value, so the per-range count
    // `find` totals over a window exceeds the unique document count. Four filler
    // docs interleave values across the whole high span, landing each in every
    // high leaf; the summed count runs well past the five unique docs. DESC,
    // k=1, and the only child match sits alone in the lowest leaf.
    let mut tree = NumericRangeTree::new(false);
    for d in 1..=4u64 {
        for j in 0..100u64 {
            tree.add(d, 1000.0 + (j as f64) * 100.0 + (d as f64), true, 0);
        }
    }
    let match_id = 99999u64;
    tree.add(match_id, 0.5, false, 0);
    assert!(
        tree.num_leaves() >= 3,
        "fixture needs several high leaves to inflate the per-range total"
    );

    let num_docs = 5; // docs 1..=4 plus the match doc
    let mut filter = full_range();
    filter.limit = 1;
    let source = NumericScoreSource::filtered(&tree, filter, false, 1, num_docs, 1);
    let mut it = new_numeric_top_k_filtered(
        source,
        IdList::<true>::new(vec![match_id]),
        NonZeroUsize::new(1).unwrap(),
    );

    let mut got = Vec::new();
    while let Some(result) = it.read().unwrap() {
        got.push((result.doc_id, result.as_numeric().expect("numeric result")));
    }
    assert_eq!(got, vec![(match_id, 0.5)]);
}

#[test]
fn rewind_restarts_iteration() {
    let tree = build_tree(20, false, 0);
    let mut source = NumericScoreSource::with_range_batch_size(&tree, full_range(), false, 1);

    let drain_all = |source: &mut NumericScoreSource| {
        let mut all = Vec::new();
        while let Some(batch) = source.next_batch().unwrap() {
            all.extend(drain(batch));
        }
        all
    };

    let first = drain_all(&mut source);
    assert!(!first.is_empty());
    source.rewind();
    let second = drain_all(&mut source);
    assert_eq!(first, second);
}

#[test]
fn filtered_retry_keeps_high_match_across_windows() {
    // Multi-leaf tree, doc_id == value == i. DESC, k=2, and the child matches the
    // highest- and lowest-valued docs, which fall in different value-windows. The
    // high match (doc 20) is collected in the first window; it must survive the
    // expansion that reaches the low match (doc 1), so the heap accumulates
    // across windows rather than restarting on each retry.
    let tree = build_tree(20, false, 0);
    assert!(tree.num_leaves() > 1, "fixture must split into many ranges");

    let mut filter = full_range();
    filter.limit = 1;
    let source = NumericScoreSource::filtered(&tree, filter, false, 1, 20, 2);
    let mut it = new_numeric_top_k_filtered(
        source,
        IdList::<true>::new(vec![1u64, 20u64]),
        NonZeroUsize::new(2).unwrap(),
    );

    let mut got = Vec::new();
    while let Some(result) = it.read().unwrap() {
        got.push((result.doc_id, result.as_numeric().expect("numeric result")));
    }
    assert_eq!(got, vec![(20, 20.0), (1, 1.0)]);
}

#[test]
fn filtered_rewind_after_expansion_repeats_results() {
    // Driving to completion forces at least one window expansion; an outer rewind
    // must reset to the initial window and reproduce the same results.
    let tree = build_tree(20, false, 0);
    let child = vec![1u64, 20u64];

    let mut filter = full_range();
    filter.limit = 1;
    let source = NumericScoreSource::filtered(&tree, filter, false, 1, 20, 2);
    let mut it = new_numeric_top_k_filtered(
        source,
        IdList::<true>::new(child),
        NonZeroUsize::new(2).unwrap(),
    );

    let mut first = Vec::new();
    while let Some(result) = it.read().unwrap() {
        first.push((result.doc_id, result.as_numeric().expect("numeric result")));
    }

    it.rewind();

    let mut second = Vec::new();
    while let Some(result) = it.read().unwrap() {
        second.push((result.doc_id, result.as_numeric().expect("numeric result")));
    }

    assert_eq!(first, second);
    assert_eq!(first, vec![(20, 20.0), (1, 1.0)]);
}

#[test]
fn unfiltered_excludes_deleted_docs() {
    // Doc 1 holds the top value but has been deleted; its numeric-index entry
    // survives until GC. Top-k must skip it and return the next-best live docs.
    let tree = tree_from(&[(1, 5.0), (2, 1.0), (3, 4.0), (4, 2.0), (5, 3.0)]);
    let deleted = DeletedDocs::from_iter([1]);
    let source = NumericScoreSource::unfiltered(&tree, full_range(), false).with_validity(deleted);
    let mut it = new_numeric_top_k_unfiltered(source, NonZeroUsize::new(3).unwrap());

    let mut got = Vec::new();
    while let Some(result) = it.read().unwrap() {
        got.push((result.doc_id, result.as_numeric().expect("numeric result")));
    }
    assert_eq!(got, vec![(3, 4.0), (5, 3.0), (4, 2.0)]);
}

#[test]
fn filtered_excludes_deleted_docs() {
    // The child passes docs 2 and 4, and doc 4 has the higher value — but it is
    // deleted, so only the live match survives the liveness filter.
    let tree = tree_from(&[(1, 1.0), (2, 2.0), (3, 3.0), (4, 100.0), (5, 5.0)]);
    let deleted = DeletedDocs::from_iter([4]);
    let child_ids = vec![2u64, 4u64];
    let source = NumericScoreSource::filtered(&tree, full_range(), false, 1, 5, child_ids.len())
        .with_validity(deleted);
    let mut it = new_numeric_top_k_filtered(
        source,
        IdList::<true>::new(child_ids),
        NonZeroUsize::new(2).unwrap(),
    );

    let mut got = Vec::new();
    while let Some(result) = it.read().unwrap() {
        got.push((result.doc_id, result.as_numeric().expect("numeric result")));
    }
    assert_eq!(got, vec![(2, 2.0)]);
}

#[test]
fn unfiltered_excludes_field_expired_docs() {
    // Doc 1 holds the top value but its sort field has expired; the numeric-index
    // entry survives until GC. Pre-heap filtering must drop it so the heap still
    // fills k live results, best-first — the same fill-to-k the optimizer's field
    // predicate gives.
    let tree = tree_from(&[(1, 5.0), (2, 1.0), (3, 4.0), (4, 2.0), (5, 3.0)]);
    let expired = ExpiredDocs::from_iter([1]);
    let source =
        NumericScoreSource::unfiltered(&tree, full_range(), false).with_expiration(expired);
    let mut it = new_numeric_top_k_unfiltered(source, NonZeroUsize::new(3).unwrap());

    let mut got = Vec::new();
    while let Some(result) = it.read().unwrap() {
        got.push((result.doc_id, result.as_numeric().expect("numeric result")));
    }
    assert_eq!(got, vec![(3, 4.0), (5, 3.0), (4, 2.0)]);
}

#[test]
fn validity_and_expiration_compose() {
    // Deletion and field-TTL are independent oracles applied together: doc 1 is
    // deleted and doc 3 is field-expired, so both drop and the heap fills from
    // the remaining live docs.
    let tree = tree_from(&[(1, 5.0), (2, 1.0), (3, 4.0), (4, 2.0), (5, 3.0)]);
    let source = NumericScoreSource::unfiltered(&tree, full_range(), false)
        .with_validity(DeletedDocs::from_iter([1]))
        .with_expiration(ExpiredDocs::from_iter([3]));
    let mut it = new_numeric_top_k_unfiltered(source, NonZeroUsize::new(3).unwrap());

    let mut got = Vec::new();
    while let Some(result) = it.read().unwrap() {
        got.push((result.doc_id, result.as_numeric().expect("numeric result")));
    }
    assert_eq!(got, vec![(5, 3.0), (4, 2.0), (2, 1.0)]);
}

/// A clock context whose deadline is already in the past, probed on every call.
fn expired_clock() -> TimeoutContextClock {
    TimeoutContextClock::new(Duration::from_nanos(1), 1)
}

/// Counts `check_timeout` calls and never times out. The shared counter is
/// readable after the source has taken ownership of the context, so a test can
/// assert exactly how many polls a phase performed.
#[derive(Clone, Default)]
struct CountingTimeout(Rc<Cell<u32>>);

impl CountingTimeout {
    fn calls(&self) -> u32 {
        self.0.get()
    }
}

impl TimeoutContext for CountingTimeout {
    fn check_timeout(&mut self) -> Result<(), RQEIteratorError> {
        self.0.set(self.0.get() + 1);
        Ok(())
    }
}

#[test]
fn collection_times_out_during_materialization() {
    // Multi-leaf tree so `next_batch` reads several ranges' records; the first
    // per-record poll crosses the already-elapsed deadline.
    let tree = build_tree(20, false, 0);
    let mut source =
        NumericScoreSource::unfiltered(&tree, full_range(), false).with_timeout(expired_clock());

    assert!(matches!(
        source.next_batch(),
        Err(RQEIteratorError::TimedOut)
    ));
}

#[test]
fn iterator_read_propagates_timeout() {
    // Eager collection runs inside the first `read()`, so the timeout surfaces
    // there rather than being swallowed.
    let tree = build_tree(20, false, 0);
    let source =
        NumericScoreSource::unfiltered(&tree, full_range(), false).with_timeout(expired_clock());
    let mut it = new_numeric_top_k_unfiltered(source, NonZeroUsize::new(3).unwrap());

    assert!(matches!(it.read(), Err(RQEIteratorError::TimedOut)));
}

#[test]
fn filtering_scan_is_timeout_aware() {
    // Single-leaf tree of four records, materialized into one batch. Materialization
    // polls once per record and once before the sort; the stale-record filtering
    // scan then adds one poll per record. A deadline primed to survive
    // materialization must therefore surface inside that scan.
    let pairs = [(1u64, 4.0), (2, 3.0), (3, 2.0), (4, 1.0)];
    let tree = tree_from(&pairs);
    // Pin the number of polls materialization performs, so the filtered case can
    // prime a one-shot timeout that fires only once the filtering scan begins.
    let counter = CountingTimeout::default();
    let mut unfiltered =
        NumericScoreSource::unfiltered(&tree, full_range(), false).with_timeout(counter.clone());
    assert!(unfiltered.next_batch().unwrap().is_some());
    let materialization_polls = counter.calls();
    assert_eq!(materialization_polls, pairs.len() as u32 + 1);

    // A one-shot timeout that survives materialization fires on the filtering
    // scan's first poll, proving the scan honors the deadline.
    let mut filtered = NumericScoreSource::unfiltered(&tree, full_range(), false)
        .with_validity(DeletedDocs::from_iter([2]))
        .with_timeout(TimeoutOnce::new(materialization_polls));
    assert!(matches!(
        filtered.next_batch(),
        Err(RQEIteratorError::TimedOut)
    ));
}

#[test]
fn amortized_check_does_not_probe_below_granularity() {
    // Deadline is in the past, but with a granularity far above the record count
    // the clock is never probed, so the batch is produced without timing out.
    let tree = build_tree(20, false, 0);
    let never_probes = TimeoutContextClock::new(Duration::from_nanos(1), 1_000_000);
    let mut source =
        NumericScoreSource::unfiltered(&tree, full_range(), false).with_timeout(never_probes);

    assert!(source.next_batch().unwrap().is_some());
}

#[test]
fn no_timeout_default_never_times_out() {
    // The explicit `NoTimeout` context (the struct default) yields the full
    // result set, matching the behavior of every other test's default source.
    let tree = tree_from(&[(1, 5.0), (2, 1.0), (3, 4.0)]);
    let source = NumericScoreSource::unfiltered(&tree, full_range(), false).with_timeout(NoTimeout);
    let mut it = new_numeric_top_k_unfiltered(source, NonZeroUsize::new(3).unwrap());

    let mut got = Vec::new();
    while let Some(result) = it.read().unwrap() {
        got.push((result.doc_id, result.as_numeric().expect("numeric result")));
    }
    assert_eq!(got, vec![(1, 5.0), (3, 4.0), (2, 1.0)]);
}

/// Times out exactly once, after `succeed_first` probes, then never again.
///
/// A test double for the rewind-after-timeout recovery path: a real absolute
/// deadline keeps firing, but a one-shot lets the rewound pass run to completion
/// so its results can be checked. Rewinding the source does not touch the
/// timeout context, so the fired state persists across the rewind, exactly as an
/// absolute deadline would.
struct TimeoutOnce {
    succeed_first: u32,
    fired: bool,
}

impl TimeoutOnce {
    fn new(succeed_first: u32) -> Self {
        Self {
            succeed_first,
            fired: false,
        }
    }
}

impl TimeoutContext for TimeoutOnce {
    fn check_timeout(&mut self) -> Result<(), RQEIteratorError> {
        if !self.fired {
            if self.succeed_first == 0 {
                self.fired = true;
                return Err(RQEIteratorError::TimedOut);
            }
            self.succeed_first -= 1;
        }
        Ok(())
    }
}

/// Collect an unfiltered top-k iterator to exhaustion into `(doc_id, score)`.
fn drain_top_k<'a, V: DocValidity + 'a, E: ExpirationChecker + 'a, T: TimeoutContext + 'a>(
    it: &mut numeric_score_source::NumericTopKIterator<'a, V, E, T>,
) -> Vec<(DocId, f64)> {
    let mut got = Vec::new();
    while let Some(result) = it.read().unwrap() {
        got.push((result.doc_id, result.as_numeric().expect("numeric result")));
    }
    got
}

#[test]
fn rewind_after_timeout_reproduces_full_results() {
    // k == doc count forces the whole index to be read (the heap never fills, so
    // no early `Stop`), guaranteeing the one-shot timeout fires mid-collection.
    let tree = build_tree(20, false, 0);
    let k = NonZeroUsize::new(20).unwrap();

    let reference = {
        let source = NumericScoreSource::with_range_batch_size(&tree, full_range(), false, 1);
        drain_top_k(&mut new_numeric_top_k_unfiltered(source, k))
    };

    // Time out partway through the first collection; earlier batches have already
    // landed in the heap when the deadline fires.
    let source = NumericScoreSource::with_range_batch_size(&tree, full_range(), false, 1)
        .with_timeout(TimeoutOnce::new(8));
    let mut it = new_numeric_top_k_unfiltered(source, k);
    assert!(matches!(it.read(), Err(RQEIteratorError::TimedOut)));

    // Rewind discards the partial heap and re-resolves the ranges from the start;
    // the one-shot deadline has fired, so the second pass runs clean and must
    // reproduce the reference exactly — no dropped, duplicated, or reordered docs.
    it.rewind();
    assert_eq!(drain_top_k(&mut it), reference);
}

#[test]
fn rewind_after_persistent_timeout_times_out_again() {
    // A deadline already in the past keeps firing. Rewinding after a timeout must
    // leave the iterator in a clean, re-runnable state (no stale collecting phase,
    // no panic) and time out again, since the deadline is absolute and rewind
    // does not extend it.
    let tree = build_tree(20, false, 0);
    let source = NumericScoreSource::with_range_batch_size(&tree, full_range(), false, 1)
        .with_timeout(expired_clock());
    let mut it = new_numeric_top_k_unfiltered(source, NonZeroUsize::new(5).unwrap());

    assert!(matches!(it.read(), Err(RQEIteratorError::TimedOut)));
    it.rewind();
    assert!(matches!(it.read(), Err(RQEIteratorError::TimedOut)));
}
