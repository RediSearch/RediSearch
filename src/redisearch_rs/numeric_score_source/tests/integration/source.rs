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

use std::{iter, num::NonZeroUsize};

use inverted_index::NumericFilter;
use numeric_range_tree::NumericRangeTree;
use numeric_range_tree::test_utils::build_tree;
use numeric_score_source::{
    NumericScoreSource, new_numeric_top_k_filtered, new_numeric_top_k_unfiltered,
};
use rqe_core::DocId;
use rqe_iterators::{IdList, RQEIterator};
use top_k::{ScoreBatch, ScoreSource};

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
