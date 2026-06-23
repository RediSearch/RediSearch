/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Tests for [`NumericScoreSource`], driven by a `MetricSortedById`
//! source (a pure-Rust numeric-valued iterator).

use std::{iter, num::NonZeroUsize};

use itertools::Itertools;
use numeric_score_source::{
    NumericScoreSource, new_numeric_top_k_filtered, new_numeric_top_k_unfiltered,
};
use rqe_core::DocId;
use rqe_iterators::metric::MetricSortedById;
use rqe_iterators::{IdList, RQEIterator};
use top_k::{ScoreBatch, ScoreSource};

/// Wrap `(doc_id, score)` pairs (ascending by id) as a `NumericScoreSource`.
fn source(pairs: &[(DocId, f64)]) -> NumericScoreSource<'static, MetricSortedById<'static>> {
    let ids = pairs.iter().map(|(id, _)| *id).collect::<Vec<_>>();
    let scores = pairs.iter().map(|(_, s)| *s).collect::<Vec<_>>();
    NumericScoreSource::new(MetricSortedById::new(ids, scores))
}

/// Like [`source`] but with an explicit per-batch cap, to exercise multi-batch
/// behavior on small inputs.
fn source_with_batch_size(
    pairs: &[(DocId, f64)],
    batch_size: usize,
) -> NumericScoreSource<'static, MetricSortedById<'static>> {
    let ids = pairs.iter().map(|(id, _)| *id).collect::<Vec<_>>();
    let scores = pairs.iter().map(|(_, s)| *s).collect::<Vec<_>>();
    NumericScoreSource::with_batch_size(MetricSortedById::new(ids, scores), batch_size)
}

/// Drain a batch into a `Vec`.
fn drain<B: ScoreBatch>(mut batch: B) -> Vec<(DocId, f64)> {
    iter::from_fn(|| batch.next()).collect()
}

#[test]
fn next_batch_yields_every_record_in_doc_id_order() {
    let mut src = source(&[(1, 3.0), (2, 1.0), (3, 2.0)]);
    let mut batch = src.next_batch().unwrap().expect("a batch");
    assert_eq!(
        iter::from_fn(|| batch.next()).collect_vec(),
        vec![(1, 3.0), (2, 1.0), (3, 2.0)]
    );
}

#[test]
fn empty_source_yields_no_batch() {
    let mut src = source(&[]);
    assert!(src.next_batch().unwrap().is_none());
}

#[test]
fn next_batch_splits_into_bounded_batches() {
    // batch_size 2 over 5 records: bounded batches in doc-id order, every record
    // appearing once across batches, none larger than the cap, then EOF.
    let mut src = source_with_batch_size(&[(1, 9.0), (2, 8.0), (3, 7.0), (4, 6.0), (5, 5.0)], 2);

    let mut batches = Vec::new();
    while let Some(batch) = src.next_batch().unwrap() {
        let drained = drain(batch);
        assert!(drained.len() <= 2, "batch exceeded the cap: {drained:?}");
        batches.push(drained);
    }

    // Two full batches plus a trailing partial one.
    assert_eq!(
        batches,
        vec![
            vec![(1, 9.0), (2, 8.0)],
            vec![(3, 7.0), (4, 6.0)],
            vec![(5, 5.0)]
        ]
    );
    // Source is exhausted: a further read stays empty without a rewind.
    assert!(src.next_batch().unwrap().is_none());
}

/// Drive a filtered top-k iterator to exhaustion, collecting yielded
/// `(doc_id, score)` pairs. The child filter passes exactly `child_ids`.
fn run_filtered(
    pairs: &[(DocId, f64)],
    child_ids: Vec<DocId>,
    batch_size: usize,
    k: usize,
    ascending: bool,
) -> Vec<(DocId, f64)> {
    let mut it = new_numeric_top_k_filtered(
        source_with_batch_size(pairs, batch_size),
        IdList::<true>::new(child_ids),
        NonZeroUsize::new(k).unwrap(),
        ascending,
    );
    let mut got = Vec::new();
    while let Some(result) = it.read().unwrap() {
        // SAFETY: the numeric source builds every result with `build_numeric`,
        // so each yielded record is numeric.
        let score = unsafe { result.as_numeric_unchecked() };
        got.push((result.doc_id, score));
    }
    got
}

#[test]
fn filtered_top_k_finds_high_score_beyond_first_batch() {
    // Regression test for bounded batches: the best-scoring match (doc 4, the
    // second batch with batch_size 2) must win even though an earlier batch
    // already filled the heap to k. A premature `Stop` on a full heap would
    // wrongly return doc 2 from the first batch.
    let pairs = [(1, 1.0), (2, 2.0), (3, 3.0), (4, 100.0), (5, 5.0)];
    let got = run_filtered(&pairs, vec![2, 4], 2, 1, false);
    assert_eq!(got, vec![(4, 100.0)]);
}

#[test]
fn source_emits_one_batch_then_rewinds() {
    let mut src = source(&[(1, 1.0), (2, 2.0)]);
    assert!(src.next_batch().unwrap().is_some());
    // Single-batch source: a second read without rewinding is empty.
    assert!(src.next_batch().unwrap().is_none());

    src.rewind();
    let mut batch = src.next_batch().unwrap().expect("a batch after rewind");
    assert_eq!(
        iter::from_fn(|| batch.next()).collect_vec(),
        vec![(1, 1.0), (2, 2.0)]
    );
}

/// Drive an unfiltered top-k iterator to exhaustion, collecting the yielded
/// `(doc_id, score)` pairs.
fn run_unfiltered(pairs: &[(DocId, f64)], k: usize, ascending: bool) -> Vec<(DocId, f64)> {
    let mut it =
        new_numeric_top_k_unfiltered(source(pairs), NonZeroUsize::new(k).unwrap(), ascending);
    let mut got = Vec::new();
    while let Some(result) = it.read().unwrap() {
        // SAFETY: the numeric source builds every result with `build_numeric`,
        // so each yielded record is numeric.
        let score = unsafe { result.as_numeric_unchecked() };
        got.push((result.doc_id, score));
    }
    got
}

#[test]
fn unfiltered_caps_at_k_descending() {
    // More records than k: LIMIT k must return the k highest values (DESC),
    // not every record in doc-id order. Yields best-first.
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
fn unfiltered_fewer_than_k_yields_all() {
    let got = run_unfiltered(&[(1, 3.0), (2, 1.0)], 5, false);
    assert_eq!(got, vec![(1, 3.0), (2, 1.0)]);
}
