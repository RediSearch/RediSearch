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

use std::num::NonZeroUsize;

use numeric_score_source::{NumericScoreSource, new_numeric_top_k_unfiltered};
use rqe_core::DocId;
use rqe_iterators::RQEIterator;
use rqe_iterators::metric::MetricSortedById;
use top_k::{ScoreBatch, ScoreSource};

/// Wrap `(doc_id, score)` pairs (ascending by id) as a `NumericScoreSource`.
fn source(pairs: &[(DocId, f64)]) -> NumericScoreSource<'static, MetricSortedById<'static>> {
    let ids = pairs.iter().map(|(id, _)| *id).collect::<Vec<_>>();
    let scores = pairs.iter().map(|(_, s)| *s).collect::<Vec<_>>();
    NumericScoreSource::new(MetricSortedById::new(ids, scores))
}

/// Drain a batch into a `Vec`.
fn drain<B: ScoreBatch>(mut batch: B) -> Vec<(DocId, f64)> {
    let mut got = Vec::new();
    while let Some(pair) = batch.next() {
        got.push(pair);
    }
    got
}

#[test]
fn next_batch_yields_every_record_in_doc_id_order() {
    let mut src = source(&[(1, 3.0), (2, 1.0), (3, 2.0)]);
    let batch = src.next_batch().unwrap().expect("a batch");
    assert_eq!(drain(batch), vec![(1, 3.0), (2, 1.0), (3, 2.0)]);
}

#[test]
fn empty_source_yields_no_batch() {
    let mut src = source(&[]);
    assert!(src.next_batch().unwrap().is_none());
}

#[test]
fn source_emits_one_batch_then_rewinds() {
    let mut src = source(&[(1, 1.0), (2, 2.0)]);
    assert!(src.next_batch().unwrap().is_some());
    // Single-batch source: a second read without rewinding is empty.
    assert!(src.next_batch().unwrap().is_none());

    src.rewind();
    let batch = src.next_batch().unwrap().expect("a batch after rewind");
    assert_eq!(drain(batch), vec![(1, 1.0), (2, 2.0)]);
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
