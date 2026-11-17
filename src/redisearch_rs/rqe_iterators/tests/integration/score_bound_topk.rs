/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! A top-K score sorter driven over random [`Tree`]s of [`Blocked`](crate::utils::blocked::Blocked)
//! leaves, once with plain [`RQEIterator::read`]s and once the way the sorter drives
//! [`RQEIterator::read_with_threshold`]: plain reads until its heap is full, then the heap minimum
//! as the threshold. Both runs must accept the same candidates into the heap, in the same order,
//! and end with the same top K.

use std::collections::BTreeMap;

use proptest::prelude::*;
use rqe_core::DocId;
use rqe_iterators::{
    RQEIterator,
    score_bound::{MinCompetitiveScore, can_drop},
};

use crate::utils::blocked::{Tree, tree};

/// A heap of the best `k` `(doc, score)` pairs, ordered as the score sorter orders them: higher
/// score first, and between equal scores the lower doc id. Documents arrive in increasing id
/// order, so a candidate that only ties the worst entry never displaces it.
struct TopK {
    k: usize,
    entries: Vec<(DocId, f64)>,
}

impl TopK {
    const fn new(k: usize) -> Self {
        Self {
            k,
            entries: Vec::new(),
        }
    }

    /// The heap minimum once the heap is full.
    fn theta(&self) -> Option<f64> {
        (self.entries.len() == self.k).then(|| {
            self.entries
                .iter()
                .map(|&(_, score)| score)
                .fold(f64::INFINITY, f64::min)
        })
    }

    /// Offers a candidate, returning whether it entered the heap.
    fn offer(&mut self, doc: DocId, score: f64) -> bool {
        if self.entries.len() < self.k {
            self.entries.push((doc, score));
            return true;
        }
        let (worst, &(worst_doc, worst_score)) = self
            .entries
            .iter()
            .enumerate()
            .min_by(|(_, a), (_, b)| a.1.total_cmp(&b.1).then(b.0.cmp(&a.0)))
            .expect("a full heap is not empty");
        if score > worst_score || (score == worst_score && doc < worst_doc) {
            self.entries[worst] = (doc, score);
            return true;
        }
        false
    }

    fn sorted(&self) -> Vec<(DocId, f64)> {
        let mut entries = self.entries.clone();
        entries.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
        entries
    }
}

/// The threshold the sorter passes down for heap minimum `theta`: the minimum itself, or `None`
/// when it could not drop anything.
fn min_competitive(theta: f64) -> Option<MinCompetitiveScore> {
    MinCompetitiveScore::new(theta)
}

/// What one sorter run saw.
struct Run {
    /// Candidates the heap accepted, in order.
    accepted: Vec<(DocId, f64)>,
    top: Vec<(DocId, f64)>,
}

/// Runs the sorter with plain reads only.
fn plain_run(tree: &Tree, k: usize) -> Run {
    let mut it = tree.build(&mut Vec::new());
    let mut heap = TopK::new(k);
    let mut accepted = Vec::new();
    while let Some(doc) = it.read().expect("read failed").map(|r| r.doc_id) {
        let score = it.current_max_score();
        if heap.offer(doc, score) {
            accepted.push((doc, score));
        }
    }
    Run {
        accepted,
        top: heap.sorted(),
    }
}

/// Runs the sorter the way `pullCandidate` does, checking every read against the exact scores.
fn thresholded_run(tree: &Tree, k: usize, scores: &BTreeMap<DocId, f64>) -> Run {
    let mut logs = Vec::new();
    let mut it = tree.build(&mut logs);
    let mut heap = TopK::new(k);
    let mut accepted = Vec::new();
    let mut previous: DocId = 0;
    loop {
        let threshold = heap.theta().and_then(min_competitive);
        let yielded = match threshold {
            Some(min) => it.read_with_threshold(min),
            None => it.read(),
        }
        .expect("read failed")
        .map(|r| r.doc_id);

        // Every document passed over in this call scores below the threshold in force.
        let passed_to = yielded.unwrap_or(DocId::MAX);
        for (&doc, &score) in scores.range(previous + 1..passed_to) {
            assert!(
                threshold.is_some_and(|min| can_drop(score, min)),
                "doc {doc} scoring {score} passed over under {threshold:?}",
            );
        }
        let Some(doc) = yielded else { break };
        assert!(doc > previous, "doc {doc} after {previous}");
        let score = it.current_max_score();
        assert_eq!(
            scores.get(&doc).copied(),
            Some(score),
            "doc {doc} is not a match with that score",
        );
        if let Some(min) = threshold {
            assert!(
                !can_drop(score, min),
                "doc {doc} scoring {score} returned under {min:?}",
            );
        }
        if heap.offer(doc, score) {
            accepted.push((doc, score));
        }
        previous = doc;
    }
    assert!(it.read().expect("read failed").is_none(), "EOF is terminal");
    let thresholded = logs
        .iter()
        .filter(|log| log.borrow().thresholded_reads > 0)
        .count();
    assert!(
        thresholded <= usize::from(matches!(tree, Tree::Leaf(_))),
        "only the root may receive the threshold",
    );
    Run {
        accepted,
        top: heap.sorted(),
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    #[test]
    #[cfg_attr(miri, ignore = "Hundreds of random trees: too slow under Miri")]
    fn thresholded_top_k_matches_plain_top_k(
        tree in tree(400, 120),
        k in prop::sample::select(vec![1usize, 2, 3, 5, 10, 50]),
    ) {
        let scores = tree.scores();
        let plain = plain_run(&tree, k);
        let thresholded = thresholded_run(&tree, k, &scores);

        // The oracle's top K, independent of either run.
        let mut expected: Vec<_> = scores.into_iter().collect();
        expected.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
        expected.truncate(k);

        prop_assert_eq!(&plain.top, &expected, "the plain run's top {} differs from the exact scores", k);
        prop_assert_eq!(thresholded.accepted, plain.accepted, "the heap accepted different candidates");
        prop_assert_eq!(thresholded.top, plain.top, "different top {}", k);
    }
}
