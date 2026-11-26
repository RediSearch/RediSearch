/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! [`constant_leaf_bound`] is at least what `ext/default.c` gives a document for the results of
//! a disk tag or numeric leaf.

use query_eval::{
    constant_leaf_bound::{ConstantLeaf, constant_leaf_bound},
    scorers::BuiltInScorer,
};

const BOUNDED: [BuiltInScorer; 5] = [
    BuiltInScorer::TfIdf,
    BuiltInScorer::TfIdfDocNorm,
    BuiltInScorer::Bm25,
    BuiltInScorer::Bm25Std,
    BuiltInScorer::DocScore,
];

/// Scores outside [0, 1] included: `SCORE_FIELD` values are not range-checked.
const SCORES: [f32; 9] = [-2.0, -0.5, 0.0, 1e-4, 0.25, 0.5, 1.0, 2.5, 17.0];
/// `maxTermFreq` and `docLen` values, the scorers' norms.
const NORMS: [u32; 5] = [0, 1, 2, 7, 1000];
const AVG_DOC_LENS: [f64; 5] = [0.0, 0.5, 1.0, 4.0, 123.4];
/// The query weight a tag result carries.
const TAG_WEIGHTS: [f64; 4] = [0.0, 0.5, 1.0, 3.0];

/// A result as a disk leaf yields it.
#[derive(Debug, Clone, Copy)]
struct LeafResult {
    is_virtual: bool,
    freq: u32,
    weight: f64,
}

#[derive(Debug, Clone, Copy)]
struct Doc {
    score: f32,
    max_term_freq: u32,
    doc_len: u32,
}

/// The score `ext/default.c` gives `doc` when `r` is the whole result, with slop 1 and no
/// minimum score (both only lower it). Each arm transcribes the scorer's branch for a result
/// that is neither a term nor an aggregate.
fn reference_score(scorer: BuiltInScorer, r: LeafResult, doc: Doc, avg_doc_len: f64) -> f64 {
    let score = f64::from(doc.score);
    let f = f64::from(r.freq);
    match scorer {
        BuiltInScorer::TfIdf | BuiltInScorer::TfIdfDocNorm => {
            let norm = if scorer == BuiltInScorer::TfIdf {
                doc.max_term_freq
            } else {
                doc.doc_len
            };
            if doc.score == 0.0 || norm == 0 {
                return 0.0;
            }
            score * (r.weight * f) / f64::from(norm)
        }
        BuiltInScorer::Bm25 => {
            let (b, k1) = (0.5f32, 1.2f32);
            let ret = if r.freq != 0 {
                r.weight * f
                    / (f + f64::from(k1) * (f64::from(1.0f32 - b) + f64::from(b) * avg_doc_len))
            } else {
                0.0
            };
            score * ret
        }
        BuiltInScorer::Bm25Std => {
            if r.is_virtual && r.freq != 0 && r.weight != 0.0 {
                unreachable!(
                    "no disk leaf bounded here yields a virtual result of non-zero frequency"
                )
            }
            0.0
        }
        BuiltInScorer::DocScore => score,
        _ => unreachable!("not a bounded scorer"),
    }
}

fn docs() -> impl Iterator<Item = Doc> {
    SCORES.into_iter().flat_map(|score| {
        NORMS.into_iter().flat_map(move |max_term_freq| {
            NORMS.into_iter().map(move |doc_len| Doc {
                score,
                max_term_freq,
                doc_len,
            })
        })
    })
}

fn results(leaf: ConstantLeaf) -> Vec<LeafResult> {
    match leaf {
        ConstantLeaf::Tag => TAG_WEIGHTS
            .into_iter()
            .map(|weight| LeafResult {
                is_virtual: true,
                freq: 0,
                weight,
            })
            .collect(),
        ConstantLeaf::Numeric => vec![LeafResult {
            is_virtual: false,
            freq: 1,
            weight: 1.0,
        }],
    }
}

#[test]
fn bound_covers_every_document_the_leaf_can_score() {
    let max_doc_score = SCORES.into_iter().fold(f32::MIN, f32::max);
    for scorer in BOUNDED {
        for leaf in [ConstantLeaf::Tag, ConstantLeaf::Numeric] {
            for avg_doc_len in AVG_DOC_LENS {
                let bound =
                    constant_leaf_bound(scorer, leaf, f64::from(max_doc_score), avg_doc_len)
                        .expect("a bounded scorer");
                for r in results(leaf) {
                    for doc in docs() {
                        let score = reference_score(scorer, r, doc, avg_doc_len);
                        assert!(
                            score <= bound,
                            "{scorer:?} {leaf:?} {r:?} {doc:?} avg {avg_doc_len}: {score} > {bound}"
                        );
                    }
                }
            }
        }
    }
}

/// The bound is not looser than it needs to be: some document reaches it exactly.
#[test]
fn bound_is_reached() {
    for scorer in BOUNDED {
        for leaf in [ConstantLeaf::Tag, ConstantLeaf::Numeric] {
            let bound = constant_leaf_bound(scorer, leaf, 2.5, 4.0).expect("a bounded scorer");
            let best = results(leaf)
                .into_iter()
                .flat_map(|r| {
                    docs()
                        .filter(|doc| doc.score <= 2.5)
                        .map(move |doc| reference_score(scorer, r, doc, 4.0))
                })
                .fold(0.0, f64::max);
            assert_eq!(best, bound, "{scorer:?} {leaf:?}");
        }
    }
}

#[test]
fn text_scorers_give_tags_nothing() {
    for scorer in [
        BuiltInScorer::TfIdf,
        BuiltInScorer::TfIdfDocNorm,
        BuiltInScorer::Bm25,
        BuiltInScorer::Bm25Std,
    ] {
        assert_eq!(
            constant_leaf_bound(scorer, ConstantLeaf::Tag, 3.0, 4.0),
            Some(0.0)
        );
    }
}

#[test]
fn a_negative_max_doc_score_counts_as_zero() {
    assert_eq!(
        constant_leaf_bound(BuiltInScorer::TfIdf, ConstantLeaf::Numeric, -1.0, 4.0),
        Some(0.0)
    );
    assert_eq!(
        constant_leaf_bound(BuiltInScorer::DocScore, ConstantLeaf::Tag, -1.0, 4.0),
        Some(0.0)
    );
}

#[test]
fn no_bound_without_a_known_max_doc_score_or_a_bounded_scorer() {
    for max_doc_score in [f64::INFINITY, f64::NAN] {
        assert_eq!(
            constant_leaf_bound(
                BuiltInScorer::DocScore,
                ConstantLeaf::Tag,
                max_doc_score,
                4.0
            ),
            None
        );
    }
    for scorer in [
        BuiltInScorer::DisMax,
        BuiltInScorer::Bm25StdTanh,
        BuiltInScorer::Bm25StdNorm,
        BuiltInScorer::Hamming,
        BuiltInScorer::Sparse,
    ] {
        assert_eq!(
            constant_leaf_bound(scorer, ConstantLeaf::Numeric, 1.0, 4.0),
            None
        );
    }
}
