/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Score bounds for the disk leaves whose results do not depend on the document's text.
//!
//! A tag, numeric or geo leaf yields results with a fixed frequency and weight, so the most
//! it adds to a document's score under `ext/default.c` is a fixed fraction of the document's
//! score. With the index-wide maximum document score that gives one bound for every document
//! the leaf yields, which [`ConstantBound`](rqe_iterators::ConstantBound) reports.

use crate::scorers::BuiltInScorer;

/// A disk leaf that [`constant_leaf_bound`] can bound, by the results it yields.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConstantLeaf {
    /// A tag value: `Virtual` results of frequency 0, which every text scorer scores 0.
    Tag,
    /// A numeric range or geo radius: `Numeric` results of frequency 1 and weight 1, alone
    /// or under a union. A union of them carries weight 0, so the bound for a single one
    /// covers it too.
    Numeric,
}

/// Raises every bound, so that summing children's bounds stays at least the score the scorer
/// computes in a different order of operations.
const SCORE_BOUND_REL_EPSILON: f64 = 1e-9;

/// The legacy `BM25` constants of `bm25Recursive`, with their `float` type.
const BM25_B: f32 = 0.5;
const BM25_K1: f32 = 1.2;

/// At least what a [`ConstantLeaf`] adds to the score of any document it yields, under
/// `scorer`, or `None` when that cannot be bounded.
///
/// `max_doc_score` is at least every indexed document's score: scores read from
/// `SCORE_FIELD` are not range-checked, so it can exceed 1. `avg_doc_len` is the
/// `RSIndexStats::avgDocLen` the scorer reads.
///
/// Supports the scorers disk term leaves bound: `TFIDF`, `TFIDF.DOCNORM`, `BM25`, `BM25STD`
/// and `DOCSCORE`. Under `DOCSCORE` every leaf gets the whole bound, as it can be the only
/// matching child of a union. A negative `max_doc_score` counts as 0: a negative score
/// divided by a larger norm grows towards 0, never above it.
pub fn constant_leaf_bound(
    scorer: BuiltInScorer,
    leaf: ConstantLeaf,
    max_doc_score: f64,
    avg_doc_len: f64,
) -> Option<f64> {
    if !max_doc_score.is_finite() || !(avg_doc_len >= 0.0) {
        return None;
    }
    let max_doc_score = max_doc_score.max(0.0);
    let bound = match (scorer, leaf) {
        (BuiltInScorer::DocScore, _) => max_doc_score,
        (
            BuiltInScorer::TfIdf
            | BuiltInScorer::TfIdfDocNorm
            | BuiltInScorer::Bm25
            | BuiltInScorer::Bm25Std,
            ConstantLeaf::Tag,
        ) => 0.0,
        // `weight · freq = 1`, divided by a norm of at least 1.
        (BuiltInScorer::TfIdf | BuiltInScorer::TfIdfDocNorm, ConstantLeaf::Numeric) => {
            max_doc_score
        }
        (BuiltInScorer::Bm25, ConstantLeaf::Numeric) => {
            let k =
                f64::from(BM25_K1) * (f64::from(1.0f32 - BM25_B) + f64::from(BM25_B) * avg_doc_len);
            max_doc_score * (1.0 / (1.0 + k))
        }
        // `bm25StdRecursive` scores only terms and `Virtual` results of non-zero frequency.
        (BuiltInScorer::Bm25Std, ConstantLeaf::Numeric) => 0.0,
        (
            BuiltInScorer::DisMax
            | BuiltInScorer::Bm25StdTanh
            | BuiltInScorer::Bm25StdNorm
            | BuiltInScorer::Hamming
            | BuiltInScorer::Sparse,
            _,
        ) => return None,
    };
    Some(bound * (1.0 + SCORE_BOUND_REL_EPSILON))
}
