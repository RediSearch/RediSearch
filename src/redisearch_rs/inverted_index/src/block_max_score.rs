/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Block Max Score Optimization utilities.
//!
//! This module provides utilities for computing block-level score upper bounds,
//! which enables skipping entire blocks during query execution when it can be
//! determined that no document in a block can contribute enough to the final
//! score to affect the top-K results.

use crate::IndexBlock;

/// Block scorer with embedded scoring parameters.
///
/// Each variant contains the parameters needed for that specific scoring function.
/// This design unifies the scorer type and its context, ensuring type safety
/// (e.g., BM25 parameters can't be accidentally used with TF-IDF scorer).
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BlockScorer {
    /// TF-IDF scoring: (TF / DocLen) × IDF × DocScore
    ///
    /// Parameters:
    /// - `idf`: Inverse Document Frequency for the term
    TfIdf {
        /// Inverse Document Frequency: log(N / df)
        idf: f64,
    },

    /// BM25 scoring with length normalization.
    ///
    /// Formula: IDF × (TF × (k1 + 1)) / (TF + k1 × (1 - b + b × docLen/avgDocLen))
    Bm25 {
        /// Inverse Document Frequency: log(N / df)
        idf: f64,
        /// Average document length in the index
        avg_doc_len: f64,
        /// Term frequency saturation parameter (typical: 1.2)
        k1: f64,
        /// Document length normalization parameter (typical: 0.75)
        b: f64,
    },

    /// Document score only (ignores term frequency).
    ///
    /// Simply returns the document's a-priori score.
    DocScore,
}

impl Default for BlockScorer {
    fn default() -> Self {
        Self::TfIdf { idf: 1.0 }
    }
}

impl BlockScorer {
    /// Create a TF-IDF scorer with the given IDF value.
    pub fn tfidf(idf: f64) -> Self {
        Self::TfIdf { idf }
    }

    /// Create a BM25 scorer with the given parameters.
    pub fn bm25(idf: f64, avg_doc_len: f64, k1: f64, b: f64) -> Self {
        Self::Bm25 {
            idf,
            avg_doc_len,
            k1,
            b,
        }
    }

    /// Create a BM25 scorer with default k1=1.2 and b=0.75 parameters.
    pub fn bm25_default(idf: f64, avg_doc_len: f64) -> Self {
        Self::bm25(idf, avg_doc_len, 1.2, 0.75)
    }

    /// Create a DocScore scorer.
    pub fn doc_score() -> Self {
        Self::DocScore
    }

    /// Compute the maximum possible score for any document in this block.
    ///
    /// Returns `f64::MAX` if the block doesn't have scoring metadata,
    /// indicating that no skipping is possible for this block.
    pub fn block_max_score(&self, block: &IndexBlock) -> f64 {
        match self {
            Self::TfIdf { idf } => Self::compute_tfidf(block, *idf),
            Self::Bm25 {
                idf,
                avg_doc_len,
                k1,
                b,
            } => Self::compute_bm25(block, *idf, *avg_doc_len, *k1, *b),
            Self::DocScore => Self::compute_docscore(block),
        }
    }

    /// TF-IDF: (TF / DocLen) × IDF × DocScore
    fn compute_tfidf(block: &IndexBlock, idf: f64) -> f64 {
        if !block.has_scoring_metadata() {
            return f64::MAX; // No skipping possible
        }

        let tf = block.max_freq() as f64;
        let doc_len = block.min_doc_len() as f64;
        let doc_score = block.max_doc_score() as f64;

        // Avoid division by zero
        if doc_len == 0.0 {
            return f64::MAX;
        }

        (tf / doc_len) * idf * doc_score.max(1.0)
    }

    /// BM25: IDF × (TF × (k1 + 1)) / (TF + k1 × (1 - b + b × docLen/avgDocLen))
    fn compute_bm25(block: &IndexBlock, idf: f64, avg_doc_len: f64, k1: f64, b: f64) -> f64 {
        if !block.has_scoring_metadata() {
            return f64::MAX;
        }

        let tf = block.max_freq() as f64;
        let doc_len = block.min_doc_len() as f64;

        // Avoid division by zero
        if avg_doc_len == 0.0 {
            return f64::MAX;
        }

        let len_norm = 1.0 - b + b * (doc_len / avg_doc_len);
        let denominator = tf + k1 * len_norm;

        // Avoid division by zero
        if denominator == 0.0 {
            return f64::MAX;
        }

        idf * (tf * (k1 + 1.0)) / denominator
    }

    /// DocScore: just returns max_doc_score
    fn compute_docscore(block: &IndexBlock) -> f64 {
        if !block.has_scoring_metadata() {
            return f64::MAX;
        }

        block.max_doc_score() as f64
    }
}

#[cfg(test)]
mod tests;
