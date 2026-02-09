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

/// Parameters needed to compute block max scores.
///
/// This structure holds the scoring parameters that are constant for a given
/// term/query but needed to compute the maximum possible score for a block.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BlockScoreContext {
    /// Inverse Document Frequency for the term.
    /// IDF = log(N / df) where N is total docs and df is document frequency.
    pub idf: f64,

    /// Average document length in the index.
    pub avg_doc_len: f64,

    /// BM25 k1 parameter (term frequency saturation).
    /// Typical value: 1.2
    pub bm25_k1: f64,

    /// BM25 b parameter (document length normalization).
    /// Typical value: 0.75
    pub bm25_b: f64,
}

impl Default for BlockScoreContext {
    fn default() -> Self {
        Self {
            idf: 1.0,
            avg_doc_len: 100.0,
            bm25_k1: 1.2,
            bm25_b: 0.75,
        }
    }
}

/// Trait for computing block-level score upper bounds.
///
/// Different scoring functions (TF-IDF, BM25, DOCSCORE) have different formulas
/// for computing scores. This trait abstracts over those differences.
pub trait BlockMaxScorer {
    /// Compute the maximum possible score for any document in this block.
    ///
    /// Returns `f64::MAX` if the block doesn't have scoring metadata,
    /// indicating that no skipping is possible for this block.
    fn block_max_score(block: &IndexBlock, ctx: &BlockScoreContext) -> f64;
}

/// TF-IDF scorer: Score = (TF / DocLen) × IDF × DocScore
///
/// To maximize: use max_freq, min_doc_len, max_doc_score
pub struct TfIdfBlockScorer;

impl BlockMaxScorer for TfIdfBlockScorer {
    fn block_max_score(block: &IndexBlock, ctx: &BlockScoreContext) -> f64 {
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

        (tf / doc_len) * ctx.idf * doc_score.max(1.0)
    }
}

/// BM25 scorer.
///
/// BM25 formula: IDF × (TF × (k1 + 1)) / (TF + k1 × (1 - b + b × docLen/avgDocLen))
///
/// To maximize: use max_freq for TF, min_doc_len for docLen
pub struct Bm25BlockScorer;

impl BlockMaxScorer for Bm25BlockScorer {
    fn block_max_score(block: &IndexBlock, ctx: &BlockScoreContext) -> f64 {
        if !block.has_scoring_metadata() {
            return f64::MAX;
        }

        let tf = block.max_freq() as f64;
        let doc_len = block.min_doc_len() as f64;

        // Avoid division by zero
        if ctx.avg_doc_len == 0.0 {
            return f64::MAX;
        }

        let len_norm = 1.0 - ctx.bm25_b + ctx.bm25_b * (doc_len / ctx.avg_doc_len);
        let denominator = tf + ctx.bm25_k1 * len_norm;

        // Avoid division by zero
        if denominator == 0.0 {
            return f64::MAX;
        }

        ctx.idf * (tf * (ctx.bm25_k1 + 1.0)) / denominator
    }
}

/// DOCSCORE scorer: Score = DocScore (ignores term frequency).
///
/// This scorer only considers the document's a-priori score.
pub struct DocScoreBlockScorer;

impl BlockMaxScorer for DocScoreBlockScorer {
    fn block_max_score(block: &IndexBlock, _ctx: &BlockScoreContext) -> f64 {
        if !block.has_scoring_metadata() {
            return f64::MAX;
        }

        block.max_doc_score() as f64
    }
}

#[cfg(test)]
mod tests;

