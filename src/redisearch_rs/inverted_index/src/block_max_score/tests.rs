/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use super::*;

/// Helper to create a test block with specific scoring metadata.
fn test_block_with_metadata(max_freq: u32, min_doc_len: u32, max_doc_score: f32) -> IndexBlock {
    IndexBlock::test_with_scoring_metadata(max_freq, min_doc_len, max_doc_score)
}

// ============================================================================
// BlockScorer construction tests
// ============================================================================

#[test]
fn block_scorer_default_is_tfidf() {
    let scorer = BlockScorer::default();
    assert!(matches!(scorer, BlockScorer::TfIdf { idf: 1.0 }));
}

#[test]
fn block_scorer_constructors() {
    let tfidf = BlockScorer::tfidf(2.5);
    assert!(matches!(tfidf, BlockScorer::TfIdf { idf } if (idf - 2.5).abs() < 1e-10));

    let bm25 = BlockScorer::bm25(3.0, 150.0, 1.5, 0.8);
    assert!(matches!(
        bm25,
        BlockScorer::Bm25 { idf, avg_doc_len, k1, b }
        if (idf - 3.0).abs() < 1e-10
            && (avg_doc_len - 150.0).abs() < 1e-10
            && (k1 - 1.5).abs() < 1e-10
            && (b - 0.8).abs() < 1e-10
    ));

    let bm25_default = BlockScorer::bm25_default(4.0, 200.0);
    assert!(matches!(
        bm25_default,
        BlockScorer::Bm25 { idf, avg_doc_len, k1, b }
        if (idf - 4.0).abs() < 1e-10
            && (avg_doc_len - 200.0).abs() < 1e-10
            && (k1 - 1.2).abs() < 1e-10
            && (b - 0.75).abs() < 1e-10
    ));

    let docscore = BlockScorer::doc_score();
    assert!(matches!(docscore, BlockScorer::DocScore));
}

// ============================================================================
// TF-IDF scorer tests
// ============================================================================

#[test]
fn tfidf_returns_zero_for_empty_block() {
    // An empty block (num_entries = 0) has valid metadata but no documents,
    // so it should return 0 score (no documents can contribute).
    let block = IndexBlock::new(1);
    let scorer = BlockScorer::tfidf(1.0);

    let score = scorer.block_max_score(&block);
    assert_eq!(score, 0.0);
}

#[test]
fn tfidf_returns_max_for_block_without_metadata() {
    // A block with num_entries > 0 but max_freq = 0 indicates metadata was not tracked
    let block = IndexBlock::test_with_scoring_metadata(0, 100, 1.0);
    let scorer = BlockScorer::tfidf(1.0);

    let score = scorer.block_max_score(&block);
    assert_eq!(score, f64::MAX);
}

#[test]
fn tfidf_computes_correct_score() {
    // Block with max_freq=10, min_doc_len=50, max_doc_score=2.0
    let block = test_block_with_metadata(10, 50, 2.0);
    let scorer = BlockScorer::tfidf(5.0);

    // Expected: (10 / 50) * 5.0 * 2.0 = 0.2 * 5.0 * 2.0 = 2.0
    let score = scorer.block_max_score(&block);
    assert!((score - 2.0).abs() < 1e-10);
}

#[test]
fn tfidf_uses_doc_score_of_1_when_zero() {
    // Block with max_doc_score=0.0 should use 1.0 as minimum
    let block = test_block_with_metadata(10, 50, 0.0);
    let scorer = BlockScorer::tfidf(5.0);

    // Expected: (10 / 50) * 5.0 * 1.0 = 1.0
    let score = scorer.block_max_score(&block);
    assert!((score - 1.0).abs() < 1e-10);
}

// ============================================================================
// BM25 scorer tests
// ============================================================================

#[test]
fn bm25_returns_zero_for_empty_block() {
    let block = IndexBlock::new(1);
    let scorer = BlockScorer::bm25_default(1.0, 100.0);

    let score = scorer.block_max_score(&block);
    assert_eq!(score, 0.0);
}

#[test]
fn bm25_returns_max_for_block_without_metadata() {
    let block = IndexBlock::test_with_scoring_metadata(0, 100, 1.0);
    let scorer = BlockScorer::bm25_default(1.0, 100.0);

    let score = scorer.block_max_score(&block);
    assert_eq!(score, f64::MAX);
}

#[test]
fn bm25_computes_correct_score() {
    // Block with max_freq=10, min_doc_len=100 (same as avg)
    let block = test_block_with_metadata(10, 100, 1.0);
    let scorer = BlockScorer::bm25(5.0, 100.0, 1.2, 0.75);

    // len_norm = 1 - 0.75 + 0.75 * (100/100) = 1.0
    // denominator = 10 + 1.2 * 1.0 = 11.2
    // score = 5.0 * (10 * 2.2) / 11.2 = 5.0 * 22 / 11.2 = 9.821...
    let score = scorer.block_max_score(&block);
    let expected = 5.0 * (10.0 * 2.2) / 11.2;
    assert!((score - expected).abs() < 1e-10);
}

#[test]
fn bm25_shorter_docs_get_higher_scores() {
    let scorer = BlockScorer::bm25(5.0, 100.0, 1.2, 0.75);

    // Same freq, but shorter doc
    let short_doc_block = test_block_with_metadata(10, 50, 1.0);
    let long_doc_block = test_block_with_metadata(10, 200, 1.0);

    let short_score = scorer.block_max_score(&short_doc_block);
    let long_score = scorer.block_max_score(&long_doc_block);

    // Shorter docs should have higher BM25 scores
    assert!(short_score > long_score);
}

// ============================================================================
// DocScore scorer tests
// ============================================================================

#[test]
fn docscore_returns_zero_for_empty_block() {
    let block = IndexBlock::new(1);
    let scorer = BlockScorer::doc_score();

    let score = scorer.block_max_score(&block);
    assert_eq!(score, 0.0);
}

#[test]
fn docscore_returns_max_for_block_without_metadata() {
    let block = IndexBlock::test_with_scoring_metadata(0, 100, 1.0);
    let scorer = BlockScorer::doc_score();

    let score = scorer.block_max_score(&block);
    assert_eq!(score, f64::MAX);
}

#[test]
fn docscore_returns_max_doc_score() {
    let block = test_block_with_metadata(10, 50, 3.5);
    let scorer = BlockScorer::doc_score();

    let score = scorer.block_max_score(&block);
    assert!((score - 3.5).abs() < 1e-10);
}

#[test]
fn docscore_ignores_freq_and_doc_len() {
    let scorer = BlockScorer::doc_score();

    // Different freq and doc_len, same doc_score
    let block1 = test_block_with_metadata(1, 10, 5.0);
    let block2 = test_block_with_metadata(100, 1000, 5.0);

    let score1 = scorer.block_max_score(&block1);
    let score2 = scorer.block_max_score(&block2);

    assert!((score1 - score2).abs() < 1e-10);
}
