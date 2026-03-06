/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Benchmark helpers for block-max score optimization.
//!
//! This module provides data generators and benchmark setup for testing
//! the block-max score optimization in Top-K query scenarios.
//!
//! # Correctness Verification
//!
//! Run the module tests to verify that `query_top_k_with_skipping` returns
//! the same results as `query_top_k_baseline`:
//!
//! ```bash
//! cargo test -p inverted_index_bencher block_max_score
//! ```

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};

use ffi::t_docId;
use inverted_index::{
    IndexReader, InvertedIndex, RSIndexResult, block_max_score::BlockScorer, freqs_only::FreqsOnly,
};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

/// Document metadata stored in a simulated DocTable.
#[derive(Clone, Copy, Debug)]
pub struct DocMetadata {
    /// Document length in tokens.
    pub doc_len: u32,
    /// A-priori document score (e.g., PageRank-like).
    pub doc_score: f32,
}

/// Distribution types for generating test data.
#[derive(Clone, Copy, Debug)]
pub enum DataDistribution {
    /// All values roughly equal.
    Uniform,
    /// Power-law distribution (few high values, many low).
    Zipfian,
}

/// Complete benchmark setup with DocTable and InvertedIndex.
pub struct TopKBenchmarkSetup {
    /// Simulated document table: doc_id -> metadata.
    pub doc_table: HashMap<t_docId, DocMetadata>,
    /// The inverted index with block-level scoring metadata.
    pub index: InvertedIndex<FreqsOnly>,
    /// Pre-computed IDF for the term.
    pub idf: f64,
    /// Average document length (for BM25).
    pub avg_doc_len: f64,
    /// Total number of documents indexed.
    pub num_docs: usize,
}

impl TopKBenchmarkSetup {
    /// Create a new benchmark setup with the given number of documents.
    ///
    /// # Arguments
    /// * `num_docs` - Number of documents to index
    /// * `distribution` - Distribution for generating TF, doc_len, doc_score
    /// * `seed` - Random seed for reproducibility
    pub fn new(num_docs: usize, distribution: DataDistribution, seed: u64) -> Self {
        let mut rng = StdRng::seed_from_u64(seed);
        let mut doc_table = HashMap::with_capacity(num_docs);
        let flags = ffi::IndexFlags_Index_StoreFreqs;
        let mut index = InvertedIndex::<FreqsOnly>::new(flags);

        let mut total_doc_len: u64 = 0;

        for doc_id in 1..=num_docs as t_docId {
            let (tf, doc_len, doc_score) = match distribution {
                DataDistribution::Uniform => {
                    let tf = rng.random_range(1..=10);
                    let doc_len = rng.random_range(100..=1000);
                    let doc_score = 1.0f32;
                    (tf, doc_len, doc_score)
                }
                DataDistribution::Zipfian => {
                    // Simulate Zipfian: most docs have low TF, few have high
                    let r: f64 = rng.random();
                    let tf = (1.0 / r.powf(0.5)).min(100.0) as u32;
                    let doc_len = rng.random_range(50..=2000);
                    // Some docs have boosted scores
                    let doc_score = if rng.random::<f64>() < 0.1 {
                        rng.random_range(1.5..=3.0) as f32
                    } else {
                        1.0f32
                    };
                    (tf.max(1), doc_len, doc_score)
                }
            };

            doc_table.insert(doc_id, DocMetadata { doc_len, doc_score });
            total_doc_len += doc_len as u64;

            let record = RSIndexResult::virt().doc_id(doc_id).frequency(tf);
            index
                .add_record_with_metadata(&record, doc_len, doc_score)
                .expect("failed to add record");
        }

        let idf = (1.0 + (num_docs as f64 + 1.0) / (num_docs as f64)).ln();
        let avg_doc_len = total_doc_len as f64 / num_docs as f64;

        Self {
            doc_table,
            index,
            idf,
            avg_doc_len,
            num_docs,
        }
    }
}

/// Result entry for the min-heap (ordered by score ascending).
#[derive(PartialEq, PartialOrd)]
struct HeapEntry {
    score: f64,
    doc_id: t_docId,
}

impl Eq for HeapEntry {}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap_or(std::cmp::Ordering::Equal)
    }
}

/// Compute score based on scorer type.
fn compute_score(freq: u32, doc_meta: &DocMetadata, scorer: &BlockScorer) -> f64 {
    match scorer {
        BlockScorer::TfIdf { idf } => {
            (freq as f64 / doc_meta.doc_len as f64) * idf * doc_meta.doc_score as f64
        }
        BlockScorer::Bm25 {
            idf,
            avg_doc_len,
            k1,
            b,
        } => {
            let tf = freq as f64;
            let doc_len = doc_meta.doc_len as f64;
            let numerator = tf * (k1 + 1.0);
            let denominator = tf + k1 * (1.0 - b + b * doc_len / avg_doc_len);
            idf * (numerator / denominator) * doc_meta.doc_score as f64
        }
        BlockScorer::DocScore => doc_meta.doc_score as f64,
    }
}

/// Run Top-K query WITHOUT block skipping (baseline).
pub fn query_top_k_baseline(
    setup: &TopKBenchmarkSetup,
    k: usize,
    scorer: &BlockScorer,
) -> Vec<(t_docId, f64)> {
    let mut heap: BinaryHeap<Reverse<HeapEntry>> = BinaryHeap::with_capacity(k);
    let mut reader = setup.index.reader();
    let mut result = RSIndexResult::virt();

    while reader.next_record(&mut result).unwrap_or(false) {
        let doc_meta = &setup.doc_table[&result.doc_id];
        let score = compute_score(result.freq, doc_meta, scorer);

        if heap.len() < k {
            heap.push(Reverse(HeapEntry {
                score,
                doc_id: result.doc_id,
            }));
        } else if score > heap.peek().unwrap().0.score {
            heap.pop();
            heap.push(Reverse(HeapEntry {
                score,
                doc_id: result.doc_id,
            }));
        }
    }

    // Extract results sorted by score descending
    heap.into_sorted_vec()
        .into_iter()
        .map(|Reverse(e)| (e.doc_id, e.score))
        .collect()
}

/// Statistics from a Top-K query with block skipping.
#[derive(Debug, Clone, Default)]
pub struct SkipStats {
    /// Total number of blocks in the index.
    pub total_blocks: usize,
    /// Number of blocks that were skipped.
    pub blocks_skipped: usize,
    /// Number of blocks that were read.
    pub blocks_read: usize,
    /// Number of documents read.
    pub docs_read: usize,
}

impl std::fmt::Display for SkipStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let skip_pct = if self.total_blocks > 0 {
            100.0 * self.blocks_skipped as f64 / self.total_blocks as f64
        } else {
            0.0
        };
        write!(
            f,
            "blocks: {}/{} skipped ({:.1}%), docs_read: {}",
            self.blocks_skipped, self.total_blocks, skip_pct, self.docs_read
        )
    }
}

/// Run Top-K query WITH block skipping using advance_to_next_promising_block.
///
/// This implementation reads all records from the current block, then uses
/// `advance_to_next_promising_block` to skip blocks whose max score is below
/// the current threshold.
///
/// Returns both the results and skip statistics.
pub fn query_top_k_with_skipping_stats(
    setup: &TopKBenchmarkSetup,
    k: usize,
    scorer: &BlockScorer,
) -> (Vec<(t_docId, f64)>, SkipStats) {
    let mut heap: BinaryHeap<Reverse<HeapEntry>> = BinaryHeap::with_capacity(k);
    let mut reader = setup.index.reader();
    let mut result = RSIndexResult::virt();
    let mut min_score = 0.0;

    let total_blocks = setup.index.number_of_blocks();
    let mut blocks_skipped = 0usize;
    let mut blocks_read = 0usize;
    let mut docs_read = 0usize;
    let mut last_block_idx = usize::MAX;

    loop {
        // Check if current block is worth reading (only when heap is full)
        if heap.len() == k && reader.current_block_max_score(scorer) < min_score {
            // Count blocks skipped by advance_to_next_promising_block
            let before_idx = reader.current_block_index();
            if !reader.advance_to_next_promising_block(min_score, scorer) {
                // All remaining blocks were skipped
                blocks_skipped += total_blocks.saturating_sub(before_idx);
                break;
            }
            let after_idx = reader.current_block_index();
            blocks_skipped += after_idx - before_idx;
        }

        // Read next record
        if !reader.next_record(&mut result).unwrap_or(false) {
            break;
        }

        docs_read += 1;

        // Track block reads
        let current_block = reader.current_block_index();
        if current_block != last_block_idx {
            blocks_read += 1;
            last_block_idx = current_block;
        }

        let doc_meta = &setup.doc_table[&result.doc_id];
        let score = compute_score(result.freq, doc_meta, scorer);

        if heap.len() < k {
            heap.push(Reverse(HeapEntry {
                score,
                doc_id: result.doc_id,
            }));
        } else if score > heap.peek().unwrap().0.score {
            heap.pop();
            heap.push(Reverse(HeapEntry {
                score,
                doc_id: result.doc_id,
            }));
        }

        // Update threshold after heap modification
        if heap.len() == k {
            min_score = heap.peek().unwrap().0.score;
        }
    }

    let results = heap
        .into_sorted_vec()
        .into_iter()
        .map(|Reverse(e)| (e.doc_id, e.score))
        .collect();

    let stats = SkipStats {
        total_blocks,
        blocks_skipped,
        blocks_read,
        docs_read,
    };

    (results, stats)
}

/// Run Top-K query WITH block skipping (without returning stats).
pub fn query_top_k_with_skipping(
    setup: &TopKBenchmarkSetup,
    k: usize,
    scorer: &BlockScorer,
) -> Vec<(t_docId, f64)> {
    query_top_k_with_skipping_stats(setup, k, scorer).0
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Verify that baseline and with_skipping return identical results.
    #[test]
    fn baseline_and_skipping_return_same_results() {
        for num_docs in [1000, 5000] {
            for distribution in [DataDistribution::Uniform, DataDistribution::Zipfian] {
                let setup = TopKBenchmarkSetup::new(num_docs, distribution, 42);

                for k in [10, 50, 100] {
                    // Test with TF-IDF scorer
                    let scorer = BlockScorer::tfidf(setup.idf);
                    let baseline = query_top_k_baseline(&setup, k, &scorer);
                    let with_skipping = query_top_k_with_skipping(&setup, k, &scorer);

                    assert_eq!(
                        baseline.len(),
                        with_skipping.len(),
                        "TF-IDF: Result count mismatch for num_docs={num_docs}, k={k}, dist={distribution:?}"
                    );

                    for (i, ((b_id, b_score), (s_id, s_score))) in
                        baseline.iter().zip(with_skipping.iter()).enumerate()
                    {
                        assert_eq!(
                            b_id, s_id,
                            "TF-IDF: Doc ID mismatch at position {i} for num_docs={num_docs}, k={k}, dist={distribution:?}"
                        );
                        assert!(
                            (b_score - s_score).abs() < 1e-10,
                            "TF-IDF: Score mismatch at position {i}: baseline={b_score}, skipping={s_score}"
                        );
                    }

                    // Test with BM25 scorer
                    let scorer = BlockScorer::bm25(setup.idf, setup.avg_doc_len, 1.2, 0.75);
                    let baseline = query_top_k_baseline(&setup, k, &scorer);
                    let with_skipping = query_top_k_with_skipping(&setup, k, &scorer);

                    assert_eq!(
                        baseline.len(),
                        with_skipping.len(),
                        "BM25: Result count mismatch for num_docs={num_docs}, k={k}, dist={distribution:?}"
                    );

                    for (i, ((b_id, _), (s_id, _))) in
                        baseline.iter().zip(with_skipping.iter()).enumerate()
                    {
                        assert_eq!(
                            b_id, s_id,
                            "BM25: Doc ID mismatch at position {i} for num_docs={num_docs}, k={k}, dist={distribution:?}"
                        );
                    }
                }
            }
        }
    }
}
