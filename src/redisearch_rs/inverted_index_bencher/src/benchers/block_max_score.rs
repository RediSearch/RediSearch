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
            index.add_record(&record).expect("failed to add record");
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

/// Run Top-K query WITH block skipping using skip_to_with_threshold.
pub fn query_top_k_with_skipping(
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

        // Update threshold and skip to next promising block
        if heap.len() == k {
            let min_score = heap.peek().unwrap().0.score;
            // Skip to next doc, but use threshold to skip low-score blocks
            let next_doc = result.doc_id + 1;
            if !reader.skip_to_with_threshold(next_doc, min_score, scorer) {
                break;
            }
        }
    }

    heap.into_sorted_vec()
        .into_iter()
        .map(|Reverse(e)| (e.doc_id, e.score))
        .collect()
}
