/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Criterion benchmarks for block-max score optimization.
//!
//! These benchmarks compare Top-K query performance with and without
//! block-level score skipping.

use std::hint::black_box;
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use inverted_index::block_max_score::BlockScorer;
use inverted_index_bencher::benchers::block_max_score::{
    DataDistribution, TopKBenchmarkSetup, query_top_k_baseline, query_top_k_with_skipping,
    query_top_k_with_skipping_stats,
};

/// Benchmark Top-K queries with different index sizes, distributions, scorers, and k values.
fn bench_top_k_query(c: &mut Criterion) {
    let mut group = c.benchmark_group("top_k_query");

    // Test different index sizes
    for num_docs in [10_000, 100_000, 1_000_000] {
        // Test different distributions
        for (dist_name, distribution) in [
            ("uniform", DataDistribution::Uniform),
            ("zipfian", DataDistribution::Zipfian),
        ] {
            let setup = TopKBenchmarkSetup::new(num_docs, distribution, 42);

            let scorers = [
                ("tfidf", BlockScorer::tfidf(setup.idf)),
                (
                    "bm25",
                    BlockScorer::bm25(setup.idf, setup.avg_doc_len, 1.2, 0.75),
                ),
                ("docscore", BlockScorer::doc_score()),
            ];

            // Test different k values
            for k in [10, 100, 1000] {
                for (scorer_name, scorer) in &scorers {
                    let param_name = format!("{num_docs}_{dist_name}_{scorer_name}_k{k}");

                    // Print skip stats once before benchmarking
                    let (_, stats) = query_top_k_with_skipping_stats(&setup, k, scorer);
                    eprintln!("[{param_name}] {stats}");

                    group.bench_with_input(
                        BenchmarkId::new("baseline", &param_name),
                        &(&setup, scorer, k),
                        |b, (setup, scorer, k)| {
                            b.iter(|| black_box(query_top_k_baseline(setup, *k, scorer)))
                        },
                    );

                    group.bench_with_input(
                        BenchmarkId::new("with_skipping", &param_name),
                        &(&setup, scorer, k),
                        |b, (setup, scorer, k)| {
                            b.iter(|| black_box(query_top_k_with_skipping(setup, *k, scorer)))
                        },
                    );
                }
            }
        }
    }

    group.finish();
}

criterion_group!(
    name = benches;
    config = Criterion::default()
        .measurement_time(Duration::from_secs(3))
        .warm_up_time(Duration::from_secs(1));
    targets = bench_top_k_query,
);

criterion_main!(benches);
