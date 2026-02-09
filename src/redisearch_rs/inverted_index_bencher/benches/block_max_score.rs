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

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use inverted_index::block_max_score::BlockScorer;
use inverted_index_bencher::benchers::block_max_score::{
    DataDistribution, TopKBenchmarkSetup, query_top_k_baseline, query_top_k_with_skipping,
};

/// Benchmark Top-K queries with different index sizes and distributions.
fn bench_top_k_query(c: &mut Criterion) {
    let mut group = c.benchmark_group("top_k_query");

    // Test different index sizes
    for num_docs in [10_000, 100_000] {
        // Test different distributions
        for (dist_name, distribution) in [
            ("uniform", DataDistribution::Uniform),
            ("zipfian", DataDistribution::Zipfian),
        ] {
            let setup = TopKBenchmarkSetup::new(num_docs, distribution, 42);
            let scorer = BlockScorer::tfidf(setup.idf);
            let k = 10;

            let param_name = format!("{num_docs}_{dist_name}");

            group.bench_with_input(
                BenchmarkId::new("baseline", &param_name),
                &(&setup, &scorer, k),
                |b, (setup, scorer, k)| {
                    b.iter(|| black_box(query_top_k_baseline(setup, *k, scorer)))
                },
            );

            group.bench_with_input(
                BenchmarkId::new("with_skipping", &param_name),
                &(&setup, &scorer, k),
                |b, (setup, scorer, k)| {
                    b.iter(|| black_box(query_top_k_with_skipping(setup, *k, scorer)))
                },
            );
        }
    }

    group.finish();
}

/// Benchmark Top-K queries with different K values.
fn bench_different_k_values(c: &mut Criterion) {
    let mut group = c.benchmark_group("top_k_varying_k");

    let setup = TopKBenchmarkSetup::new(100_000, DataDistribution::Zipfian, 42);
    let scorer = BlockScorer::tfidf(setup.idf);

    for k in [10, 100, 1000] {
        group.bench_with_input(
            BenchmarkId::new("baseline", k),
            &(&setup, &scorer, k),
            |b, (setup, scorer, k)| {
                b.iter(|| black_box(query_top_k_baseline(setup, *k, scorer)))
            },
        );

        group.bench_with_input(
            BenchmarkId::new("with_skipping", k),
            &(&setup, &scorer, k),
            |b, (setup, scorer, k)| {
                b.iter(|| black_box(query_top_k_with_skipping(setup, *k, scorer)))
            },
        );
    }

    group.finish();
}

/// Benchmark Top-K queries with different scorers.
fn bench_different_scorers(c: &mut Criterion) {
    let mut group = c.benchmark_group("top_k_scorers");

    let setup = TopKBenchmarkSetup::new(100_000, DataDistribution::Zipfian, 42);
    let k = 10;

    let scorers = [
        ("tfidf", BlockScorer::tfidf(setup.idf)),
        ("bm25", BlockScorer::bm25(setup.idf, setup.avg_doc_len, 1.2, 0.75)),
        ("docscore", BlockScorer::doc_score()),
    ];

    for (name, scorer) in &scorers {
        group.bench_with_input(
            BenchmarkId::new("baseline", name),
            &(&setup, scorer, k),
            |b, (setup, scorer, k)| {
                b.iter(|| black_box(query_top_k_baseline(setup, *k, scorer)))
            },
        );

        group.bench_with_input(
            BenchmarkId::new("with_skipping", name),
            &(&setup, scorer, k),
            |b, (setup, scorer, k)| {
                b.iter(|| black_box(query_top_k_with_skipping(setup, *k, scorer)))
            },
        );
    }

    group.finish();
}

criterion_group!(
    name = benches;
    config = Criterion::default()
        .measurement_time(Duration::from_secs(3))
        .warm_up_time(Duration::from_secs(1));
    targets = bench_top_k_query,
        bench_different_k_values,
        bench_different_scorers,
);

criterion_main!(benches);
