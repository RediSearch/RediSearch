/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Benchmark iterators

use criterion::{Criterion, criterion_group, criterion_main};
use rqe_iterators_bencher::benchers;

fn benchmark_empty(c: &mut Criterion) {
    let bencher = benchers::empty::Bencher::default();
    bencher.bench(c);
}

fn benchmark_id_list(c: &mut Criterion) {
    let bencher = benchers::id_list::Bencher::default();
    bencher.bench(c);
}

fn benchmark_metric(c: &mut Criterion) {
    let bencher = benchers::metric::Bencher::default();
    bencher.bench(c);
}

fn benchmark_wildcard(c: &mut Criterion) {
    let bencher = benchers::wildcard::Bencher::default();
    bencher.bench(c);
}

fn benchmark_inverted_index_numeric_full(c: &mut Criterion) {
    let bencher = benchers::inverted_index::NumericFullBencher::default();
    bencher.bench(c);
}

criterion_group!(
    benches,
    benchmark_empty,
    benchmark_id_list,
    benchmark_metric,
    benchmark_wildcard,
    benchmark_inverted_index_numeric_full,
);

criterion_main!(benches);
