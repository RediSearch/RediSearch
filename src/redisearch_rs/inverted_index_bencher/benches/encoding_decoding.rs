/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Benchmarks the numeric encoding and decoding

use std::time::Duration;

use criterion::{Criterion, criterion_group, criterion_main};
use inverted_index_bencher::benchers;

fn benchmark_numeric(c: &mut Criterion) {
    let bencher = benchers::numeric::Bencher::new();

    bencher.encoding(c);
    bencher.decoding(c);
}

fn benchmark_freqs_only(c: &mut Criterion) {
    let bencher = benchers::freqs_only::Bencher::new();

    bencher.encoding(c);
    bencher.decoding(c);
}

fn benchmark_freqs_fields(c: &mut Criterion) {
    let bencher = benchers::freqs_fields::Bencher::default();
    bencher.encoding(c);
    bencher.decoding(c);

    let bencher = benchers::freqs_fields::Bencher::wide();
    bencher.encoding(c);
    bencher.decoding(c);
}

fn benchmark_fields_only(c: &mut Criterion) {
    let bencher = benchers::fields_only::Bencher::default();
    bencher.encoding(c);
    bencher.decoding(c);

    let bencher = benchers::fields_only::Bencher::wide();
    bencher.encoding(c);
    bencher.decoding(c);
}

fn benchmark_doc_ids_only(c: &mut Criterion) {
    let bencher = benchers::doc_ids_only::Bencher::default();
    bencher.encoding(c);
    bencher.decoding(c);
}

fn benchmark_raw_doc_ids_only(c: &mut Criterion) {
    let bencher = benchers::raw_doc_ids_only::Bencher::default();
    bencher.encoding(c);
    bencher.decoding(c);
}

fn benchmark_full(c: &mut Criterion) {
    let bencher = benchers::full::Bencher::default();
    bencher.encoding(c);
    bencher.decoding(c);

    let bencher = benchers::full::Bencher::wide();
    bencher.encoding(c);
    bencher.decoding(c);
}

fn benchmark_fields_offsets(c: &mut Criterion) {
    let bencher = benchers::fields_offsets::Bencher::default();
    bencher.encoding(c);
    bencher.decoding(c);

    let bencher = benchers::fields_offsets::Bencher::wide();
    bencher.encoding(c);
    bencher.decoding(c);
}

fn benchmark_offsets_only(c: &mut Criterion) {
    let bencher = benchers::offsets_only::Bencher::default();
    bencher.encoding(c);
    bencher.decoding(c);
}

fn benchmark_freqs_offsets(c: &mut Criterion) {
    let bencher = benchers::freqs_offsets::Bencher::default();
    bencher.encoding(c);
    bencher.decoding(c);
}

criterion_group!(
    name = benches;
    config = Criterion::default().measurement_time(Duration::from_millis(500)).warm_up_time(Duration::from_millis(200));
    targets = benchmark_numeric,
        benchmark_freqs_only,
        benchmark_freqs_fields,
        benchmark_fields_only,
        benchmark_doc_ids_only,
        benchmark_raw_doc_ids_only,
        benchmark_full,
        benchmark_fields_offsets,
        benchmark_offsets_only,
        benchmark_freqs_offsets,
);

criterion_main!(benches);
