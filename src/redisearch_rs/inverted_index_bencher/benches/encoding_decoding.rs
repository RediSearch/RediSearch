/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Benchmarks the numeric encoding and decoding

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

fn benchmark_full(c: &mut Criterion) {
    let bencher = benchers::full::Bencher::default();
    bencher.encoding(c);
    bencher.decoding(c);

    let bencher = benchers::full::Bencher::wide();
    bencher.encoding(c);
    bencher.decoding(c);
}

criterion_group!(
    benches,
    benchmark_numeric,
    benchmark_freqs_only,
    benchmark_freqs_fields,
    benchmark_fields_only,
    benchmark_doc_ids_only,
    benchmark_full,
);

criterion_main!(benches);
