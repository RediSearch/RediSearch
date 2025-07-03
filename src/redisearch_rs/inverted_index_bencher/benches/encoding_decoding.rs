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

criterion_group!(benches, benchmark_numeric);
criterion_main!(benches);
