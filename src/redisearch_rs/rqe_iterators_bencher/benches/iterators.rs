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

criterion_group!(benches, benchmark_empty,);

criterion_main!(benches);
