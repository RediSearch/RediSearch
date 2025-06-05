/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Benchmark the vector writer operations for varints.

use criterion::{Criterion, criterion_group, criterion_main};
use std::time::Duration;
use varint_bencher::VarintBencher;

fn benchmark_vector_writer(c: &mut Criterion) {
    let bencher = VarintBencher::new("Vector Writer".to_owned(), Duration::from_secs(10));
    bencher.vector_writer_group(c);
}

criterion_group!(vector_writer, benchmark_vector_writer);
criterion_main!(vector_writer);
