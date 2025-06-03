/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Benchmark the core varint operations: encoding and decoding of integers and field masks.

use criterion::{Criterion, criterion_group, criterion_main};
use std::time::Duration;
use varint_bencher::VarintBencher;

fn benchmark_small_values(c: &mut Criterion) {
    let bencher = VarintBencher::new("Small Values (0-127)".to_owned(), Duration::from_secs(3));

    bencher.encode_group(c);
    bencher.decode_group(c);
    bencher.encode_field_mask_group(c);
    bencher.decode_field_mask_group(c);
}

fn benchmark_medium_values(c: &mut Criterion) {
    let bencher = VarintBencher::new(
        "Medium Values (128-16383)".to_owned(),
        Duration::from_secs(5),
    );

    bencher.encode_group(c);
    bencher.decode_group(c);
    bencher.encode_field_mask_group(c);
    bencher.decode_field_mask_group(c);
}

fn benchmark_large_values(c: &mut Criterion) {
    let bencher = VarintBencher::new("Large Values (16384+)".to_owned(), Duration::from_secs(5));

    bencher.encode_group(c);
    bencher.decode_group(c);
    bencher.encode_field_mask_group(c);
    bencher.decode_field_mask_group(c);
}

fn benchmark_mixed_values(c: &mut Criterion) {
    let bencher = VarintBencher::new("Mixed Values".to_owned(), Duration::from_secs(8));

    bencher.encode_group(c);
    bencher.decode_group(c);
    bencher.encode_field_mask_group(c);
    bencher.decode_field_mask_group(c);
}

criterion_group!(small_values, benchmark_small_values);
criterion_group!(medium_values, benchmark_medium_values);
criterion_group!(large_values, benchmark_large_values);
criterion_group!(mixed_values, benchmark_mixed_values);
criterion_main!(small_values, medium_values, large_values, mixed_values);
