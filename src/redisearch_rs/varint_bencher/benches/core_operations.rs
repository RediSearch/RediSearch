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

fn benchmark_core_operations(c: &mut Criterion) {
    let bencher = VarintBencher::new("Varint".to_owned(), Duration::from_secs(10));

    bencher.encode_group(c);
    bencher.decode_group(c);
    bencher.encode_field_mask_group(c);
    bencher.decode_field_mask_group(c);
}

criterion_group!(core_operations, benchmark_core_operations);
criterion_main!(core_operations);
