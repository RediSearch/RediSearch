/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use criterion::{Criterion, criterion_group, criterion_main};
use quantile::QuantStream;
use std::{hint::black_box, num::NonZeroUsize};

extern crate redisearch_rs;
redis_mock::mock_or_stub_missing_redis_c_symbols!();

fn stream(c: &mut Criterion) {
    for (name, count, capacity, targets) in [
        ("small", 24, 500, &[][..]),
        ("ascending", 10000, 500, &[][..]),
        ("shuffled", 10000, 500, &[][..]),
        ("descending", 10000, 500, &[][..]),
        ("duplicates", 10000, 500, &[][..]),
        ("targeted", 10000, 500, &[0.5, 0.9, 0.99][..]),
        ("small_buffer", 10000, 7, &[][..]),
    ] {
        let values: Vec<f64> = (0..count)
            .map(|i| match name {
                "ascending" => i as f64,
                "descending" => (count - i) as f64,
                "duplicates" => (i % 3) as f64,
                _ => ((i * 997) % count) as f64,
            })
            .collect();
        let mut group = c.benchmark_group(name);
        group.bench_function("c", |b| {
            b.iter(|| {
                // SAFETY: targets is readable for its length, capacity is positive,
                // and the new stream is exclusively used here and freed exactly once.
                #[expect(clippy::multiple_unsafe_ops_per_block)]
                unsafe {
                    let stream = ffi::NewQuantileStream(targets.as_ptr(), targets.len(), capacity);
                    for &value in &values {
                        ffi::QS_Insert(stream, black_box(value));
                    }
                    for q in [0.5, 0.9, 0.99] {
                        black_box(ffi::QS_Query(stream, q));
                    }
                    ffi::QS_Free(stream);
                }
            })
        });
        group.bench_function("rust", |b| {
            b.iter(|| {
                let mut stream = QuantStream::new(targets, NonZeroUsize::new(capacity).unwrap());
                for &value in &values {
                    stream.insert(black_box(value));
                }
                for q in [0.5, 0.9, 0.99] {
                    black_box(stream.query(q));
                }
            })
        });
        group.finish();
    }
}

criterion_group!(benches, stream);
criterion_main!(benches);
