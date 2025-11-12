/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Benchmark the vector writer operations for varints.

use criterion::{BatchSize, Criterion, black_box, criterion_group, criterion_main};
use varint::VectorWriter;

fn benchmark_vector_writer(c: &mut Criterion) {
    let values: Vec<_> = (0..100).map(|i| i * 1000).collect();
    c.bench_function("Sequential inputs", |b| {
        b.iter_batched(
            || VectorWriter::new(1024),
            |mut writer| {
                for value in &values {
                    let _size = writer.write(black_box(*value)).unwrap();
                }
                black_box(writer.bytes_len());
            },
            BatchSize::SmallInput,
        )
    });
}

criterion_group!(vector_writer, benchmark_vector_writer);
criterion_main!(vector_writer);
