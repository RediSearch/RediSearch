/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use criterion::{
    BatchSize, BenchmarkGroup, Criterion,
    measurement::{Measurement, WallTime},
};
use encode_decode::varint;
use std::{hint::black_box, time::Duration};

use crate::FieldMask;

use crate::c_varint::{CVarintVectorWriter, c_varint_ops};

/// A helper struct for benchmarking varint operations against C implementation.
pub struct VarintBencher {
    /// Test values for benchmarking.
    test_values: Vec<u32>,

    /// Field mask test values.
    field_mask_values: Vec<FieldMask>,

    /// How long to run benchmarks overall.
    measurement_time: Duration,

    /// The prefix added to the label of each benchmark group.
    prefix: String,
}

impl VarintBencher {
    /// Creates a new `VarintBencher` instance with different value ranges.
    pub fn new(prefix: String, measurement_time: Duration) -> Self {
        let test_values = generate_test_values();
        let field_mask_values = test_values.iter().map(|&v| v as FieldMask).collect();

        Self {
            prefix,
            test_values,
            field_mask_values,
            measurement_time,
        }
    }

    fn benchmark_group<'a>(
        &self,
        c: &'a mut Criterion,
        label: &str,
    ) -> BenchmarkGroup<'a, WallTime> {
        let mut group = c.benchmark_group(format!("{}|{}", self.prefix, label));
        group.measurement_time(self.measurement_time);
        group.warm_up_time(Duration::from_secs(5));
        group
    }

    /// Benchmark varint encoding operations.
    /// Benchmark single varint encoding.
    pub fn encode_group(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Encode");
        encode_rust_benchmark(&mut group, &self.test_values);
        encode_c_benchmark(&mut group, &self.test_values);
        group.finish();
    }

    /// Benchmark field mask encoding operations.
    pub fn encode_field_mask_group(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Encode FieldMask");
        encode_field_mask_rust_benchmark(&mut group, &self.field_mask_values);
        encode_field_mask_c_benchmark(&mut group, &self.field_mask_values);
        group.finish();
    }

    /// Benchmark varint decoding operations.
    pub fn decode_group(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Decode");
        decode_rust_benchmark(&mut group, &self.test_values);
        decode_c_benchmark(&mut group, &self.test_values);
        group.finish();
    }

    /// Benchmark field mask decoding operations.
    pub fn decode_field_mask_group(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Decode FieldMask");
        decode_field_mask_rust_benchmark(&mut group, &self.field_mask_values);
        decode_field_mask_c_benchmark(&mut group, &self.field_mask_values);
        group.finish();
    }

    /// Benchmark vector writer operations.
    pub fn vector_writer_group(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Vector Writer");
        vector_writer_rust_benchmark(&mut group, &self.test_values);
        vector_writer_c_benchmark(&mut group, &self.test_values);
        group.finish();
    }
}

/// Generate test values covering different varint sizes:
/// - Single byte: 0-127
/// - Two bytes: 128-16383
/// - Three bytes: 16384-2097151
/// - Four bytes: 2097152-268435455
/// - Five bytes: 268435456-u32::MAX
fn generate_test_values() -> Vec<u32> {
    let mut values = Vec::new();

    // Edge cases.
    values.extend([0, 1, u32::MAX]);

    // Single byte values (0-127).
    values.extend([10, 50, 100, 127]);

    // Two byte values (128-16383).
    values.extend([128, 1000, 8000, 16383]);

    // Three byte values (16384-2097151).
    values.extend([16384, 100000, 1000000, 2097151]);

    // Four byte values (2097152-268435455).
    values.extend([2097152, 50000000, 200000000, 268435455]);

    // Five byte values (268435456-u32::MAX).
    values.extend([268435456, 1000000000, 3000000000, u32::MAX - 1]);

    // Sequential pattern for delta encoding tests.
    values.extend((0..100).map(|i| i * 1000));

    values
}

fn encode_rust_benchmark<M: Measurement>(group: &mut BenchmarkGroup<'_, M>, values: &[u32]) {
    let mut buf = Vec::with_capacity(1024);
    group.bench_function("Rust", |b| {
        b.iter(|| {
            for &value in values {
                varint::write(black_box(value), &mut buf).unwrap();
            }
        })
    });
}

fn encode_c_benchmark<M: Measurement>(group: &mut BenchmarkGroup<'_, M>, values: &[u32]) {
    // Allocate buffer once for the entire benchmark
    let buffer_ptr = unsafe { crate::RedisModule_Alloc.unwrap()(1024) };

    group.bench_function("C", |b| {
        b.iter(|| {
            for &value in values {
                // Only time the encoding operation
                let mut buffer = crate::ffi::Buffer {
                    data: buffer_ptr as *mut i8,
                    offset: 0,
                    cap: 1024,
                };
                let _bytes_written = c_varint_ops::write(black_box(value), &mut buffer);
                black_box(buffer.offset);
            }
        })
    });

    // Free buffer after benchmark
    unsafe {
        crate::RedisModule_Free.unwrap()(buffer_ptr);
    }
}

fn encode_field_mask_rust_benchmark<M: Measurement>(
    group: &mut BenchmarkGroup<'_, M>,
    values: &[FieldMask],
) {
    let mut buf = Vec::new();

    group.bench_function("Rust", |b| {
        b.iter(|| {
            for &value in values {
                varint::write_field_mask(black_box(value), &mut buf).unwrap();
            }
        })
    });
}

fn encode_field_mask_c_benchmark<M: Measurement>(
    group: &mut BenchmarkGroup<'_, M>,
    values: &[FieldMask],
) {
    // Allocate buffer once for the entire benchmark
    let buffer_ptr = unsafe { crate::RedisModule_Alloc.unwrap()(1024) };

    group.bench_function("C", |b| {
        b.iter(|| {
            for &value in values {
                // Only time the encoding operation
                let mut buffer = crate::ffi::Buffer {
                    data: buffer_ptr as *mut i8,
                    offset: 0,
                    cap: 1024,
                };
                let _bytes_written = c_varint_ops::write_field_mask(black_box(value), &mut buffer);
                black_box(buffer.offset);
            }
        })
    });

    // Free buffer after benchmark
    unsafe {
        crate::RedisModule_Free.unwrap()(buffer_ptr);
    }
}

fn vector_writer_rust_benchmark<M: Measurement>(group: &mut BenchmarkGroup<'_, M>, values: &[u32]) {
    group.bench_function("Rust", |b| {
        b.iter_batched(
            || varint::VectorWriter::new(1024),
            |mut writer| {
                for &value in values {
                    let _size = writer.write(black_box(value)).unwrap();
                }
                black_box(writer.bytes_len());
            },
            BatchSize::SmallInput,
        )
    });
}

fn vector_writer_c_benchmark<M: Measurement>(group: &mut BenchmarkGroup<'_, M>, values: &[u32]) {
    group.bench_function("C", |b| {
        b.iter_batched(
            || CVarintVectorWriter::new(1024),
            |mut writer| {
                for &value in values {
                    let _size = writer.write(black_box(value));
                }
                black_box(writer.bytes_len());
            },
            BatchSize::SmallInput,
        )
    });
}

fn decode_rust_benchmark<M: Measurement>(group: &mut BenchmarkGroup<'_, M>, values: &[u32]) {
    // Pre-encode the values
    let encoded_values: Vec<Vec<u8>> = values
        .iter()
        .map(|&value| {
            let mut buf = Vec::new();
            varint::write(value, &mut buf).unwrap();
            buf
        })
        .collect();

    group.bench_function("Rust", |b| {
        b.iter(|| {
            for encoded in &encoded_values {
                let mut reader = encoded.as_slice();
                let decoded = varint::read(&mut reader).unwrap();
                black_box(decoded);
            }
        })
    });
}

fn decode_field_mask_rust_benchmark<M: Measurement>(
    group: &mut BenchmarkGroup<'_, M>,
    values: &[FieldMask],
) {
    // Pre-encode the field masks
    let encoded_values: Vec<Vec<u8>> = values
        .iter()
        .map(|&value| {
            let mut buf = Vec::new();
            varint::write_field_mask(value, &mut buf).unwrap();
            buf
        })
        .collect();

    group.bench_function("Rust", |b| {
        b.iter(|| {
            for encoded in &encoded_values {
                let mut reader = encoded.as_slice();
                let decoded = varint::read_field_mask(&mut reader).unwrap();
                black_box(decoded);
            }
        })
    });
}

fn decode_c_benchmark<M: Measurement>(group: &mut BenchmarkGroup<'_, M>, values: &[u32]) {
    // Pre-encode the values using C implementation
    let encoded_values: Vec<Vec<u8>> = values
        .iter()
        .map(|&value| c_varint_ops::write_to_vec(value))
        .collect();

    group.bench_function("C", |b| {
        b.iter(|| {
            for encoded in &encoded_values {
                let decoded = c_varint_ops::read(encoded);
                black_box(decoded);
            }
        })
    });
}

fn decode_field_mask_c_benchmark<M: Measurement>(
    group: &mut BenchmarkGroup<'_, M>,
    values: &[FieldMask],
) {
    // Pre-encode the field masks using C implementation
    let encoded_values: Vec<Vec<u8>> = values
        .iter()
        .map(|&value| c_varint_ops::write_field_mask_to_vec(value))
        .collect();

    group.bench_function("C", |b| {
        b.iter(|| {
            for encoded in &encoded_values {
                let decoded = c_varint_ops::read_field_mask(encoded);
                black_box(decoded);
            }
        })
    });
}
