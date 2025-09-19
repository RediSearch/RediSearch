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
use std::{hint::black_box, time::Duration};
use varint::VarintEncode;

/// A helper struct for benchmarking varint operations.
pub struct VarintBencher {
    /// `u32` benchmarking inputs.
    u32_values: Vec<BenchInputs<u32>>,
    /// `u64` benchmarking inputs.
    u64_values: Vec<BenchInputs<u64>>,

    /// How long to run benchmarks overall.
    measurement_time: Duration,
}

impl VarintBencher {
    /// Creates a new `VarintBencher` instance with different value ranges.
    pub fn new(measurement_time: Duration) -> Self {
        let test_values = generate_test_values();
        let u64_values = test_values
            .iter()
            .map(|input| BenchInputs {
                values: input.values.iter().map(|&v| v as u64).collect(),
                n_bytes: input.n_bytes,
            })
            .collect();

        Self {
            u32_values: test_values,
            u64_values,
            measurement_time,
        }
    }

    fn benchmark_group<'a>(
        &self,
        c: &'a mut Criterion,
        label: &str,
    ) -> BenchmarkGroup<'a, WallTime> {
        let mut group = c.benchmark_group(format!("Varint | {label}"));
        group.measurement_time(self.measurement_time);
        group.warm_up_time(Duration::from_secs(5));
        group
    }

    /// Benchmark varint encoding operations.
    pub fn encode_u32(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Encode u32");
        for bench_input in &self.u32_values {
            encode_u32_benchmark(&mut group, bench_input);
        }
        group.finish();
    }

    /// Benchmark u64 varint encoding operations.
    pub fn encode_u64(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Encode u64");
        for bench_input in &self.u64_values {
            encode_u64_benchmark(&mut group, bench_input);
        }
        group.finish();
    }

    /// Benchmark varint decoding operations.
    pub fn decode_u32(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Decode u32");
        for bench_input in &self.u32_values {
            decode_u32_benchmark(&mut group, bench_input);
        }
        group.finish();
    }

    /// Benchmark u64 varint decoding operations.
    pub fn decode_u64(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Decode u64");
        for bench_input in &self.u64_values {
            decode_u64_benchmark(&mut group, bench_input);
        }
        group.finish();
    }
}

/// Generate test values covering different varint sizes:
/// - Single byte: 0-127
/// - Two bytes: 128-16383
/// - Three bytes: 16384-2097151
/// - Four bytes: 2097152-268435455
/// - Five bytes: 268435456-u32::MAX
fn generate_test_values() -> Vec<BenchInputs<u32>> {
    vec![
        // Single byte values (0-127).
        BenchInputs {
            values: vec![10, 50, 100, 127],
            n_bytes: 1,
        },
        // Two byte values (128-16383).
        BenchInputs {
            values: vec![128, 1000, 8000, 16383],
            n_bytes: 2,
        },
        // Three byte values (16384-2097151).
        BenchInputs {
            values: vec![16384, 100000, 1000000, 2097151],
            n_bytes: 3,
        },
        // Four byte values (2097152-268435455).
        BenchInputs {
            values: vec![2097152, 50000000, 200000000, 268435455],
            n_bytes: 4,
        },
        // Five byte values (268435456-u32::MAX).
        BenchInputs {
            values: vec![268435456, 1000000000, 3000000000, u32::MAX - 1],
            n_bytes: 5,
        },
    ]
}

pub struct BenchInputs<T> {
    pub values: Vec<T>,
    /// The number of bytes required to encode each value.
    pub n_bytes: usize,
}

fn encode_u32_benchmark<M: Measurement>(
    group: &mut BenchmarkGroup<'_, M>,
    inputs: &BenchInputs<u32>,
) {
    let BenchInputs { values, n_bytes } = inputs;
    group.bench_function(format!("{n_bytes} bytes"), |b| {
        b.iter_batched_ref(
            || Vec::with_capacity(1024),
            |buf| {
                for &value in values {
                    black_box(value).write_as_varint(&mut *buf).unwrap();
                }
            },
            BatchSize::SmallInput,
        );
    });
}

fn encode_u64_benchmark<M: Measurement>(
    group: &mut BenchmarkGroup<'_, M>,
    inputs: &BenchInputs<u64>,
) {
    let BenchInputs { values, n_bytes } = inputs;
    group.bench_function(format!("{n_bytes} bytes"), |b| {
        b.iter_batched_ref(
            || Vec::with_capacity(1024),
            |buf| {
                for &value in values {
                    black_box(value).write_as_varint(&mut *buf).unwrap();
                }
            },
            BatchSize::SmallInput,
        );
    });
}

fn decode_u32_benchmark<M: Measurement>(
    group: &mut BenchmarkGroup<'_, M>,
    inputs: &BenchInputs<u32>,
) {
    let BenchInputs { values, n_bytes } = inputs;
    // Pre-encode the values.
    let encoded_values: Vec<Vec<u8>> = values
        .iter()
        .map(|&value| {
            let mut buf = Vec::new();
            value.write_as_varint(&mut buf).unwrap();
            buf
        })
        .collect();

    group.bench_function(format!("{n_bytes} bytes"), |b| {
        b.iter(|| {
            for encoded in &encoded_values {
                let mut reader = encoded.as_slice();
                let decoded = u32::read_as_varint(&mut reader).unwrap();
                black_box(decoded);
            }
        })
    });
}

fn decode_u64_benchmark<M: Measurement>(
    group: &mut BenchmarkGroup<'_, M>,
    inputs: &BenchInputs<u64>,
) {
    let BenchInputs { values, n_bytes } = inputs;
    // Pre-encode the u64.
    let encoded_values: Vec<Vec<u8>> = values
        .iter()
        .map(|&value| {
            let mut buf = Vec::new();
            value.write_as_varint(&mut buf).unwrap();
            buf
        })
        .collect();

    group.bench_function(format!("{n_bytes} bytes"), |b| {
        b.iter(|| {
            for encoded in &encoded_values {
                let mut reader = encoded.as_slice();
                let decoded = u64::read_as_varint(&mut reader).unwrap();
                black_box(decoded);
            }
        })
    });
}
