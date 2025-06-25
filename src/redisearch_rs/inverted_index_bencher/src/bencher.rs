/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{collections::HashMap, io::Cursor, ptr::NonNull, time::Duration};

use buffer::{Buffer, BufferReader, BufferWriter};
use criterion::{
    BatchSize, BenchmarkGroup, BenchmarkId, Criterion, black_box,
    measurement::{Measurement, WallTime},
};
use inverted_index::{Decoder, Delta, Encoder, numeric::Numeric};
use itertools::Itertools;

use crate::ffi::{TestBuffer, encode_numeric, read_numeric};

pub struct NumericBencher {
    test_values: Vec<BenchGroup>,
}

impl NumericBencher {
    const MEASUREMENT_TIME: Duration = Duration::from_secs(10);
    const WARMUP_TIME: Duration = Duration::from_secs(5);

    pub fn new() -> Self {
        let test_values = generate_test_values();

        Self { test_values }
    }

    fn benchmark_group<'a>(
        &self,
        c: &'a mut Criterion,
        label: &str,
    ) -> BenchmarkGroup<'a, WallTime> {
        let mut group = c.benchmark_group(label);
        group.measurement_time(Self::MEASUREMENT_TIME);
        group.warm_up_time(Self::WARMUP_TIME);
        group
    }

    pub fn encoding(&self, c: &mut Criterion) {
        for BenchGroup { group, inputs } in &self.test_values {
            let group = format!("Encode | {}", group);
            let mut group = self.benchmark_group(c, &group);

            for input in inputs {
                numeric_c_encode(&mut group, input);
                numeric_rust_encode(&mut group, input);
            }

            group.finish();
        }
    }

    pub fn decoding(&self, c: &mut Criterion) {
        for BenchGroup { group, inputs } in &self.test_values {
            let group = format!("Decode | {}", group);
            let mut group = self.benchmark_group(c, &group);

            for input in inputs {
                numeric_c_decode(&mut group, input);
                numeric_rust_decode(&mut group, input);
            }

            group.finish();
        }
    }
}

#[derive(Clone)]
struct BenchEncodingInputs<'a> {
    group: &'a str,
    values: Vec<f64>,
    value_size_fn: fn(f64) -> usize,
}

struct BenchGroup {
    group: String,
    inputs: Vec<BenchInput>,
}

struct BenchInput {
    /// Stores the value, delta and the encoded buffer for this benchmark run
    values: Vec<(f64, usize, Vec<u8>)>,
    delta_size: usize,
    value_size: usize,
}

fn generate_test_values() -> Vec<BenchGroup> {
    let encoding_values = vec![
        BenchEncodingInputs {
            group: "TinyInt",
            values: vec![0.0, 3.0, 7.0],
            value_size_fn: |_| 0,
        },
        BenchEncodingInputs {
            group: "PosInt",
            values: vec![
                // 1 byte
                8.0,
                10.0,
                100.0,
                // 5 bytes
                10_000_000_000.0,
                100_000_000_000.0,
                1_000_000_000_000.0,
                // 8 bytes
                100_000_000_000_000_000.0,
                1_000_000_000_000_000_000.0,
                10_000_000_000_000_000_000.0,
                u64::MAX as _,
            ],
            value_size_fn: |n| ((n + 1.0).log2() / 8.0).ceil() as _,
        },
        BenchEncodingInputs {
            group: "NegInt",
            values: vec![
                // 1 byte
                -8.0,
                -10.0,
                -100.0,
                // 5 bytes
                -10_000_000_000.0,
                -100_000_000_000.0,
                -1_000_000_000_000.0,
                // 8 bytes
                -100_000_000_000_000_000.0,
                -1_000_000_000_000_000_000.0,
                -10_000_000_000_000_000_000.0,
                -(u64::MAX as f64),
            ],
            value_size_fn: |n| ((n.abs() + 1.0).log2() / 8.0).ceil() as _,
        },
        BenchEncodingInputs {
            group: "Float - Pos Inf",
            values: vec![f64::INFINITY],
            value_size_fn: |_| 0,
        },
        BenchEncodingInputs {
            group: "Float - Neg Inf",
            values: vec![f64::NEG_INFINITY],
            value_size_fn: |_| 0,
        },
        BenchEncodingInputs {
            group: "Float - Small Pos",
            values: vec![7.125, 15.75, 42.5],
            value_size_fn: |_| 4,
        },
        BenchEncodingInputs {
            group: "Float - Small Neg",
            values: vec![-7.125, -15.75, -42.5],
            value_size_fn: |_| 4,
        },
        BenchEncodingInputs {
            group: "Float - Big Pos",
            values: vec![1.1, 0.3, 12.999999999999998],
            value_size_fn: |_| 8,
        },
        BenchEncodingInputs {
            group: "Float - Big Neg",
            values: vec![-1.1, -0.3, -12.999999999999998],
            value_size_fn: |_| 8,
        },
    ];

    let deltas = vec![
        // 0 bytes
        0,
        // 4 bytes
        100_000_000,
        // 7 bytes
        1_000_000_000_000_000,
    ];

    encoding_values
        .into_iter()
        .cartesian_product(deltas)
        .map(
            |(
                BenchEncodingInputs {
                    group,
                    values,
                    value_size_fn,
                },
                delta,
            )| {
                let inputs = values
                    .into_iter()
                    // We need to find the actual resulting output for the decoding benchmarks
                    .map(|value| {
                        let record = inverted_index::RSIndexResult::numeric(0, value);
                        let mut buffer = Cursor::new(Vec::new());
                        let _grew_size =
                            Numeric::encode(&mut buffer, Delta::new(delta), &record).unwrap();
                        let buffer = buffer.into_inner();

                        (value, delta, buffer)
                    })
                    // Find the input and delta sizes in bytes to group the benchmarks
                    .map(|(value, delta, buffer)| {
                        let value_size = value_size_fn(value);
                        let delta_size = (((delta + 1) as f64).log2() / 8.0).ceil() as usize;

                        assert_eq!(
                            buffer.len(),
                            1 + delta_size + value_size,
                            "the wrong size calculation is used for group: {group}"
                        );
                        (value, delta, buffer, value_size, delta_size)
                    })
                    .fold(
                        HashMap::new(),
                        |mut map, (value, delta, buffer, value_size, delta_size)| {
                            map.entry((value_size, delta_size))
                                .or_insert(vec![])
                                .push((value, delta, buffer));

                            map
                        },
                    )
                    .into_iter()
                    .map(|((value_size, delta_size), values)| BenchInput {
                        values,
                        delta_size,
                        value_size,
                    })
                    .collect();

                BenchGroup {
                    group: group.to_string(),
                    inputs,
                }
            },
        )
        .collect()
}

fn numeric_c_encode<M: Measurement>(group: &mut BenchmarkGroup<'_, M>, input: &BenchInput) {
    let BenchInput {
        values,
        delta_size,
        value_size,
    } = input;

    group.bench_function(
        BenchmarkId::new(
            "C",
            format!("Value size: {value_size}/Delta size: {delta_size}"),
        ),
        |b| {
            b.iter_batched(
                || TestBuffer::with_capacity((1 + delta_size + value_size) * values.len()),
                |mut buffer| {
                    for (value, delta, _) in values {
                        let mut record = inverted_index::RSIndexResult::numeric(0, *value);
                        let grew_size = encode_numeric(&mut buffer, &mut record, *delta as _);

                        black_box(grew_size);
                    }
                },
                BatchSize::SmallInput,
            );
        },
    );
}

fn numeric_c_decode<M: Measurement>(group: &mut BenchmarkGroup<'_, M>, input: &BenchInput) {
    let BenchInput {
        values,
        delta_size,
        value_size,
    } = input;

    group.bench_function(
        BenchmarkId::new(
            "C",
            format!("Value size: {value_size}/Delta size: {delta_size}"),
        ),
        |b| {
            for (_, _, buffer) in values {
                let buffer_ptr = NonNull::new(buffer.as_ptr() as *mut _).unwrap();
                let mut buffer = unsafe { Buffer::new(buffer_ptr, buffer.len(), buffer.len()) };

                b.iter(|| {
                    let (_filtered, result) = read_numeric(&mut buffer, 100);

                    black_box(result);
                });
            }
        },
    );
}

fn numeric_rust_encode<M: Measurement>(group: &mut BenchmarkGroup<'_, M>, input: &BenchInput) {
    let BenchInput {
        values,
        delta_size,
        value_size,
    } = input;

    group.bench_function(
        BenchmarkId::new(
            "Rust",
            format!("Value size: {value_size}/Delta size: {delta_size}"),
        ),
        |b| {
            b.iter_batched(
                || TestBuffer::with_capacity((1 + delta_size + value_size) * values.len()),
                |mut buffer| {
                    for (value, delta, _) in values {
                        let buffer_writer = BufferWriter::new(&mut buffer.0);
                        let record = inverted_index::RSIndexResult::numeric(0, *value);

                        let grew_size =
                            Numeric::encode(buffer_writer, Delta::new(*delta), &record).unwrap();

                        black_box(grew_size);
                    }
                },
                BatchSize::SmallInput,
            );
        },
    );
}

fn numeric_rust_decode<M: Measurement>(group: &mut BenchmarkGroup<'_, M>, input: &BenchInput) {
    let BenchInput {
        values,
        delta_size,
        value_size,
    } = input;

    group.bench_function(
        BenchmarkId::new(
            "Rust",
            format!("Value size: {value_size}/Delta size: {delta_size}"),
        ),
        |b| {
            for (_, _, buffer) in values {
                let buffer_ptr = NonNull::new(buffer.as_ptr() as *mut _).unwrap();
                let buffer = unsafe { Buffer::new(buffer_ptr, buffer.len(), buffer.len()) };

                b.iter(|| {
                    let mut buffer_reader = BufferReader::new(&buffer);

                    let result = Numeric.decode(&mut buffer_reader, 100);

                    let _ = black_box(result);
                });
            }
        },
    );
}
