/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{collections::HashMap, io::Cursor};

use criterion::{
    BatchSize, BenchmarkGroup, BenchmarkId, Criterion, black_box, measurement::Measurement,
};
use inverted_index::{
    Decoder, Encoder, IdDelta, RSIndexResult,
    numeric::{Numeric, NumericDelta},
};
use itertools::Itertools;

pub struct Bencher {
    test_values: Vec<BenchGroup>,
}

impl Default for Bencher {
    fn default() -> Self {
        Self::new()
    }
}

impl Bencher {
    pub fn new() -> Self {
        let test_values = generate_test_values();

        Self { test_values }
    }

    pub fn encoding(&self, c: &mut Criterion) {
        let mut group = c.benchmark_group("Encode Numeric");

        for input in &self.test_values {
            encode(&mut group, input);
        }

        group.finish();
    }

    pub fn decoding(&self, c: &mut Criterion) {
        let mut group = c.benchmark_group("Decode Numeric");

        for input in &self.test_values {
            decode(&mut group, input);
        }

        group.finish();
    }
}

#[derive(Clone)]
struct BenchEncodingInputs<'a> {
    name: &'a str,
    values: Vec<f64>,
    value_size_fn: fn(f64) -> usize,
}

struct BenchGroup {
    name: String,
    values: Vec<(f64, u64, Vec<u8>)>,
    delta_size: usize,
    value_size: usize,
}

fn generate_test_values() -> Vec<BenchGroup> {
    let encoding_values = vec![
        BenchEncodingInputs {
            name: "Tiny",
            values: vec![0.0, 3.0, 7.0],
            value_size_fn: |_| 0,
        },
        BenchEncodingInputs {
            name: "Integer Positive",
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
            name: "Integer Negative",
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
            name: "Float | Positive Infinite",
            values: vec![f64::INFINITY],
            value_size_fn: |_| 0,
        },
        BenchEncodingInputs {
            name: "Float | Negative Infinite",
            values: vec![f64::NEG_INFINITY],
            value_size_fn: |_| 0,
        },
        BenchEncodingInputs {
            name: "Float | Small Positive",
            values: vec![7.125, 15.75, 42.5],
            value_size_fn: |_| 4,
        },
        BenchEncodingInputs {
            name: "Float | Small Negative",
            values: vec![-7.125, -15.75, -42.5],
            value_size_fn: |_| 4,
        },
        BenchEncodingInputs {
            name: "Float | Big Positive",
            values: vec![1.1, 0.3, 12.999999999999998],
            value_size_fn: |_| 8,
        },
        BenchEncodingInputs {
            name: "Float | Big Negative",
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
        .flat_map(
            |(
                BenchEncodingInputs {
                    name,
                    values,
                    value_size_fn,
                },
                delta,
            )| {
                values
                    .into_iter()
                    // We need to find the actual resulting output for the decoding benchmarks
                    .map(|value| {
                        let record = inverted_index::RSIndexResult::numeric(value);
                        let mut buffer = Cursor::new(Vec::new());
                        let _grew_size = Numeric::encode(
                            &mut buffer,
                            NumericDelta::from_u64(delta).unwrap(),
                            &record,
                        )
                        .unwrap();
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
                            "the wrong size calculation is used for group: {name}"
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
                    .map(|((value_size, delta_size), values)| BenchGroup {
                        name: name.to_string(),
                        values,
                        delta_size,
                        value_size,
                    })
            },
        )
        .collect()
}

fn encode<M: Measurement>(group: &mut BenchmarkGroup<'_, M>, input: &BenchGroup) {
    let BenchGroup {
        name,
        values,
        delta_size,
        value_size,
    } = input;

    group.bench_function(
        BenchmarkId::new(
            name,
            format!("Value size: {value_size}/Delta size: {delta_size}"),
        ),
        |b| {
            b.iter_batched_ref(
                || {
                    Cursor::new(Vec::with_capacity(
                        (1 + delta_size + value_size) * values.len(),
                    ))
                },
                |mut buffer| {
                    for (value, delta, _) in values {
                        let record = inverted_index::RSIndexResult::numeric(*value);

                        let grew_size = Numeric::encode(
                            &mut buffer,
                            NumericDelta::from_u64(*delta).unwrap(),
                            &record,
                        )
                        .unwrap();

                        black_box(grew_size);
                    }
                },
                BatchSize::SmallInput,
            );
        },
    );
}

fn decode<M: Measurement>(group: &mut BenchmarkGroup<'_, M>, input: &BenchGroup) {
    let BenchGroup {
        name,
        values,
        delta_size,
        value_size,
    } = input;

    group.bench_function(
        BenchmarkId::new(
            name,
            format!("Value size: {value_size}/Delta size: {delta_size}"),
        ),
        |b| {
            for (_, _, buffer) in values {
                b.iter_batched_ref(
                    || (Cursor::new(buffer.as_ref()), RSIndexResult::numeric(0.0)),
                    |(cursor, result)| {
                        let res = Numeric::decode(cursor, 100, result);

                        let _ = black_box(res);
                    },
                    BatchSize::SmallInput,
                );
            }
        },
    );
}
