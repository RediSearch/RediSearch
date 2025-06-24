/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{io::Cursor, ptr::NonNull, time::Duration};

use buffer::{Buffer, BufferReader, BufferWriter};
use criterion::{
    BatchSize, BenchmarkGroup, Criterion, black_box,
    measurement::{Measurement, WallTime},
};
use inverted_index::{Decoder, Delta, Encoder, numeric::Numeric};
use itertools::Itertools;

use crate::ffi::{TestBuffer, encode_numeric, read_numeric};

pub struct NumericBencher {
    test_values: Vec<BenchInputs>,
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
        for BenchInputs {
            group,
            values,
            n_bytes,
        } in &self.test_values
        {
            let values: Vec<_> = values.iter().map(|v| v.0).collect();

            let group = format!("Encode | {}", group);
            let mut group = self.benchmark_group(c, &group);
            numeric_c_encode(&mut group, &values, *n_bytes);
            numeric_rust_encode(&mut group, &values, *n_bytes);
            group.finish();
        }
    }

    pub fn decoding(&self, c: &mut Criterion) {
        for BenchInputs {
            group,
            values,
            n_bytes,
        } in &self.test_values
        {
            let values: Vec<_> = values.iter().map(|v| v.1.clone()).collect();

            let group = format!("Decode | {}", group);
            let mut group = self.benchmark_group(c, &group);
            numeric_c_decode(&mut group, &values, *n_bytes);
            numeric_rust_decode(&mut group, &values, *n_bytes);
            group.finish();
        }
    }
}

struct BenchEncodingInputs<'a> {
    group: &'a str,
    values: Vec<f64>,
}

struct BenchInputs {
    group: String,
    values: Vec<(f64, Vec<u8>)>,
    n_bytes: usize,
}

fn generate_test_values() -> Vec<BenchInputs> {
    let encoding_values = vec![
        BenchEncodingInputs {
            group: "TinyInt",
            values: vec![0.0, 3.0, 7.0],
        },
        BenchEncodingInputs {
            group: "PosInt",
            values: vec![
                // 1 byte
                8.0,
                10.0,
                100.0,
                // 2 bytes
                256.0,
                1_000.0,
                10_000.0,
                // 3 bytes
                100_000.0,
                1_000_000.0,
                10_000_000.0,
                // 4 bytes
                100_000_000.0,
                1_000_000_000.0,
                // 5 bytes
                10_000_000_000.0,
                100_000_000_000.0,
                1_000_000_000_000.0,
                // 6 bytes
                10_000_000_000_000.0,
                100_000_000_000_000.0,
                // 7 bytes
                1_000_000_000_000_000.0,
                10_000_000_000_000_000.0,
                // 8 bytes
                100_000_000_000_000_000.0,
                1_000_000_000_000_000_000.0,
                10_000_000_000_000_000_000.0,
                u64::MAX as _,
            ],
        },
        BenchEncodingInputs {
            group: "NegInt",
            values: vec![
                // 1 byte
                -8.0,
                -10.0,
                -100.0,
                // 2 bytes
                -256.0,
                -1_000.0,
                -10_000.0,
                // 3 bytes
                -100_000.0,
                -1_000_000.0,
                -10_000_000.0,
                // 4 bytes
                -100_000_000.0,
                -1_000_000_000.0,
                // 5 bytes
                -10_000_000_000.0,
                -100_000_000_000.0,
                -1_000_000_000_000.0,
                // 6 bytes
                -10_000_000_000_000.0,
                -100_000_000_000_000.0,
                // 7 bytes
                -1_000_000_000_000_000.0,
                -10_000_000_000_000_000.0,
                // 8 bytes
                -100_000_000_000_000_000.0,
                -1_000_000_000_000_000_000.0,
                -10_000_000_000_000_000_000.0,
                -(u64::MAX as f64),
            ],
        },
        BenchEncodingInputs {
            group: "Float - Pos Inf",
            values: vec![f64::INFINITY],
        },
        BenchEncodingInputs {
            group: "Float - Neg Inf",
            values: vec![f64::NEG_INFINITY],
        },
        BenchEncodingInputs {
            group: "Float - Small Pos",
            values: vec![7.125, 15.75, 42.5],
        },
        BenchEncodingInputs {
            group: "Float - Small Neg",
            values: vec![-7.125, -15.75, -42.5],
        },
        BenchEncodingInputs {
            group: "Float - Big Pos",
            values: vec![1.1, 0.3, 12.999999999999998],
        },
        BenchEncodingInputs {
            group: "Float - Big Neg",
            values: vec![-1.1, -0.3, -12.999999999999998],
        },
    ];

    encoding_values
        .into_iter()
        .map(
            |BenchEncodingInputs {
                 group: name,
                 values,
             }| {
                values
                    .into_iter()
                    .map(|value| {
                        let record = inverted_index::RSIndexResult::numeric(0, value);
                        let mut buffer = Cursor::new(Vec::new());
                        let _grew_size =
                            Numeric::encode(&mut buffer, Delta::new(684), &record).unwrap();
                        let buffer = buffer.into_inner();
                        (value, buffer)
                    })
                    .chunk_by(|v| v.1.len())
                    .into_iter()
                    .map(|(n_bytes, values)| BenchInputs {
                        group: name.to_string(),
                        values: values.collect(),
                        n_bytes,
                    })
                    .collect::<Vec<_>>()
            },
        )
        .flatten()
        .collect()
}

fn numeric_c_encode<M: Measurement>(
    group: &mut BenchmarkGroup<'_, M>,
    values: &[f64],
    n_bytes: usize,
) {
    group.bench_function(format!("C | {n_bytes} buffer bytes"), |b| {
        b.iter_batched(
            || TestBuffer::with_capacity(64),
            |mut buffer| {
                for &value in values {
                    let mut record = inverted_index::RSIndexResult::numeric(0, value);
                    let grew_size = encode_numeric(&mut buffer, &mut record, 684);

                    black_box(grew_size);
                }
            },
            BatchSize::SmallInput,
        );
    });
}

fn numeric_c_decode<M: Measurement>(
    group: &mut BenchmarkGroup<'_, M>,
    values: &[Vec<u8>],
    n_bytes: usize,
) {
    group.bench_function(format!("C | {n_bytes} buffer bytes"), |b| {
        b.iter(|| {
            for value in values {
                let buffer_ptr = NonNull::new(value.as_ptr() as *mut _).unwrap();
                let mut buffer = unsafe { Buffer::new(buffer_ptr, value.len(), value.len()) };
                let (_filtered, result) = read_numeric(&mut buffer, 100);

                black_box(result);
            }
        });
    });
}

fn numeric_rust_encode<M: Measurement>(
    group: &mut BenchmarkGroup<'_, M>,
    values: &[f64],
    n_bytes: usize,
) {
    group.bench_function(format!("Rust | {n_bytes} buffer bytes"), |b| {
        b.iter_batched(
            || TestBuffer::with_capacity(64),
            |mut buffer| {
                for &value in values {
                    let buffer_writer = BufferWriter::new(&mut buffer.0);
                    let delta = Delta::new(684);
                    let record = inverted_index::RSIndexResult::numeric(0, value);

                    let grew_size = Numeric::encode(buffer_writer, delta, &record).unwrap();

                    black_box(grew_size);
                }
            },
            BatchSize::LargeInput,
        );
    });
}

fn numeric_rust_decode<M: Measurement>(
    group: &mut BenchmarkGroup<'_, M>,
    values: &[Vec<u8>],
    n_bytes: usize,
) {
    group.bench_function(format!("Rust | {n_bytes} buffer bytes"), |b| {
        b.iter(|| {
            for value in values {
                let buffer_ptr = NonNull::new(value.as_ptr() as *mut _).unwrap();
                let buffer = unsafe { Buffer::new(buffer_ptr, value.len(), value.len()) };
                let mut buffer_reader = BufferReader::new(&buffer);

                let result = Numeric.decode(&mut buffer_reader, 100);

                let _ = black_box(result);
            }
        });
    });
}
