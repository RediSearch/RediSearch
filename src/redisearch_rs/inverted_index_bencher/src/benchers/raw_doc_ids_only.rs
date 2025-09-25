/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{io::Cursor, ptr::NonNull, time::Duration, vec};

use buffer::Buffer;
use criterion::{
    BatchSize, BenchmarkGroup, Criterion, black_box,
    measurement::{Measurement, WallTime},
};
use inverted_index::{Decoder, Encoder, RSIndexResult, raw_doc_ids_only::RawDocIdsOnly};

use crate::ffi::{TestBuffer, encode_raw_doc_ids_only, read_raw_doc_ids_only};

pub struct Bencher {
    test_values: Vec<TestValue>,
}

#[derive(Debug)]
struct TestValue {
    delta: u32,

    encoded: Vec<u8>,
}

impl Default for Bencher {
    fn default() -> Self {
        Bencher::new()
    }
}

impl Bencher {
    const MEASUREMENT_TIME: Duration = Duration::from_millis(500);
    const WARMUP_TIME: Duration = Duration::from_millis(200);

    fn new() -> Self {
        let deltas = vec![0, 1, 256, 65536, u16::MAX as u32, u32::MAX];

        let test_values = deltas
            .into_iter()
            .map(|delta| {
                let record = RSIndexResult::term().doc_id(100);

                let mut buffer = Cursor::new(Vec::new());
                let _grew_size = RawDocIdsOnly::default()
                    .encode(&mut buffer, delta, &record)
                    .unwrap();
                let encoded = buffer.into_inner();

                TestValue { delta, encoded }
            })
            .collect();

        Self { test_values }
    }

    fn benchmark_group<'a>(
        &self,
        c: &'a mut Criterion,
        label: &str,
    ) -> BenchmarkGroup<'a, WallTime> {
        let label = label.to_string();
        let mut group = c.benchmark_group(label);
        group.measurement_time(Self::MEASUREMENT_TIME);
        group.warm_up_time(Self::WARMUP_TIME);
        group
    }

    pub fn encoding(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Encode - RawDocIdsOnly");
        self.c_encode(&mut group);
        self.rust_encode(&mut group);
        group.finish();
    }

    pub fn decoding(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Decode - RawDocIdsOnly");
        self.c_decode(&mut group);
        self.rust_decode(&mut group);
        group.finish();
    }

    fn c_encode<M: Measurement>(&self, group: &mut BenchmarkGroup<'_, M>) {
        // Use a single buffer big enough to hold all encoded values
        let buffer_size = self.test_values.iter().map(|test| test.encoded.len()).sum();

        group.bench_function("C", |b| {
            b.iter_batched_ref(
                || TestBuffer::with_capacity(buffer_size),
                |mut buffer| {
                    for test in &self.test_values {
                        let mut record = RSIndexResult::term().doc_id(100);

                        let grew_size =
                            encode_raw_doc_ids_only(&mut buffer, &mut record, test.delta as u64);

                        black_box(grew_size);
                    }
                },
                BatchSize::SmallInput,
            );
        });
    }

    fn rust_encode<M: Measurement>(&self, group: &mut BenchmarkGroup<'_, M>) {
        // Use a single buffer big enough to hold all encoded values
        let buffer_size = self.test_values.iter().map(|test| test.encoded.len()).sum();

        group.bench_function("Rust", |b| {
            b.iter_batched_ref(
                || Cursor::new(Vec::with_capacity(buffer_size)),
                |mut buffer| {
                    for test in &self.test_values {
                        let record = RSIndexResult::term().doc_id(100);

                        let grew_size = RawDocIdsOnly::default()
                            .encode(&mut buffer, test.delta, &record)
                            .unwrap();

                        black_box(grew_size);
                    }
                },
                BatchSize::SmallInput,
            );
        });
    }

    fn c_decode<M: Measurement>(&self, group: &mut BenchmarkGroup<'_, M>) {
        group.bench_function("C", |b| {
            for test in &self.test_values {
                b.iter_batched_ref(
                    || {
                        let buffer_ptr = NonNull::new(test.encoded.as_ptr() as *mut _).unwrap();
                        unsafe { Buffer::new(buffer_ptr, test.encoded.len(), test.encoded.len()) }
                    },
                    |mut buffer| {
                        let (_filtered, result) = read_raw_doc_ids_only(&mut buffer, 100);

                        black_box(result);
                    },
                    BatchSize::SmallInput,
                );
            }
        });
    }

    fn rust_decode<M: Measurement>(&self, group: &mut BenchmarkGroup<'_, M>) {
        group.bench_function("Rust", |b| {
            for test in &self.test_values {
                b.iter_batched_ref(
                    || Cursor::new(test.encoded.as_ref()),
                    |buffer| {
                        let mut record = RSIndexResult::term();
                        let result = RawDocIdsOnly::default()
                            .decode(buffer, 100, &mut record)
                            .unwrap();

                        let _ = black_box(result);
                    },
                    BatchSize::SmallInput,
                );
            }
        });
    }
}
