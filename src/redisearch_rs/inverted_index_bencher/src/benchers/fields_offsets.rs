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
use ffi::t_fieldMask;
use inverted_index::{
    Decoder, Encoder,
    fields_offsets::{FieldsOffsets, FieldsOffsetsWide},
    test_utils::TestTermRecord,
};
use itertools::Itertools;

use crate::ffi::{TestBuffer, encode_fields_offsets, read_fields_offsets};

pub struct Bencher {
    test_values: Vec<TestValue>,
    wide: bool,
}

#[derive(Debug)]
struct TestValue {
    delta: u32,
    field_mask: t_fieldMask,
    term_offsets: Vec<i8>,

    encoded: Vec<u8>,
}

impl Default for Bencher {
    fn default() -> Self {
        Bencher::new(false)
    }
}

impl Bencher {
    const MEASUREMENT_TIME: Duration = Duration::from_millis(500);
    const WARMUP_TIME: Duration = Duration::from_millis(200);

    pub fn wide() -> Self {
        Self::new(true)
    }

    fn new(wide: bool) -> Self {
        let deltas = vec![0, u32::MAX];
        let mut field_masks_values = vec![0, 10, 100, 1_000, 10_000, u32::MAX as t_fieldMask - 1];
        #[cfg(target_pointer_width = "64")]
        if wide {
            // Add a larger field mask for wide mode
            field_masks_values.extend(vec![u32::MAX as t_fieldMask, u128::MAX]);
        }
        let term_offsets_values = vec![
            vec![0],
            vec![1; 10],
            vec![1; 100],
            vec![1; 1_000],
            vec![1; 10_000],
        ];

        let test_values = deltas
            .into_iter()
            .cartesian_product(field_masks_values)
            .cartesian_product(term_offsets_values)
            .map(|((delta, field_mask), term_offsets)| {
                let record = TestTermRecord::new(100, field_mask, 1, term_offsets.clone());
                let mut buffer = Cursor::new(Vec::new());

                let _grew_size = if wide {
                    FieldsOffsetsWide
                        .encode(&mut buffer, delta, &record.record)
                        .unwrap()
                } else {
                    FieldsOffsets
                        .encode(&mut buffer, delta, &record.record)
                        .unwrap()
                };

                let encoded = buffer.into_inner();

                TestValue {
                    delta,
                    encoded,
                    field_mask,
                    term_offsets,
                }
            })
            .collect();

        Self { test_values, wide }
    }

    fn benchmark_group<'a>(
        &self,
        c: &'a mut Criterion,
        label: &str,
    ) -> BenchmarkGroup<'a, WallTime> {
        let mut label = label.to_string();
        if self.wide {
            label.push_str(" Wide");
        }
        let mut group = c.benchmark_group(label);
        group.measurement_time(Self::MEASUREMENT_TIME);
        group.warm_up_time(Self::WARMUP_TIME);
        group
    }

    pub fn encoding(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Encode - FieldsOffsets");
        self.c_encode(&mut group);
        self.rust_encode(&mut group);
        group.finish();
    }

    pub fn decoding(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Decode - FieldsOffsets");
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
                |buffer| {
                    for test in &self.test_values {
                        let mut record =
                            TestTermRecord::new(100, test.field_mask, 1, test.term_offsets.clone());

                        let grew_size = encode_fields_offsets(
                            buffer,
                            &mut record.record,
                            test.delta as u64,
                            self.wide,
                        );

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
                        let record =
                            TestTermRecord::new(100, test.field_mask, 1, test.term_offsets.clone());

                        let grew_size = if self.wide {
                            FieldsOffsetsWide
                                .encode(&mut buffer, test.delta, &record.record)
                                .unwrap()
                        } else {
                            FieldsOffsets
                                .encode(&mut buffer, test.delta, &record.record)
                                .unwrap()
                        };

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
                    |buffer| {
                        let (_filtered, result) = read_fields_offsets(buffer, 100, self.wide);

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
                        let result = if self.wide {
                            FieldsOffsetsWide.decode_new(buffer, 100).unwrap()
                        } else {
                            FieldsOffsets.decode_new(buffer, 100).unwrap()
                        };

                        let _ = black_box(result);
                    },
                    BatchSize::SmallInput,
                );
            }
        });
    }
}
