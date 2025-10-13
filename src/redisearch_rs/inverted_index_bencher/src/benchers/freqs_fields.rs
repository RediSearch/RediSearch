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
    Decoder, Encoder, RSIndexResult,
    freqs_fields::{FreqsFields, FreqsFieldsWide},
};
use itertools::Itertools;

use crate::ffi::{TestBuffer, encode_freqs_fields, read_freqs_flags};

// The encode C implementation relies on this symbol. Re-export it to ensure it is not discarded by the linker.
#[allow(unused_imports)]
pub use varint_ffi::WriteVarintFieldMask;

pub struct Bencher {
    test_values: Vec<TestValue>,
    wide: bool,
}

#[derive(Debug)]
struct TestValue {
    delta: u32,
    freq: u32,
    field_mask: t_fieldMask,

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
        let freq_values = vec![0, 2, 256, u16::MAX as u32, u32::MAX];
        let deltas = vec![0, 1, 256, 65536, u16::MAX as u32, u32::MAX];
        let mut field_masks_values =
            vec![0, 1, 10, 100, 1_000, 10_000, u32::MAX as t_fieldMask - 1];
        // field mask larger than 32 bits are only supported on 64-bit systems
        #[cfg(target_pointer_width = "64")]
        if wide {
            // Add a larger field mask for wide mode
            field_masks_values.extend(vec![u32::MAX as t_fieldMask, u128::MAX]);
        }

        let test_values = freq_values
            .into_iter()
            .cartesian_product(deltas)
            .cartesian_product(field_masks_values)
            .map(|((freq, delta), field_mask)| {
                let record = RSIndexResult::term()
                    .doc_id(100)
                    .field_mask(field_mask)
                    .frequency(freq);

                let mut buffer = Cursor::new(Vec::new());
                let _grew_size = if wide {
                    FreqsFieldsWide
                        .encode(&mut buffer, delta, &record)
                        .unwrap()
                } else {
                    FreqsFields
                        .encode(&mut buffer, delta, &record)
                        .unwrap()
                };
                let encoded = buffer.into_inner();

                TestValue {
                    freq,
                    delta,
                    encoded,
                    field_mask,
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
        let mut group = self.benchmark_group(c, "Encode - FreqsFields");
        self.c_encode(&mut group);
        self.rust_encode(&mut group);
        group.finish();
    }

    pub fn decoding(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Decode - FreqsFields");
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
                        let mut record = RSIndexResult::term()
                            .doc_id(100)
                            .field_mask(test.field_mask)
                            .frequency(test.freq);

                        let grew_size = encode_freqs_fields(
                            &mut buffer,
                            &mut record,
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
                        let record = RSIndexResult::term()
                            .doc_id(100)
                            .field_mask(test.field_mask)
                            .frequency(test.freq);

                        let grew_size = if self.wide {
                            FreqsFieldsWide
                                .encode(&mut buffer, test.delta, &record)
                                .unwrap()
                        } else {
                            FreqsFields
                                .encode(&mut buffer, test.delta, &record)
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
                    |mut buffer| {
                        let (_filtered, result) = read_freqs_flags(&mut buffer, 100, self.wide);

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
                        if self.wide {
                            let decoder = FreqsFieldsWide;
                            let result = decoder.decode_new(buffer, 100).unwrap();
                            let _ = black_box(result);
                        } else {
                            let decoder = FreqsFields;
                            let result = decoder.decode_new(buffer, 100).unwrap();
                            let _ = black_box(result);
                        }
                    },
                    BatchSize::SmallInput,
                );
            }
        });
    }
}
