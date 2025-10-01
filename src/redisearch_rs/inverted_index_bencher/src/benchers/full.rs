/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{io::Cursor, time::Duration, vec};

use criterion::{
    BatchSize, BenchmarkGroup, Criterion, black_box,
    measurement::{Measurement, WallTime},
};
use ffi::t_fieldMask;
use inverted_index::{
    Decoder, Encoder,
    full::{Full, FullWide},
    test_utils::TestTermRecord,
};
use itertools::Itertools;

// The encode C implementation relies on this symbol. Re-export it to ensure it is not discarded by the linker.
#[allow(unused_imports)]
pub use types_ffi::RSOffsetVector_GetData;

pub struct Bencher {
    test_values: Vec<TestValue>,
    wide: bool,
}

#[derive(Debug)]
struct TestValue {
    delta: u32,
    freq: u32,
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
        let freq_values = vec![0, u32::MAX];
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

        let test_values = freq_values
            .into_iter()
            .cartesian_product(deltas)
            .cartesian_product(field_masks_values)
            .cartesian_product(term_offsets_values)
            .map(|(((freq, delta), field_mask), term_offsets)| {
                let record = TestTermRecord::new(100, field_mask, freq, term_offsets.clone());
                let mut buffer = Cursor::new(Vec::new());

                let _grew_size = if wide {
                    FullWide.encode(&mut buffer, delta, &record.record).unwrap()
                } else {
                    Full.encode(&mut buffer, delta, &record.record).unwrap()
                };

                let encoded = buffer.into_inner();

                TestValue {
                    freq,
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
        let mut group = self.benchmark_group(c, "Encode - Full");
        self.rust_encode(&mut group);
        group.finish();
    }

    pub fn decoding(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Decode - Full");
        self.rust_decode(&mut group);
        group.finish();
    }

    fn rust_encode<M: Measurement>(&self, group: &mut BenchmarkGroup<'_, M>) {
        // Use a single buffer big enough to hold all encoded values
        let buffer_size = self.test_values.iter().map(|test| test.encoded.len()).sum();

        group.bench_function("Rust", |b| {
            b.iter_batched_ref(
                || Cursor::new(Vec::with_capacity(buffer_size)),
                |mut buffer| {
                    for test in &self.test_values {
                        let record = TestTermRecord::new(
                            100,
                            test.field_mask,
                            test.freq,
                            test.term_offsets.clone(),
                        );

                        let grew_size = if self.wide {
                            FullWide
                                .encode(&mut buffer, test.delta, &record.record)
                                .unwrap()
                        } else {
                            Full.encode(&mut buffer, test.delta, &record.record)
                                .unwrap()
                        };

                        black_box(grew_size);
                    }
                },
                BatchSize::SmallInput,
            );
        });
    }

    fn rust_decode<M: Measurement>(&self, group: &mut BenchmarkGroup<'_, M>) {
        group.bench_function("Rust", |b| {
            for test in &self.test_values {
                b.iter_batched_ref(
                    || Cursor::new(test.encoded.as_ref()),
                    |buffer| {
                        let result = if self.wide {
                            FullWide.decode_new(buffer, 100).unwrap()
                        } else {
                            Full.decode_new(buffer, 100).unwrap()
                        };

                        let _ = black_box(result);
                    },
                    BatchSize::SmallInput,
                );
            }
        });
    }
}
