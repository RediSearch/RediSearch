/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{io::Cursor, vec};

use criterion::{BatchSize, Criterion, black_box};
use ffi::t_fieldMask;
use inverted_index::{
    Decoder, Encoder, RSIndexResult,
    full::{Full, FullWide},
    test_utils::TestTermRecord,
};
use itertools::Itertools;

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
                    FullWide::encode(&mut buffer, delta, &record.record).unwrap()
                } else {
                    Full::encode(&mut buffer, delta, &record.record).unwrap()
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

    pub fn encoding(&self, c: &mut Criterion) {
        // Use a single buffer big enough to hold all encoded values
        let buffer_size = self.test_values.iter().map(|test| test.encoded.len()).sum();
        let id = format!("Encode Full{}", if self.wide { "Wide" } else { "" });

        c.bench_function(&id, |b| {
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
                            FullWide::encode(&mut buffer, test.delta, &record.record).unwrap()
                        } else {
                            Full::encode(&mut buffer, test.delta, &record.record).unwrap()
                        };

                        black_box(grew_size);
                    }
                },
                BatchSize::SmallInput,
            );
        });
    }

    pub fn decoding(&self, c: &mut Criterion) {
        let id = format!("Decode Full{}", if self.wide { "Wide" } else { "" });

        c.bench_function(&id, |b| {
            for test in &self.test_values {
                b.iter_batched_ref(
                    || (Cursor::new(test.encoded.as_ref()), RSIndexResult::term()),
                    |(cursor, result)| {
                        let res = if self.wide {
                            FullWide::decode(cursor, 100, result)
                        } else {
                            Full::decode(cursor, 100, result)
                        };

                        let _ = black_box(res);
                    },
                    BatchSize::SmallInput,
                );
            }
        });
    }
}
