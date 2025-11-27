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
    freqs_fields::{FreqsFields, FreqsFieldsWide},
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
                    FreqsFieldsWide::encode(&mut buffer, delta, &record).unwrap()
                } else {
                    FreqsFields::encode(&mut buffer, delta, &record).unwrap()
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

    pub fn encoding(&self, c: &mut Criterion) {
        // Use a single buffer big enough to hold all encoded values
        let buffer_size = self.test_values.iter().map(|test| test.encoded.len()).sum();
        let id = format!("Encode FreqsFields{}", if self.wide { "Wide" } else { "" });

        c.bench_function(&id, |b| {
            b.iter_batched_ref(
                || Cursor::new(Vec::with_capacity(buffer_size)),
                |mut buffer| {
                    for test in &self.test_values {
                        let record = RSIndexResult::term()
                            .doc_id(100)
                            .field_mask(test.field_mask)
                            .frequency(test.freq);

                        let grew_size = if self.wide {
                            FreqsFieldsWide::encode(&mut buffer, test.delta, &record).unwrap()
                        } else {
                            FreqsFields::encode(&mut buffer, test.delta, &record).unwrap()
                        };

                        black_box(grew_size);
                    }
                },
                BatchSize::SmallInput,
            );
        });
    }

    pub fn decoding(&self, c: &mut Criterion) {
        let id = format!("Decode FreqsFields{}", if self.wide { "Wide" } else { "" });

        c.bench_function(&id, |b| {
            for test in &self.test_values {
                b.iter_batched_ref(
                    || (Cursor::new(test.encoded.as_ref()), RSIndexResult::term()),
                    |(cursor, result)| {
                        if self.wide {
                            let res = FreqsFieldsWide::decode(cursor, 100, result);
                            let _ = black_box(res);
                        } else {
                            let res = FreqsFields::decode(cursor, 100, result);
                            let _ = black_box(res);
                        }
                    },
                    BatchSize::SmallInput,
                );
            }
        });
    }
}
