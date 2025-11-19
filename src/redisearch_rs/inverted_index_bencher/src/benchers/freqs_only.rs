/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::io::Cursor;

use criterion::{BatchSize, Criterion, black_box};
use inverted_index::{Decoder, Encoder, RSIndexResult, freqs_only::FreqsOnly};
use itertools::Itertools;

pub struct Bencher {
    test_values: Vec<TestValue>,
}

#[derive(Debug)]
struct TestValue {
    freq: u32,
    delta: u32,
    encoded: Vec<u8>,
}

impl Default for Bencher {
    fn default() -> Self {
        Self::new()
    }
}

impl Bencher {
    pub fn new() -> Self {
        let freq_values = vec![0, 2, 256, u16::MAX as u32, u32::MAX];
        let deltas = vec![0, 1, 256, 65536, u16::MAX as u32, u32::MAX];

        let test_values = freq_values
            .into_iter()
            .cartesian_product(deltas)
            .map(|(freq, delta)| {
                let record = inverted_index::RSIndexResult::virt()
                    .doc_id(100)
                    .frequency(freq);
                let mut buffer = Cursor::new(Vec::new());
                let _grew_size = FreqsOnly::encode(&mut buffer, delta, &record).unwrap();
                let encoded = buffer.into_inner();

                TestValue {
                    freq,
                    delta,
                    encoded,
                }
            })
            .collect();

        Self { test_values }
    }

    pub fn encoding(&self, c: &mut Criterion) {
        // Use a single buffer big enough to hold all encoded values
        let buffer_size = self.test_values.iter().map(|test| test.encoded.len()).sum();

        c.bench_function("Encode FreqsOnly", |b| {
            b.iter_batched_ref(
                || Cursor::new(Vec::with_capacity(buffer_size)),
                |mut buffer| {
                    for test in &self.test_values {
                        let record = inverted_index::RSIndexResult::virt()
                            .doc_id(100)
                            .frequency(test.freq);

                        let grew_size =
                            FreqsOnly::encode(&mut buffer, test.delta, &record).unwrap();

                        black_box(grew_size);
                    }
                },
                BatchSize::SmallInput,
            );
        });
    }

    pub fn decoding(&self, c: &mut Criterion) {
        c.bench_function("Decode FreqsOnly", |b| {
            for test in &self.test_values {
                b.iter_batched_ref(
                    || (Cursor::new(test.encoded.as_ref()), RSIndexResult::term()),
                    |(cursor, result)| {
                        let res = FreqsOnly::decode(cursor, 100, result);
                        let _ = black_box(res);
                    },
                    BatchSize::SmallInput,
                );
            }
        });
    }
}
