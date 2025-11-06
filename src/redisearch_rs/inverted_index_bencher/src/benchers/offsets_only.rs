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
use inverted_index::{
    Decoder, Encoder, RSIndexResult, offsets_only::OffsetsOnly, test_utils::TestTermRecord,
};
use itertools::Itertools;

pub struct Bencher {
    test_values: Vec<TestValue>,
}

#[derive(Debug)]
struct TestValue {
    delta: u32,
    term_offsets: Vec<i8>,

    encoded: Vec<u8>,
}

impl Default for Bencher {
    fn default() -> Self {
        Bencher::new()
    }
}

impl Bencher {
    fn new() -> Self {
        let deltas = vec![0, u32::MAX];
        let term_offsets_values = vec![
            vec![0],
            vec![1; 10],
            vec![1; 100],
            vec![1; 1_000],
            vec![1; 10_000],
        ];

        let test_values = deltas
            .into_iter()
            .cartesian_product(term_offsets_values)
            .map(|(delta, term_offsets)| {
                let record = TestTermRecord::new(100, 0, 1, term_offsets.clone());
                let mut buffer = Cursor::new(Vec::new());

                let _grew_size = OffsetsOnly::encode(&mut buffer, delta, &record.record).unwrap();

                let encoded = buffer.into_inner();

                TestValue {
                    delta,
                    encoded,
                    term_offsets,
                }
            })
            .collect();

        Self { test_values }
    }

    pub fn encoding(&self, c: &mut Criterion) {
        // Use a single buffer big enough to hold all encoded values
        let buffer_size = self.test_values.iter().map(|test| test.encoded.len()).sum();

        c.bench_function("Encode OffsetsOnly", |b| {
            b.iter_batched_ref(
                || Cursor::new(Vec::with_capacity(buffer_size)),
                |mut buffer| {
                    for test in &self.test_values {
                        let record = TestTermRecord::new(100, 0, 1, test.term_offsets.clone());

                        let grew_size =
                            OffsetsOnly::encode(&mut buffer, test.delta, &record.record).unwrap();

                        black_box(grew_size);
                    }
                },
                BatchSize::SmallInput,
            );
        });
    }

    pub fn decoding(&self, c: &mut Criterion) {
        c.bench_function("Decode OffsetsOnly", |b| {
            for test in &self.test_values {
                b.iter_batched_ref(
                    || (Cursor::new(test.encoded.as_ref()), RSIndexResult::term()),
                    |(cursor, result)| {
                        let res = OffsetsOnly::decode(cursor, 100, result);

                        let _ = black_box(res);
                    },
                    BatchSize::SmallInput,
                );
            }
        });
    }
}
