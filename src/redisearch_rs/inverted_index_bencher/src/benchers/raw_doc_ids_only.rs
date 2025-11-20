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
use inverted_index::{Decoder, Encoder, RSIndexResult, raw_doc_ids_only::RawDocIdsOnly};

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
    fn new() -> Self {
        let deltas = vec![0, 1, 256, 65536, u16::MAX as u32, u32::MAX];

        let test_values = deltas
            .into_iter()
            .map(|delta| {
                let record = RSIndexResult::term().doc_id(100);

                let mut buffer = Cursor::new(Vec::new());
                let _grew_size = RawDocIdsOnly::encode(&mut buffer, delta, &record).unwrap();
                let encoded = buffer.into_inner();

                TestValue { delta, encoded }
            })
            .collect();

        Self { test_values }
    }

    pub fn encoding(&self, c: &mut Criterion) {
        // Use a single buffer big enough to hold all encoded values
        let buffer_size = self.test_values.iter().map(|test| test.encoded.len()).sum();

        c.bench_function("Encode RawDocIdsOnly", |b| {
            b.iter_batched_ref(
                || Cursor::new(Vec::with_capacity(buffer_size)),
                |mut buffer| {
                    for test in &self.test_values {
                        let record = RSIndexResult::term().doc_id(100);

                        let grew_size =
                            RawDocIdsOnly::encode(&mut buffer, test.delta, &record).unwrap();

                        black_box(grew_size);
                    }
                },
                BatchSize::SmallInput,
            );
        });
    }

    pub fn decoding(&self, c: &mut Criterion) {
        c.bench_function("Decode RawDocsIdsOnly", |b| {
            for test in &self.test_values {
                b.iter_batched_ref(
                    || (Cursor::new(test.encoded.as_ref()), RSIndexResult::term()),
                    |(cursor, result)| {
                        let res = RawDocIdsOnly::decode(cursor, 100, result);

                        let _ = black_box(res);
                    },
                    BatchSize::SmallInput,
                );
            }
        });
    }
}
