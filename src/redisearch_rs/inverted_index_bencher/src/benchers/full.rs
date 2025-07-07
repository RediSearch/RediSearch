/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{io::Cursor, mem::ManuallyDrop, ptr::NonNull, time::Duration, vec};

use buffer::Buffer;
use criterion::{
    BatchSize, BenchmarkGroup, Criterion, black_box,
    measurement::{Measurement, WallTime},
};
use inverted_index::{Decoder, Delta, Encoder, RSOffsetVector, RSTermRecord, full::Full};
use itertools::Itertools;

use crate::ffi::{TestBuffer, encode_freqs_only, read_freq_offsets_flags};

pub struct Bencher {
    test_values: Vec<TestValue>,
}

#[derive(Debug)]
struct TestValue {
    delta: u64,
    freq: u32,
    field_mask: u128,
    term_offsets: Vec<i8>,

    encoded: Vec<u8>,
}

/// Wrapper around `inverted_index::RSIndexResult` ensuring the term and offsets
/// pointers used internally stay valid for the duration of the bench.
#[derive(Debug)]
struct TestRecord {
    record: inverted_index::RSIndexResult,
    // both term and offsets need to stay alive during the bench
    _term: ffi::RSQueryTerm,
    _offsets: Vec<i8>,
}

impl TestRecord {
    fn new(doc_id: u64, field_mask: u128, freq: u32, offsets: Vec<i8>) -> Self {
        let mut record = inverted_index::RSIndexResult::token_record(
            doc_id,
            field_mask,
            freq,
            offsets.len() as u32,
        );
        record.weight = 1.0;

        const TEST_STR: &str = "test";
        let test_str_ptr = TEST_STR.as_ptr() as *mut _;
        let mut term = ffi::RSQueryTerm {
            str_: test_str_ptr,
            len: TEST_STR.len(),
            idf: 5.0,
            id: 1,
            flags: 0,
            bm25_idf: 10.0,
        };
        let offsets_ptr = offsets.as_ptr() as *mut _;

        record.data.term = ManuallyDrop::new(RSTermRecord {
            term: &mut term,
            offsets: RSOffsetVector {
                data: offsets_ptr,
                len: offsets.len() as u32,
            },
        });

        Self {
            record,
            _term: term,
            _offsets: offsets,
        }
    }
}

impl Bencher {
    const MEASUREMENT_TIME: Duration = Duration::from_millis(500);
    const WARMUP_TIME: Duration = Duration::from_millis(200);

    pub fn new() -> Self {
        let freq_values = vec![0, 2, 256, u16::MAX as u32, u32::MAX];
        let deltas = vec![0, 1, 256, 65536, u16::MAX as u64, u32::MAX as u64];
        // Full encoder cannot handle field masks larger than u32
        let field_masks_values = vec![0, 1, 256, 65536, u16::MAX as u128, u32::MAX as u128];
        let term_offsets_values = vec![vec![0], vec![1, 2, 3], vec![1; 100]];

        let test_values = freq_values
            .into_iter()
            .cartesian_product(deltas)
            .cartesian_product(field_masks_values)
            .cartesian_product(term_offsets_values)
            .map(|(((freq, delta), field_mask), term_offsets)| {
                let record = TestRecord::new(100, field_mask, freq, term_offsets.clone());
                let mut buffer = Cursor::new(Vec::new());
                let _grew_size = Full::default()
                    .encode(&mut buffer, Delta::new(delta as usize), &record.record)
                    .unwrap();
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
        let mut group = self.benchmark_group(c, "Encode - Full");

        self.c_encode(&mut group);
        self.rust_encode(&mut group);

        group.finish();
    }

    pub fn decoding(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Decode - Full");

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
                        let mut record = TestRecord::new(
                            100,
                            test.field_mask,
                            test.freq,
                            test.term_offsets.clone(),
                        );
                        let grew_size =
                            encode_freqs_only(&mut buffer, &mut record.record, test.delta);

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
                        let record = TestRecord::new(
                            100,
                            test.field_mask,
                            test.freq,
                            test.term_offsets.clone(),
                        );
                        let grew_size = Full::default()
                            .encode(&mut buffer, Delta::new(test.delta as usize), &record.record)
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
                        let (_filtered, result) = read_freq_offsets_flags(&mut buffer, 100);

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
                    || Cursor::new(&test.encoded),
                    |buffer| {
                        let result = Full::default().decode(buffer, 100);
                        let _ = black_box(result);
                    },
                    BatchSize::SmallInput,
                );
            }
        });
    }
}
