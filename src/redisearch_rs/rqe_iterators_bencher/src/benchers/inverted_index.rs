/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Benchmark inverted index iterator.

use std::time::Duration;

use criterion::{
    BenchmarkGroup, Criterion,
    measurement::{Measurement, WallTime},
};
use inverted_index::{InvertedIndex, RSIndexResult, numeric::Numeric};
use rqe_iterators::{RQEIterator, SkipToOutcome, inverted_index::NumericFull};

use crate::ffi;

const MEASUREMENT_TIME: Duration = Duration::from_millis(500);
const WARMUP_TIME: Duration = Duration::from_millis(200);
/// The number of documents in the index.
const INDEX_SIZE: u64 = 1_000_000;
/// The delta between the document IDs in the sparse index.
const SPARSE_DELTA: u64 = 1000;
/// The increment when skipping to a document ID.
const SKIP_TO_STEP: u64 = 100;

fn benchmark_group<'a>(
    c: &'a mut Criterion,
    it_name: &str,
    test: &str,
) -> BenchmarkGroup<'a, WallTime> {
    let label = format!("Iterator - InvertedIndex - {it_name} - {test}");
    let mut group = c.benchmark_group(label);
    group.measurement_time(MEASUREMENT_TIME);
    group.warm_up_time(WARMUP_TIME);
    group
}

#[derive(Default)]
pub struct NumericFullBencher;

impl NumericFullBencher {
    pub fn bench(&self, c: &mut Criterion) {
        self.read_dense(c);
        self.read_sparse(c);
        self.skip_to_dense(c);
        self.skip_to_sparse(c);
    }

    fn read_dense(&self, c: &mut Criterion) {
        let mut group = benchmark_group(c, "NumericFull", "Read Dense");
        self.c_read_dense(&mut group);
        self.rust_read_dense(&mut group);
        group.finish();
    }

    fn read_sparse(&self, c: &mut Criterion) {
        let mut group = benchmark_group(c, "NumericFull", "Read Sparse");
        self.c_read_sparse(&mut group);
        self.rust_read_sparse(&mut group);
        group.finish();
    }

    fn skip_to_dense(&self, c: &mut Criterion) {
        let mut group = benchmark_group(c, "NumericFull", "SkipTo Dense");
        self.c_skip_to_dense(&mut group);
        self.rust_skip_to_dense(&mut group);
        group.finish();
    }

    fn skip_to_sparse(&self, c: &mut Criterion) {
        let mut group = benchmark_group(c, "NumericFull", "SkipTo Sparse");
        self.c_skip_to_sparse(&mut group);
        self.rust_skip_to_sparse(&mut group);
        group.finish();
    }

    fn c_index(delta: u64) -> ffi::InvertedIndex {
        let ii = ffi::InvertedIndex::new(ffi::IndexFlags_Index_StoreNumeric);
        for doc_id in 1..INDEX_SIZE {
            ii.write_numeric_entry(doc_id * delta, doc_id as f64);
        }
        ii
    }

    fn rust_index(delta: u64) -> InvertedIndex<Numeric> {
        let mut ii = InvertedIndex::new(ffi::IndexFlags_Index_StoreNumeric, Numeric::default());
        for doc_id in 1..INDEX_SIZE {
            let record = RSIndexResult::numeric(doc_id as f64).doc_id(doc_id * delta);
            ii.add_record(&record).expect("failed to add record");
        }
        ii
    }

    fn c_read_dense<M: Measurement>(&self, group: &mut BenchmarkGroup<'_, M>) {
        group.bench_function("C", |b| {
            b.iter_batched_ref(
                || Self::c_index(1),
                |ii| {
                    let it = ii.iterator_numeric_full();
                    while it.read() == ::ffi::IteratorStatus_ITERATOR_OK {
                        criterion::black_box(it.current());
                    }
                    it.free();
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }

    fn c_read_sparse<M: Measurement>(&self, group: &mut BenchmarkGroup<'_, M>) {
        group.bench_function("C", |b| {
            b.iter_batched_ref(
                || Self::c_index(SPARSE_DELTA),
                |ii| {
                    let it = ii.iterator_numeric_full();
                    while it.read() == ::ffi::IteratorStatus_ITERATOR_OK {
                        criterion::black_box(it.current());
                    }
                    it.free();
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }

    fn rust_read_dense<M: Measurement>(&self, group: &mut BenchmarkGroup<'_, M>) {
        group.bench_function("Rust", |b| {
            b.iter_batched_ref(
                || Self::rust_index(1),
                |ii| {
                    let mut it = NumericFull::new(ii.reader());
                    while let Ok(Some(current)) = it.read() {
                        criterion::black_box(current);
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }

    fn rust_read_sparse<M: Measurement>(&self, group: &mut BenchmarkGroup<'_, M>) {
        group.bench_function("Rust", |b| {
            b.iter_batched_ref(
                || Self::rust_index(SPARSE_DELTA),
                |ii| {
                    let mut it = NumericFull::new(ii.reader());
                    while let Ok(Some(current)) = it.read() {
                        criterion::black_box(current);
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }

    fn c_skip_to_dense<M: Measurement>(&self, group: &mut BenchmarkGroup<'_, M>) {
        group.bench_function("C", |b| {
            b.iter_batched_ref(
                || Self::c_index(1),
                |ii| {
                    let it = ii.iterator_numeric_full();
                    while it.skip_to(it.last_doc_id() + SKIP_TO_STEP)
                        != ::ffi::IteratorStatus_ITERATOR_EOF
                    {
                        criterion::black_box(it.current());
                    }
                    it.free();
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }

    fn c_skip_to_sparse<M: Measurement>(&self, group: &mut BenchmarkGroup<'_, M>) {
        group.bench_function("C", |b| {
            b.iter_batched_ref(
                || Self::c_index(SPARSE_DELTA),
                |ii| {
                    let it = ii.iterator_numeric_full();
                    while it.skip_to(it.last_doc_id() + SKIP_TO_STEP)
                        != ::ffi::IteratorStatus_ITERATOR_EOF
                    {
                        criterion::black_box(it.current());
                    }
                    it.free();
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }

    fn rust_skip_to_dense<M: Measurement>(&self, group: &mut BenchmarkGroup<'_, M>) {
        group.bench_function("Rust", |b| {
            b.iter_batched_ref(
                || Self::rust_index(1),
                |ii| {
                    let mut it = NumericFull::new(ii.reader());
                    while let Ok(Some(outcome)) = it.skip_to(it.last_doc_id() + SKIP_TO_STEP) {
                        match outcome {
                            SkipToOutcome::Found(current) | SkipToOutcome::NotFound(current) => {
                                criterion::black_box(current);
                            }
                        }
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }

    fn rust_skip_to_sparse<M: Measurement>(&self, group: &mut BenchmarkGroup<'_, M>) {
        group.bench_function("Rust", |b| {
            b.iter_batched_ref(
                || Self::rust_index(SPARSE_DELTA),
                |ii| {
                    let mut it = NumericFull::new(ii.reader());
                    while let Ok(Some(outcome)) = it.skip_to(it.last_doc_id() + SKIP_TO_STEP) {
                        match outcome {
                            SkipToOutcome::Found(current) | SkipToOutcome::NotFound(current) => {
                                criterion::black_box(current);
                            }
                        }
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }
}
