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

use ::ffi::RSQueryTerm;
use criterion::{
    BenchmarkGroup, Criterion,
    measurement::{Measurement, WallTime},
};
use inverted_index::{InvertedIndex, RSIndexResult, full::Full, numeric::Numeric};
use rqe_iterators::{
    RQEIterator, SkipToOutcome,
    inverted_index::{NumericFull, TermFull},
};

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

pub struct TermFullBencher {
    /// The offsets used in records, need to be kept alive for the duration of the benchmark
    offsets: Vec<u8>,
    /// The term used in records, need to be kept alive for the duration of the benchmark
    term: *mut RSQueryTerm,
}

impl Default for TermFullBencher {
    fn default() -> Self {
        const TEST_STR: &str = "term";
        let test_str_ptr = TEST_STR.as_ptr() as *mut _;
        let term = Box::new(RSQueryTerm {
            str_: test_str_ptr,
            len: TEST_STR.len(),
            idf: 5.0,
            id: 1,
            flags: 0,
            bm25_idf: 10.0,
        });
        let term = Box::into_raw(term);

        Self {
            offsets: vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
            term,
        }
    }
}

impl Drop for TermFullBencher {
    fn drop(&mut self) {
        unsafe {
            let _ = Box::from_raw(self.term);
        }
    }
}

impl TermFullBencher {
    pub fn bench(&self, c: &mut Criterion) {
        self.read_dense(c);
        self.read_sparse(c);
        self.skip_to_dense(c);
        self.skip_to_sparse(c);
    }

    fn read_dense(&self, c: &mut Criterion) {
        let mut group = benchmark_group(c, "TermFull", "Read Dense");
        self.c_read_dense(&mut group);
        self.rust_read_dense(&mut group);
        group.finish();
    }

    fn read_sparse(&self, c: &mut Criterion) {
        let mut group = benchmark_group(c, "TermFull", "Read Sparse");
        self.c_read_sparse(&mut group);
        self.rust_read_sparse(&mut group);
        group.finish();
    }

    fn skip_to_dense(&self, c: &mut Criterion) {
        let mut group = benchmark_group(c, "TermFull", "SkipTo Dense");
        self.c_skip_to_dense(&mut group);
        self.rust_skip_to_dense(&mut group);
        group.finish();
    }

    fn skip_to_sparse(&self, c: &mut Criterion) {
        let mut group = benchmark_group(c, "TermFull", "SkipTo Sparse");
        self.c_skip_to_sparse(&mut group);
        self.rust_skip_to_sparse(&mut group);
        group.finish();
    }

    fn c_index(&self, sparse: bool) -> ffi::InvertedIndex {
        let flags = ffi::IndexFlags_Index_StoreFreqs
            | ffi::IndexFlags_Index_StoreTermOffsets
            | ffi::IndexFlags_Index_StoreFieldFlags
            | ffi::IndexFlags_Index_StoreByteOffsets;
        let ii = ffi::InvertedIndex::new(flags);

        let delta = if sparse { SPARSE_DELTA } else { 1 };
        for doc_id in 1..INDEX_SIZE {
            let actual_doc_id = doc_id * delta;
            ii.write_term_entry(actual_doc_id, 1, 1, self.term, &self.offsets);
        }
        ii
    }

    fn rust_index(&self, sparse: bool) -> InvertedIndex<Full> {
        let flags = ffi::IndexFlags_Index_StoreFreqs
            | ffi::IndexFlags_Index_StoreTermOffsets
            | ffi::IndexFlags_Index_StoreFieldFlags
            | ffi::IndexFlags_Index_StoreByteOffsets;
        let mut ii = InvertedIndex::new(flags, Full::default());

        let delta = if sparse { SPARSE_DELTA } else { 1 };
        for doc_id in 1..INDEX_SIZE {
            let actual_doc_id = doc_id * delta;
            let record = RSIndexResult::term_with_term_ptr(
                self.term,
                inverted_index::RSOffsetVector::with_data(
                    self.offsets.as_ptr() as _,
                    self.offsets.len() as _,
                ),
                actual_doc_id,
                1,
                1,
            );
            ii.add_record(&record).expect("failed to add record");
        }
        ii
    }

    fn c_read_dense<M: Measurement>(&self, group: &mut BenchmarkGroup<'_, M>) {
        group.bench_function("C", |b| {
            b.iter_batched_ref(
                || self.c_index(false),
                |ii| {
                    let it = ii.iterator_term_full();
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
                || self.c_index(true),
                |ii| {
                    let it = ii.iterator_term_full();
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
                || self.rust_index(false),
                |ii| {
                    let mut it = TermFull::new(ii.reader());
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
                || self.rust_index(true),
                |ii| {
                    let mut it = TermFull::new(ii.reader());
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
                || self.c_index(false),
                |ii| {
                    let it = ii.iterator_term_full();
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
                || self.c_index(true),
                |ii| {
                    let it = ii.iterator_term_full();
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
                || self.rust_index(false),
                |ii| {
                    let mut it = TermFull::new(ii.reader());
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
                || self.rust_index(true),
                |ii| {
                    let mut it = TermFull::new(ii.reader());
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
