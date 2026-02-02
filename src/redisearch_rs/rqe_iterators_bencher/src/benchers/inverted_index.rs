/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Benchmark inverted index iterator.

use std::{hint::black_box, time::Duration};

use criterion::{
    BenchmarkGroup, Criterion,
    measurement::{Measurement, WallTime},
};
use field::{FieldExpirationPredicate, FieldFilterContext, FieldMaskOrIndex};
use inverted_index::{
    IndexReader, RSIndexResult, doc_ids_only::DocIdsOnly, opaque::OpaqueEncoding,
};
use rqe_iterators::{FieldExpirationChecker, RQEIterator, SkipToOutcome, inverted_index::Numeric};

use crate::ffi::QueryIterator;
use rqe_iterators_test_utils::TestContext;

const MEASUREMENT_TIME: Duration = Duration::from_millis(2000);
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

pub struct NumericBencher {
    context_dense: TestContext,
    context_sparse: TestContext,
    context_dense_multi: TestContext,
    context_sparse_multi: TestContext,
    context_dense_expired: TestContext,
    context_sparse_expired: TestContext,
    context_dense_multi_expired: TestContext,
    context_sparse_multi_expired: TestContext,
}

impl Default for NumericBencher {
    fn default() -> Self {
        let dense_iter =
            || (1..INDEX_SIZE).map(|doc_id| RSIndexResult::numeric(doc_id as f64).doc_id(doc_id));
        let sparse_iter = || {
            (1..INDEX_SIZE)
                .map(|doc_id| RSIndexResult::numeric(doc_id as f64).doc_id(doc_id * SPARSE_DELTA))
        };

        // Create expired contexts and mark every other record as expired
        let mut context_dense_expired = TestContext::numeric(dense_iter(), false);
        let mut context_sparse_expired = TestContext::numeric(sparse_iter(), false);
        let mut context_dense_multi_expired = TestContext::numeric(dense_iter(), true);
        let mut context_sparse_multi_expired = TestContext::numeric(sparse_iter(), true);

        // Mark every other record (even doc_ids) as expired
        let dense_even_ids: Vec<_> = (1..INDEX_SIZE).filter(|id| id % 2 == 0).collect();
        let sparse_even_ids: Vec<_> = (1..INDEX_SIZE)
            .filter(|id| id % 2 == 0)
            .map(|id| id * SPARSE_DELTA)
            .collect();

        let dense_field_index = context_dense_expired.field_spec().index;
        let sparse_field_index = context_sparse_expired.field_spec().index;
        let dense_multi_field_index = context_dense_multi_expired.field_spec().index;
        let sparse_multi_field_index = context_sparse_multi_expired.field_spec().index;

        context_dense_expired.mark_index_expired(
            dense_even_ids.clone(),
            field::FieldMaskOrIndex::Index(dense_field_index),
        );
        context_sparse_expired.mark_index_expired(
            sparse_even_ids.clone(),
            field::FieldMaskOrIndex::Index(sparse_field_index),
        );
        context_dense_multi_expired.mark_index_expired(
            dense_even_ids,
            field::FieldMaskOrIndex::Index(dense_multi_field_index),
        );
        context_sparse_multi_expired.mark_index_expired(
            sparse_even_ids,
            field::FieldMaskOrIndex::Index(sparse_multi_field_index),
        );

        Self {
            context_dense: TestContext::numeric(dense_iter(), false),
            context_sparse: TestContext::numeric(sparse_iter(), false),
            context_dense_multi: TestContext::numeric(dense_iter(), true),
            context_sparse_multi: TestContext::numeric(sparse_iter(), true),
            context_dense_expired,
            context_sparse_expired,
            context_dense_multi_expired,
            context_sparse_multi_expired,
        }
    }
}

impl NumericBencher {
    pub fn bench(&self, c: &mut Criterion) {
        self.read_dense(c);
        self.read_dense_multi(c);
        self.read_dense_expired(c);
        self.read_dense_multi_expired(c);
        self.skip_to_dense(c);
        self.skip_to_sparse(c);
        self.skip_to_dense_multi(c);
        self.skip_to_sparse_multi(c);
        self.skip_to_dense_expired(c);
        self.skip_to_sparse_expired(c);
        self.skip_to_dense_multi_expired(c);
        self.skip_to_sparse_multi_expired(c);
    }

    fn read_dense(&self, c: &mut Criterion) {
        let mut group = benchmark_group(c, "Numeric", "Read Dense");
        self.rust_read(&mut group, &self.context_dense);
        group.finish();
    }

    fn read_dense_multi(&self, c: &mut Criterion) {
        let mut group = benchmark_group(c, "Numeric", "Read Dense Multi");
        self.rust_read(&mut group, &self.context_dense_multi);
        group.finish();
    }

    fn read_dense_expired(&self, c: &mut Criterion) {
        let mut group = benchmark_group(c, "Numeric", "Read Dense Expired");
        self.rust_read(&mut group, &self.context_dense_expired);
        group.finish();
    }

    fn read_dense_multi_expired(&self, c: &mut Criterion) {
        let mut group = benchmark_group(c, "Numeric", "Read Dense Multi Expired");
        self.rust_read(&mut group, &self.context_dense_multi_expired);
        group.finish();
    }

    fn skip_to_dense(&self, c: &mut Criterion) {
        let mut group = benchmark_group(c, "Numeric", "SkipTo Dense");
        self.rust_skip_to(&mut group, &self.context_dense);
        group.finish();
    }

    fn skip_to_sparse(&self, c: &mut Criterion) {
        let mut group = benchmark_group(c, "Numeric", "SkipTo Sparse");
        self.rust_skip_to(&mut group, &self.context_sparse);
        group.finish();
    }

    fn skip_to_dense_multi(&self, c: &mut Criterion) {
        let mut group = benchmark_group(c, "Numeric", "SkipTo Dense Multi");
        self.rust_skip_to(&mut group, &self.context_dense_multi);
        group.finish();
    }

    fn skip_to_sparse_multi(&self, c: &mut Criterion) {
        let mut group = benchmark_group(c, "Numeric", "SkipTo Sparse Multi");
        self.rust_skip_to(&mut group, &self.context_sparse_multi);
        group.finish();
    }

    fn skip_to_dense_expired(&self, c: &mut Criterion) {
        let mut group = benchmark_group(c, "Numeric", "SkipTo Dense Expired");
        self.rust_skip_to(&mut group, &self.context_dense_expired);
        group.finish();
    }

    fn skip_to_sparse_expired(&self, c: &mut Criterion) {
        let mut group = benchmark_group(c, "Numeric", "SkipTo Sparse Expired");
        self.rust_skip_to(&mut group, &self.context_sparse_expired);
        group.finish();
    }

    fn skip_to_dense_multi_expired(&self, c: &mut Criterion) {
        let mut group = benchmark_group(c, "Numeric", "SkipTo Dense Multi Expired");
        self.rust_skip_to(&mut group, &self.context_dense_multi_expired);
        group.finish();
    }

    fn skip_to_sparse_multi_expired(&self, c: &mut Criterion) {
        let mut group = benchmark_group(c, "Numeric", "SkipTo Sparse Multi Expired");
        self.rust_skip_to(&mut group, &self.context_sparse_multi_expired);
        group.finish();
    }

    fn rust_read<M: Measurement>(&self, group: &mut BenchmarkGroup<'_, M>, context: &TestContext) {
        group.bench_function("Rust", |b| {
            let ii = {
                use inverted_index::{numeric::Numeric, opaque::OpaqueEncoding};
                Numeric::from_mut_opaque(context.numeric_inverted_index()).inner_mut()
            };
            let fs = context.field_spec();

            b.iter(|| {
                let reader = ii.reader();
                let reader_flags = reader.flags();
                let checker = FieldExpirationChecker::new(
                    context.sctx,
                    FieldFilterContext {
                        field: FieldMaskOrIndex::Index(fs.index),
                        predicate: FieldExpirationPredicate::Default,
                    },
                    reader_flags,
                );

                let mut it = Numeric::new(reader, checker, None, None, None);

                while let Ok(Some(current)) = it.read() {
                    black_box(current);
                }
            });
        });
    }

    fn rust_skip_to<M: Measurement>(
        &self,
        group: &mut BenchmarkGroup<'_, M>,
        context: &TestContext,
    ) {
        group.bench_function("Rust", |b| {
            let ii = {
                use inverted_index::{numeric::Numeric, opaque::OpaqueEncoding};
                Numeric::from_mut_opaque(context.numeric_inverted_index()).inner_mut()
            };
            let fs = context.field_spec();

            b.iter(|| {
                let reader = ii.reader();
                let reader_flags = reader.flags();
                let checker = FieldExpirationChecker::new(
                    context.sctx,
                    FieldFilterContext {
                        field: FieldMaskOrIndex::Index(fs.index),
                        predicate: FieldExpirationPredicate::Default,
                    },
                    reader_flags,
                );

                let mut it = Numeric::new(reader, checker, None, None, None);

                while let Ok(Some(outcome)) = it.skip_to(it.last_doc_id() + SKIP_TO_STEP) {
                    match outcome {
                        SkipToOutcome::Found(current) | SkipToOutcome::NotFound(current) => {
                            black_box(current);
                        }
                    }
                }
            });
        });
    }
}

/*
pub struct TermBencher<E> {
    /// Name of the benchmark group
    group_name: String,
    /// Inverted index flags to use for the benchmark
    ii_flags: u32,
    /// The offsets used in records, need to be kept alive for the duration of the benchmark
    offsets: Vec<u8>,
    /// The term used in records, need to be kept alive for the duration of the benchmark
    term: *mut RSQueryTerm,
    /// Needed to carry the encoder used by the benchmark.
    _phantom_enc: PhantomData<E>,
}

impl<E> Drop for TermBencher<E> {
    fn drop(&mut self) {
        unsafe {
            let _ = Term_Free(self.term);
        }
    }
}

impl<E> TermBencher<E>
where
    E: Encoder + DecodedBy,
    E::Decoder: TermDecoder,
{
    pub fn new(decoder_name: &str, ii_flags: u32) -> Self {
        let group_name = format!("Term - {decoder_name}");
        const TEST_STR: &str = "term";
        let term = QueryTermBuilder {
            token: TEST_STR,
            idf: 5.0,
            id: 1,
            flags: 0,
            bm25_idf: 10.0,
        }
        .allocate();

        Self {
            group_name,
            ii_flags,
            offsets: vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
            term,
            _phantom_enc: PhantomData,
        }
    }

    pub fn bench(&self, c: &mut Criterion) {
        self.read_dense(c);
        self.skip_to_dense(c);
        self.skip_to_sparse(c);
    }

    fn read_dense(&self, c: &mut Criterion) {
        let mut group = benchmark_group(c, &self.group_name, "Read Dense");
        self.c_read_dense(&mut group);
        self.rust_read_dense(&mut group);
        group.finish();
    }

    fn skip_to_dense(&self, c: &mut Criterion) {
        let mut group = benchmark_group(c, &self.group_name, "SkipTo Dense");
        self.c_skip_to_dense(&mut group);
        self.rust_skip_to_dense(&mut group);
        group.finish();
    }

    fn skip_to_sparse(&self, c: &mut Criterion) {
        let mut group = benchmark_group(c, &self.group_name, "SkipTo Sparse");
        self.c_skip_to_sparse(&mut group);
        self.rust_skip_to_sparse(&mut group);
        group.finish();
    }

    fn c_index(&self, sparse: bool) -> ffi::InvertedIndex {
        let ii = ffi::InvertedIndex::new(self.ii_flags);

        let delta = if sparse { SPARSE_DELTA } else { 1 };
        for doc_id in 1..INDEX_SIZE {
            let actual_doc_id = doc_id * delta;
            ii.write_term_entry(actual_doc_id, 1, 1, self.term, &self.offsets);
        }
        ii
    }

    fn rust_index(&self, sparse: bool) -> InvertedIndex<E> {
        let mut ii = InvertedIndex::<E>::new(self.ii_flags);

        let delta = if sparse { SPARSE_DELTA } else { 1 };
        for doc_id in 1..INDEX_SIZE {
            let actual_doc_id = doc_id * delta;
            let record = RSIndexResult::term_with_term_ptr(
                self.term,
                inverted_index::RSOffsetSlice::from_bytes(&self.offsets),
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
                    let it = ii.iterator_term();
                    while it.read() == ::ffi::IteratorStatus_ITERATOR_OK {
                        black_box(it.current());
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
                    let mut it = Term::new_simple(ii.reader());
                    while let Ok(Some(current)) = it.read() {
                        black_box(current);
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
                    let it = ii.iterator_term();
                    while it.skip_to(it.last_doc_id() + SKIP_TO_STEP)
                        != ::ffi::IteratorStatus_ITERATOR_EOF
                    {
                        black_box(it.current());
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
                    let it = ii.iterator_term();
                    while it.skip_to(it.last_doc_id() + SKIP_TO_STEP)
                        != ::ffi::IteratorStatus_ITERATOR_EOF
                    {
                        black_box(it.current());
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
                    let mut it = Term::new_simple(ii.reader());
                    while let Ok(Some(outcome)) = it.skip_to(it.last_doc_id() + SKIP_TO_STEP) {
                        match outcome {
                            SkipToOutcome::Found(current) | SkipToOutcome::NotFound(current) => {
                                black_box(current);
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
                    let mut it = Term::new_simple(ii.reader());
                    while let Ok(Some(outcome)) = it.skip_to(it.last_doc_id() + SKIP_TO_STEP) {
                        match outcome {
                            SkipToOutcome::Found(current) | SkipToOutcome::NotFound(current) => {
                                black_box(current);
                            }
                        }
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }
}
 */

/// Benchmarks for the wildcard inverted index iterator.
pub struct WildcardBencher {
    context_dense: TestContext,
    context_sparse: TestContext,
}

impl Default for WildcardBencher {
    fn default() -> Self {
        let dense_iter = || 1..INDEX_SIZE;
        let sparse_iter = || (1..INDEX_SIZE).map(|i| i * SPARSE_DELTA);

        Self {
            context_dense: TestContext::wildcard(dense_iter()),
            context_sparse: TestContext::wildcard(sparse_iter()),
        }
    }
}

impl WildcardBencher {
    pub fn bench(&self, c: &mut Criterion) {
        self.read_dense(c);
        self.skip_to_dense(c);
        self.skip_to_sparse(c);
    }

    fn read_dense(&self, c: &mut Criterion) {
        let mut group = benchmark_group(c, "Wildcard", "Read Dense");
        self.c_read(&mut group, &self.context_dense);
        self.rust_read(&mut group, &self.context_dense);
        group.finish();
    }

    fn skip_to_dense(&self, c: &mut Criterion) {
        let mut group = benchmark_group(c, "Wildcard", "SkipTo Dense");
        self.c_skip_to(&mut group, &self.context_dense);
        self.rust_skip_to(&mut group, &self.context_dense);
        group.finish();
    }

    fn skip_to_sparse(&self, c: &mut Criterion) {
        let mut group = benchmark_group(c, "Wildcard", "SkipTo Sparse");
        self.c_skip_to(&mut group, &self.context_sparse);
        self.rust_skip_to(&mut group, &self.context_sparse);
        group.finish();
    }

    fn c_read<M: Measurement>(&self, group: &mut BenchmarkGroup<'_, M>, context: &TestContext) {
        group.bench_function("C", |b| {
            let ii_ptr = context.wildcard_index_ptr();

            b.iter(|| {
                let it = unsafe { QueryIterator::new_wildcard(ii_ptr, context.sctx, 1.0) };

                while it.read() == ::ffi::IteratorStatus_ITERATOR_OK {
                    black_box(it.current());
                }
                it.free();
            });
        });
    }

    fn c_skip_to<M: Measurement>(&self, group: &mut BenchmarkGroup<'_, M>, context: &TestContext) {
        group.bench_function("C", |b| {
            let ii_ptr = context.wildcard_index_ptr();

            b.iter(|| {
                let it = unsafe { QueryIterator::new_wildcard(ii_ptr, context.sctx, 1.0) };

                while it.skip_to(it.last_doc_id() + SKIP_TO_STEP)
                    != ::ffi::IteratorStatus_ITERATOR_EOF
                {
                    black_box(it.current());
                }
                it.free();
            });
        });
    }

    fn rust_read<M: Measurement>(&self, group: &mut BenchmarkGroup<'_, M>, context: &TestContext) {
        use rqe_iterators::inverted_index::Wildcard;

        group.bench_function("Rust", |b| {
            let ii = DocIdsOnly::from_opaque(context.wildcard_inverted_index());
            b.iter(|| {
                let mut it = Wildcard::new(ii.reader(), context.sctx, 1.0);
                while let Ok(Some(current)) = it.read() {
                    black_box(current);
                }
            });
        });
    }

    fn rust_skip_to<M: Measurement>(
        &self,
        group: &mut BenchmarkGroup<'_, M>,
        context: &TestContext,
    ) {
        use rqe_iterators::inverted_index::Wildcard;

        group.bench_function("Rust", |b| {
            let ii = DocIdsOnly::from_opaque(context.wildcard_inverted_index());
            b.iter(|| {
                let mut it = Wildcard::new(ii.reader(), context.sctx, 1.0);
                while let Ok(Some(outcome)) = it.skip_to(it.last_doc_id() + SKIP_TO_STEP) {
                    match outcome {
                        SkipToOutcome::Found(current) | SkipToOutcome::NotFound(current) => {
                            black_box(current);
                        }
                    }
                }
            });
        });
    }
}
