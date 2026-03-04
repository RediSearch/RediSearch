/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::hint::black_box;

use criterion::{BenchmarkGroup, Criterion, measurement::Measurement};
use field::{FieldExpirationPredicate, FieldFilterContext, FieldMaskOrIndex};
use inverted_index::{IndexReader, RSIndexResult};
use rqe_iterators::{FieldExpirationChecker, RQEIterator, SkipToOutcome, inverted_index::Numeric};
use rqe_iterators_test_utils::TestContext;

use super::{INDEX_SIZE, SKIP_TO_STEP, SPARSE_DELTA, benchmark_group};

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
            let ii = context.numeric_inverted_index();
            let fs = context.field_spec();

            b.iter(|| {
                let reader = ii.reader();
                let reader_flags = reader.flags();
                // SAFETY: `context.sctx` is a valid `RedisSearchCtx` with a valid `spec`,
                // both remaining valid for the benchmark's lifetime.
                let checker = unsafe {
                    FieldExpirationChecker::new(
                        context.sctx,
                        FieldFilterContext {
                            field: FieldMaskOrIndex::Index(fs.index),
                            predicate: FieldExpirationPredicate::Default,
                        },
                        reader_flags,
                    )
                };

                // SAFETY: `range_tree` is None so no pointer invariants apply.
                let mut it = unsafe { Numeric::new(reader, checker, None, None, None) };

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
            let ii = context.numeric_inverted_index();
            let fs = context.field_spec();

            b.iter(|| {
                let reader = ii.reader();
                let reader_flags = reader.flags();
                // SAFETY: `context.sctx` is a valid `RedisSearchCtx` with a valid `spec`,
                // both remaining valid for the benchmark's lifetime.
                let checker = unsafe {
                    FieldExpirationChecker::new(
                        context.sctx,
                        FieldFilterContext {
                            field: FieldMaskOrIndex::Index(fs.index),
                            predicate: FieldExpirationPredicate::Default,
                        },
                        reader_flags,
                    )
                };

                // SAFETY: `range_tree` is None so no pointer invariants apply.
                let mut it = unsafe { Numeric::new(reader, checker, None, None, None) };

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
