/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Benchmarks for the wildcard inverted index iterator.

use std::hint::black_box;

use criterion::{BenchmarkGroup, Criterion, measurement::Measurement};
use inverted_index::{doc_ids_only::DocIdsOnly, opaque::OpaqueEncoding};
use rqe_iterators::{RQEIterator, SkipToOutcome, inverted_index::Wildcard};
use rqe_iterators_test_utils::TestContext;

use super::{INDEX_SIZE, SKIP_TO_STEP, SPARSE_DELTA, benchmark_group};

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
        self.rust_read(&mut group, &self.context_dense);
        group.finish();
    }

    fn skip_to_dense(&self, c: &mut Criterion) {
        let mut group = benchmark_group(c, "Wildcard", "SkipTo Dense");
        self.rust_skip_to(&mut group, &self.context_dense);
        group.finish();
    }

    fn skip_to_sparse(&self, c: &mut Criterion) {
        let mut group = benchmark_group(c, "Wildcard", "SkipTo Sparse");
        self.rust_skip_to(&mut group, &self.context_sparse);
        group.finish();
    }

    fn rust_read<M: Measurement>(&self, group: &mut BenchmarkGroup<'_, M>, context: &TestContext) {
        group.bench_function("Rust", |b| {
            let ii = DocIdsOnly::from_opaque(context.wildcard_inverted_index());
            b.iter(|| {
                let mut it = Wildcard::new(ii.reader(), 1.0);
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
            let ii = DocIdsOnly::from_opaque(context.wildcard_inverted_index());
            b.iter(|| {
                let mut it = Wildcard::new(ii.reader(), 1.0);
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
