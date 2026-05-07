/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Benchmark NOT iterator (optimized version).

use std::{hint::black_box, time::Duration};

use criterion::{BenchmarkGroup, Criterion, measurement::WallTime};
use rqe_iterators::{
    RQEIterator, empty::Empty, id_list::IdListSorted, not_optimized::NotOptimized,
    wildcard::new_wildcard_iterator_optimized,
};
use rqe_iterators_test_utils::TestContext;

pub struct Bencher {
    context_all: TestContext,
    context_sparse: TestContext,
}

impl Default for Bencher {
    fn default() -> Self {
        let context_all = TestContext::wildcard(1..Self::MAX_DOC_ID);
        let context_sparse = TestContext::wildcard((1..Self::MAX_DOC_ID).step_by(100));
        // SAFETY: no iterators have been created from these contexts yet.
        // We set index_all=true so the wildcard iterator returns all doc IDs up to MAX_DOC_ID
        // rather than consulting existingDocs.
        unsafe {
            context_all.set_index_all(true);
            context_sparse.set_index_all(true);
        }

        Self {
            context_all,
            context_sparse,
        }
    }
}

impl Bencher {
    const MEASUREMENT_TIME: Duration = Duration::from_millis(500);
    const WARMUP_TIME: Duration = Duration::from_millis(200);

    const WEIGHT: f64 = 1.0;
    const MAX_DOC_ID: u64 = 1_000_000;
    /// Step size for sparse child data (every 200th doc).
    const SPARSE_STEP: usize = 200;
    /// Step size for skip_to() calls.
    const SKIP_TO_STEP: u64 = 100;

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

    /// Dense child data: 99% of docs (all except every 100th doc).
    fn dense_child() -> Vec<u64> {
        (1..Self::MAX_DOC_ID).filter(|x| x % 100 != 0).collect()
    }

    /// Sparse child data: every 200th doc (half the sparse wildcard).
    fn sparse_child() -> Vec<u64> {
        (1..Self::MAX_DOC_ID).step_by(Self::SPARSE_STEP).collect()
    }

    pub fn bench(&self, c: &mut Criterion) {
        self.read_empty_child(c);
        self.read_dense_child(c);
        self.read_sparse_wc(c);
        self.skip_to_empty_child(c);
        self.skip_to_sparse_child(c);
        self.skip_to_dense_child(c);
    }

    /// Benchmark NOT-optimized with empty child (all wildcard docs returned).
    fn read_empty_child(&self, c: &mut Criterion) {
        let context = &self.context_all;
        let mut group = self.benchmark_group(c, "Iterator - NotOptimized - Read Empty Child");

        group.bench_function("Rust", |b| {
            b.iter_batched_ref(
                || {
                    // SAFETY: context has index_all=true and existingDocs wired by TestContext::wildcard.
                    let wc = unsafe { new_wildcard_iterator_optimized(context.sctx, Self::WEIGHT) };
                    NotOptimized::new(wc, Empty, Self::MAX_DOC_ID, Self::WEIGHT, None)
                },
                |it| {
                    while let Ok(Some(current)) = it.read() {
                        black_box(current);
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });

        group.finish();
    }

    /// Benchmark NOT-optimized with dense child (few docs returned).
    fn read_dense_child(&self, c: &mut Criterion) {
        let context = &self.context_all;
        let mut group = self.benchmark_group(c, "Iterator - NotOptimized - Read Dense Child");

        group.bench_function("Rust", |b| {
            b.iter_batched_ref(
                || {
                    // SAFETY: context has index_all=true and existingDocs wired by TestContext::wildcard.
                    let wc = unsafe { new_wildcard_iterator_optimized(context.sctx, Self::WEIGHT) };
                    NotOptimized::new(
                        wc,
                        IdListSorted::new(Self::dense_child()),
                        Self::MAX_DOC_ID,
                        Self::WEIGHT,
                        None,
                    )
                },
                |it| {
                    while let Ok(Some(current)) = it.read() {
                        black_box(current);
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });

        group.finish();
    }

    /// Benchmark NOT-optimized with sparse wildcard (only 1% of docs exist).
    fn read_sparse_wc(&self, c: &mut Criterion) {
        let context = &self.context_sparse;
        let mut group = self.benchmark_group(c, "Iterator - NotOptimized - Read Sparse WC");

        group.bench_function("Rust", |b| {
            b.iter_batched_ref(
                || {
                    // SAFETY: context has index_all=true and existingDocs wired by TestContext::wildcard.
                    let wc = unsafe { new_wildcard_iterator_optimized(context.sctx, Self::WEIGHT) };
                    NotOptimized::new(
                        wc,
                        IdListSorted::new(Self::sparse_child()),
                        Self::MAX_DOC_ID,
                        Self::WEIGHT,
                        None,
                    )
                },
                |it| {
                    while let Ok(Some(current)) = it.read() {
                        black_box(current);
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });

        group.finish();
    }

    /// Benchmark NOT-optimized SkipTo with empty child.
    fn skip_to_empty_child(&self, c: &mut Criterion) {
        let context = &self.context_all;
        let mut group = self.benchmark_group(c, "Iterator - NotOptimized - SkipTo Empty Child");

        group.bench_function("Rust", |b| {
            b.iter_batched_ref(
                || {
                    // SAFETY: context has index_all=true and existingDocs wired by TestContext::wildcard.
                    let wc = unsafe { new_wildcard_iterator_optimized(context.sctx, Self::WEIGHT) };
                    NotOptimized::new(wc, Empty, Self::MAX_DOC_ID, Self::WEIGHT, None)
                },
                |it| {
                    while let Ok(Some(current)) = it.skip_to(it.last_doc_id() + Self::SKIP_TO_STEP)
                    {
                        black_box(current);
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });

        group.finish();
    }

    /// Benchmark NOT-optimized SkipTo with sparse child.
    fn skip_to_sparse_child(&self, c: &mut Criterion) {
        let context = &self.context_all;
        let mut group = self.benchmark_group(c, "Iterator - NotOptimized - SkipTo Sparse Child");

        group.bench_function("Rust", |b| {
            b.iter_batched_ref(
                || {
                    // SAFETY: context has index_all=true and existingDocs wired by TestContext::wildcard.
                    let wc = unsafe { new_wildcard_iterator_optimized(context.sctx, Self::WEIGHT) };
                    NotOptimized::new(
                        wc,
                        IdListSorted::new(Self::sparse_child()),
                        Self::MAX_DOC_ID,
                        Self::WEIGHT,
                        None,
                    )
                },
                |it| {
                    while let Ok(Some(current)) = it.skip_to(it.last_doc_id() + Self::SKIP_TO_STEP)
                    {
                        black_box(current);
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });

        group.finish();
    }

    /// Benchmark NOT-optimized SkipTo with dense child.
    fn skip_to_dense_child(&self, c: &mut Criterion) {
        let context = &self.context_all;
        let mut group = self.benchmark_group(c, "Iterator - NotOptimized - SkipTo Dense Child");

        group.bench_function("Rust", |b| {
            b.iter_batched_ref(
                || {
                    // SAFETY: context has index_all=true and existingDocs wired by TestContext::wildcard.
                    let wc = unsafe { new_wildcard_iterator_optimized(context.sctx, Self::WEIGHT) };
                    NotOptimized::new(
                        wc,
                        IdListSorted::new(Self::dense_child()),
                        Self::MAX_DOC_ID,
                        Self::WEIGHT,
                        None,
                    )
                },
                |it| {
                    while let Ok(Some(current)) = it.skip_to(it.last_doc_id() + Self::SKIP_TO_STEP)
                    {
                        black_box(current);
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });

        group.finish();
    }
}
