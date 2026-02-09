/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Benchmark NOT iterator (non-optimized version only).

use std::{hint::black_box, time::Duration};

use criterion::{BenchmarkGroup, Criterion, measurement::WallTime};
use rqe_iterators::{RQEIterator, empty::Empty, id_list::IdListSorted, not::Not};

use crate::ffi::{IteratorStatus_ITERATOR_OK, QueryIterator};

#[derive(Default)]
pub struct Bencher;

impl Bencher {
    const MEASUREMENT_TIME: Duration = Duration::from_millis(500);
    const WARMUP_TIME: Duration = Duration::from_millis(200);

    const WEIGHT: f64 = 1.0;
    const MAX_DOC_ID: u64 = 1_000_000;

    /// Duration is irrelevant since we skip timeout checks in benchmarks.
    const NOT_ITERATOR_TIMEOUT: Duration = Duration::ZERO;

    /// Skip timeout checks in benchmarks to avoid any overhead.
    const SKIP_TIMEOUT_CHECKS: bool = true;

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

    pub fn bench(&self, c: &mut Criterion) {
        self.read_empty_child(c);
        self.read_sparse_child(c);
        self.read_dense_child(c);
        self.skip_to_empty_child(c);
        self.skip_to_sparse_child(c);
        self.skip_to_dense_child(c);
    }

    /// Benchmark NOT with empty child (all docs returned)
    fn read_empty_child(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Iterator - Not - Read Empty Child");

        // Rust implementation
        group.bench_function("Rust", |b| {
            b.iter_batched_ref(
                || {
                    Not::new(
                        Empty,
                        Self::MAX_DOC_ID,
                        1.0,
                        Self::NOT_ITERATOR_TIMEOUT,
                        Self::SKIP_TIMEOUT_CHECKS,
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

        // C implementation (non-optimized)
        group.bench_function("C", |b| {
            b.iter_batched_ref(
                || {
                    let child = QueryIterator::new_empty();
                    QueryIterator::new_not_non_optimized(child, Self::MAX_DOC_ID, Self::WEIGHT)
                },
                |it| {
                    while it.read() == IteratorStatus_ITERATOR_OK {
                        black_box(it.current());
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });

        group.finish();
    }

    /// Benchmark NOT with sparse child (most docs returned)
    fn read_sparse_child(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Iterator - Not - Read Sparse Child");

        // Rust implementation
        group.bench_function("Rust", |b| {
            b.iter_batched_ref(
                || {
                    // Child has 1% of docs (every 100th doc)
                    let data: Vec<_> = (1..Self::MAX_DOC_ID).step_by(100).collect();
                    Not::new(
                        IdListSorted::new(data),
                        Self::MAX_DOC_ID,
                        1.0,
                        Self::NOT_ITERATOR_TIMEOUT,
                        Self::SKIP_TIMEOUT_CHECKS,
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

        // C implementation (non-optimized)
        group.bench_function("C", |b| {
            b.iter_batched_ref(
                || {
                    let data = (1..Self::MAX_DOC_ID).step_by(100).collect();
                    let child = QueryIterator::new_id_list(data);
                    QueryIterator::new_not_non_optimized(child, Self::MAX_DOC_ID, Self::WEIGHT)
                },
                |it| {
                    while it.read() == IteratorStatus_ITERATOR_OK {
                        black_box(it.current());
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });

        group.finish();
    }

    /// Benchmark NOT with dense child (few docs returned)
    fn read_dense_child(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Iterator - Not - Read Dense Child");

        // Rust implementation
        group.bench_function("Rust", |b| {
            b.iter_batched_ref(
                || {
                    // Child has 99% of docs (all except every 100th doc)
                    let data: Vec<_> = (1..Self::MAX_DOC_ID).filter(|x| x % 100 != 0).collect();
                    Not::new(
                        IdListSorted::new(data),
                        Self::MAX_DOC_ID,
                        1.0,
                        Self::NOT_ITERATOR_TIMEOUT,
                        Self::SKIP_TIMEOUT_CHECKS,
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

        // C implementation (non-optimized)
        group.bench_function("C", |b| {
            b.iter_batched_ref(
                || {
                    let data = (1..Self::MAX_DOC_ID).filter(|x| x % 100 != 0).collect();
                    let child = QueryIterator::new_id_list(data);
                    QueryIterator::new_not_non_optimized(child, Self::MAX_DOC_ID, Self::WEIGHT)
                },
                |it| {
                    while it.read() == IteratorStatus_ITERATOR_OK {
                        black_box(it.current());
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });

        group.finish();
    }

    /// Benchmark NOT SkipTo with empty child
    fn skip_to_empty_child(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Iterator - Not - SkipTo Empty Child");
        let step = 100;

        // Rust implementation
        group.bench_function("Rust", |b| {
            b.iter_batched_ref(
                || {
                    Not::new(
                        Empty,
                        Self::MAX_DOC_ID,
                        1.0,
                        Self::NOT_ITERATOR_TIMEOUT,
                        Self::SKIP_TIMEOUT_CHECKS,
                    )
                },
                |it| {
                    while let Ok(Some(current)) = it.skip_to(it.last_doc_id() + step) {
                        black_box(current);
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });

        // C implementation (non-optimized)
        group.bench_function("C", |b| {
            b.iter_batched_ref(
                || {
                    let child = QueryIterator::new_empty();
                    QueryIterator::new_not_non_optimized(child, Self::MAX_DOC_ID, Self::WEIGHT)
                },
                |it| {
                    while it.skip_to(it.last_doc_id() + step) == IteratorStatus_ITERATOR_OK {
                        black_box(it.current());
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });

        group.finish();
    }

    /// Benchmark NOT SkipTo with sparse child
    fn skip_to_sparse_child(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Iterator - Not - SkipTo Sparse Child");
        let step = 100;

        // Rust implementation
        group.bench_function("Rust", |b| {
            b.iter_batched_ref(
                || {
                    let data: Vec<_> = (1..Self::MAX_DOC_ID).step_by(100).collect();
                    Not::new(
                        IdListSorted::new(data),
                        Self::MAX_DOC_ID,
                        1.0,
                        Self::NOT_ITERATOR_TIMEOUT,
                        Self::SKIP_TIMEOUT_CHECKS,
                    )
                },
                |it| {
                    while let Ok(Some(current)) = it.skip_to(it.last_doc_id() + step) {
                        black_box(current);
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });

        // C implementation (non-optimized)
        group.bench_function("C", |b| {
            b.iter_batched_ref(
                || {
                    let data = (1..Self::MAX_DOC_ID).step_by(100).collect();
                    let child = QueryIterator::new_id_list(data);
                    QueryIterator::new_not_non_optimized(child, Self::MAX_DOC_ID, Self::WEIGHT)
                },
                |it| {
                    while it.skip_to(it.last_doc_id() + step) == IteratorStatus_ITERATOR_OK {
                        black_box(it.current());
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });

        group.finish();
    }

    /// Benchmark NOT SkipTo with dense child
    fn skip_to_dense_child(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Iterator - Not - SkipTo Dense Child");
        let step = 100;

        // Rust implementation
        group.bench_function("Rust", |b| {
            b.iter_batched_ref(
                || {
                    let data: Vec<_> = (1..Self::MAX_DOC_ID).filter(|x| x % 100 != 0).collect();
                    Not::new(
                        IdListSorted::new(data),
                        Self::MAX_DOC_ID,
                        1.0,
                        Self::NOT_ITERATOR_TIMEOUT,
                        Self::SKIP_TIMEOUT_CHECKS,
                    )
                },
                |it| {
                    while let Ok(Some(current)) = it.skip_to(it.last_doc_id() + step) {
                        black_box(current);
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });

        // C implementation (non-optimized)
        group.bench_function("C", |b| {
            b.iter_batched_ref(
                || {
                    let data = (1..Self::MAX_DOC_ID).filter(|x| x % 100 != 0).collect();
                    let child = QueryIterator::new_id_list(data);
                    QueryIterator::new_not_non_optimized(child, Self::MAX_DOC_ID, Self::WEIGHT)
                },
                |it| {
                    while it.skip_to(it.last_doc_id() + step) == IteratorStatus_ITERATOR_OK {
                        black_box(it.current());
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });

        group.finish();
    }
}
