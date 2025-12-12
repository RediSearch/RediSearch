/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Benchmark NOT iterator.

use std::time::Duration;

use criterion::{
    BenchmarkGroup, Criterion,
    measurement::{Measurement, WallTime},
};
use rqe_iterators::{RQEIterator, id_list::SortedIdList, not_iterator::Not};

use crate::ffi;

#[derive(Default)]
pub struct Bencher;

impl Bencher {
    const MEASUREMENT_TIME: Duration = Duration::from_millis(500);
    const WARMUP_TIME: Duration = Duration::from_millis(200);

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
        self.read_sparse_exclusions(c);
        self.read_dense_exclusions(c);
        self.skip_to_sparse_exclusions(c);
        self.skip_to_dense_exclusions(c);
    }

    // Sparse exclusions: few docs excluded from a large range
    fn read_sparse_exclusions(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Iterator - Not - Read Sparse Exclusions");
        self.c_read_sparse_exclusions(&mut group);
        self.rust_read_sparse_exclusions(&mut group);
        group.finish();
    }

    // Dense exclusions: many docs excluded, few returned
    fn read_dense_exclusions(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Iterator - Not - Read Dense Exclusions");
        self.c_read_dense_exclusions(&mut group);
        self.rust_read_dense_exclusions(&mut group);
        group.finish();
    }

    fn skip_to_sparse_exclusions(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Iterator - Not - SkipTo Sparse Exclusions");
        self.c_skip_to_sparse_exclusions(&mut group);
        self.rust_skip_to_sparse_exclusions(&mut group);
        group.finish();
    }

    fn skip_to_dense_exclusions(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Iterator - Not - SkipTo Dense Exclusions");
        self.c_skip_to_dense_exclusions(&mut group);
        self.rust_skip_to_dense_exclusions(&mut group);
        group.finish();
    }

    // Sparse exclusions: exclude every 1000th doc from 1M range
    fn rust_read_sparse_exclusions<M: Measurement>(&self, group: &mut BenchmarkGroup<'_, M>) {
        group.bench_function("Rust", |b| {
            b.iter_batched_ref(
                || {
                    // Exclude 1000 docs from 1M range (every 1000th doc)
                    let excluded: Vec<_> = (1..=1_000_000u64).step_by(1000).collect();
                    Not::new(SortedIdList::new(excluded), 1_000_000)
                },
                |it| {
                    while let Ok(Some(current)) = it.read() {
                        criterion::black_box(current);
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }

    // Dense exclusions: exclude 90% of docs (every doc except every 10th)
    fn rust_read_dense_exclusions<M: Measurement>(&self, group: &mut BenchmarkGroup<'_, M>) {
        group.bench_function("Rust", |b| {
            b.iter_batched_ref(
                || {
                    // Exclude 900K docs from 1M range (keep only every 10th doc)
                    let excluded: Vec<_> = (1..=1_000_000u64)
                        .filter(|x| x % 10 != 0)
                        .collect();
                    Not::new(SortedIdList::new(excluded), 1_000_000)
                },
                |it| {
                    while let Ok(Some(current)) = it.read() {
                        criterion::black_box(current);
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }

    fn rust_skip_to_sparse_exclusions<M: Measurement>(&self, group: &mut BenchmarkGroup<'_, M>) {
        group.bench_function("Rust", |b| {
            let step = 100;
            b.iter_batched_ref(
                || {
                    let excluded: Vec<_> = (1..=1_000_000u64).step_by(1000).collect();
                    Not::new(SortedIdList::new(excluded), 1_000_000)
                },
                |it| {
                    while let Ok(Some(current)) = it.skip_to(it.last_doc_id() + step) {
                        criterion::black_box(current);
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }

    fn rust_skip_to_dense_exclusions<M: Measurement>(&self, group: &mut BenchmarkGroup<'_, M>) {
        group.bench_function("Rust", |b| {
            let step = 100;
            b.iter_batched_ref(
                || {
                    let excluded: Vec<_> = (1..=1_000_000u64)
                        .filter(|x| x % 10 != 0)
                        .collect();
                    Not::new(SortedIdList::new(excluded), 1_000_000)
                },
                |it| {
                    while let Ok(Some(current)) = it.skip_to(it.last_doc_id() + step) {
                        criterion::black_box(current);
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }

    // ===== C benchmarks =====

    fn c_read_sparse_exclusions<M: Measurement>(&self, group: &mut BenchmarkGroup<'_, M>)
    where
        M::Value: From<Duration>,
    {
        group.bench_function("C", |b| {
            b.iter_custom(|iters| {
                let mut total_time = Duration::ZERO;
                for _ in 0..iters {
                    let result = ffi::QueryIterator::benchmark_not_read_sparse_direct(1_000_000);
                    total_time += Duration::from_nanos(result.time_ns);
                }
                total_time.into()
            });
        });
    }

    fn c_read_dense_exclusions<M: Measurement>(&self, group: &mut BenchmarkGroup<'_, M>)
    where
        M::Value: From<Duration>,
    {
        group.bench_function("C", |b| {
            b.iter_custom(|iters| {
                let mut total_time = Duration::ZERO;
                for _ in 0..iters {
                    let result = ffi::QueryIterator::benchmark_not_read_dense_direct(1_000_000);
                    total_time += Duration::from_nanos(result.time_ns);
                }
                total_time.into()
            });
        });
    }

    fn c_skip_to_sparse_exclusions<M: Measurement>(&self, group: &mut BenchmarkGroup<'_, M>)
    where
        M::Value: From<Duration>,
    {
        group.bench_function("C", |b| {
            b.iter_custom(|iters| {
                let mut total_time = Duration::ZERO;
                for _ in 0..iters {
                    let result =
                        ffi::QueryIterator::benchmark_not_skip_to_sparse_direct(1_000_000, 100);
                    total_time += Duration::from_nanos(result.time_ns);
                }
                total_time.into()
            });
        });
    }

    fn c_skip_to_dense_exclusions<M: Measurement>(&self, group: &mut BenchmarkGroup<'_, M>)
    where
        M::Value: From<Duration>,
    {
        group.bench_function("C", |b| {
            b.iter_custom(|iters| {
                let mut total_time = Duration::ZERO;
                for _ in 0..iters {
                    let result =
                        ffi::QueryIterator::benchmark_not_skip_to_dense_direct(1_000_000, 100);
                    total_time += Duration::from_nanos(result.time_ns);
                }
                total_time.into()
            });
        });
    }
}
