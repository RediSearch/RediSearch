/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Benchmark ID-list iterator.

use std::time::Duration;

use crate::ffi;
use criterion::{
    BenchmarkGroup, Criterion,
    measurement::{Measurement, WallTime},
};
use rqe_iterators::{RQEIterator, id_list::SortedIdList};

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
        self.read_dense(c);
        self.read_sparse(c);
        self.skip_to_dense(c);
        self.skip_to_sparse(c);
    }

    fn read_dense(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Iterator - IdList - Read Dense");
        self.c_read_dense(&mut group);
        self.rust_read_dense(&mut group);
        group.finish();
    }

    fn read_sparse(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Iterator - IdList - Read Sparse");
        self.c_read_sparse(&mut group);
        self.rust_read_sparse(&mut group);
        group.finish();
    }

    fn skip_to_dense(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Iterator - IdList - SkipTo Dense");
        self.c_skip_to_dense(&mut group);
        self.rust_skip_to_dense(&mut group);
        group.finish();
    }

    fn skip_to_sparse(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Iterator - IdList - SkipTo Sparse");
        self.c_skip_to_sparse(&mut group);
        self.rust_skip_to_sparse(&mut group);
        group.finish();
    }

    fn c_read_dense<M: Measurement>(&self, group: &mut BenchmarkGroup<'_, M>) {
        group.bench_function("C", |b| {
            b.iter_batched_ref(
                || {
                    let data = (1..1_000_000).collect();
                    ffi::QueryIterator::new_id_list(data)
                },
                |it| {
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
                || {
                    let data = (1..1_000_000).map(|x| x * 1000).collect();
                    ffi::QueryIterator::new_id_list(data)
                },
                |it| {
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
                || {
                    let data = (1..1_000_000).collect();
                    SortedIdList::new(data)
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
    fn rust_read_sparse<M: Measurement>(&self, group: &mut BenchmarkGroup<'_, M>) {
        group.bench_function("Rust", |b| {
            b.iter_batched_ref(
                || {
                    let data = (1..1_000_000).map(|x| x * 1000).collect();
                    SortedIdList::new(data)
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

    fn c_skip_to_dense<M: Measurement>(&self, group: &mut BenchmarkGroup<'_, M>) {
        group.bench_function("C", |b| {
            let step = 100;
            b.iter_batched_ref(
                || {
                    let data = (1..1_000_000).collect();
                    ffi::QueryIterator::new_id_list(data)
                },
                |it| {
                    while it.skip_to(it.last_doc_id() + step) != ::ffi::IteratorStatus_ITERATOR_EOF
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
            let step = 100;
            b.iter_batched_ref(
                || {
                    let data = (1..1_000_000).map(|x| x * 1000).collect();
                    ffi::QueryIterator::new_id_list(data)
                },
                |it| {
                    while it.skip_to(it.last_doc_id() + step) != ::ffi::IteratorStatus_ITERATOR_EOF
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
            let step = 100;
            b.iter_batched_ref(
                || {
                    let data = (1..1_000_000).collect();
                    SortedIdList::new(data)
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
    fn rust_skip_to_sparse<M: Measurement>(&self, group: &mut BenchmarkGroup<'_, M>) {
        group.bench_function("Rust", |b| {
            let step = 100;
            b.iter_batched_ref(
                || {
                    let data = (1..1_000_000).map(|x| x * 1000).collect();
                    SortedIdList::new(data)
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
}
