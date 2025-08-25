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

use criterion::{
    BenchmarkGroup, Criterion,
    measurement::{Measurement, WallTime},
};
use rqe_iterators::{RQEIterator, id_list::IdList};

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
            let data = (1..1_000_000).collect();
            let it = ffi::QueryIterator::new_id_list(data);
            b.iter(|| {
                it.read();
                if it.at_eof() {
                    it.rewind();
                }
            });
            it.free();
        });
    }
    fn c_read_sparse<M: Measurement>(&self, group: &mut BenchmarkGroup<'_, M>) {
        group.bench_function("C", |b| {
            let data = (1..1_000_000).step_by(1000).collect();
            let it = ffi::QueryIterator::new_id_list(data);
            b.iter(|| {
                it.read();
                if it.at_eof() {
                    it.rewind();
                }
            });
            it.free();
        });
    }

    fn rust_read_dense<M: Measurement>(&self, group: &mut BenchmarkGroup<'_, M>) {
        group.bench_function("Rust", |b| {
            let data = (1..1_000_000).collect();
            let mut it = IdList::new(data);
            b.iter(|| {
                let _ = it.read();
                if it.at_eof() {
                    it.rewind();
                }
            });
        });
    }
    fn rust_read_sparse<M: Measurement>(&self, group: &mut BenchmarkGroup<'_, M>) {
        group.bench_function("Rust", |b| {
            let data = (1..1_000_000).step_by(1000).collect();
            let mut it = IdList::new(data);
            b.iter(|| {
                let _ = it.read();
                if it.at_eof() {
                    it.rewind();
                }
            });
        });
    }

    fn c_skip_to_dense<M: Measurement>(&self, group: &mut BenchmarkGroup<'_, M>) {
        group.bench_function("C", |b| {
            let data = (1..1_000_000).collect();
            let it = ffi::QueryIterator::new_id_list(data);
            let step = 100;
            b.iter(|| {
                it.skip_to(it.last_doc_id() + step);
                if it.at_eof() {
                    it.rewind();
                }
            });
            it.free();
        });
    }
    fn c_skip_to_sparse<M: Measurement>(&self, group: &mut BenchmarkGroup<'_, M>) {
        group.bench_function("C", |b| {
            let data = (1..1_000_000).step_by(1000).collect();
            let it = ffi::QueryIterator::new_id_list(data);
            let step = 100;
            b.iter(|| {
                it.skip_to(it.last_doc_id() + step);
                if it.at_eof() {
                    it.rewind();
                }
            });
            it.free();
        });
    }

    fn rust_skip_to_dense<M: Measurement>(&self, group: &mut BenchmarkGroup<'_, M>) {
        group.bench_function("Rust", |b| {
            let data = (1..1_000_000).collect();
            let mut it = IdList::new(data);
            let step = 100;
            b.iter(|| {
                if matches!(it.skip_to(it.last_doc_id() + step), Ok(None)) {
                    it.rewind();
                }
            });
        });
    }
    fn rust_skip_to_sparse<M: Measurement>(&self, group: &mut BenchmarkGroup<'_, M>) {
        group.bench_function("Rust", |b| {
            let data = (1..1_000_000).step_by(1000).collect();
            let mut it = IdList::new(data);
            let step = 100;
            b.iter(|| {
                if matches!(it.skip_to(it.last_doc_id() + step), Ok(None)) {
                    it.rewind();
                }
            });
        });
    }
}
