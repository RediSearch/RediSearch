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
use rqe_iterators::{RQEIterator, empty::Empty, id_list::IdListSorted, not::NotOptimized};

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

    /// All existing documents (wildcard), 1..MAX_DOC_ID.
    fn all_docs() -> Vec<u64> {
        (1..Self::MAX_DOC_ID).collect()
    }

    /// Dense child data: 99% of docs (all except every 100th doc).
    fn dense_child() -> Vec<u64> {
        (1..Self::MAX_DOC_ID).filter(|x| x % 100 != 0).collect()
    }

    /// Sparse wildcard data: every 100th doc.
    fn sparse_wc() -> Vec<u64> {
        (1..Self::MAX_DOC_ID).step_by(100).collect()
    }

    /// Sparse child data: every 200th doc (half the sparse wildcard).
    fn sparse_child() -> Vec<u64> {
        (1..Self::MAX_DOC_ID).step_by(200).collect()
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
        let mut group = self.benchmark_group(c, "Iterator - NotOptimized - Read Empty Child");

        group.bench_function("Rust", |b| {
            b.iter(|| {
                let wc = IdListSorted::new(Self::all_docs());
                let mut it = NotOptimized::new(
                    wc,
                    Empty,
                    Self::MAX_DOC_ID,
                    Self::WEIGHT,
                    Self::NOT_ITERATOR_TIMEOUT,
                    Self::SKIP_TIMEOUT_CHECKS,
                );
                while let Ok(Some(current)) = it.read() {
                    black_box(current);
                }
            });
        });

        group.finish();
    }

    /// Benchmark NOT-optimized with dense child (few docs returned).
    fn read_dense_child(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Iterator - NotOptimized - Read Dense Child");

        group.bench_function("Rust", |b| {
            b.iter(|| {
                let wc = IdListSorted::new(Self::all_docs());
                let mut it = NotOptimized::new(
                    wc,
                    IdListSorted::new(Self::dense_child()),
                    Self::MAX_DOC_ID,
                    Self::WEIGHT,
                    Self::NOT_ITERATOR_TIMEOUT,
                    Self::SKIP_TIMEOUT_CHECKS,
                );
                while let Ok(Some(current)) = it.read() {
                    black_box(current);
                }
            });
        });

        group.finish();
    }

    /// Benchmark NOT-optimized with sparse wildcard (only 1% of docs exist).
    fn read_sparse_wc(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Iterator - NotOptimized - Read Sparse WC");

        group.bench_function("Rust", |b| {
            b.iter(|| {
                let mut it = NotOptimized::new(
                    IdListSorted::new(Self::sparse_wc()),
                    IdListSorted::new(Self::sparse_child()),
                    Self::MAX_DOC_ID,
                    Self::WEIGHT,
                    Self::NOT_ITERATOR_TIMEOUT,
                    Self::SKIP_TIMEOUT_CHECKS,
                );
                while let Ok(Some(current)) = it.read() {
                    black_box(current);
                }
            });
        });

        group.finish();
    }

    /// Benchmark NOT-optimized SkipTo with empty child.
    fn skip_to_empty_child(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Iterator - NotOptimized - SkipTo Empty Child");
        let step = 100;

        group.bench_function("Rust", |b| {
            b.iter(|| {
                let wc = IdListSorted::new(Self::all_docs());
                let mut it = NotOptimized::new(
                    wc,
                    Empty,
                    Self::MAX_DOC_ID,
                    Self::WEIGHT,
                    Self::NOT_ITERATOR_TIMEOUT,
                    Self::SKIP_TIMEOUT_CHECKS,
                );
                while let Ok(Some(current)) = it.skip_to(it.last_doc_id() + step) {
                    black_box(current);
                }
            });
        });

        group.finish();
    }

    /// Benchmark NOT-optimized SkipTo with sparse child.
    fn skip_to_sparse_child(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Iterator - NotOptimized - SkipTo Sparse Child");
        let step = 100;

        group.bench_function("Rust", |b| {
            b.iter(|| {
                let wc = IdListSorted::new(Self::all_docs());
                let mut it = NotOptimized::new(
                    wc,
                    IdListSorted::new(Self::sparse_wc()),
                    Self::MAX_DOC_ID,
                    Self::WEIGHT,
                    Self::NOT_ITERATOR_TIMEOUT,
                    Self::SKIP_TIMEOUT_CHECKS,
                );
                while let Ok(Some(current)) = it.skip_to(it.last_doc_id() + step) {
                    black_box(current);
                }
            });
        });

        group.finish();
    }

    /// Benchmark NOT-optimized SkipTo with dense child.
    fn skip_to_dense_child(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Iterator - NotOptimized - SkipTo Dense Child");
        let step = 100;

        group.bench_function("Rust", |b| {
            b.iter(|| {
                let wc = IdListSorted::new(Self::all_docs());
                let mut it = NotOptimized::new(
                    wc,
                    IdListSorted::new(Self::dense_child()),
                    Self::MAX_DOC_ID,
                    Self::WEIGHT,
                    Self::NOT_ITERATOR_TIMEOUT,
                    Self::SKIP_TIMEOUT_CHECKS,
                );
                while let Ok(Some(current)) = it.skip_to(it.last_doc_id() + step) {
                    black_box(current);
                }
            });
        });

        group.finish();
    }
}
