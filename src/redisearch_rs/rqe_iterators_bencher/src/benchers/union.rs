/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Benchmark union iterator.
//!
//! Compares C and Rust implementations of the union iterator
//! using SortedIdList as child iterators.

use std::{hint::black_box, time::Duration};

use criterion::{
    BenchmarkGroup, Criterion,
    measurement::{Measurement, WallTime},
};
use rqe_iterators::{RQEIterator, UnionFullFlat, UnionQuickFlat, id_list::IdListSorted};

use crate::ffi::{self, IteratorStatus_ITERATOR_OK};

#[derive(Default)]
pub struct Bencher;

/// Number of child iterators for union benchmarks (few children).
const NUM_CHILDREN_FEW: usize = 5;
/// Number of child iterators for union benchmarks (many children).
const NUM_CHILDREN_MANY: usize = 50;
/// Size of each child iterator's ID list.
const CHILD_SIZE: u64 = 100_000;
/// Step size for skip_to benchmarks.
const STEP: u64 = 100;

/// Generate IDs for high overlap scenario with specified number of children.
/// All children contain identical IDs (100% overlap).
fn high_overlap_ids_n(num_children: usize) -> Vec<Vec<u64>> {
    (0..num_children)
        .map(|_| (1..=CHILD_SIZE).collect())
        .collect()
}

/// Generate IDs for high overlap scenario (few children).
fn high_overlap_ids() -> Vec<Vec<u64>> {
    high_overlap_ids_n(NUM_CHILDREN_FEW)
}

/// Generate IDs for high overlap scenario (many children).
fn high_overlap_ids_many() -> Vec<Vec<u64>> {
    high_overlap_ids_n(NUM_CHILDREN_MANY)
}

/// Generate IDs for low overlap scenario with specified number of children.
/// Children have staggered starting points with minimal overlap.
fn low_overlap_ids_n(num_children: usize) -> Vec<Vec<u64>> {
    (0..num_children)
        .map(|i| {
            let offset = (i as u64) * (CHILD_SIZE / 10);
            (1..=CHILD_SIZE).map(|x| x + offset).collect()
        })
        .collect()
}

/// Generate IDs for low overlap scenario (few children).
fn low_overlap_ids() -> Vec<Vec<u64>> {
    low_overlap_ids_n(NUM_CHILDREN_FEW)
}

/// Generate IDs for low overlap scenario (many children).
fn low_overlap_ids_many() -> Vec<Vec<u64>> {
    low_overlap_ids_n(NUM_CHILDREN_MANY)
}

/// Generate IDs for disjoint scenario with specified number of children.
/// Each child has completely unique IDs (0% overlap).
fn disjoint_ids_n(num_children: usize) -> Vec<Vec<u64>> {
    (0..num_children)
        .map(|i| {
            let start = (i as u64) * CHILD_SIZE + 1;
            (start..=start + CHILD_SIZE - 1).collect()
        })
        .collect()
}

/// Generate IDs for disjoint scenario (few children).
fn disjoint_ids() -> Vec<Vec<u64>> {
    disjoint_ids_n(NUM_CHILDREN_FEW)
}

/// Generate IDs for disjoint scenario (many children).
fn disjoint_ids_many() -> Vec<Vec<u64>> {
    disjoint_ids_n(NUM_CHILDREN_MANY)
}

/// Generate IDs for varying sizes scenario (realistic workload).
/// First child is smallest, others are progressively larger.
fn varying_size_ids() -> Vec<Vec<u64>> {
    (0..NUM_CHILDREN_FEW)
        .map(|i| {
            let size = CHILD_SIZE * (i as u64 + 1) / NUM_CHILDREN_FEW as u64;
            (1..=size.max(1)).collect()
        })
        .collect()
}

/// Convert ID vectors to Rust IdListSorted iterators.
fn ids_to_rust_children(ids: Vec<Vec<u64>>) -> Vec<IdListSorted<'static>> {
    ids.into_iter().map(IdListSorted::new).collect()
}

impl Bencher {
    const MEASUREMENT_TIME: Duration = Duration::from_millis(3000);
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
        // Read benchmarks
        self.read_high_overlap_few(c);
        self.read_low_overlap_few(c);
        self.read_disjoint_few(c);
        self.read_varying_sizes(c);
        self.read_high_overlap_many(c);
        self.read_low_overlap_many(c);
        self.read_disjoint_many(c);

        // SkipTo benchmarks
        self.skip_to_high_overlap_few(c);
        self.skip_to_low_overlap_few(c);
        self.skip_to_disjoint_few(c);
        self.skip_to_high_overlap_many(c);
        self.skip_to_low_overlap_many(c);
        self.skip_to_disjoint_many(c);
    }

    // Read benchmarks - 5 children

    fn read_high_overlap_few(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Iterator - Union - Read High Overlap 5 Children");
        self.bench_read(&mut group, high_overlap_ids);
        group.finish();
    }

    fn read_low_overlap_few(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Iterator - Union - Read Low Overlap 5 Children");
        self.bench_read(&mut group, low_overlap_ids);
        group.finish();
    }

    fn read_disjoint_few(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Iterator - Union - Read Disjoint 5 Children");
        self.bench_read(&mut group, disjoint_ids);
        group.finish();
    }

    fn read_varying_sizes(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Iterator - Union - Read Varying Sizes");
        self.bench_read(&mut group, varying_size_ids);
        group.finish();
    }

    // Read benchmarks - 50 children

    fn read_high_overlap_many(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Iterator - Union - Read High Overlap 50 Children");
        self.bench_read(&mut group, high_overlap_ids_many);
        group.finish();
    }

    fn read_low_overlap_many(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Iterator - Union - Read Low Overlap 50 Children");
        self.bench_read(&mut group, low_overlap_ids_many);
        group.finish();
    }

    fn read_disjoint_many(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Iterator - Union - Read Disjoint 50 Children");
        self.bench_read(&mut group, disjoint_ids_many);
        group.finish();
    }

    // SkipTo benchmarks - 5 children

    fn skip_to_high_overlap_few(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Iterator - Union - SkipTo High Overlap 5 Children");
        self.bench_skip_to(&mut group, high_overlap_ids);
        group.finish();
    }

    fn skip_to_low_overlap_few(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Iterator - Union - SkipTo Low Overlap 5 Children");
        self.bench_skip_to(&mut group, low_overlap_ids);
        group.finish();
    }

    fn skip_to_disjoint_few(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Iterator - Union - SkipTo Disjoint 5 Children");
        self.bench_skip_to(&mut group, disjoint_ids);
        group.finish();
    }

    // SkipTo benchmarks - 50 children

    fn skip_to_high_overlap_many(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Iterator - Union - SkipTo High Overlap 50 Children");
        self.bench_skip_to(&mut group, high_overlap_ids_many);
        group.finish();
    }

    fn skip_to_low_overlap_many(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Iterator - Union - SkipTo Low Overlap 50 Children");
        self.bench_skip_to(&mut group, low_overlap_ids_many);
        group.finish();
    }

    fn skip_to_disjoint_many(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Iterator - Union - SkipTo Disjoint 50 Children");
        self.bench_skip_to(&mut group, disjoint_ids_many);
        group.finish();
    }

    /// Benchmark Union iterator read() operation.
    /// Compares C Full, C Quick, Rust Full, and Rust Quick variants.
    fn bench_read<M, F>(&self, group: &mut BenchmarkGroup<'_, M>, make_ids: F)
    where
        M: Measurement,
        F: Fn() -> Vec<Vec<u64>>,
    {
        // C Full implementation benchmark (aggregates all matching children)
        group.bench_function("C Full", |b| {
            b.iter_batched_ref(
                || ffi::QueryIterator::new_union(&make_ids(), 1.0, false, false),
                |it| {
                    while it.read() == IteratorStatus_ITERATOR_OK {
                        black_box(it.current());
                    }
                    it.free();
                },
                criterion::BatchSize::SmallInput,
            );
        });

        // C Quick implementation benchmark (returns after first match)
        group.bench_function("C Quick", |b| {
            b.iter_batched_ref(
                || ffi::QueryIterator::new_union(&make_ids(), 1.0, false, true),
                |it| {
                    while it.read() == IteratorStatus_ITERATOR_OK {
                        black_box(it.current());
                    }
                    it.free();
                },
                criterion::BatchSize::SmallInput,
            );
        });

        // Rust Full variant (aggregates all matching children)
        group.bench_function("Rust Full", |b| {
            b.iter_batched_ref(
                || UnionFullFlat::new(ids_to_rust_children(make_ids())),
                |it| {
                    while let Ok(Some(current)) = it.read() {
                        black_box(current);
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });

        // Rust Quick variant (returns after first match)
        group.bench_function("Rust Quick", |b| {
            b.iter_batched_ref(
                || UnionQuickFlat::new(ids_to_rust_children(make_ids())),
                |it| {
                    while let Ok(Some(current)) = it.read() {
                        black_box(current);
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }

    /// Benchmark Union iterator skip_to() operation.
    /// Compares C Full, C Quick, Rust Full, and Rust Quick variants.
    fn bench_skip_to<M, F>(&self, group: &mut BenchmarkGroup<'_, M>, make_ids: F)
    where
        M: Measurement,
        F: Fn() -> Vec<Vec<u64>>,
    {
        // C Full implementation benchmark (aggregates all matching children)
        group.bench_function("C Full", |b| {
            b.iter_batched_ref(
                || ffi::QueryIterator::new_union(&make_ids(), 1.0, false, false),
                |it| {
                    while it.skip_to(it.last_doc_id() + STEP) == IteratorStatus_ITERATOR_OK {
                        black_box(it.current());
                    }
                    it.free();
                },
                criterion::BatchSize::SmallInput,
            );
        });

        // C Quick implementation benchmark (returns after first match)
        group.bench_function("C Quick", |b| {
            b.iter_batched_ref(
                || ffi::QueryIterator::new_union(&make_ids(), 1.0, false, true),
                |it| {
                    while it.skip_to(it.last_doc_id() + STEP) == IteratorStatus_ITERATOR_OK {
                        black_box(it.current());
                    }
                    it.free();
                },
                criterion::BatchSize::SmallInput,
            );
        });

        // Rust Full variant (aggregates all matching children)
        group.bench_function("Rust Full", |b| {
            b.iter_batched_ref(
                || UnionFullFlat::new(ids_to_rust_children(make_ids())),
                |it| {
                    while let Ok(Some(_)) = it.skip_to(it.last_doc_id() + STEP) {
                        black_box(it.current());
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });

        // Rust Quick variant (returns after first match)
        group.bench_function("Rust Quick", |b| {
            b.iter_batched_ref(
                || UnionQuickFlat::new(ids_to_rust_children(make_ids())),
                |it| {
                    while let Ok(Some(_)) = it.skip_to(it.last_doc_id() + STEP) {
                        black_box(it.current());
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }
}
