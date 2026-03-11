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
//! using SortedIdList as child iterators. Also compares Rust Flat vs Heap variants.

use std::{hint::black_box, time::Duration};

use criterion::{
    BenchmarkGroup, Criterion,
    measurement::{Measurement, WallTime},
};
use rqe_iterators::{
    RQEIterator, UnionFullFlat, UnionFullHeap, UnionQuickFlat, UnionQuickHeap,
    id_list::IdListSorted,
};

use crate::ffi::{self, IteratorStatus_ITERATOR_OK};

#[derive(Default)]
pub struct Bencher;

/// Number of child iterators for union benchmarks (few children).
const NUM_CHILDREN_FEW: usize = 5;
/// Number of child iterators for union benchmarks (many children).
const NUM_CHILDREN_MANY: usize = 50;
/// Size of each child iterator's ID list.
const CHILD_SIZE: u64 = 100_000;

/// Generate IDs for high overlap scenario with specified number of children.
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

/// Generate IDs for disjoint scenario with specified number of children.
fn disjoint_ids_n(num_children: usize) -> Vec<Vec<u64>> {
    (0..num_children)
        .map(|i| {
            let start = (i as u64) * CHILD_SIZE + 1;
            (start..=start + CHILD_SIZE - 1).collect()
        })
        .collect()
}

/// Generate IDs for disjoint scenario (no overlap between children).
/// Each child has completely unique IDs.
fn disjoint_ids() -> Vec<Vec<u64>> {
    disjoint_ids_n(NUM_CHILDREN_FEW)
}

/// Generate IDs for disjoint scenario (many children).
fn disjoint_ids_many() -> Vec<Vec<u64>> {
    disjoint_ids_n(NUM_CHILDREN_MANY)
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
        // Flat benchmarks (O(n) min-finding, no heap overhead)
        self.bench_flat(c);
        // Heap benchmarks (O(log n) min-finding via custom DocIdMinHeap)
        self.bench_heap(c);
    }

    fn bench_flat(&self, c: &mut Criterion) {
        // Full mode - 5 children
        let mut group = self.benchmark_group(c, "Union Flat 5 Children High Overlap");
        self.bench_read_flat(&mut group, high_overlap_ids);
        group.finish();

        let mut group = self.benchmark_group(c, "Union Flat 5 Children Disjoint");
        self.bench_read_flat(&mut group, disjoint_ids);
        group.finish();

        // Full mode - 50 children
        let mut group = self.benchmark_group(c, "Union Flat 50 Children High Overlap");
        self.bench_read_flat(&mut group, high_overlap_ids_many);
        group.finish();

        let mut group = self.benchmark_group(c, "Union Flat 50 Children Disjoint");
        self.bench_read_flat(&mut group, disjoint_ids_many);
        group.finish();
    }

    fn bench_heap(&self, c: &mut Criterion) {
        // Full mode - 5 children
        let mut group = self.benchmark_group(c, "Union Heap 5 Children High Overlap");
        self.bench_read_heap(&mut group, high_overlap_ids);
        group.finish();

        let mut group = self.benchmark_group(c, "Union Heap 5 Children Disjoint");
        self.bench_read_heap(&mut group, disjoint_ids);
        group.finish();

        // Full mode - 50 children
        let mut group = self.benchmark_group(c, "Union Heap 50 Children High Overlap");
        self.bench_read_heap(&mut group, high_overlap_ids_many);
        group.finish();

        let mut group = self.benchmark_group(c, "Union Heap 50 Children Disjoint");
        self.bench_read_heap(&mut group, disjoint_ids_many);
        group.finish();
    }

    /// Benchmark Flat variant (O(n) min-finding, no heap overhead).
    /// Compares C Flat, Rust Full Flat, and Rust Quick Flat.
    fn bench_read_flat<M, F>(&self, group: &mut BenchmarkGroup<'_, M>, make_ids: F)
    where
        M: Measurement,
        F: Fn() -> Vec<Vec<u64>>,
    {
        // C Flat implementation benchmark
        group.bench_function("C Full", |b| {
            b.iter_batched_ref(
                || ffi::QueryIterator::new_union(&make_ids(), 1.0, false),
                |it| {
                    while it.read() == IteratorStatus_ITERATOR_OK {
                        black_box(it.current());
                    }
                    it.free();
                },
                criterion::BatchSize::SmallInput,
            );
        });

        // Rust Full Flat variant
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

        // Rust Quick Flat variant (returns after first match)
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

    /// Benchmark Heap variant (O(log n) min-finding via custom DocIdMinHeap).
    /// Compares C Heap, Rust Full Heap, and Rust Quick Heap.
    fn bench_read_heap<M, F>(&self, group: &mut BenchmarkGroup<'_, M>, make_ids: F)
    where
        M: Measurement,
        F: Fn() -> Vec<Vec<u64>>,
    {
        // C Heap implementation benchmark
        group.bench_function("C Full", |b| {
            b.iter_batched_ref(
                || ffi::QueryIterator::new_union(&make_ids(), 1.0, true),
                |it| {
                    while it.read() == IteratorStatus_ITERATOR_OK {
                        black_box(it.current());
                    }
                    it.free();
                },
                criterion::BatchSize::SmallInput,
            );
        });

        // Rust Full Heap variant
        group.bench_function("Rust Full", |b| {
            b.iter_batched_ref(
                || UnionFullHeap::new(ids_to_rust_children(make_ids())),
                |it| {
                    while let Ok(Some(current)) = it.read() {
                        black_box(current);
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });

        // Rust Quick Heap variant (returns after first match)
        group.bench_function("Rust Quick", |b| {
            b.iter_batched_ref(
                || UnionQuickHeap::new(ids_to_rust_children(make_ids())),
                |it| {
                    while let Ok(Some(current)) = it.read() {
                        black_box(current);
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }

}
