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
use rqe_iterators::{RQEIterator, UnionFullFlat, UnionFullHeap, id_list::IdListSorted};

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

/// Generate IDs for low overlap scenario (sparse overlap between children).
/// Children have staggered starting points with minimal overlap.
fn low_overlap_ids() -> Vec<Vec<u64>> {
    (0..NUM_CHILDREN_FEW)
        .map(|i| {
            let offset = (i as u64) * (CHILD_SIZE / 2);
            (1..=CHILD_SIZE).map(|x| x + offset).collect()
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
        // Few children (5) - Flat should be faster
        self.read_high_overlap(c);
        self.read_disjoint(c);
        // Many children (50) - Heap should be faster
        self.read_high_overlap_many(c);
        self.read_disjoint_many(c);
    }

    fn read_high_overlap(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Union Read 5 Children High Overlap");
        self.bench_read(&mut group, high_overlap_ids);
        group.finish();
    }

    fn read_disjoint(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Union Read 5 Children Disjoint");
        self.bench_read(&mut group, disjoint_ids);
        group.finish();
    }

    fn read_high_overlap_many(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Union Read 50 Children High Overlap");
        self.bench_read(&mut group, high_overlap_ids_many);
        group.finish();
    }

    fn read_disjoint_many(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Union Read 50 Children Disjoint");
        self.bench_read(&mut group, disjoint_ids_many);
        group.finish();
    }

    fn bench_read<M, F>(&self, group: &mut BenchmarkGroup<'_, M>, make_ids: F)
    where
        M: Measurement,
        F: Fn() -> Vec<Vec<u64>>,
    {
        // C Flat implementation benchmark
        group.bench_function("C Flat", |b| {
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

        // C Heap implementation benchmark
        group.bench_function("C Heap", |b| {
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

        // Rust Flat variant (O(n) min-finding, no heap overhead)
        group.bench_function("Rust Flat", |b| {
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

        // Rust Heap variant (O(log n) min-finding via custom DocIdMinHeap)
        group.bench_function("Rust Heap", |b| {
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
    }

    fn bench_skip_to<M, F>(&self, group: &mut BenchmarkGroup<'_, M>, make_ids: F)
    where
        M: Measurement,
        F: Fn() -> Vec<Vec<u64>>,
    {
        // C Flat implementation benchmark
        group.bench_function("C Flat", |b| {
            b.iter_batched_ref(
                || ffi::QueryIterator::new_union(&make_ids(), 1.0, false),
                |it| {
                    while it.skip_to(it.last_doc_id() + STEP) == IteratorStatus_ITERATOR_OK {
                        black_box(it.current());
                    }
                    it.free();
                },
                criterion::BatchSize::SmallInput,
            );
        });

        // C Heap implementation benchmark
        group.bench_function("C Heap", |b| {
            b.iter_batched_ref(
                || ffi::QueryIterator::new_union(&make_ids(), 1.0, true),
                |it| {
                    while it.skip_to(it.last_doc_id() + STEP) == IteratorStatus_ITERATOR_OK {
                        black_box(it.current());
                    }
                    it.free();
                },
                criterion::BatchSize::SmallInput,
            );
        });

        // Rust Flat variant
        group.bench_function("Rust Flat", |b| {
            b.iter_batched_ref(
                || UnionFullFlat::new(ids_to_rust_children(make_ids())),
                |it| {
                    while let Ok(Some(current)) = it.skip_to(it.last_doc_id() + STEP) {
                        black_box(current);
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });

        // Rust Heap variant
        group.bench_function("Rust Heap", |b| {
            b.iter_batched_ref(
                || UnionFullHeap::new(ids_to_rust_children(make_ids())),
                |it| {
                    while let Ok(Some(current)) = it.skip_to(it.last_doc_id() + STEP) {
                        black_box(current);
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }
}
