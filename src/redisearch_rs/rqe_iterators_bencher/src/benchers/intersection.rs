/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Benchmark intersection iterator.
//!
//! Compares C and Rust implementations of the intersection iterator
//! using SortedIdList as child iterators.

use std::time::Duration;

use criterion::{
    BenchmarkGroup, Criterion,
    measurement::{Measurement, WallTime},
};
use rqe_iterators::{Intersection, RQEIterator, id_list::SortedIdList};

use crate::ffi::{self, IteratorStatus_ITERATOR_OK};

#[derive(Default)]
pub struct Bencher;

/// Number of child iterators for intersection benchmarks.
const NUM_CHILDREN: usize = 5;
/// Size of each child iterator's ID list.
const CHILD_SIZE: u64 = 100_000;
/// Weight for intersection results.
const WEIGHT: f64 = 1.0;
/// Step size for skip_to benchmarks.
const STEP: u64 = 100;

/// Generate IDs for high overlap scenario (dense intersection results).
/// Each child contains IDs 1..CHILD_SIZE, so all documents appear in all children.
fn high_overlap_ids() -> Vec<Vec<u64>> {
    (0..NUM_CHILDREN)
        .map(|_| (1..=CHILD_SIZE).collect())
        .collect()
}

/// Generate IDs for low overlap scenario (sparse intersection results).
/// Children have staggered starting points, reducing intersection size.
fn low_overlap_ids() -> Vec<Vec<u64>> {
    (0..NUM_CHILDREN)
        .map(|i| {
            let offset = (i as u64) * (CHILD_SIZE / 10);
            (1..=CHILD_SIZE).map(|x| x + offset).collect()
        })
        .collect()
}

/// Generate IDs for varying sizes scenario (realistic workload).
/// First child is smallest (drives the intersection), others are progressively larger.
fn varying_size_ids() -> Vec<Vec<u64>> {
    (0..NUM_CHILDREN)
        .map(|i| {
            let size = CHILD_SIZE * (i as u64 + 1) / NUM_CHILDREN as u64;
            (1..=size.max(1)).collect()
        })
        .collect()
}

/// Convert ID vectors to Rust SortedIdList iterators.
fn ids_to_rust_children(ids: &[Vec<u64>]) -> Vec<SortedIdList<'static>> {
    ids.iter()
        .map(|id_vec| SortedIdList::new(id_vec.clone()))
        .collect()
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
        self.read_high_overlap(c);
        self.read_low_overlap(c);
        self.read_varying_sizes(c);
        self.skip_to_high_overlap(c);
        self.skip_to_low_overlap(c);
    }

    fn read_high_overlap(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Iterator - Intersection - Read High Overlap");
        self.bench_read(&mut group, high_overlap_ids);
        group.finish();
    }

    fn read_low_overlap(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Iterator - Intersection - Read Low Overlap");
        self.bench_read(&mut group, low_overlap_ids);
        group.finish();
    }

    fn read_varying_sizes(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Iterator - Intersection - Read Varying Sizes");
        self.bench_read(&mut group, varying_size_ids);
        group.finish();
    }

    fn skip_to_high_overlap(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Iterator - Intersection - SkipTo High Overlap");
        self.bench_skip_to(&mut group, high_overlap_ids);
        group.finish();
    }

    fn skip_to_low_overlap(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Iterator - Intersection - SkipTo Low Overlap");
        self.bench_skip_to(&mut group, low_overlap_ids);
        group.finish();
    }

    fn bench_read<M, F>(&self, group: &mut BenchmarkGroup<'_, M>, make_ids: F)
    where
        M: Measurement,
        F: Fn() -> Vec<Vec<u64>>,
    {
        // C implementation benchmark
        group.bench_function("C", |b| {
            b.iter_batched_ref(
                || ffi::QueryIterator::new_intersection(&make_ids(), WEIGHT),
                |it| {
                    while it.read() == IteratorStatus_ITERATOR_OK {
                        criterion::black_box(it.current());
                    }
                    it.free();
                },
                criterion::BatchSize::SmallInput,
            );
        });

        // Rust implementation benchmark
        group.bench_function("Rust", |b| {
            b.iter_batched_ref(
                || Intersection::new(ids_to_rust_children(&make_ids())),
                |it| {
                    while let Ok(Some(current)) = it.read() {
                        criterion::black_box(current);
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
        // C implementation benchmark
        group.bench_function("C", |b| {
            b.iter_batched_ref(
                || ffi::QueryIterator::new_intersection(&make_ids(), WEIGHT),
                |it| {
                    while it.skip_to(it.last_doc_id() + STEP) == IteratorStatus_ITERATOR_OK {
                        criterion::black_box(it.current());
                    }
                    it.free();
                },
                criterion::BatchSize::SmallInput,
            );
        });

        // Rust implementation benchmark
        group.bench_function("Rust", |b| {
            b.iter_batched_ref(
                || Intersection::new(ids_to_rust_children(&make_ids())),
                |it| {
                    while let Ok(Some(current)) = it.skip_to(it.last_doc_id() + STEP) {
                        criterion::black_box(current);
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }
}
