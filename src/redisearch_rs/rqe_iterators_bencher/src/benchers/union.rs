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
//!
//! ## ID Generation
//!
//! IDs are generated randomly within configurable ranges to simulate realistic
//! workloads. The overlap between children is controlled by adjusting the ID
//! range each child samples from:
//!
//! - **High overlap**: All children sample from the same range, creating natural
//!   overlap through random collision.
//! - **Low overlap**: Children sample from staggered ranges with partial overlap.
//! - **Disjoint Sequential**: Each child samples from a completely separate range.

use std::{hint::black_box, time::Duration};

use criterion::{
    BenchmarkGroup, Criterion,
    measurement::{Measurement, WallTime},
};
use rand::{Rng, SeedableRng, rngs::StdRng};
use rqe_iterators::{
    RQEIterator, UnionFullFlat, UnionFullHeap, UnionQuickFlat, UnionQuickHeap,
    id_list::IdListSorted,
};

#[derive(Default)]
pub struct Bencher;

/// Number of child iterators for union benchmarks (few children).
const NUM_CHILDREN_FEW: usize = 5;
/// Number of child iterators for union benchmarks (many children).
const NUM_CHILDREN_MANY: usize = 50;
/// Number of IDs per child iterator.
const IDS_PER_CHILD: u64 = 100_000;
/// Step size for skip_to benchmarks.
const STEP: u64 = 100;
/// Seed for reproducible random number generation.
const RNG_SEED: u64 = 42;

/// Parameters for generating benchmark data.
#[derive(Clone, Copy)]
struct DataGenParams {
    /// Number of child iterators.
    num_children: usize,
    /// Number of IDs to generate per child.
    ids_per_child: u64,
    /// Maximum document ID in the range (controls density).
    /// Lower values = denser data, higher values = sparser data.
    id_range_max: u64,
    /// Controls how child ID ranges overlap.
    overlap: Overlap,
    /// Seed for random number generation.
    seed: u64,
}

/// Controls how child iterator ID ranges overlap.
#[derive(Clone, Copy)]
enum Overlap {
    /// All children sample from the same range `[1, id_range_max]`.
    /// Creates high overlap through random collision.
    High,
    /// Each child samples from a staggered range with partial overlap.
    /// Child `i` samples from `[i * stride, i * stride + id_range_max]`
    /// where `stride = id_range_max / (2 * num_children)`.
    Low,
    /// Each child samples from a completely separate, sequential range.
    /// Child `i` samples from `[i * id_range_max + 1, (i + 1) * id_range_max]`.
    DisjointSequential,
    /// Children have disjoint doc IDs but interleaved across the ID space.
    /// Child `i` gets IDs `i+1, num_children+i+1, 2*num_children+i+1, ...`
    /// This forces the heap root to change on every read.
    DisjointInterleaved,
}

impl DataGenParams {
    /// Create params for high overlap scenario.
    const fn high_overlap(num_children: usize) -> Self {
        Self {
            num_children,
            ids_per_child: IDS_PER_CHILD,
            // Sampling 100K IDs from 200K range creates ~40% overlap per pair
            id_range_max: IDS_PER_CHILD * 2,
            overlap: Overlap::High,
            seed: RNG_SEED,
        }
    }

    /// Create params for low overlap scenario.
    const fn low_overlap(num_children: usize) -> Self {
        Self {
            num_children,
            ids_per_child: IDS_PER_CHILD,
            // Larger range with staggered windows = less overlap
            id_range_max: IDS_PER_CHILD * 2,
            overlap: Overlap::Low,
            seed: RNG_SEED,
        }
    }

    /// Create params for disjoint sequential scenario.
    const fn disjoint_sequential(num_children: usize) -> Self {
        Self {
            num_children,
            ids_per_child: IDS_PER_CHILD,
            // Each child gets its own range of this size
            id_range_max: IDS_PER_CHILD * 2,
            overlap: Overlap::DisjointSequential,
            seed: RNG_SEED,
        }
    }

    /// Create params for disjoint interleaved scenario.
    const fn disjoint_interleaved(num_children: usize) -> Self {
        Self {
            num_children,
            ids_per_child: IDS_PER_CHILD,
            id_range_max: IDS_PER_CHILD * 2,
            overlap: Overlap::DisjointInterleaved,
            seed: RNG_SEED,
        }
    }

    /// Create params for varying sizes scenario.
    const fn varying_sizes() -> Self {
        Self {
            num_children: NUM_CHILDREN_FEW,
            ids_per_child: IDS_PER_CHILD,
            id_range_max: IDS_PER_CHILD * 2,
            overlap: Overlap::High,
            seed: RNG_SEED,
        }
    }
}

/// Generate random IDs for benchmark children based on parameters.
///
/// IDs are generated randomly, sorted, and deduplicated (matching C++ MockIterator behavior).
fn generate_ids(params: DataGenParams) -> Vec<Vec<u64>> {
    let mut rng = StdRng::seed_from_u64(params.seed);

    (0..params.num_children)
        .map(|child_idx| {
            // Determine the ID range for this child based on overlap strategy
            let (range_start, range_end) = match params.overlap {
                Overlap::High => {
                    // All children sample from the same range
                    (1, params.id_range_max)
                }
                Overlap::Low => {
                    // Staggered ranges with partial overlap
                    let stride = params.id_range_max / (2 * params.num_children as u64).max(1);
                    let start = (child_idx as u64) * stride + 1;
                    let end = start + params.id_range_max - 1;
                    (start, end)
                }
                Overlap::DisjointSequential => {
                    // Completely separate sequential ranges for each child
                    let start = (child_idx as u64) * params.id_range_max + 1;
                    let end = start + params.id_range_max - 1;
                    (start, end)
                }
                Overlap::DisjointInterleaved => {
                    // Handled separately below — use dummy range
                    (0, 0)
                }
            };

            if matches!(params.overlap, Overlap::DisjointInterleaved) {
                // Deterministic interleaved IDs: child i gets IDs i+1, n+i+1, 2n+i+1, ...
                // where n = num_children. Each child gets exactly ids_per_child IDs.
                let n = params.num_children as u64;
                let ids: Vec<u64> = (0..params.ids_per_child)
                    .map(|k| k * n + (child_idx as u64) + 1)
                    .collect();
                return ids;
            }

            // Generate random IDs within the range
            let mut ids: Vec<u64> = (0..params.ids_per_child)
                .map(|_| rng.random_range(range_start..=range_end))
                .collect();

            // Sort and deduplicate (matches C++ MockIterator::Init behavior)
            ids.sort_unstable();
            ids.dedup();

            ids
        })
        .collect()
}

/// Generate IDs for varying sizes scenario.
/// First child is smallest, others are progressively larger.
fn generate_varying_size_ids(params: DataGenParams) -> Vec<Vec<u64>> {
    let mut rng = StdRng::seed_from_u64(params.seed);

    (0..params.num_children)
        .map(|child_idx| {
            // Progressive sizes: child 0 gets 1/N of ids_per_child, child N-1 gets full
            let size = params.ids_per_child * (child_idx as u64 + 1) / params.num_children as u64;
            let size = size.max(1);

            // All children sample from the same range for high overlap
            let mut ids: Vec<u64> = (0..size)
                .map(|_| rng.random_range(1..=params.id_range_max))
                .collect();

            ids.sort_unstable();
            ids.dedup();

            ids
        })
        .collect()
}

// Convenience functions for benchmark scenarios

fn high_overlap_ids() -> Vec<Vec<u64>> {
    generate_ids(DataGenParams::high_overlap(NUM_CHILDREN_FEW))
}

fn high_overlap_ids_many() -> Vec<Vec<u64>> {
    generate_ids(DataGenParams::high_overlap(NUM_CHILDREN_MANY))
}

fn low_overlap_ids() -> Vec<Vec<u64>> {
    generate_ids(DataGenParams::low_overlap(NUM_CHILDREN_FEW))
}

fn low_overlap_ids_many() -> Vec<Vec<u64>> {
    generate_ids(DataGenParams::low_overlap(NUM_CHILDREN_MANY))
}

fn disjoint_sequential_ids() -> Vec<Vec<u64>> {
    generate_ids(DataGenParams::disjoint_sequential(NUM_CHILDREN_FEW))
}

fn disjoint_sequential_ids_many() -> Vec<Vec<u64>> {
    generate_ids(DataGenParams::disjoint_sequential(NUM_CHILDREN_MANY))
}

fn disjoint_interleaved_ids() -> Vec<Vec<u64>> {
    generate_ids(DataGenParams::disjoint_interleaved(NUM_CHILDREN_FEW))
}

fn disjoint_interleaved_ids_many() -> Vec<Vec<u64>> {
    generate_ids(DataGenParams::disjoint_interleaved(NUM_CHILDREN_MANY))
}

fn varying_size_ids() -> Vec<Vec<u64>> {
    generate_varying_size_ids(DataGenParams::varying_sizes())
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
        self.read_disjoint_sequential_few(c);
        self.read_varying_sizes(c);
        self.read_high_overlap_many(c);
        self.read_low_overlap_many(c);
        self.read_disjoint_sequential_many(c);
        self.read_disjoint_interleaved_few(c);
        self.read_disjoint_interleaved_many(c);

        // SkipTo benchmarks
        self.skip_to_high_overlap_few(c);
        self.skip_to_low_overlap_few(c);
        self.skip_to_disjoint_sequential_few(c);
        self.skip_to_high_overlap_many(c);
        self.skip_to_low_overlap_many(c);
        self.skip_to_disjoint_sequential_many(c);
        self.skip_to_disjoint_interleaved_few(c);
        self.skip_to_disjoint_interleaved_many(c);
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

    fn read_disjoint_sequential_few(&self, c: &mut Criterion) {
        let mut group =
            self.benchmark_group(c, "Iterator - Union - Read Disjoint Sequential 5 Children");
        self.bench_read(&mut group, disjoint_sequential_ids);
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

    fn read_disjoint_sequential_many(&self, c: &mut Criterion) {
        let mut group =
            self.benchmark_group(c, "Iterator - Union - Read Disjoint Sequential 50 Children");
        self.bench_read(&mut group, disjoint_sequential_ids_many);
        group.finish();
    }

    fn read_disjoint_interleaved_few(&self, c: &mut Criterion) {
        let mut group =
            self.benchmark_group(c, "Iterator - Union - Read Disjoint Interleaved 5 Children");
        self.bench_read(&mut group, disjoint_interleaved_ids);
        group.finish();
    }

    fn read_disjoint_interleaved_many(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(
            c,
            "Iterator - Union - Read Disjoint Interleaved 50 Children",
        );
        self.bench_read(&mut group, disjoint_interleaved_ids_many);
        group.finish();
    }

    // SkipTo benchmarks - 5 children

    fn skip_to_high_overlap_few(&self, c: &mut Criterion) {
        let mut group =
            self.benchmark_group(c, "Iterator - Union - SkipTo High Overlap 5 Children");
        self.bench_skip_to(&mut group, high_overlap_ids);
        group.finish();
    }

    fn skip_to_low_overlap_few(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Iterator - Union - SkipTo Low Overlap 5 Children");
        self.bench_skip_to(&mut group, low_overlap_ids);
        group.finish();
    }

    fn skip_to_disjoint_sequential_few(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(
            c,
            "Iterator - Union - SkipTo Disjoint Sequential 5 Children",
        );
        self.bench_skip_to(&mut group, disjoint_sequential_ids);
        group.finish();
    }

    // SkipTo benchmarks - 50 children

    fn skip_to_high_overlap_many(&self, c: &mut Criterion) {
        let mut group =
            self.benchmark_group(c, "Iterator - Union - SkipTo High Overlap 50 Children");
        self.bench_skip_to(&mut group, high_overlap_ids_many);
        group.finish();
    }

    fn skip_to_low_overlap_many(&self, c: &mut Criterion) {
        let mut group =
            self.benchmark_group(c, "Iterator - Union - SkipTo Low Overlap 50 Children");
        self.bench_skip_to(&mut group, low_overlap_ids_many);
        group.finish();
    }

    fn skip_to_disjoint_sequential_many(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(
            c,
            "Iterator - Union - SkipTo Disjoint Sequential 50 Children",
        );
        self.bench_skip_to(&mut group, disjoint_sequential_ids_many);
        group.finish();
    }

    fn skip_to_disjoint_interleaved_few(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(
            c,
            "Iterator - Union - SkipTo Disjoint Interleaved 5 Children",
        );
        self.bench_skip_to(&mut group, disjoint_interleaved_ids);
        group.finish();
    }

    fn skip_to_disjoint_interleaved_many(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(
            c,
            "Iterator - Union - SkipTo Disjoint Interleaved 50 Children",
        );
        self.bench_skip_to(&mut group, disjoint_interleaved_ids_many);
        group.finish();
    }

    /// Benchmark Union iterator read() operation.
    /// Compares C Flat, C Heap, and Rust Flat/Heap variants (both Full and Quick modes).
    fn bench_read<M, F>(&self, group: &mut BenchmarkGroup<'_, M>, make_ids: F)
    where
        M: Measurement,
        F: Fn() -> Vec<Vec<u64>>,
    {
        // Rust Flat Full variant (aggregates all matching children)
        group.bench_function("Flat Full/Rust", |b| {
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

        // Rust Flat Quick variant (returns after first match)
        group.bench_function("Flat Quick/Rust", |b| {
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

        // Rust Heap Full variant (aggregates all matching children)
        group.bench_function("Heap Full/Rust", |b| {
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

        // Rust Heap Quick variant (returns after first match)
        group.bench_function("Heap Quick/Rust", |b| {
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

    /// Benchmark Union iterator skip_to() operation.
    /// Compares C Flat, C Heap, and Rust Flat/Heap variants (both Full and Quick modes).
    fn bench_skip_to<M, F>(&self, group: &mut BenchmarkGroup<'_, M>, make_ids: F)
    where
        M: Measurement,
        F: Fn() -> Vec<Vec<u64>>,
    {
        // Rust Flat Full variant (aggregates all matching children)
        group.bench_function("Flat Full/Rust", |b| {
            b.iter_batched_ref(
                || UnionFullFlat::new(ids_to_rust_children(make_ids())),
                |it| {
                    while let Ok(Some(outcome)) = it.skip_to(it.last_doc_id() + STEP) {
                        black_box(outcome);
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });

        // Rust Flat Quick variant (returns after first match)
        group.bench_function("Flat Quick/Rust", |b| {
            b.iter_batched_ref(
                || UnionQuickFlat::new(ids_to_rust_children(make_ids())),
                |it| {
                    while let Ok(Some(outcome)) = it.skip_to(it.last_doc_id() + STEP) {
                        black_box(outcome);
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });

        // Rust Heap Full variant (aggregates all matching children)
        group.bench_function("Heap Full/Rust", |b| {
            b.iter_batched_ref(
                || UnionFullHeap::new(ids_to_rust_children(make_ids())),
                |it| {
                    while let Ok(Some(outcome)) = it.skip_to(it.last_doc_id() + STEP) {
                        black_box(outcome);
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });

        // Rust Heap Quick variant (returns after first match)
        group.bench_function("Heap Quick/Rust", |b| {
            b.iter_batched_ref(
                || UnionQuickHeap::new(ids_to_rust_children(make_ids())),
                |it| {
                    while let Ok(Some(outcome)) = it.skip_to(it.last_doc_id() + STEP) {
                        black_box(outcome);
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }
}
