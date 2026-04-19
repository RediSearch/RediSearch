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
//! Doc IDs are generated linearly based on the number of children:
//! `total_docs = num_children * ids_per_child`.
//!
//! For each doc ID, we randomly decide which children will contain it:
//! 1. First, randomly choose how many children will have this doc (1 to num_children)
//! 2. Then, randomly select which specific children will have it
//!
//! The overlap is controlled by parameters that affect how many children
//! typically share each document:
//!
//! - **High overlap**: Each doc appears in many children (high average children per doc)
//! - **Low overlap**: Each doc appears in few children (low average children per doc)
//! - **Disjoint**: Each doc appears in exactly one child (no overlap)

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

use crate::ffi::{self, IteratorStatus_ITERATOR_OK};

#[derive(Default)]
pub struct Bencher;

/// Number of child iterators for union benchmarks (few children).
const NUM_CHILDREN_FEW: usize = 5;
/// Number of child iterators for union benchmarks (many children).
const NUM_CHILDREN_MANY: usize = 50;
/// Number of IDs per child iterator for the "few children" scenarios.
const IDS_PER_CHILD_FEW: u64 = 100_000;
/// Number of IDs per child iterator for the "many children" scenarios.
const IDS_PER_CHILD_MANY: u64 = 20_000;

/// Select the appropriate `ids_per_child` based on the number of children.
const fn ids_per_child_for(num_children: usize) -> u64 {
    if num_children <= NUM_CHILDREN_FEW {
        IDS_PER_CHILD_FEW
    } else {
        IDS_PER_CHILD_MANY
    }
}
/// Step size for skip_to benchmarks.
const STEP: u64 = 100;
/// Seed for reproducible random number generation.
const RNG_SEED: u64 = 42;

/// Parameters for generating benchmark data.
#[derive(Clone, Copy)]
struct DataGenParams {
    /// Number of child iterators.
    num_children: usize,
    /// Number of IDs to generate per child (target average).
    ids_per_child: u64,
    /// Controls how child ID ranges overlap.
    overlap: Overlap,
    /// Seed for random number generation.
    seed: u64,
}

/// Controls how child iterator ID ranges overlap.
#[derive(Clone, Copy)]
enum Overlap {
    /// High overlap: each doc appears in many children.
    /// For each doc, randomly choose 50-100% of children to include it.
    High,
    /// Low overlap: each doc appears in few children.
    /// For each doc, randomly choose 1-2 children to include it.
    Low,
    /// Disjoint: each doc appears in exactly one randomly chosen child.
    Disjoint,
}

impl DataGenParams {
    /// Create params for high overlap scenario.
    const fn high_overlap(num_children: usize) -> Self {
        Self {
            num_children,
            ids_per_child: ids_per_child_for(num_children),
            overlap: Overlap::High,
            seed: RNG_SEED,
        }
    }

    /// Create params for low overlap scenario.
    const fn low_overlap(num_children: usize) -> Self {
        Self {
            num_children,
            ids_per_child: ids_per_child_for(num_children),
            overlap: Overlap::Low,
            seed: RNG_SEED,
        }
    }

    /// Create params for disjoint scenario.
    const fn disjoint(num_children: usize) -> Self {
        Self {
            num_children,
            ids_per_child: ids_per_child_for(num_children),
            overlap: Overlap::Disjoint,
            seed: RNG_SEED,
        }
    }

    /// Create params for varying sizes scenario.
    const fn varying_sizes() -> Self {
        Self {
            num_children: NUM_CHILDREN_FEW,
            ids_per_child: ids_per_child_for(NUM_CHILDREN_FEW),
            overlap: Overlap::High,
            seed: RNG_SEED,
        }
    }
}

/// Generate IDs for benchmark children based on parameters.
///
/// The doc ID range is linear: `[1, num_children * ids_per_child]`.
/// For each doc ID, we randomly decide which children will contain it
/// based on the overlap strategy.
fn generate_ids(params: DataGenParams) -> Vec<Vec<u64>> {
    let mut rng = StdRng::seed_from_u64(params.seed);
    let num_children = params.num_children;
    let total_docs = (num_children as u64) * params.ids_per_child;

    // Initialize empty ID lists for each child
    let mut children_ids: Vec<Vec<u64>> = vec![Vec::new(); num_children];

    for doc_id in 1..=total_docs {
        // Decide which children will have this doc based on overlap strategy
        let selected_children: Vec<usize> = match params.overlap {
            Overlap::High => {
                // High overlap: randomly choose up to 75% of children
                let max_children = (num_children * 3 / 4).max(1);
                let min_children = 2.min(max_children);
                let num_selected = rng.random_range(min_children..=max_children);
                random_sample(&mut rng, num_children, num_selected)
            }
            Overlap::Low => {
                // Low overlap: randomly choose 1-2 children
                let num_selected = rng.random_range(1..=2.min(num_children));
                random_sample(&mut rng, num_children, num_selected)
            }
            Overlap::Disjoint => {
                // Each doc goes to exactly one randomly chosen child
                let child_idx = rng.random_range(0..num_children);
                vec![child_idx]
            }
        };

        // Add this doc to the selected children
        for child_idx in selected_children {
            children_ids[child_idx].push(doc_id);
        }
    }

    children_ids
}

/// Randomly sample `count` unique indices from `[0, total)`.
fn random_sample(rng: &mut StdRng, total: usize, count: usize) -> Vec<usize> {
    use rand::seq::SliceRandom;

    let mut indices: Vec<usize> = (0..total).collect();
    indices.shuffle(rng);
    indices.truncate(count);
    indices
}

/// Generate IDs for varying sizes scenario.
/// First child is smallest, others are progressively larger.
/// Uses doc-centric generation with weighted probability for each child.
fn generate_varying_size_ids(params: DataGenParams) -> Vec<Vec<u64>> {
    let mut rng = StdRng::seed_from_u64(params.seed);
    let num_children = params.num_children;
    let total_docs = (num_children as u64) * params.ids_per_child;

    // Initialize empty ID lists for each child
    let mut children_ids: Vec<Vec<u64>> = vec![Vec::new(); num_children];

    // Calculate weights: child i has weight (i+1)/num_children
    // This makes later children more likely to contain each doc
    for doc_id in 1..=total_docs {
        // High overlap: randomly choose 50-100% of children, but weighted
        let min_children = (num_children / 2).max(1);
        let num_selected = rng.random_range(min_children..=num_children);

        // Select children with probability proportional to their index
        for child_idx in 0..num_children {
            // Probability increases with child index
            let prob = (child_idx + 1) as f64 / num_children as f64;
            if rng.random_bool(prob) || children_ids[child_idx].len() < num_selected {
                if children_ids
                    .iter()
                    .filter(|c| c.last() == Some(&doc_id))
                    .count()
                    < num_selected
                {
                    children_ids[child_idx].push(doc_id);
                }
            }
        }

        // Ensure at least one child has this doc
        if children_ids.iter().all(|c| c.last() != Some(&doc_id)) {
            // Pick a random child weighted towards higher indices
            let idx = rng.random_range(0..num_children);
            children_ids[idx].push(doc_id);
        }
    }

    children_ids
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

fn disjoint_ids() -> Vec<Vec<u64>> {
    generate_ids(DataGenParams::disjoint(NUM_CHILDREN_FEW))
}

fn disjoint_ids_many() -> Vec<Vec<u64>> {
    generate_ids(DataGenParams::disjoint(NUM_CHILDREN_MANY))
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

    fn skip_to_disjoint_few(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Iterator - Union - SkipTo Disjoint 5 Children");
        self.bench_skip_to(&mut group, disjoint_ids);
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

    fn skip_to_disjoint_many(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Iterator - Union - SkipTo Disjoint 50 Children");
        self.bench_skip_to(&mut group, disjoint_ids_many);
        group.finish();
    }

    /// Benchmark Union iterator read() operation.
    /// Compares C Flat, C Heap, and Rust Flat/Heap variants (both Full and Quick modes).
    fn bench_read<M, F>(&self, group: &mut BenchmarkGroup<'_, M>, make_ids: F)
    where
        M: Measurement,
        F: Fn() -> Vec<Vec<u64>>,
    {
        // C Flat Full implementation benchmark (aggregates all matching children)
        group.bench_function("Flat Full/C", |b| {
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

        // C Flat Quick implementation benchmark (returns after first match)
        group.bench_function("Flat Quick/C", |b| {
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

        // C Heap Full implementation benchmark (aggregates all matching children)
        group.bench_function("Heap Full/C", |b| {
            b.iter_batched_ref(
                || ffi::QueryIterator::new_union(&make_ids(), 1.0, true, false),
                |it| {
                    while it.read() == IteratorStatus_ITERATOR_OK {
                        black_box(it.current());
                    }
                    it.free();
                },
                criterion::BatchSize::SmallInput,
            );
        });

        // C Heap Quick implementation benchmark (returns after first match)
        group.bench_function("Heap Quick/C", |b| {
            b.iter_batched_ref(
                || ffi::QueryIterator::new_union(&make_ids(), 1.0, true, true),
                |it| {
                    while it.read() == IteratorStatus_ITERATOR_OK {
                        black_box(it.current());
                    }
                    it.free();
                },
                criterion::BatchSize::SmallInput,
            );
        });

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
        // C Flat Full implementation benchmark (aggregates all matching children)
        group.bench_function("Flat Full/C", |b| {
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

        // C Flat Quick implementation benchmark (returns after first match)
        group.bench_function("Flat Quick/C", |b| {
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

        // C Heap Full implementation benchmark (aggregates all matching children)
        group.bench_function("Heap Full/C", |b| {
            b.iter_batched_ref(
                || ffi::QueryIterator::new_union(&make_ids(), 1.0, true, false),
                |it| {
                    while it.skip_to(it.last_doc_id() + STEP) == IteratorStatus_ITERATOR_OK {
                        black_box(it.current());
                    }
                    it.free();
                },
                criterion::BatchSize::SmallInput,
            );
        });

        // C Heap Quick implementation benchmark (returns after first match)
        group.bench_function("Heap Quick/C", |b| {
            b.iter_batched_ref(
                || ffi::QueryIterator::new_union(&make_ids(), 1.0, true, true),
                |it| {
                    while it.skip_to(it.last_doc_id() + STEP) == IteratorStatus_ITERATOR_OK {
                        black_box(it.current());
                    }
                    it.free();
                },
                criterion::BatchSize::SmallInput,
            );
        });

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
