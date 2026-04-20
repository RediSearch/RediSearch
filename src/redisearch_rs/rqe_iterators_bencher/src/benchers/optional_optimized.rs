/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Benchmark OptionalOptimized iterator.
//!
//! Two groups of benchmarks:
//!
//! 1. `Read`/`SkipTo` across child doc ratios (0–90%), with all 1M docs in `existingDocs`.
//!    Used to profile `OptionalOptimized` in isolation.
//!
//! 2. Sparse doc space comparison: only 1-in-100 doc IDs exist (10K out of 1M).
//!    `Optional` and `OptionalOptimized` are benchmarked side-by-side with identical child data,
//!    to show where the optimized variant wins: it skips the 990K gaps that `Optional` must
//!    traverse one by one.

use std::{hint::black_box, time::Duration};

use criterion::{BenchmarkGroup, Criterion, measurement::WallTime};
use rand::{Rng as _, SeedableRng as _, rngs::StdRng};
use rqe_iterators::{
    IdList, RQEIterator, SkipToOutcome, optional::Optional, optional_optimized::OptionalOptimized,
    wildcard::new_wildcard_iterator_optimized,
};
use rqe_iterators_test_utils::TestContext;

pub struct Bencher {
    /// All 1M doc IDs exist — used for the ratio-based benchmarks.
    context: TestContext,
    /// Only 1-in-100 doc IDs exist (10K docs) — used for the sparse comparison.
    context_sparse: TestContext,
}

impl Default for Bencher {
    fn default() -> Self {
        let context = TestContext::wildcard(1..=Self::MAX_DOC_ID);
        let context_sparse =
            TestContext::wildcard((1..=Self::MAX_DOC_ID).step_by(Self::SPARSE_STEP));
        // SAFETY: no iterators have been created from these contexts yet.
        unsafe {
            context.set_index_all(true);
            context_sparse.set_index_all(true);
        }
        Self {
            context,
            context_sparse,
        }
    }
}

impl Bencher {
    const MEASUREMENT_TIME: Duration = Duration::from_millis(1000);
    const WARMUP_TIME: Duration = Duration::from_millis(200);

    const MAX_DOC_ID: u64 = 1_000_000;
    const STEP: u64 = 10;
    const WEIGHT: f64 = 1.0;
    /// Child doc ratios (%), from 0% to 90% in steps of 10.
    const CHILD_RATIOS: [u64; 10] = [0, 10, 20, 30, 40, 50, 60, 70, 80, 90];
    /// 1-in-100 docs exist in the sparse scenario (10K out of 1M).
    const SPARSE_STEP: usize = 100;
    /// Fraction of existing sparse docs included in the child for the comparison benchmarks.
    const SPARSE_CHILD_RATIO: f64 = 0.5;

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
        self.read(c);
        self.skip_to(c);
        self.sparse_comparison_read(c);
        self.sparse_comparison_skip_to(c);
    }

    fn read(&self, c: &mut Criterion) {
        let context = &self.context;
        let mut group = self.benchmark_group(c, "Iterator - OptionalOptimized - Read");

        for &ratio in &Self::CHILD_RATIOS {
            let child_ratio_f = ratio as f64 / 100.0;
            let child_ids = Self::make_child_doc_ids(child_ratio_f);

            group.bench_function(format!("Rust child_ratio={ratio}"), |b| {
                b.iter_batched_ref(
                    || {
                        // SAFETY: context has index_all=true and existingDocs wired by TestContext::wildcard.
                        let wcii = unsafe { new_wildcard_iterator_optimized(context.sctx, 0.) };
                        let child = IdList::<'_, true>::new(child_ids.clone());
                        OptionalOptimized::new(wcii, child, Self::MAX_DOC_ID, Self::WEIGHT)
                    },
                    |it| {
                        while let Ok(Some(cur)) = it.read() {
                            black_box(cur.doc_id);
                            black_box(cur.weight);
                            black_box(cur.freq);
                        }
                    },
                    criterion::BatchSize::SmallInput,
                );
            });
        }

        group.finish();
    }

    fn skip_to(&self, c: &mut Criterion) {
        let context = &self.context;
        let mut group = self.benchmark_group(c, "Iterator - OptionalOptimized - SkipTo");

        for &ratio in &Self::CHILD_RATIOS {
            let child_ratio_f = ratio as f64 / 100.0;
            let child_ids = Self::make_child_doc_ids(child_ratio_f);

            group.bench_function(format!("Rust child_ratio={ratio}"), |b| {
                b.iter_batched_ref(
                    || {
                        // SAFETY: context has index_all=true and existingDocs wired by TestContext::wildcard.
                        let wcii = unsafe { new_wildcard_iterator_optimized(context.sctx, 0.) };
                        let child = IdList::<'_, true>::new(child_ids.clone());
                        OptionalOptimized::new(wcii, child, Self::MAX_DOC_ID, Self::WEIGHT)
                    },
                    |it| {
                        while let Ok(Some(outcome)) = it.skip_to(it.last_doc_id() + Self::STEP) {
                            match outcome {
                                SkipToOutcome::Found(r) | SkipToOutcome::NotFound(r) => {
                                    black_box(r.doc_id);
                                    black_box(r.weight);
                                    black_box(r.freq);
                                }
                            }
                        }
                    },
                    criterion::BatchSize::SmallInput,
                );
            });
        }

        group.finish();
    }

    /// Side-by-side Read comparison in a sparse doc space (1-in-100 docs exist).
    ///
    /// Both variants use an identical child `IdList` (50% of existing docs).
    /// `Optional` iterates all 1M doc IDs; `OptionalOptimized` iterates only the 10K existing ones.
    fn sparse_comparison_read(&self, c: &mut Criterion) {
        let context_sparse = &self.context_sparse;
        let child_ids = Self::make_sparse_child_doc_ids();
        let mut group =
            self.benchmark_group(c, "Iterator - Optional vs OptionalOptimized - Sparse Read");

        group.bench_function("Optional", |b| {
            b.iter_batched(
                || {
                    let child = IdList::<'_, true>::new(child_ids.clone());
                    Optional::new(Self::MAX_DOC_ID, Self::WEIGHT, child)
                },
                |mut it| {
                    while let Ok(Some(cur)) = it.read() {
                        black_box(cur.doc_id);
                        black_box(cur.weight);
                        black_box(cur.freq);
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });

        group.bench_function("OptionalOptimized", |b| {
            b.iter_batched_ref(
                || {
                    // SAFETY: context_sparse has index_all=true and existingDocs wired by TestContext::wildcard.
                    let wcii = unsafe { new_wildcard_iterator_optimized(context_sparse.sctx, 0.) };
                    let child = IdList::<'_, true>::new(child_ids.clone());
                    OptionalOptimized::new(wcii, child, Self::MAX_DOC_ID, Self::WEIGHT)
                },
                |it| {
                    while let Ok(Some(cur)) = it.read() {
                        black_box(cur.doc_id);
                        black_box(cur.weight);
                        black_box(cur.freq);
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });

        group.finish();
    }

    /// Side-by-side SkipTo comparison in a sparse doc space (1-in-100 docs exist).
    ///
    /// Both variants use an identical child `IdList` (50% of existing docs).
    /// `Optional` iterates all 1M doc IDs; `OptionalOptimized` iterates only the 10K existing ones.
    fn sparse_comparison_skip_to(&self, c: &mut Criterion) {
        let context_sparse = &self.context_sparse;
        let child_ids = Self::make_sparse_child_doc_ids();
        let mut group = self.benchmark_group(
            c,
            "Iterator - Optional vs OptionalOptimized - Sparse SkipTo",
        );

        group.bench_function("Optional", |b| {
            b.iter_batched(
                || {
                    let child = IdList::<'_, true>::new(child_ids.clone());
                    Optional::new(Self::MAX_DOC_ID, Self::WEIGHT, child)
                },
                |mut it| {
                    while let Ok(Some(outcome)) = it.skip_to(it.last_doc_id() + Self::STEP) {
                        match outcome {
                            SkipToOutcome::Found(r) | SkipToOutcome::NotFound(r) => {
                                black_box(r.doc_id);
                                black_box(r.weight);
                                black_box(r.freq);
                            }
                        }
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });

        group.bench_function("OptionalOptimized", |b| {
            b.iter_batched_ref(
                || {
                    // SAFETY: context_sparse has index_all=true and existingDocs wired by TestContext::wildcard.
                    let wcii = unsafe { new_wildcard_iterator_optimized(context_sparse.sctx, 0.) };
                    let child = IdList::<'_, true>::new(child_ids.clone());
                    OptionalOptimized::new(wcii, child, Self::MAX_DOC_ID, Self::WEIGHT)
                },
                |it| {
                    while let Ok(Some(outcome)) = it.skip_to(it.last_doc_id() + Self::STEP) {
                        match outcome {
                            SkipToOutcome::Found(r) | SkipToOutcome::NotFound(r) => {
                                black_box(r.doc_id);
                                black_box(r.weight);
                                black_box(r.freq);
                            }
                        }
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });

        group.finish();
    }

    fn make_child_doc_ids(child_ratio: f64) -> Vec<u64> {
        let mut rng = StdRng::seed_from_u64(42);
        let mut ids = Vec::new();
        for doc_id in 1..=Self::MAX_DOC_ID {
            if rng.random::<f64>() < child_ratio {
                ids.push(doc_id);
            }
        }
        ids
    }

    /// Build the child for the sparse comparison: `SPARSE_CHILD_RATIO` of the existing docs.
    ///
    /// Both `Optional` and `OptionalOptimized` use this exact same child so results are comparable.
    fn make_sparse_child_doc_ids() -> Vec<u64> {
        let mut rng = StdRng::seed_from_u64(42);
        (1..=Self::MAX_DOC_ID)
            .step_by(Self::SPARSE_STEP)
            .filter(|_| rng.random::<f64>() < Self::SPARSE_CHILD_RATIO)
            .collect()
    }
}
