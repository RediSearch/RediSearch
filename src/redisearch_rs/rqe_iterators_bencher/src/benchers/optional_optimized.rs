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
//! Sparse = few existing docs relative to max_doc_id — where the optimization matters most.
//! Dense  = most doc IDs occupied — both variants should be close.
//! Full   = all docs present, child matches all — upper bound on optimized throughput.

use std::{hint::black_box, time::Duration};

use crate::ffi::{self, IndexFlags_Index_DocIdsOnly};
use criterion::{BenchmarkGroup, Criterion, measurement::WallTime};
use rand::{Rng as _, SeedableRng as _, rngs::StdRng};
use rqe_iterators::{
    IdList, RQEIterator, empty::Empty, optional_optimized::OptionalOptimized,
    wildcard::new_wildcard_iterator_optimized,
};
use rqe_iterators_test_utils::MockContext;

#[derive(Default)]
pub struct Bencher;

impl Bencher {
    const MEASUREMENT_TIME: Duration = Duration::from_millis(1000);
    const WARMUP_TIME: Duration = Duration::from_millis(200);

    const LARGE_MAX: u64 = 1_000_000;
    const WEIGHT: f64 = 1.0;

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
        self.bench_sparse_read(c);
        self.bench_dense_read(c);
        self.bench_full_read(c);
    }

    /// Sparse: few existing docs relative to max_doc_id — where the optimization matters most.
    /// Uses a small wcii (1_000 existing docs) with an Empty child (all virtual results).
    fn bench_sparse_read(&self, c: &mut Criterion) {
        const EXISTING: u64 = 1_000;

        let existing_docs_ii = ffi::InvertedIndex::new(IndexFlags_Index_DocIdsOnly);
        for doc_id in 1..=EXISTING {
            existing_docs_ii.write_doc_id(doc_id);
        }

        let mock_ctx = MockContext::new(Self::LARGE_MAX, EXISTING as usize);
        // SAFETY: mock_ctx, existing_docs_ii, and any iterator created below are all
        // dropped at the end of this function, in the right order.
        unsafe {
            let sctx = mock_ctx.sctx().as_ptr();
            let spec = (*sctx).spec;
            (*(*spec).rule).index_all = true;
            (*spec).existingDocs = existing_docs_ii.ii;
        }

        let mut group = self.benchmark_group(c, "Iterator - OptionalOptimized - Read Sparse");

        group.bench_function("Rust", |b| {
            b.iter_batched_ref(
                || {
                    // SAFETY: mock_ctx has index_all=true and existingDocs wired above.
                    let (wcii, _) = unsafe { new_wildcard_iterator_optimized(mock_ctx.sctx(), 0.) };
                    OptionalOptimized::new(wcii, Empty, Self::LARGE_MAX, Self::WEIGHT)
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

    /// Dense: most doc IDs occupied — child is an IdList covering ~90% of docs.
    fn bench_dense_read(&self, c: &mut Criterion) {
        let existing_docs_ii = ffi::InvertedIndex::new(IndexFlags_Index_DocIdsOnly);
        for doc_id in 1..=Self::LARGE_MAX {
            existing_docs_ii.write_doc_id(doc_id);
        }

        let mock_ctx = MockContext::new(Self::LARGE_MAX, Self::LARGE_MAX as usize);
        // SAFETY: same lifetime guarantee as bench_sparse_read.
        unsafe {
            let sctx = mock_ctx.sctx().as_ptr();
            let spec = (*sctx).spec;
            (*(*spec).rule).index_all = true;
            (*spec).existingDocs = existing_docs_ii.ii;
        }

        let child_ids = Self::make_child_doc_ids(0.9);

        let mut group = self.benchmark_group(c, "Iterator - OptionalOptimized - Read Dense");

        group.bench_function("Rust", |b| {
            b.iter_batched_ref(
                || {
                    // SAFETY: mock_ctx has index_all=true and existingDocs wired above.
                    let (wcii, _) = unsafe { new_wildcard_iterator_optimized(mock_ctx.sctx(), 0.) };
                    let child = IdList::<'_, true>::new(child_ids.clone());
                    OptionalOptimized::new(wcii, child, Self::LARGE_MAX, Self::WEIGHT)
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

    /// Full: all docs present, child matches all — upper bound on optimized throughput.
    fn bench_full_read(&self, c: &mut Criterion) {
        let existing_docs_ii = ffi::InvertedIndex::new(IndexFlags_Index_DocIdsOnly);
        for doc_id in 1..=Self::LARGE_MAX {
            existing_docs_ii.write_doc_id(doc_id);
        }

        let mock_ctx = MockContext::new(Self::LARGE_MAX, Self::LARGE_MAX as usize);
        // SAFETY: same lifetime guarantee as bench_sparse_read.
        unsafe {
            let sctx = mock_ctx.sctx().as_ptr();
            let spec = (*sctx).spec;
            (*(*spec).rule).index_all = true;
            (*spec).existingDocs = existing_docs_ii.ii;
        }

        let full_child_ids: Vec<u64> = (1..=Self::LARGE_MAX).collect();

        let mut group = self.benchmark_group(c, "Iterator - OptionalOptimized - Read Full");

        group.bench_function("Rust", |b| {
            b.iter_batched_ref(
                || {
                    // SAFETY: mock_ctx has index_all=true and existingDocs wired above.
                    let (wcii, _) = unsafe { new_wildcard_iterator_optimized(mock_ctx.sctx(), 0.) };
                    let child = IdList::<'_, true>::new(full_child_ids.clone());
                    OptionalOptimized::new(wcii, child, Self::LARGE_MAX, Self::WEIGHT)
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

    fn make_child_doc_ids(density: f64) -> Vec<u64> {
        let mut rng = StdRng::seed_from_u64(42);
        let mut ids = Vec::new();
        for doc_id in 1..=Self::LARGE_MAX {
            if rng.random::<f64>() < density {
                ids.push(doc_id);
            }
        }
        ids.sort();
        ids
    }
}
