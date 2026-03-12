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
//!
//! Both C and Rust use the same `DocIdsOnly` inverted index as `existingDocs`, so their
//! `wcii` paths are comparable: the C iterator reads from it via `NewWildcardIterator_Optimized`,
//! while the Rust iterator reads from it via `new_wildcard_iterator_optimized`.

use std::{hint::black_box, ptr, time::Duration};

use crate::ffi;
use ::ffi::IteratorStatus_ITERATOR_OK;
use ::ffi::QueryEvalCtx;
use criterion::{BenchmarkGroup, Criterion, measurement::WallTime};
use rand::{Rng as _, SeedableRng as _, rngs::StdRng};
use rqe_iterators::{
    IdList, RQEIterator, optional_optimized::OptionalOptimized,
    wildcard::new_wildcard_iterator_optimized,
};
use rqe_iterators_test_utils::TestContext;

/// Owned `QueryEvalCtx` that borrows its `sctx`/`docTable` from a [`TestContext`].
///
/// Allocated with the global allocator so that it does not participate in Stacked
/// Borrows tracking for the underlying spec/sctx data.
struct OwnedQueryEvalCtx(*mut QueryEvalCtx);

impl OwnedQueryEvalCtx {
    /// # Safety
    /// `ctx` must outlive this `OwnedQueryEvalCtx`.
    unsafe fn new(ctx: &TestContext) -> Self {
        unsafe {
            let layout = std::alloc::Layout::new::<QueryEvalCtx>();
            let raw = std::alloc::alloc_zeroed(layout) as *mut QueryEvalCtx;
            assert!(!raw.is_null(), "QueryEvalCtx allocation failed");
            (*raw).sctx = ctx.sctx.as_ptr();
            (*raw).docTable = ptr::addr_of_mut!((*ctx.spec.as_ptr()).docs);
            Self(raw)
        }
    }

    fn as_ptr(&self) -> *mut QueryEvalCtx {
        self.0
    }
}

impl Drop for OwnedQueryEvalCtx {
    fn drop(&mut self) {
        unsafe {
            std::alloc::dealloc(self.0.cast(), std::alloc::Layout::new::<QueryEvalCtx>());
        }
    }
}

pub struct Bencher {
    // Must be declared before the TestContexts they borrow from, so they are
    // dropped first (Rust drops fields in declaration order).
    qctx_sparse: OwnedQueryEvalCtx,
    qctx_dense: OwnedQueryEvalCtx,
    context_sparse: TestContext,
    context_dense: TestContext,
}

impl Default for Bencher {
    fn default() -> Self {
        let context_dense = TestContext::wildcard(1..=Self::DENSE_MAX);
        let context_sparse = TestContext::wildcard(1..=Self::SPARSE_MAX);
        // SAFETY: both contexts have a valid spec and rule created by TestContext::wildcard.
        // We set index_all=true so NewOptionalIterator takes the optimized path.
        unsafe {
            for ctx in [&context_dense, &context_sparse] {
                let spec = (*ctx.sctx.as_ptr()).spec;
                (*(*spec).rule).index_all = true;
            }
        }
        // SAFETY: qctx_* are declared before context_* in the struct, so they are
        // dropped first, before the TestContexts they borrow from.
        let qctx_sparse = unsafe { OwnedQueryEvalCtx::new(&context_sparse) };
        let qctx_dense = unsafe { OwnedQueryEvalCtx::new(&context_dense) };
        Self {
            context_sparse,
            context_dense,
            qctx_sparse,
            qctx_dense,
        }
    }
}

impl Bencher {
    const MEASUREMENT_TIME: Duration = Duration::from_millis(1000);
    const WARMUP_TIME: Duration = Duration::from_millis(200);

    const SPARSE_MAX: u64 = 1_000;
    const DENSE_MAX: u64 = 1_000_000;
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
    /// Uses a small wcii (SPARSE_MAX existing docs) with an out-of-range IdList child (all virtual results).
    fn bench_sparse_read(&self, c: &mut Criterion) {
        let context = &self.context_sparse;
        let mut group = self.benchmark_group(c, "Iterator - OptionalOptimized - Read Sparse");

        group.bench_function("C", |b| {
            b.iter_batched_ref(
                || {
                    ffi::QueryIterator::new_optional_optimized(
                        // new_empty() would trigger OptionalIteratorReducer's short-circuit,
                        // returning a plain wildcard instead of OptionalOptimizedIterator.
                        ffi::QueryIterator::new_id_list(vec![Self::SPARSE_MAX + 1]),
                        self.qctx_sparse.as_ptr(),
                        Self::SPARSE_MAX,
                        Self::WEIGHT,
                    )
                },
                |it| {
                    while it.read() == IteratorStatus_ITERATOR_OK {
                        black_box(it.current());
                    }
                    it.free();
                },
                criterion::BatchSize::SmallInput,
            );
        });

        group.bench_function("Rust", |b| {
            b.iter_batched_ref(
                || {
                    // SAFETY: context has index_all=true and existingDocs wired by TestContext::wildcard.
                    let (wcii, _) = unsafe { new_wildcard_iterator_optimized(context.sctx, 0.) };
                    // Use an out-of-range IdList rather than Empty to match the C benchmark
                    // structure: Empty would also work here (Rust has no OptionalIteratorReducer
                    // short-circuit), but this keeps the child shape identical to C.
                    let child = IdList::<'_, true>::new(vec![Self::SPARSE_MAX + 1]);
                    OptionalOptimized::new(wcii, child, Self::SPARSE_MAX, Self::WEIGHT)
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
        let context = &self.context_dense;
        let child_ids = Self::make_child_doc_ids(0.9);
        let mut group = self.benchmark_group(c, "Iterator - OptionalOptimized - Read Dense");

        group.bench_function("C", |b| {
            b.iter_batched_ref(
                || {
                    ffi::QueryIterator::new_optional_optimized(
                        ffi::QueryIterator::new_id_list(child_ids.clone()),
                        self.qctx_dense.as_ptr(),
                        Self::DENSE_MAX,
                        Self::WEIGHT,
                    )
                },
                |it| {
                    while it.read() == IteratorStatus_ITERATOR_OK {
                        black_box(it.current());
                    }
                    it.free();
                },
                criterion::BatchSize::SmallInput,
            );
        });

        group.bench_function("Rust", |b| {
            b.iter_batched_ref(
                || {
                    // SAFETY: context has index_all=true and existingDocs wired by TestContext::wildcard.
                    let (wcii, _) = unsafe { new_wildcard_iterator_optimized(context.sctx, 0.) };
                    let child = IdList::<'_, true>::new(child_ids.clone());
                    OptionalOptimized::new(wcii, child, Self::DENSE_MAX, Self::WEIGHT)
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
        let context = &self.context_dense;
        let full_child_ids: Vec<u64> = (1..=Self::DENSE_MAX).collect();
        let mut group = self.benchmark_group(c, "Iterator - OptionalOptimized - Read Full");

        group.bench_function("C", |b| {
            b.iter_batched_ref(
                || {
                    ffi::QueryIterator::new_optional_optimized(
                        ffi::QueryIterator::new_id_list(full_child_ids.clone()),
                        self.qctx_dense.as_ptr(),
                        Self::DENSE_MAX,
                        Self::WEIGHT,
                    )
                },
                |it| {
                    while it.read() == IteratorStatus_ITERATOR_OK {
                        black_box(it.current());
                    }
                    it.free();
                },
                criterion::BatchSize::SmallInput,
            );
        });

        group.bench_function("Rust", |b| {
            b.iter_batched_ref(
                || {
                    // SAFETY: context has index_all=true and existingDocs wired by TestContext::wildcard.
                    let (wcii, _) = unsafe { new_wildcard_iterator_optimized(context.sctx, 0.) };
                    let child = IdList::<'_, true>::new(full_child_ids.clone());
                    OptionalOptimized::new(wcii, child, Self::DENSE_MAX, Self::WEIGHT)
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
        for doc_id in 1..=Self::DENSE_MAX {
            if rng.random::<f64>() < density {
                ids.push(doc_id);
            }
        }
        ids.sort();
        ids
    }
}
