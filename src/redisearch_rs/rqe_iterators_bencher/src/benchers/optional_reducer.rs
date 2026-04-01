/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Benchmark the optional iterator reducer ([`rqe_iterators::optional_reducer::new_optional_iterator`]).
//!
//! The reducer applies shortcircuit reductions before constructing the iterator.
//! Four code paths are benchmarked:
//!
//! - **Empty child** → `WildcardFallback` (drops child, creates a plain wildcard)
//! - **Wildcard child** → `WildcardPassthrough` (applies weight, returns child as-is)
//! - **Regular child, non-optimized index** → `Plain(Optional)`
//! - **Regular child, optimized index** → `Optimized(OptionalOptimized)`
//!
//! Each scenario measures construction overhead only (no iteration).
//! Both C (`NewOptionalIterator`) and Rust (`new_optional_iterator`) are compared.

use std::{
    alloc::{Layout, alloc_zeroed, dealloc},
    hint::black_box,
    ptr::{self, NonNull},
    time::Duration,
};

use ::ffi::{DocTable, IndexSpec, QueryEvalCtx, RedisSearchCtx, t_docId};
use criterion::{BenchmarkGroup, Criterion, measurement::WallTime};
use rqe_iterators::{
    IdList,
    empty::Empty,
    optional_reducer::new_optional_iterator,
    wildcard::Wildcard,
};
use rqe_iterators_test_utils::TestContext;

use crate::ffi;

// ── Context helpers ──────────────────────────────────────────────────────────

/// Minimal heap-allocated [`QueryEvalCtx`] for non-optimized paths.
///
/// All fields are zeroed: `sctx.spec.rule` is null, so the reducer always
/// takes the plain (non-optimized) branch.  `docTable.maxDocId` is set so
/// that the fallback wildcard created in the empty-child shortcircuit has
/// the correct size.
struct MinimalQueryCtx {
    qctx: *mut QueryEvalCtx,
    doc_table: *mut DocTable,
    sctx: *mut RedisSearchCtx,
    spec: *mut IndexSpec,
}

impl MinimalQueryCtx {
    fn new(max_doc_id: t_docId) -> Self {
        unsafe {
            let spec = alloc_zeroed(Layout::new::<IndexSpec>()) as *mut IndexSpec;
            let sctx = alloc_zeroed(Layout::new::<RedisSearchCtx>()) as *mut RedisSearchCtx;
            let doc_table = alloc_zeroed(Layout::new::<DocTable>()) as *mut DocTable;
            let qctx = alloc_zeroed(Layout::new::<QueryEvalCtx>()) as *mut QueryEvalCtx;
            (*doc_table).maxDocId = max_doc_id;
            (*sctx).spec = spec;
            (*qctx).sctx = sctx;
            (*qctx).docTable = doc_table;
            Self { qctx, doc_table, sctx, spec }
        }
    }

    fn as_ptr(&self) -> *mut QueryEvalCtx {
        self.qctx
    }

    fn as_non_null(&self) -> NonNull<QueryEvalCtx> {
        NonNull::new(self.qctx).unwrap()
    }
}

impl Drop for MinimalQueryCtx {
    fn drop(&mut self) {
        unsafe {
            dealloc(self.qctx.cast(), Layout::new::<QueryEvalCtx>());
            dealloc(self.doc_table.cast(), Layout::new::<DocTable>());
            dealloc(self.sctx.cast(), Layout::new::<RedisSearchCtx>());
            dealloc(self.spec.cast(), Layout::new::<IndexSpec>());
        }
    }
}

/// Heap-allocated [`QueryEvalCtx`] that borrows `sctx` and `docTable` from a
/// [`TestContext`] with `index_all = true`.
///
/// The qctx must be dropped before the [`TestContext`] it references.
struct OwnedQueryEvalCtx(*mut QueryEvalCtx);

impl OwnedQueryEvalCtx {
    /// # Safety
    /// `ctx` must outlive this `OwnedQueryEvalCtx`.
    unsafe fn new(ctx: &TestContext) -> Self {
        unsafe {
            let raw = alloc_zeroed(Layout::new::<QueryEvalCtx>()) as *mut QueryEvalCtx;
            assert!(!raw.is_null(), "QueryEvalCtx allocation failed");
            (*raw).sctx = ctx.sctx.as_ptr();
            (*raw).docTable = ptr::addr_of_mut!((*ctx.spec.as_ptr()).docs);
            Self(raw)
        }
    }

    fn as_ptr(&self) -> *mut QueryEvalCtx {
        self.0
    }

    fn as_non_null(&self) -> NonNull<QueryEvalCtx> {
        NonNull::new(self.0).unwrap()
    }
}

impl Drop for OwnedQueryEvalCtx {
    fn drop(&mut self) {
        unsafe {
            dealloc(self.0.cast(), Layout::new::<QueryEvalCtx>());
        }
    }
}

// ── Bencher ───────────────────────────────────────────────────────────────────

pub struct Bencher {
    // Dropped before `context` (declaration order = drop order).
    qctx_optimized: OwnedQueryEvalCtx,
    qctx_plain: MinimalQueryCtx,
    // Kept alive so that `qctx_optimized`'s borrowed pointers remain valid.
    #[expect(dead_code, reason = "kept alive for qctx_optimized's raw pointer borrows")]
    context: TestContext,
}

impl Default for Bencher {
    fn default() -> Self {
        let context = TestContext::wildcard(1..=Self::MAX_DOC_ID);
        // SAFETY: TestContext::wildcard creates a valid spec with a valid rule.
        // We set index_all=true so the reducer takes the optimized path.
        unsafe {
            let spec = (*context.sctx.as_ptr()).spec;
            (*(*spec).rule).index_all = true;
        }
        // SAFETY: qctx_optimized is declared before `context` so it is dropped first.
        let qctx_optimized = unsafe { OwnedQueryEvalCtx::new(&context) };
        let qctx_plain = MinimalQueryCtx::new(Self::MAX_DOC_ID);
        Self { qctx_optimized, qctx_plain, context }
    }
}

impl Bencher {
    const MEASUREMENT_TIME: Duration = Duration::from_millis(1000);
    const WARMUP_TIME: Duration = Duration::from_millis(200);

    const MAX_DOC_ID: t_docId = 1_000;
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
        self.bench_shortcircuit_empty(c);
        self.bench_shortcircuit_wildcard(c);
        self.bench_plain_child(c);
        self.bench_optimized_child(c);
    }

    /// Shortcircuit 1: empty child → WildcardFallback.
    ///
    /// The reducer drops the child and returns a fresh plain wildcard.
    fn bench_shortcircuit_empty(&self, c: &mut Criterion) {
        let mut group =
            self.benchmark_group(c, "Iterator - OptionalReducer - Shortcircuit Empty Child");

        group.bench_function("C", |b| {
            b.iter_batched(
                ffi::QueryIterator::new_empty,
                |child| {
                    // C OptionalIteratorReducer detects empty child → creates wildcard.
                    let it = black_box(ffi::QueryIterator::new_optional_optimized(
                        child,
                        self.qctx_plain.as_ptr(),
                        Self::MAX_DOC_ID,
                        Self::WEIGHT,
                    ));
                    it.free();
                },
                criterion::BatchSize::SmallInput,
            );
        });

        group.bench_function("Rust", |b| {
            b.iter_batched(
                || Empty,
                |child| {
                    // SAFETY: qctx_plain has a valid sctx->spec with null rule, so
                    // new_wildcard_iterator takes the plain-Wildcard fallback path.
                    let result = unsafe {
                        new_optional_iterator(
                            child,
                            Self::WEIGHT,
                            self.qctx_plain.as_non_null(),
                            Self::MAX_DOC_ID,
                        )
                    };
                    drop(black_box(result));
                },
                criterion::BatchSize::SmallInput,
            );
        });

        group.finish();
    }

    /// Shortcircuit 2: wildcard child → WildcardPassthrough.
    ///
    /// The reducer applies the weight to the current result and returns the child
    /// as-is without allocating a new iterator.
    fn bench_shortcircuit_wildcard(&self, c: &mut Criterion) {
        let mut group =
            self.benchmark_group(c, "Iterator - OptionalReducer - Shortcircuit Wildcard Child");

        group.bench_function("C", |b| {
            b.iter_batched(
                || ffi::QueryIterator::new_wildcard_non_optimized(Self::MAX_DOC_ID, 0.0),
                |child| {
                    // C OptionalIteratorReducer detects wildcard child → applies weight, returns as-is.
                    // qctx_plain is valid but not accessed in this shortcircuit branch.
                    let it = black_box(ffi::QueryIterator::new_optional_optimized(
                        child,
                        self.qctx_plain.as_ptr(),
                        Self::MAX_DOC_ID,
                        Self::WEIGHT,
                    ));
                    it.free();
                },
                criterion::BatchSize::SmallInput,
            );
        });

        group.bench_function("Rust", |b| {
            b.iter_batched(
                || Wildcard::new(Self::MAX_DOC_ID, 0.0),
                |child| {
                    // SAFETY: the wildcard passthrough branch never dereferences `query`.
                    let result = unsafe {
                        new_optional_iterator(
                            child,
                            Self::WEIGHT,
                            NonNull::dangling(),
                            Self::MAX_DOC_ID,
                        )
                    };
                    drop(black_box(result));
                },
                criterion::BatchSize::SmallInput,
            );
        });

        group.finish();
    }

    /// Regular child, non-optimized index → `Plain(Optional)`.
    ///
    /// The reducer inspects the spec (null rule → non-optimized) and wraps the
    /// child in a plain [`rqe_iterators::optional::Optional`].
    fn bench_plain_child(&self, c: &mut Criterion) {
        let mut group =
            self.benchmark_group(c, "Iterator - OptionalReducer - Plain Child");

        group.bench_function("C", |b| {
            b.iter_batched(
                || ffi::QueryIterator::new_id_list(vec![1, 2, 3]),
                |child| {
                    // C OptionalIteratorReducer: spec->rule==null → plain Optional.
                    let it = black_box(ffi::QueryIterator::new_optional_optimized(
                        child,
                        self.qctx_plain.as_ptr(),
                        Self::MAX_DOC_ID,
                        Self::WEIGHT,
                    ));
                    it.free();
                },
                criterion::BatchSize::SmallInput,
            );
        });

        group.bench_function("Rust", |b| {
            b.iter_batched(
                || IdList::<'static, true>::new(vec![1u64, 2, 3]),
                |child| {
                    // SAFETY: qctx_plain has a valid sctx->spec with null rule → Plain branch.
                    let result = unsafe {
                        new_optional_iterator(
                            child,
                            Self::WEIGHT,
                            self.qctx_plain.as_non_null(),
                            Self::MAX_DOC_ID,
                        )
                    };
                    drop(black_box(result));
                },
                criterion::BatchSize::SmallInput,
            );
        });

        group.finish();
    }

    /// Regular child, optimized index → `Optimized(OptionalOptimized)`.
    ///
    /// The reducer inspects the spec (`index_all=true`) and creates an
    /// [`rqe_iterators::optional_optimized::OptionalOptimized`] with a fresh
    /// wildcard (`wcii`) drawn from `existingDocs`.
    fn bench_optimized_child(&self, c: &mut Criterion) {
        let mut group =
            self.benchmark_group(c, "Iterator - OptionalReducer - Optimized Child");

        group.bench_function("C", |b| {
            b.iter_batched(
                || ffi::QueryIterator::new_id_list(vec![1, 2, 3]),
                |child| {
                    // C OptionalIteratorReducer: index_all=true → OptionalOptimized.
                    let it = black_box(ffi::QueryIterator::new_optional_optimized(
                        child,
                        self.qctx_optimized.as_ptr(),
                        Self::MAX_DOC_ID,
                        Self::WEIGHT,
                    ));
                    it.free();
                },
                criterion::BatchSize::SmallInput,
            );
        });

        group.bench_function("Rust", |b| {
            b.iter_batched(
                || IdList::<'static, true>::new(vec![1u64, 2, 3]),
                |child| {
                    // SAFETY: qctx_optimized has index_all=true and valid existingDocs
                    // (wired by TestContext::wildcard).
                    let result = unsafe {
                        new_optional_iterator(
                            child,
                            Self::WEIGHT,
                            self.qctx_optimized.as_non_null(),
                            Self::MAX_DOC_ID,
                        )
                    };
                    drop(black_box(result));
                },
                criterion::BatchSize::SmallInput,
            );
        });

        group.finish();
    }
}
