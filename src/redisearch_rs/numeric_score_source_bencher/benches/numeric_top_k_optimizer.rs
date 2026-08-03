/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Apples-to-apples numeric top-k benchmark: Rust `NumericTopKIterator` vs the
//! *real* C `OptimizerIterator` (`iterators/optimizer_reader.c`).
//!
//! Both sides:
//! - read the same numeric range tree, doc table and search context, built once
//!   per index size by `TestContext::numeric`,
//! - walk the field's ranges in value order under an `offset`/`limit` window,
//!   widening it when the window drains before the heap is full,
//! - maintain a real bounded top-k heap over the same child filter,
//! - run without a query deadline: neither the optimizer and its child
//!   iterators nor the Rust source, on its default `NoTimeoutChecker`, polls
//!   one, so no timeout bookkeeping is priced into either arm.
//!
//! Three groups × two sides (rust / c):
//!
//! - `numeric_top_k/filtered`    — sorted-id child passing a tenth of the index.
//! - `numeric_top_k/unfiltered`  — no child filter on the Rust side, the child
//!   the C planner uses for a bare `SORTBY` (a wildcard) on the C side.
//! - `numeric_top_k/selectivity` — child selectivity swept at a fixed index size
//!   and `k`, across the range where the retry expansion starts to bite.
//!
//! Known asymmetries:
//!
//! - Window sizing dominates the unfiltered group. The C optimizer always sizes
//!   its first window from the child's selectivity, so a wildcard child bounds it
//!   at roughly `k` documents; the Rust unfiltered source reads an unbounded
//!   window and can only stop on a batch boundary, so it materializes
//!   [`RANGE_BATCH_SIZE`] whole ranges before the full heap lets it stop.
//! - The C side has no unfiltered mode: `Q_OPT_PARTIAL_RANGE` still drives the
//!   optimizer through a wildcard child, one `Read` plus one doc-table borrow per
//!   candidate that the Rust source skips entirely.
//! - The C side drops candidates whose document metadata is gone; the Rust source
//!   is benchmarked with the default `AllValid` oracle, which performs no
//!   per-document validity check.

// Pull in the lib to ensure FFI stubs and mock allocator symbols are linked.
use numeric_score_source_bencher as _;

use std::{
    ffi::{CStr, c_char, c_int},
    hint::black_box,
    num::NonZeroUsize,
    ptr,
};

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use ffi::RedisSearchCtx;
use index_result::RSIndexResult;
use inverted_index::NumericFilter;
use numeric_range_tree::{NumericRangeTree, RangeWindow};
use numeric_score_source::{
    NumericScoreSource, new_numeric_top_k_filtered, new_numeric_top_k_unfiltered,
};
use redis_module::RedisModule_Alloc;
use rqe_core::DocId;
use rqe_iterators::{IdList, RQEIterator};
use rqe_iterators_test_utils::TestContext;

/// Sort direction both sides run in (`SORTBY <field> ASC`).
const ASCENDING: bool = true;

/// Ranges materialized per batch, matching the source's own default.
const RANGE_BATCH_SIZE: usize = 8;

/// Numeric field of the schema `TestContext::numeric` creates.
const FIELD: &CStr = c"num_field";

// ── C shim (from optimizer_shim.c, compiled by build.rs) ─────────────────────

unsafe extern "C" {
    /// Drive the real C `OptimizerIterator` over a child filter and count
    /// results. A null `ids` selects a wildcard child over doc ids
    /// `1..=child_count`, otherwise the ids are taken as a sorted-id child.
    fn bench_c_optimizer(
        sctx: *mut RedisSearchCtx,
        field_name: *const c_char,
        ids: *mut DocId,
        child_count: usize,
        k: usize,
        ascending: c_int,
    ) -> usize;
}

/// Allocate a `RedisModule_Alloc`-owned copy of `ids`, so the child iterator's
/// `OwnedSlice` can free it via the matching `RedisModule_Free` (same mock
/// allocator pair). Ownership is transferred to `bench_c_optimizer`.
fn alloc_owned_ids(ids: &[DocId]) -> *mut DocId {
    // SAFETY: the mock allocator is installed once, by linking redis_mock, and
    // never reassigned.
    let alloc = unsafe { RedisModule_Alloc }.expect("mock allocator not initialised");
    // SAFETY: `alloc` returns a fresh allocation of the requested size.
    let ptr = unsafe { alloc(std::mem::size_of_val(ids)) }.cast::<DocId>();
    // SAFETY: `ptr` is a fresh allocation sized for `ids.len()` doc ids, and
    // cannot overlap `ids`.
    unsafe { ptr::copy_nonoverlapping(ids.as_ptr(), ptr, ids.len()) };
    ptr
}

/// Whole-field filter: matches every value.
///
/// `NumericFilter::default()` bounds the window at `0.0..f64::MAX`; the `±inf`
/// bounds here match the whole-field span the optimizer sorts over.
fn full_range() -> NumericFilter {
    NumericFilter {
        min: f64::NEG_INFINITY,
        max: f64::INFINITY,
        ..NumericFilter::default()
    }
}

/// Value indexed for `doc_id`, a permutation of `0..n` so that value order is
/// uncorrelated with doc-id order — otherwise a value-ordered scan would walk
/// the index in doc-id order and never exercise the intersection.
const fn value_for(doc_id: DocId, n: usize) -> f64 {
    (doc_id.wrapping_mul(2_654_435_761) % n as u64) as f64
}

/// Generate `count` sorted child ids drawn uniformly from `[1, n]`.
fn child_ids(n: usize, count: usize) -> Vec<DocId> {
    let step = (n / count).max(1);
    (0..count).map(|i| (i * step + 1).min(n) as DocId).collect()
}

/// Window size to start from, given the child's selectivity: how many documents
/// must be read in value order to expect `k` of them to pass the child. Both
/// sides start from this estimate — the C optimizer computes it internally.
fn estimate_limit(num_docs: usize, child_estimate: usize, k: usize) -> usize {
    if num_docs == 0 || child_estimate == 0 {
        return 0;
    }
    let ratio = child_estimate as f64 / num_docs as f64;
    (k as f64 / ratio) as usize + 1
}

/// An index of `n` documents shared by both sides: a numeric range tree over
/// [`value_for`], a doc table holding every indexed doc id, and the search
/// context the C optimizer reads them through.
struct Index {
    ctx: TestContext,
    n: usize,
}

impl Index {
    fn new(n: usize) -> Self {
        let records = (1..=n as DocId).map(|id| {
            RSIndexResult::build_numeric(value_for(id, n))
                .doc_id(id)
                .build()
        });
        let ctx = TestContext::numeric(records, false);

        // The C optimizer drops any candidate whose document metadata is
        // missing, so every indexed doc id needs a doc table entry.
        for i in 1..=n {
            ctx.add_document(&format!("doc{i}"));
        }

        Self { ctx, n }
    }

    /// Borrow the numeric index. Kept short-lived: the C side re-derives its own
    /// reference to the same tree through the field spec.
    fn tree(&self) -> &NumericRangeTree {
        self.ctx.numeric_range_tree_ref()
    }

    const fn sctx(&self) -> *mut RedisSearchCtx {
        self.ctx.sctx.as_ptr()
    }
}

/// Run one Rust top-k scan against the given child ids, returning result count.
fn run_rust_filtered(index: &Index, ids: &[DocId], k: NonZeroUsize) -> usize {
    let child_estimate = ids.len();
    let window = RangeWindow {
        offset: 0,
        limit: estimate_limit(index.n, child_estimate, k.get()),
    };
    let source = NumericScoreSource::filtered(
        index.tree(),
        full_range(),
        window,
        ASCENDING,
        RANGE_BATCH_SIZE,
        index.n,
        child_estimate,
    );
    // Fresh owned copy each call, mirroring the C side's per-run id allocation.
    let child = IdList::<true>::new(ids.to_vec());
    let mut it = new_numeric_top_k_filtered(source, child, k);
    let mut count = 0usize;
    while it.read().unwrap().is_some() {
        count += 1;
    }
    count
}

/// Run one Rust top-k scan with no child filter, returning result count.
fn run_rust_unfiltered(index: &Index, k: NonZeroUsize) -> usize {
    let source = NumericScoreSource::unfiltered(index.tree(), full_range(), ASCENDING);
    let mut it = new_numeric_top_k_unfiltered(source, k);
    let mut count = 0usize;
    while it.read().unwrap().is_some() {
        count += 1;
    }
    count
}

/// Run one C OptimizerIterator scan to depletion, returning result count.
/// `ids` of `None` selects the wildcard child, matching the Rust unfiltered run.
fn run_c(index: &Index, ids: Option<&[DocId]>, k: NonZeroUsize) -> usize {
    // Fresh owned copy each call: the child iterator frees it via RedisModule_Free.
    let (ids_ptr, child_count) = match ids {
        Some(ids) => (alloc_owned_ids(ids), ids.len()),
        None => (ptr::null_mut(), index.n),
    };
    // SAFETY: the search context and its spec outlive the call; `ids_ptr` is
    // either null or a sorted, owned array of `child_count` ids whose ownership
    // transfers to the call. `FIELD` names a numeric field of the spec.
    unsafe {
        bench_c_optimizer(
            index.sctx(),
            FIELD.as_ptr(),
            ids_ptr,
            child_count,
            k.get(),
            ASCENDING as c_int,
        )
    }
}

/// Validate that both sides produce the expected result count before the timed
/// loop starts. Catches iterator bugs, shim error paths, and index setup
/// mistakes that would otherwise silently corrupt the Rust/C timing comparison.
fn preflight(index: &Index, ids: Option<&[DocId]>, k: NonZeroUsize) {
    let expected = k.get().min(ids.map_or(index.n, <[DocId]>::len));

    let rust_count = match ids {
        Some(ids) => run_rust_filtered(index, ids, k),
        None => run_rust_unfiltered(index, k),
    };
    assert_eq!(
        rust_count,
        expected,
        "Rust iterator produced {rust_count} results (expected {expected}) \
         for k={k}, n={n}, child={child:?}",
        n = index.n,
        child = ids.map(<[DocId]>::len),
    );

    let c_count = run_c(index, ids, k);
    assert_eq!(
        c_count,
        expected,
        "C shim produced {c_count} results (expected {expected}) \
         for k={k}, n={n}, child={child:?}",
        n = index.n,
        child = ids.map(<[DocId]>::len),
    );
}

// ── Filtered ─────────────────────────────────────────────────────────────────

fn bench_filtered(c: &mut Criterion) {
    let mut group = c.benchmark_group("numeric_top_k/filtered");

    for n in [10_000usize, 100_000] {
        let index = Index::new(n);

        for k in [10usize, 100].map(|k| NonZeroUsize::new(k).unwrap()) {
            let ids = child_ids(n, (n / 10).max(k.get()));
            let param_str = format!("n{n}_k{k}");

            preflight(&index, Some(&ids), k);

            group.bench_with_input(BenchmarkId::new("rust", &param_str), &(n, k), |b, _| {
                b.iter(|| black_box(run_rust_filtered(&index, &ids, k)))
            });

            group.bench_with_input(BenchmarkId::new("c", &param_str), &(n, k), |b, _| {
                b.iter(|| black_box(run_c(&index, Some(&ids), k)))
            });
        }
    }

    group.finish();
}

// ── Unfiltered ───────────────────────────────────────────────────────────────

fn bench_unfiltered(c: &mut Criterion) {
    let mut group = c.benchmark_group("numeric_top_k/unfiltered");

    for n in [10_000usize, 100_000] {
        let index = Index::new(n);

        for k in [10usize, 100].map(|k| NonZeroUsize::new(k).unwrap()) {
            let param_str = format!("n{n}_k{k}");

            preflight(&index, None, k);

            group.bench_with_input(BenchmarkId::new("rust", &param_str), &(n, k), |b, _| {
                b.iter(|| black_box(run_rust_unfiltered(&index, k)))
            });

            group.bench_with_input(BenchmarkId::new("c", &param_str), &(n, k), |b, _| {
                b.iter(|| black_box(run_c(&index, None, k)))
            });
        }
    }

    group.finish();
}

// ── Child selectivity ────────────────────────────────────────────────────────
//
// At a fixed index size and top-k, sweep how much of the index the child filter
// passes. A tight filter forces the value-ordered window to be widened (and
// re-read) before the heap fills; a loose one fills it from the first window.

struct SelectivityCase {
    label: &'static str,
    child_count: usize,
}

const SELECTIVITY_CASES: &[SelectivityCase] = &[
    SelectivityCase {
        label: "sparse_1pct",
        child_count: 1_000,
    },
    SelectivityCase {
        label: "moderate_10pct",
        child_count: 10_000,
    },
    SelectivityCase {
        label: "loose_50pct",
        child_count: 50_000,
    },
];

fn bench_selectivity(c: &mut Criterion) {
    let mut group = c.benchmark_group("numeric_top_k/selectivity");

    const N: usize = 100_000;
    let k = NonZeroUsize::new(100).unwrap();
    let index = Index::new(N);

    for case in SELECTIVITY_CASES {
        let ids = child_ids(N, case.child_count);

        preflight(&index, Some(&ids), k);

        group.bench_with_input(BenchmarkId::new("rust", case.label), &(), |b, _| {
            b.iter(|| black_box(run_rust_filtered(&index, &ids, k)));
        });

        group.bench_with_input(BenchmarkId::new("c", case.label), &(), |b, _| {
            b.iter(|| black_box(run_c(&index, Some(&ids), k)));
        });
    }

    group.finish();
}

criterion_group!(benches, bench_filtered, bench_unfiltered, bench_selectivity);
criterion_main!(benches);
