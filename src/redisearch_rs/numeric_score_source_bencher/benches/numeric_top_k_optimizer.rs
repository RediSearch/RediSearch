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
//! Five groups × two sides (rust / c):
//!
//! - `numeric_top_k/filtered`    — sorted-id child passing a tenth of the index.
//! - `numeric_top_k/unfiltered`  — no child filter on the Rust side, the child
//!   the C planner uses for a bare `SORTBY` (a wildcard) on the C side.
//! - `numeric_top_k/selectivity` — child selectivity swept at a fixed index size
//!   and `k`, across the range where the retry expansion starts to bite.
//! - `numeric_top_k/expensive_child` — the same sweep with the ids behind a
//!   union of eight id lists, so that reading, skipping and rewinding the child
//!   costs what a real filter subtree costs.
//! - `numeric_top_k/bounded` — a filter over part of the field, so the ranges at
//!   its edges pass it only in part, down to a range it rejects entirely.
//!
//! Known asymmetries:
//!
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
use rqe_core::DocId;
use rqe_iterators::{IdList, RQEIterator, UnionQuickFlat};
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
    /// `1..=child_count`, otherwise the ids are taken as a sorted-id child —
    /// spread over a union of `union_arity` such lists when that exceeds 1.
    ///
    /// The numeric filter spans `min..max`, each bound inclusive when its flag
    /// is non-zero.
    ///
    /// `ids` is borrowed for the call; the shim allocates each child's own list.
    fn bench_c_optimizer(
        sctx: *mut RedisSearchCtx,
        field_name: *const c_char,
        ids: *const DocId,
        child_count: usize,
        union_arity: usize,
        k: usize,
        ascending: c_int,
        min: f64,
        max: f64,
        min_inclusive: c_int,
        max_inclusive: c_int,
    ) -> usize;
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
/// one value per doc id, a doc table holding every indexed doc id, and the
/// search context the C optimizer reads them through.
struct Index {
    ctx: TestContext,
    n: usize,
}

impl Index {
    /// Index doc ids `1..=n` under [`value_for`].
    fn new(n: usize) -> Self {
        Self::with_values(n, |id| value_for(id, n))
    }

    /// Index doc ids `1..=n`, each under `value(id)`.
    fn with_values(n: usize, value: impl Fn(DocId) -> f64) -> Self {
        let records =
            (1..=n as DocId).map(|id| RSIndexResult::build_numeric(value(id)).doc_id(id).build());
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
///
/// `union_arity` above 1 spreads the ids over that many id lists behind a union,
/// making every child read, skip and rewind cost more than a single list would.
fn run_rust_filtered(
    index: &Index,
    filter: NumericFilter,
    ids: &[DocId],
    union_arity: usize,
    k: NonZeroUsize,
) -> usize {
    let child_estimate = ids.len();
    let window = RangeWindow {
        offset: 0,
        limit: estimate_limit(index.n, child_estimate, k.get()),
    };
    let source = NumericScoreSource::filtered(
        index.tree(),
        filter,
        window,
        ASCENDING,
        RANGE_BATCH_SIZE,
        index.n,
        child_estimate,
    );
    // Fresh owned copies each call, mirroring the C side's per-run allocation.
    let mut count = 0usize;
    if union_arity > 1 {
        let child = UnionQuickFlat::new(partition_ids(ids, union_arity));
        let mut it = new_numeric_top_k_filtered(source, child, k);
        while it.read().unwrap().is_some() {
            count += 1;
        }
    } else {
        let child = IdList::<true>::new(ids.to_vec());
        let mut it = new_numeric_top_k_filtered(source, child, k);
        while it.read().unwrap().is_some() {
            count += 1;
        }
    }
    count
}

/// Spread `ids` round-robin over `arity` sorted id lists.
///
/// Every list then holds ids from across the whole doc-id space, so each step of
/// the union over them interleaves the lists rather than draining one at a time.
fn partition_ids(ids: &[DocId], arity: usize) -> Vec<IdList<'static, true>> {
    (0..arity)
        .map(|slot| {
            let part: Vec<DocId> = ids.iter().skip(slot).step_by(arity).copied().collect();
            IdList::new(part)
        })
        .collect()
}

/// Run one Rust top-k scan with no child filter, returning result count.
fn run_rust_unfiltered(index: &Index, filter: NumericFilter, k: NonZeroUsize) -> usize {
    let source = NumericScoreSource::unfiltered(index.tree(), filter, ASCENDING);
    let mut it = new_numeric_top_k_unfiltered(source, k);
    let mut count = 0usize;
    while it.read().unwrap().is_some() {
        count += 1;
    }
    count
}

/// Run one C OptimizerIterator scan to depletion, returning result count.
/// `ids` of `None` selects the wildcard child, matching the Rust unfiltered run.
fn run_c(
    index: &Index,
    filter: NumericFilter,
    ids: Option<&[DocId]>,
    union_arity: usize,
    k: NonZeroUsize,
) -> usize {
    let (ids_ptr, child_count) = match ids {
        Some(ids) => (ids.as_ptr(), ids.len()),
        None => (ptr::null(), index.n),
    };
    // SAFETY: the search context and its spec outlive the call; `ids_ptr` is
    // either null or a sorted array of `child_count` ids, only read from for the
    // duration of the call. `FIELD` names a numeric field of the spec.
    unsafe {
        bench_c_optimizer(
            index.sctx(),
            FIELD.as_ptr(),
            ids_ptr,
            child_count,
            union_arity,
            k.get(),
            ASCENDING as c_int,
            filter.min,
            filter.max,
            filter.min_inclusive as c_int,
            filter.max_inclusive as c_int,
        )
    }
}

/// Validate that both sides produce `expected` results before the timed loop
/// starts. Catches iterator bugs, shim error paths, and index setup mistakes
/// that would otherwise silently corrupt the Rust/C timing comparison.
fn preflight(
    index: &Index,
    filter: NumericFilter,
    ids: Option<&[DocId]>,
    union_arity: usize,
    k: NonZeroUsize,
    expected: usize,
) {
    let rust_count = match ids {
        Some(ids) => run_rust_filtered(index, filter, ids, union_arity, k),
        None => run_rust_unfiltered(index, filter, k),
    };
    assert_eq!(
        rust_count,
        expected,
        "Rust iterator produced {rust_count} results (expected {expected}) \
         for k={k}, n={n}, child={child:?}",
        n = index.n,
        child = ids.map(<[DocId]>::len),
    );

    let c_count = run_c(index, filter, ids, union_arity, k);
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

            preflight(&index, full_range(), Some(&ids), 1, k, k.get());

            group.bench_with_input(BenchmarkId::new("rust", &param_str), &(n, k), |b, _| {
                b.iter(|| black_box(run_rust_filtered(&index, full_range(), &ids, 1, k)))
            });

            group.bench_with_input(BenchmarkId::new("c", &param_str), &(n, k), |b, _| {
                b.iter(|| black_box(run_c(&index, full_range(), Some(&ids), 1, k)))
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

            preflight(&index, full_range(), None, 1, k, k.get());

            group.bench_with_input(BenchmarkId::new("rust", &param_str), &(n, k), |b, _| {
                b.iter(|| black_box(run_rust_unfiltered(&index, full_range(), k)))
            });

            group.bench_with_input(BenchmarkId::new("c", &param_str), &(n, k), |b, _| {
                b.iter(|| black_box(run_c(&index, full_range(), None, 1, k)))
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

        preflight(&index, full_range(), Some(&ids), 1, k, k.get());

        group.bench_with_input(BenchmarkId::new("rust", case.label), &(), |b, _| {
            b.iter(|| black_box(run_rust_filtered(&index, full_range(), &ids, 1, k)));
        });

        group.bench_with_input(BenchmarkId::new("c", case.label), &(), |b, _| {
            b.iter(|| black_box(run_c(&index, full_range(), Some(&ids), 1, k)));
        });
    }

    group.finish();
}

// ── Expensive child ──────────────────────────────────────────────────────────
//
// The same selectivities as above, but with the ids spread over a union of eight
// id lists instead of one. Every child read, skip and rewind now costs a pass
// over eight sub-iterators, which is what a real text filter subtree looks like.
//
// Both sides walk this child; only the Rust side walks it twice, once to prune
// the batch during materialization and once to intersect it. This group is what
// prices that second walk.

/// Number of id lists the expensive child's union spans.
const UNION_ARITY: usize = 8;

fn bench_expensive_child(c: &mut Criterion) {
    let mut group = c.benchmark_group("numeric_top_k/expensive_child");

    const N: usize = 100_000;
    let k = NonZeroUsize::new(100).unwrap();
    let index = Index::new(N);

    for case in SELECTIVITY_CASES {
        let ids = child_ids(N, case.child_count);

        preflight(&index, full_range(), Some(&ids), UNION_ARITY, k, k.get());

        group.bench_with_input(BenchmarkId::new("rust", case.label), &(), |b, _| {
            b.iter(|| {
                black_box(run_rust_filtered(
                    &index,
                    full_range(),
                    &ids,
                    UNION_ARITY,
                    k,
                ))
            });
        });

        group.bench_with_input(BenchmarkId::new("c", case.label), &(), |b, _| {
            b.iter(|| black_box(run_c(&index, full_range(), Some(&ids), UNION_ARITY, k)));
        });
    }

    group.finish();
}

// ── Bounded filter ───────────────────────────────────────────────────────────
//
// A filter over part of the field. The ranges at its edges hold values on both
// sides of a bound, so both sides read records the filter then rejects — down
// to a range whose only value the filter excludes.

/// Filter over the middle two fifths of [`value_for`]'s span, each bound set
/// between two indexed values so that it falls inside a range rather than on
/// a range's edge.
fn mid_window(n: usize) -> NumericFilter {
    NumericFilter {
        min: 0.3 * n as f64 + 0.5,
        max: 0.7 * n as f64 + 0.5,
        ..NumericFilter::default()
    }
}

/// Whether any range `filter` selects in `tree` holds values outside it.
fn has_partial_range(tree: &NumericRangeTree, filter: &NumericFilter) -> bool {
    tree.find(filter)
        .iter()
        .any(|r| !filter.value_in_range(r.min_val()) || !filter.value_in_range(r.max_val()))
}

fn bench_bounded(c: &mut Criterion) {
    let mut group = c.benchmark_group("numeric_top_k/bounded");

    const N: usize = 100_000;
    let index = Index::new(N);
    let filter = mid_window(N);
    assert!(has_partial_range(index.tree(), &filter));

    for k in [10usize, 1000].map(|k| NonZeroUsize::new(k).unwrap()) {
        let label = format!("unfiltered_mid_k{k}");

        preflight(&index, filter, None, 1, k, k.get());

        group.bench_with_input(BenchmarkId::new("rust", &label), &(), |b, _| {
            b.iter(|| black_box(run_rust_unfiltered(&index, filter, k)));
        });

        group.bench_with_input(BenchmarkId::new("c", &label), &(), |b, _| {
            b.iter(|| black_box(run_c(&index, filter, None, 1, k)));
        });
    }

    let k = NonZeroUsize::new(100).unwrap();
    let ids = child_ids(N, N / 10);

    preflight(&index, filter, Some(&ids), 1, k, k.get());

    group.bench_with_input(
        BenchmarkId::new("rust", "filtered_mid_k100"),
        &(),
        |b, _| {
            b.iter(|| black_box(run_rust_filtered(&index, filter, &ids, 1, k)));
        },
    );

    group.bench_with_input(BenchmarkId::new("c", "filtered_mid_k100"), &(), |b, _| {
        b.iter(|| black_box(run_c(&index, filter, Some(&ids), 1, k)));
    });

    // Every doc shares one value, so its range never splits, and the open lower
    // bound rejects all of it.
    const VALUE: f64 = 5.0;
    let single = Index::with_values(N, |_| VALUE);
    let excluding = NumericFilter {
        min: VALUE,
        max: VALUE + 1.0,
        min_inclusive: false,
        ..NumericFilter::default()
    };
    assert!(has_partial_range(single.tree(), &excluding));

    preflight(&single, excluding, None, 1, k, 0);

    group.bench_with_input(
        BenchmarkId::new("rust", "single_value_excluded"),
        &(),
        |b, _| {
            b.iter(|| black_box(run_rust_unfiltered(&single, excluding, k)));
        },
    );

    group.bench_with_input(
        BenchmarkId::new("c", "single_value_excluded"),
        &(),
        |b, _| {
            b.iter(|| black_box(run_c(&single, excluding, None, 1, k)));
        },
    );

    group.finish();
}

criterion_group!(
    benches,
    bench_filtered,
    bench_unfiltered,
    bench_selectivity,
    bench_expensive_child,
    bench_bounded
);
criterion_main!(benches);
