/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Numeric top-k benchmark for the Rust `NumericTopKIterator`.
//!
//! Every group reads a numeric range tree built once per index size by
//! `TestContext::numeric`, walking the field's ranges in value order under an
//! `offset`/`limit` window that widens when it drains before the heap is full.
//! The source runs on its default `NoTimeoutChecker`, so no timeout bookkeeping is
//! priced in.
//!
//! Groups:
//!
//! - `numeric_top_k/filtered`    — sorted-id child passing a tenth of the index.
//! - `numeric_top_k/unfiltered`  — no child filter.
//! - `numeric_top_k/selectivity` — child selectivity swept at a fixed index size
//!   and `k`, across the range where the retry expansion starts to bite.
//! - `numeric_top_k/expensive_child` — the same sweep with the ids behind a
//!   union of eight id lists, so that reading, skipping and rewinding the child
//!   costs what a real filter subtree costs.
//! - `numeric_top_k/bounded` — a filter over part of the field, so the ranges at
//!   its edges pass it only in part, down to a range it rejects entirely.
//! - `numeric_top_k/multivalue` — every document holds two values in different
//!   ranges, which the source must de-duplicate onto the best one.

// Pull in the lib to ensure FFI stubs and mock allocator symbols are linked.
use numeric_score_source_bencher as _;

use std::{hint::black_box, iter, num::NonZeroUsize};

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use index_result::RSIndexResult;
use inverted_index::NumericFilter;
use numeric_range_tree::{NumericRangeTree, RangeWindow};
use numeric_score_source::{
    NumericScoreSource, new_numeric_top_k_filtered, new_numeric_top_k_unfiltered,
};
use rqe_core::DocId;
use rqe_iterators::{IdList, RQEIterator, UnionQuickFlat};
use rqe_iterators_test_utils::TestContext;

/// Sort direction every run uses (`SORTBY <field> ASC`).
const ASCENDING: bool = true;

/// Ranges materialized per batch, matching the source's own default.
const RANGE_BATCH_SIZE: usize = 8;

/// Whole-field filter: matches every value.
///
/// `NumericFilter::default()` bounds the window at `0.0..f64::MAX`; the `±inf`
/// bounds here match the whole-field span a bare `SORTBY` scans.
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
/// must be read in value order to expect `k` of them to pass the child.
fn estimate_limit(num_docs: usize, child_estimate: usize, k: usize) -> usize {
    if num_docs == 0 || child_estimate == 0 {
        return 0;
    }
    let ratio = child_estimate as f64 / num_docs as f64;
    (k as f64 / ratio) as usize + 1
}

/// An index of `n` documents: a numeric range tree over one value per doc id.
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
        Self {
            ctx: TestContext::numeric(records, false),
            n,
        }
    }

    /// Index doc ids `1..=n`, each under both [`value_for`] and the value half
    /// the field away, so every document spans two ranges.
    fn multivalued(n: usize) -> Self {
        let ctx = TestContext::numeric(iter::empty(), false);
        let tree = ctx.numeric_range_tree_mut();
        for id in 1..=n as DocId {
            let value = value_for(id, n);
            tree.add(id, value, false, true, 0);
            tree.add(id, (value + (n / 2) as f64) % n as f64, false, true, 0);
        }
        assert!(tree.has_multivalued_docs());
        Self { ctx, n }
    }

    fn tree(&self) -> &NumericRangeTree {
        self.ctx.numeric_range_tree_ref()
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
        NumericFilter {
            ascending: ASCENDING,
            ..filter
        },
        window,
        index.n,
    )
    .with_range_batch_size(RANGE_BATCH_SIZE);
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
    let source = NumericScoreSource::unfiltered(
        index.tree(),
        NumericFilter {
            ascending: ASCENDING,
            ..filter
        },
    );
    let mut it = new_numeric_top_k_unfiltered(source, k);
    let mut count = 0usize;
    while it.read().unwrap().is_some() {
        count += 1;
    }
    count
}

/// Validate that the iterator produces `expected` results before the timed loop
/// starts. Catches iterator bugs and index setup mistakes that would otherwise
/// silently skew the timings.
fn preflight(
    index: &Index,
    filter: NumericFilter,
    ids: Option<&[DocId]>,
    union_arity: usize,
    k: NonZeroUsize,
    expected: usize,
) {
    let count = match ids {
        Some(ids) => run_rust_filtered(index, filter, ids, union_arity, k),
        None => run_rust_unfiltered(index, filter, k),
    };
    assert_eq!(
        count,
        expected,
        "iterator produced {count} results (expected {expected}) \
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
    }

    group.finish();
}

// ── Expensive child ──────────────────────────────────────────────────────────
//
// The same selectivities as above, but with the ids spread over a union of eight
// id lists instead of one. Every child read, skip and rewind now costs a pass
// over eight sub-iterators, which is what a real text filter subtree looks like.
//
// The source leapfrogs this child against the merged doc ids of each batch, so
// this group prices its seeks.

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
    }

    group.finish();
}

// ── Bounded filter ───────────────────────────────────────────────────────────
//
// A filter over part of the field. The ranges at its edges hold values on both
// sides of a bound, so the source reads records the filter then rejects — down
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

    group.finish();
}

// ── Multivalue ───────────────────────────────────────────────────────────────
//
// A multivalue field cannot be leapfrogged against the child: only a full read
// of each batch keeps a document on its best value. This group prices that
// fallback.

fn bench_multivalue(c: &mut Criterion) {
    let mut group = c.benchmark_group("numeric_top_k/multivalue");

    const N: usize = 100_000;
    let k = NonZeroUsize::new(100).unwrap();
    let index = Index::multivalued(N);
    let ids = child_ids(N, N / 10);

    preflight(&index, full_range(), None, 1, k, k.get());

    group.bench_with_input(BenchmarkId::new("rust", "unfiltered"), &(), |b, _| {
        b.iter(|| black_box(run_rust_unfiltered(&index, full_range(), k)));
    });

    for union_arity in [1, UNION_ARITY] {
        let label = format!("filtered_10pct_union{union_arity}");

        preflight(&index, full_range(), Some(&ids), union_arity, k, k.get());

        group.bench_with_input(BenchmarkId::new("rust", &label), &(), |b, _| {
            b.iter(|| {
                black_box(run_rust_filtered(
                    &index,
                    full_range(),
                    &ids,
                    union_arity,
                    k,
                ))
            });
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_filtered,
    bench_unfiltered,
    bench_selectivity,
    bench_expensive_child,
    bench_bounded,
    bench_multivalue
);
criterion_main!(benches);
