/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Sweep benchmark for union iterator strategy comparison.
//!
//! Measures Flat vs Heap performance across varying child counts for
//! IdListSorted, Numeric, and Term iterator types, in both Quick and Full modes,
//! with Disjoint and Overlap data patterns.

use std::hint::black_box;
use std::time::Duration;

use criterion::{BenchmarkGroup, BenchmarkId, Criterion, measurement::WallTime};
use ffi::IndexFlags_Index_StoreNumeric;
use inverted_index::{InvertedIndex, RSIndexResult, RSOffsetSlice, full::Full, numeric::Numeric};
use query_term::RSQueryTerm;
use rand::{Rng, SeedableRng, rngs::StdRng};
use rqe_iterators::{
    NoOpChecker, RQEIterator, UnionFullFlat, UnionFullHeap, UnionQuickFlat, UnionQuickHeap,
    id_list::IdListSorted,
    inverted_index::{Numeric as NumericIter, Term as TermIter},
};
use rqe_iterators_test_utils::MockContext;

/// Number of docs per child iterator.
const DOCS_PER_CHILD: u64 = 10_000;
/// Child counts to sweep.
const CHILD_COUNTS: &[usize] = &[2, 4, 8, 12, 16, 20, 24, 32, 48, 64];
/// Seed for reproducible random number generation.
const RNG_SEED: u64 = 42;

const MEASUREMENT_TIME: Duration = Duration::from_secs(3);
const WARMUP_TIME: Duration = Duration::from_millis(200);

/// Controls how child iterator ID ranges overlap.
#[derive(Clone, Copy)]
enum Overlap {
    /// Each child samples from a staggered range with partial overlap.
    /// Child `i` samples from `[i * stride, i * stride + id_range_max]`
    /// where `stride = id_range_max / (2 * num_children)`.
    Low,
    /// Each child samples from a completely separate, sequential range.
    /// Child `i` samples from `[i * id_range_max + 1, (i + 1) * id_range_max]`.
    DisjointSequential,
}

#[derive(Default)]
pub struct Bencher;

impl Bencher {
    fn group<'a>(&self, c: &'a mut Criterion, label: &str) -> BenchmarkGroup<'a, WallTime> {
        let mut group = c.benchmark_group(label);
        group.measurement_time(MEASUREMENT_TIME);
        group.warm_up_time(WARMUP_TIME);
        group
    }

    pub fn bench(&self, c: &mut Criterion) {
        self.idlist_low_overlap(c);
        self.idlist_disjoint_sequential(c);
        self.numeric_low_overlap(c);
        self.numeric_disjoint_sequential(c);
        self.term_low_overlap(c);
        self.term_disjoint_sequential(c);
    }

    // ── IdListSorted ────────────────────────────────────────────────

    fn idlist_low_overlap(&self, c: &mut Criterion) {
        let mut g = self.group(c, "Sweep - IdList - Low Overlap");
        for &n in CHILD_COUNTS {
            let ids = gen_id_lists(n, Overlap::Low);
            bench_idlist_variants(&mut g, n, &ids);
        }
        g.finish();
    }

    fn idlist_disjoint_sequential(&self, c: &mut Criterion) {
        let mut g = self.group(c, "Sweep - IdList - Disjoint Sequential");
        for &n in CHILD_COUNTS {
            let ids = gen_id_lists(n, Overlap::DisjointSequential);
            bench_idlist_variants(&mut g, n, &ids);
        }
        g.finish();
    }

    // ── Numeric ─────────────────────────────────────────────────────

    fn numeric_low_overlap(&self, c: &mut Criterion) {
        let mut g = self.group(c, "Sweep - Numeric - Low Overlap");
        for &n in CHILD_COUNTS {
            let indexes = build_numeric_indexes(n, Overlap::Low);
            let mock_ctx = MockContext::new(0, 0);
            bench_numeric_variants(&mut g, n, &indexes, &mock_ctx);
        }
        g.finish();
    }

    fn numeric_disjoint_sequential(&self, c: &mut Criterion) {
        let mut g = self.group(c, "Sweep - Numeric - Disjoint Sequential");
        for &n in CHILD_COUNTS {
            let indexes = build_numeric_indexes(n, Overlap::DisjointSequential);
            let mock_ctx = MockContext::new(0, 0);
            bench_numeric_variants(&mut g, n, &indexes, &mock_ctx);
        }
        g.finish();
    }

    // ── Term (Full encoding) ────────────────────────────────────────

    fn term_low_overlap(&self, c: &mut Criterion) {
        let mut g = self.group(c, "Sweep - Term - Low Overlap");
        for &n in CHILD_COUNTS {
            let indexes = build_term_indexes(n, Overlap::Low);
            let mock_ctx = MockContext::new(0, 0);
            bench_term_variants(&mut g, n, &indexes, &mock_ctx);
        }
        g.finish();
    }

    fn term_disjoint_sequential(&self, c: &mut Criterion) {
        let mut g = self.group(c, "Sweep - Term - Disjoint Sequential");
        for &n in CHILD_COUNTS {
            let indexes = build_term_indexes(n, Overlap::DisjointSequential);
            let mock_ctx = MockContext::new(0, 0);
            bench_term_variants(&mut g, n, &indexes, &mock_ctx);
        }
        g.finish();
    }
}



// ── Data generation ─────────────────────────────────────────────────

/// Compute the (range_start, range_end) for a given child based on overlap strategy.
fn child_range(child_idx: usize, num_children: usize, overlap: Overlap) -> (u64, u64) {
    let id_range_max = DOCS_PER_CHILD * 2;
    match overlap {
        Overlap::Low => {
            let stride = id_range_max / (2 * num_children as u64).max(1);
            let start = (child_idx as u64) * stride + 1;
            let end = start + id_range_max - 1;
            (start, end)
        }
        Overlap::DisjointSequential => {
            let start = (child_idx as u64) * id_range_max + 1;
            let end = start + id_range_max - 1;
            (start, end)
        }
    }
}

/// Generate random sorted, deduplicated ID lists for each child.
fn gen_id_lists(num_children: usize, overlap: Overlap) -> Vec<Vec<u64>> {
    let mut rng = StdRng::seed_from_u64(RNG_SEED);
    (0..num_children)
        .map(|i| {
            let (range_start, range_end) = child_range(i, num_children, overlap);
            let mut ids: Vec<u64> = (0..DOCS_PER_CHILD)
                .map(|_| rng.random_range(range_start..=range_end))
                .collect();
            ids.sort_unstable();
            ids.dedup();
            ids
        })
        .collect()
}

fn build_numeric_indexes(num_children: usize, overlap: Overlap) -> Vec<InvertedIndex<Numeric>> {
    let id_lists = gen_id_lists(num_children, overlap);
    id_lists
        .into_iter()
        .map(|ids| {
            let mut ii = InvertedIndex::<Numeric>::new(IndexFlags_Index_StoreNumeric);
            for doc_id in ids {
                let record = RSIndexResult::build_numeric(doc_id as f64)
                    .doc_id(doc_id)
                    .build();
                let _ = ii.add_record(&record);
            }
            ii
        })
        .collect()
}

fn build_term_indexes(num_children: usize, overlap: Overlap) -> Vec<InvertedIndex<Full>> {
    let offsets = vec![0u8, 1, 2, 3, 4, 5, 6, 7, 8, 9];
    let flags = ffi::IndexFlags_Index_StoreFreqs
        | ffi::IndexFlags_Index_StoreTermOffsets
        | ffi::IndexFlags_Index_StoreFieldFlags
        | ffi::IndexFlags_Index_StoreByteOffsets;

    let id_lists = gen_id_lists(num_children, overlap);
    id_lists
        .into_iter()
        .map(|ids| {
            let mut ii = InvertedIndex::<Full>::new(flags);
            for doc_id in ids {
                let record = RSIndexResult::build_term()
                    .doc_id(doc_id)
                    .field_mask(1)
                    .frequency(1)
                    .borrowed_record(None, RSOffsetSlice::from_slice(&offsets))
                    .build();
                ii.add_record(&record).expect("failed to add record");
            }
            ii
        })
        .collect()
}

// ── Benchmark variant helpers ───────────────────────────────────────

fn bench_idlist_variants(g: &mut BenchmarkGroup<'_, WallTime>, n: usize, ids: &[Vec<u64>]) {
    g.bench_with_input(BenchmarkId::new("Flat Quick", n), &n, |b, _| {
        b.iter_batched_ref(
            || UnionQuickFlat::new(ids.iter().map(|v| IdListSorted::new(v.clone())).collect()),
            |it| { while let Ok(Some(r)) = it.read() { black_box(r); } },
            criterion::BatchSize::SmallInput,
        );
    });
    g.bench_with_input(BenchmarkId::new("Heap Quick", n), &n, |b, _| {
        b.iter_batched_ref(
            || UnionQuickHeap::new(ids.iter().map(|v| IdListSorted::new(v.clone())).collect()),
            |it| { while let Ok(Some(r)) = it.read() { black_box(r); } },
            criterion::BatchSize::SmallInput,
        );
    });
    g.bench_with_input(BenchmarkId::new("Flat Full", n), &n, |b, _| {
        b.iter_batched_ref(
            || UnionFullFlat::new(ids.iter().map(|v| IdListSorted::new(v.clone())).collect()),
            |it| { while let Ok(Some(r)) = it.read() { black_box(r); } },
            criterion::BatchSize::SmallInput,
        );
    });
    g.bench_with_input(BenchmarkId::new("Heap Full", n), &n, |b, _| {
        b.iter_batched_ref(
            || UnionFullHeap::new(ids.iter().map(|v| IdListSorted::new(v.clone())).collect()),
            |it| { while let Ok(Some(r)) = it.read() { black_box(r); } },
            criterion::BatchSize::SmallInput,
        );
    });
}

fn bench_numeric_variants(
    g: &mut BenchmarkGroup<'_, WallTime>,
    n: usize,
    indexes: &[InvertedIndex<Numeric>],
    mock_ctx: &MockContext,
) {
    let _ = mock_ctx; // keep alive for the duration of benchmarks
    let make_children = || -> Vec<_> {
        indexes.iter().map(|ii| {
            // SAFETY: range_tree is None so no pointer invariants apply.
            unsafe { NumericIter::new(ii.reader(), NoOpChecker, None, None, None) }
        }).collect()
    };
    g.bench_with_input(BenchmarkId::new("Flat Quick", n), &n, |b, _| {
        b.iter_batched_ref(
            || UnionQuickFlat::new(make_children()),
            |it| { while let Ok(Some(r)) = it.read() { black_box(r); } },
            criterion::BatchSize::SmallInput,
        );
    });
    g.bench_with_input(BenchmarkId::new("Heap Quick", n), &n, |b, _| {
        b.iter_batched_ref(
            || UnionQuickHeap::new(make_children()),
            |it| { while let Ok(Some(r)) = it.read() { black_box(r); } },
            criterion::BatchSize::SmallInput,
        );
    });
    g.bench_with_input(BenchmarkId::new("Flat Full", n), &n, |b, _| {
        b.iter_batched_ref(
            || UnionFullFlat::new(make_children()),
            |it| { while let Ok(Some(r)) = it.read() { black_box(r); } },
            criterion::BatchSize::SmallInput,
        );
    });
    g.bench_with_input(BenchmarkId::new("Heap Full", n), &n, |b, _| {
        b.iter_batched_ref(
            || UnionFullHeap::new(make_children()),
            |it| { while let Ok(Some(r)) = it.read() { black_box(r); } },
            criterion::BatchSize::SmallInput,
        );
    });
}

fn bench_term_variants(
    g: &mut BenchmarkGroup<'_, WallTime>,
    n: usize,
    indexes: &[InvertedIndex<Full>],
    mock_ctx: &MockContext,
) {
    let sctx = mock_ctx.sctx();
    let make_children = || -> Vec<_> {
        indexes.iter().map(|ii| {
            // SAFETY: sctx points to a valid, zeroed RedisSearchCtx with a valid spec.
            unsafe {
                TermIter::new(ii.reader(), sctx, RSQueryTerm::new("term", 1, 0), 1.0, NoOpChecker)
            }
        }).collect()
    };
    g.bench_with_input(BenchmarkId::new("Flat Quick", n), &n, |b, _| {
        b.iter_batched_ref(
            || UnionQuickFlat::new(make_children()),
            |it| { while let Ok(Some(r)) = it.read() { black_box(r); } },
            criterion::BatchSize::SmallInput,
        );
    });
    g.bench_with_input(BenchmarkId::new("Heap Quick", n), &n, |b, _| {
        b.iter_batched_ref(
            || UnionQuickHeap::new(make_children()),
            |it| { while let Ok(Some(r)) = it.read() { black_box(r); } },
            criterion::BatchSize::SmallInput,
        );
    });
    g.bench_with_input(BenchmarkId::new("Flat Full", n), &n, |b, _| {
        b.iter_batched_ref(
            || UnionFullFlat::new(make_children()),
            |it| { while let Ok(Some(r)) = it.read() { black_box(r); } },
            criterion::BatchSize::SmallInput,
        );
    });
    g.bench_with_input(BenchmarkId::new("Heap Full", n), &n, |b, _| {
        b.iter_batched_ref(
            || UnionFullHeap::new(make_children()),
            |it| { while let Ok(Some(r)) = it.read() { black_box(r); } },
            criterion::BatchSize::SmallInput,
        );
    });
}