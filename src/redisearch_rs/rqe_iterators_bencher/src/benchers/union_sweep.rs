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
//!
//! ## ID Generation
//!
//! Doc IDs are generated linearly based on the number of children:
//! `total_docs = num_children * DOCS_PER_CHILD`.
//!
//! For each doc ID, we randomly decide which children will contain it based
//! on the overlap strategy.

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

use crate::ffi::{self as bench_ffi, IteratorStatus_ITERATOR_OK};

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
    /// High overlap: each doc appears in many children.
    /// For each doc, randomly choose 2 to 75% of children to include it.
    High,
    /// Low overlap: each doc appears in 1-2 children.
    /// For each doc, randomly choose 1-2 children to include it.
    Low,
    /// Disjoint: each doc appears in exactly one randomly chosen child.
    Disjoint,
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
        self.idlist_high_overlap(c);
        self.idlist_low_overlap(c);
        self.idlist_disjoint(c);
        self.numeric_high_overlap(c);
        self.numeric_low_overlap(c);
        self.numeric_disjoint(c);
        self.term_high_overlap(c);
        self.term_low_overlap(c);
        self.term_disjoint(c);
    }

    // ── IdListSorted ────────────────────────────────────────────────

    fn idlist_high_overlap(&self, c: &mut Criterion) {
        let mut g = self.group(c, "Sweep - IdList - High Overlap");
        for &n in CHILD_COUNTS {
            let ids = gen_id_lists(n, Overlap::High);
            bench_idlist_variants(&mut g, n, &ids);
        }
        g.finish();
    }

    fn idlist_low_overlap(&self, c: &mut Criterion) {
        let mut g = self.group(c, "Sweep - IdList - Low Overlap");
        for &n in CHILD_COUNTS {
            let ids = gen_id_lists(n, Overlap::Low);
            bench_idlist_variants(&mut g, n, &ids);
        }
        g.finish();
    }

    fn idlist_disjoint(&self, c: &mut Criterion) {
        let mut g = self.group(c, "Sweep - IdList - Disjoint");
        for &n in CHILD_COUNTS {
            let ids = gen_id_lists(n, Overlap::Disjoint);
            bench_idlist_variants(&mut g, n, &ids);
        }
        g.finish();
    }

    // ── Numeric ─────────────────────────────────────────────────────

    fn numeric_high_overlap(&self, c: &mut Criterion) {
        let mut g = self.group(c, "Sweep - Numeric - High Overlap");
        for &n in CHILD_COUNTS {
            let indexes = build_numeric_indexes(n, Overlap::High);
            let mock_ctx = MockContext::new(0, 0);
            bench_numeric_variants(&mut g, n, &indexes, &mock_ctx);
        }
        g.finish();
    }

    fn numeric_low_overlap(&self, c: &mut Criterion) {
        let mut g = self.group(c, "Sweep - Numeric - Low Overlap");
        for &n in CHILD_COUNTS {
            let indexes = build_numeric_indexes(n, Overlap::Low);
            let mock_ctx = MockContext::new(0, 0);
            bench_numeric_variants(&mut g, n, &indexes, &mock_ctx);
        }
        g.finish();
    }

    fn numeric_disjoint(&self, c: &mut Criterion) {
        let mut g = self.group(c, "Sweep - Numeric - Disjoint");
        for &n in CHILD_COUNTS {
            let indexes = build_numeric_indexes(n, Overlap::Disjoint);
            let mock_ctx = MockContext::new(0, 0);
            bench_numeric_variants(&mut g, n, &indexes, &mock_ctx);
        }
        g.finish();
    }

    // ── Term (Full encoding) ────────────────────────────────────────

    fn term_high_overlap(&self, c: &mut Criterion) {
        let mut g = self.group(c, "Sweep - Term - High Overlap");
        for &n in CHILD_COUNTS {
            let id_lists = gen_id_lists(n, Overlap::High);
            let indexes = build_term_indexes(&id_lists);
            let c_indexes = build_term_indexes_c(&id_lists);
            let mock_ctx = MockContext::new(0, 0);
            bench_term_variants(&mut g, n, &indexes, &c_indexes, &mock_ctx);
        }
        g.finish();
    }

    fn term_low_overlap(&self, c: &mut Criterion) {
        let mut g = self.group(c, "Sweep - Term - Low Overlap");
        for &n in CHILD_COUNTS {
            let id_lists = gen_id_lists(n, Overlap::Low);
            let indexes = build_term_indexes(&id_lists);
            let c_indexes = build_term_indexes_c(&id_lists);
            let mock_ctx = MockContext::new(0, 0);
            bench_term_variants(&mut g, n, &indexes, &c_indexes, &mock_ctx);
        }
        g.finish();
    }

    fn term_disjoint(&self, c: &mut Criterion) {
        let mut g = self.group(c, "Sweep - Term - Disjoint");
        for &n in CHILD_COUNTS {
            let id_lists = gen_id_lists(n, Overlap::Disjoint);
            let indexes = build_term_indexes(&id_lists);
            let c_indexes = build_term_indexes_c(&id_lists);
            let mock_ctx = MockContext::new(0, 0);
            bench_term_variants(&mut g, n, &indexes, &c_indexes, &mock_ctx);
        }
        g.finish();
    }
}



// ── Data generation ─────────────────────────────────────────────────

/// Generate ID lists for each child using a doc-centric strategy.
///
/// The doc ID range is linear: `[1, num_children * DOCS_PER_CHILD]`.
/// For each doc ID, we randomly decide which children will contain it
/// based on the overlap strategy. IDs are produced in ascending order
/// per child (no sorting/dedup required).
fn gen_id_lists(num_children: usize, overlap: Overlap) -> Vec<Vec<u64>> {
    let mut rng = StdRng::seed_from_u64(RNG_SEED);
    let total_docs = (num_children as u64) * DOCS_PER_CHILD;

    let mut children_ids: Vec<Vec<u64>> = vec![Vec::new(); num_children];

    for doc_id in 1..=total_docs {
        let selected_children: Vec<usize> = match overlap {
            Overlap::High => {
                let max_children = (num_children * 3 / 4).max(1);
                let min_children = 2.min(max_children);
                let num_selected = rng.random_range(min_children..=max_children);
                random_sample(&mut rng, num_children, num_selected)
            }
            Overlap::Low => {
                let num_selected = rng.random_range(1..=2.min(num_children));
                random_sample(&mut rng, num_children, num_selected)
            }
            Overlap::Disjoint => {
                let child_idx = rng.random_range(0..num_children);
                vec![child_idx]
            }
        };

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

fn build_term_indexes(id_lists: &[Vec<u64>]) -> Vec<InvertedIndex<Full>> {
    let offsets = vec![0u8, 1, 2, 3, 4, 5, 6, 7, 8, 9];
    let flags = ffi::IndexFlags_Index_StoreFreqs
        | ffi::IndexFlags_Index_StoreTermOffsets
        | ffi::IndexFlags_Index_StoreFieldFlags
        | ffi::IndexFlags_Index_StoreByteOffsets;

    id_lists
        .iter()
        .map(|ids| {
            let mut ii = InvertedIndex::<Full>::new(flags);
            for &doc_id in ids {
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

fn build_term_indexes_c(id_lists: &[Vec<u64>]) -> Vec<bench_ffi::InvertedIndex> {
    let offsets = vec![0u8, 1, 2, 3, 4, 5, 6, 7, 8, 9];
    let flags = ffi::IndexFlags_Index_StoreFreqs
        | ffi::IndexFlags_Index_StoreTermOffsets
        | ffi::IndexFlags_Index_StoreFieldFlags
        | ffi::IndexFlags_Index_StoreByteOffsets;

    id_lists
        .iter()
        .map(|ids| {
            let ii = bench_ffi::InvertedIndex::new(flags);
            for &doc_id in ids {
                ii.write_term_entry(doc_id, 1, 1, None, &offsets);
            }
            ii
        })
        .collect()
}

// ── Benchmark variant helpers ───────────────────────────────────────

fn bench_idlist_variants(g: &mut BenchmarkGroup<'_, WallTime>, n: usize, ids: &[Vec<u64>]) {
    // ── Rust ─────────────────────────────────────────────────────────
    g.bench_with_input(BenchmarkId::new("Flat Quick/Rust", n), &n, |b, _| {
        b.iter_batched_ref(
            || UnionQuickFlat::new(ids.iter().map(|v| IdListSorted::new(v.clone())).collect()),
            |it| { while let Ok(Some(r)) = it.read() { black_box(r); } },
            criterion::BatchSize::SmallInput,
        );
    });
    g.bench_with_input(BenchmarkId::new("Heap Quick/Rust", n), &n, |b, _| {
        b.iter_batched_ref(
            || UnionQuickHeap::new(ids.iter().map(|v| IdListSorted::new(v.clone())).collect()),
            |it| { while let Ok(Some(r)) = it.read() { black_box(r); } },
            criterion::BatchSize::SmallInput,
        );
    });
    g.bench_with_input(BenchmarkId::new("Flat Full/Rust", n), &n, |b, _| {
        b.iter_batched_ref(
            || UnionFullFlat::new(ids.iter().map(|v| IdListSorted::new(v.clone())).collect()),
            |it| { while let Ok(Some(r)) = it.read() { black_box(r); } },
            criterion::BatchSize::SmallInput,
        );
    });
    g.bench_with_input(BenchmarkId::new("Heap Full/Rust", n), &n, |b, _| {
        b.iter_batched_ref(
            || UnionFullHeap::new(ids.iter().map(|v| IdListSorted::new(v.clone())).collect()),
            |it| { while let Ok(Some(r)) = it.read() { black_box(r); } },
            criterion::BatchSize::SmallInput,
        );
    });

    // ── C ────────────────────────────────────────────────────────────
    g.bench_with_input(BenchmarkId::new("Flat Quick/C", n), &n, |b, _| {
        b.iter_batched_ref(
            || bench_ffi::QueryIterator::new_union(ids, 1.0, false, true),
            |it| {
                while it.read() == IteratorStatus_ITERATOR_OK { black_box(it.current()); }
                it.free();
            },
            criterion::BatchSize::SmallInput,
        );
    });
    g.bench_with_input(BenchmarkId::new("Heap Quick/C", n), &n, |b, _| {
        b.iter_batched_ref(
            || bench_ffi::QueryIterator::new_union(ids, 1.0, true, true),
            |it| {
                while it.read() == IteratorStatus_ITERATOR_OK { black_box(it.current()); }
                it.free();
            },
            criterion::BatchSize::SmallInput,
        );
    });
    g.bench_with_input(BenchmarkId::new("Flat Full/C", n), &n, |b, _| {
        b.iter_batched_ref(
            || bench_ffi::QueryIterator::new_union(ids, 1.0, false, false),
            |it| {
                while it.read() == IteratorStatus_ITERATOR_OK { black_box(it.current()); }
                it.free();
            },
            criterion::BatchSize::SmallInput,
        );
    });
    g.bench_with_input(BenchmarkId::new("Heap Full/C", n), &n, |b, _| {
        b.iter_batched_ref(
            || bench_ffi::QueryIterator::new_union(ids, 1.0, true, false),
            |it| {
                while it.read() == IteratorStatus_ITERATOR_OK { black_box(it.current()); }
                it.free();
            },
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
    g.bench_with_input(BenchmarkId::new("Flat Quick/Rust", n), &n, |b, _| {
        b.iter_batched_ref(
            || UnionQuickFlat::new(make_children()),
            |it| { while let Ok(Some(r)) = it.read() { black_box(r); } },
            criterion::BatchSize::SmallInput,
        );
    });
    g.bench_with_input(BenchmarkId::new("Heap Quick/Rust", n), &n, |b, _| {
        b.iter_batched_ref(
            || UnionQuickHeap::new(make_children()),
            |it| { while let Ok(Some(r)) = it.read() { black_box(r); } },
            criterion::BatchSize::SmallInput,
        );
    });
    g.bench_with_input(BenchmarkId::new("Flat Full/Rust", n), &n, |b, _| {
        b.iter_batched_ref(
            || UnionFullFlat::new(make_children()),
            |it| { while let Ok(Some(r)) = it.read() { black_box(r); } },
            criterion::BatchSize::SmallInput,
        );
    });
    g.bench_with_input(BenchmarkId::new("Heap Full/Rust", n), &n, |b, _| {
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
    c_indexes: &[bench_ffi::InvertedIndex],
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

    // ── Rust ─────────────────────────────────────────────────────────
    g.bench_with_input(BenchmarkId::new("Flat Quick/Rust", n), &n, |b, _| {
        b.iter_batched_ref(
            || UnionQuickFlat::new(make_children()),
            |it| { while let Ok(Some(r)) = it.read() { black_box(r); } },
            criterion::BatchSize::SmallInput,
        );
    });
    g.bench_with_input(BenchmarkId::new("Heap Quick/Rust", n), &n, |b, _| {
        b.iter_batched_ref(
            || UnionQuickHeap::new(make_children()),
            |it| { while let Ok(Some(r)) = it.read() { black_box(r); } },
            criterion::BatchSize::SmallInput,
        );
    });
    g.bench_with_input(BenchmarkId::new("Flat Full/Rust", n), &n, |b, _| {
        b.iter_batched_ref(
            || UnionFullFlat::new(make_children()),
            |it| { while let Ok(Some(r)) = it.read() { black_box(r); } },
            criterion::BatchSize::SmallInput,
        );
    });
    g.bench_with_input(BenchmarkId::new("Heap Full/Rust", n), &n, |b, _| {
        b.iter_batched_ref(
            || UnionFullHeap::new(make_children()),
            |it| { while let Ok(Some(r)) = it.read() { black_box(r); } },
            criterion::BatchSize::SmallInput,
        );
    });

    // ── C ────────────────────────────────────────────────────────────
    g.bench_with_input(BenchmarkId::new("Flat Quick/C", n), &n, |b, _| {
        b.iter_batched_ref(
            || bench_ffi::QueryIterator::new_union_term(c_indexes, false, true),
            |it| {
                while it.read() == IteratorStatus_ITERATOR_OK { black_box(it.current()); }
                it.free();
            },
            criterion::BatchSize::SmallInput,
        );
    });
    g.bench_with_input(BenchmarkId::new("Heap Quick/C", n), &n, |b, _| {
        b.iter_batched_ref(
            || bench_ffi::QueryIterator::new_union_term(c_indexes, true, true),
            |it| {
                while it.read() == IteratorStatus_ITERATOR_OK { black_box(it.current()); }
                it.free();
            },
            criterion::BatchSize::SmallInput,
        );
    });
    g.bench_with_input(BenchmarkId::new("Flat Full/C", n), &n, |b, _| {
        b.iter_batched_ref(
            || bench_ffi::QueryIterator::new_union_term(c_indexes, false, false),
            |it| {
                while it.read() == IteratorStatus_ITERATOR_OK { black_box(it.current()); }
                it.free();
            },
            criterion::BatchSize::SmallInput,
        );
    });
    g.bench_with_input(BenchmarkId::new("Heap Full/C", n), &n, |b, _| {
        b.iter_batched_ref(
            || bench_ffi::QueryIterator::new_union_term(c_indexes, true, false),
            |it| {
                while it.read() == IteratorStatus_ITERATOR_OK { black_box(it.current()); }
                it.free();
            },
            criterion::BatchSize::SmallInput,
        );
    });
}