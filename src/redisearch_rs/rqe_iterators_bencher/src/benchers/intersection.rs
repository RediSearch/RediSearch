/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Benchmark intersection iterator.
//!
//! Compares C and Rust implementations of the intersection iterator
//! using SortedIdList as child iterators.

use std::{hint::black_box, time::Duration};

use criterion::{
    BenchmarkGroup, Criterion,
    measurement::{Measurement, WallTime},
};
use ffi::{
    IndexFlags, IndexFlags_Index_StoreByteOffsets, IndexFlags_Index_StoreFieldFlags,
    IndexFlags_Index_StoreFreqs, IndexFlags_Index_StoreTermOffsets, IteratorStatus_ITERATOR_OK,
};
use inverted_index::{InvertedIndex, RSIndexResult, RSOffsetSlice, full::Full};
use query_term::RSQueryTerm;
use rqe_iterators::{
    Intersection, NoOpChecker, RQEIterator, id_list::IdListSorted, inverted_index::Term,
};
use rqe_iterators_test_utils::MockContext;

use crate::ffi::{self, InvertedIndex as CInvertedIndex};

#[derive(Default)]
pub struct Bencher;

/// Number of children values to sweep in parametric benchmarks.
const NUM_CHILDREN_CASES: &[usize] = &[2, 5, 10, 20];

/// Overlap percentage scenarios for parametric benchmarks.
///
/// Each entry is `(label, pct)` where `pct`% of `CHILD_SIZE` IDs are common to all children.
const OVERLAP_CASES: &[(&str, u8)] = &[
    ("1pct", 1),
    ("5pct", 5),
    ("10pct", 10),
    ("20pct", 20),
    ("50pct", 50),
    ("80pct", 80),
];

/// Number of child iterators for slop/order benchmarks.
const NUM_CHILDREN: usize = 5;
/// Size of each child iterator's ID list.
const CHILD_SIZE: u64 = 100_000;
/// Step size for skip_to benchmarks
const STEP: u64 = 10;

/// Number of documents for slop/order benchmarks.
const NUM_DOCS: u64 = 100_000;

/// Weight applied to intersection iterators (neutral value that does not skew scoring).
const WEIGHT: f64 = 1.0;

/// Whether to prioritize union children during intersection child sorting.
const PRIORITIZE_UNION_CHILDREN: bool = false;

/// Mirrors `INDEX_DEFAULT_FLAGS` from `spec.h`, a realistic production configuration:
/// - `StoreTermOffsets` is required for positional data; without it `max_slop`/`in_order`
///   benchmarks would be meaningless.
/// - `StoreByteOffsets` instructs the `Full` encoder to also write byte-level offsets per entry (used by
///   highlighting), keeping the benchmark data representative of a real index.
const FLAGS: IndexFlags = IndexFlags_Index_StoreFreqs
    | IndexFlags_Index_StoreTermOffsets
    | IndexFlags_Index_StoreFieldFlags
    | IndexFlags_Index_StoreByteOffsets;

/// Generate IDs with exact overlap control.
///
/// Each of the `num_children` children contains exactly `CHILD_SIZE` IDs:
/// - IDs `1..=common_count` appear in **all** children (the intersection result set).
/// - Each child then gets `CHILD_SIZE - common_count` IDs that are unique to it.
///
/// `overlap_pct` controls what fraction of `CHILD_SIZE` ends up in the intersection;
/// e.g. `overlap_pct=20` ⇒ ~20 000 common IDs out of 100 000 per child.
fn parametric_ids(num_children: usize, overlap_pct: u8) -> Vec<Vec<u64>> {
    let common_count = CHILD_SIZE * overlap_pct as u64 / 100;
    let unique_per_child = CHILD_SIZE - common_count;
    (0..num_children)
        .map(|i| {
            // Unique block for child i starts right after all previous children's unique blocks.
            let unique_start = common_count + 1 + i as u64 * unique_per_child;
            let mut ids: Vec<u64> = (1..=common_count).collect();
            ids.extend(unique_start..unique_start + unique_per_child);
            // ids is already sorted: common range then unique range (non-overlapping)
            ids
        })
        .collect()
}

/// Generate IDs for varying sizes scenario (realistic workload).
///
/// First child is smallest (drives the intersection), others are progressively larger.
/// All children share IDs 1..=smallest_size so the intersection equals the smallest child.
fn varying_size_ids() -> Vec<Vec<u64>> {
    (0..NUM_CHILDREN)
        .map(|i| {
            let size = CHILD_SIZE * (i as u64 + 1) / NUM_CHILDREN as u64;
            (1..=size.max(1)).collect()
        })
        .collect()
}

/// Convert ID vectors to Rust SortedIdList iterators.
fn ids_to_rust_children(ids: Vec<Vec<u64>>) -> Vec<IdListSorted<'static>> {
    ids.into_iter().map(IdListSorted::new).collect()
}

fn new_query_term() -> Box<RSQueryTerm> {
    RSQueryTerm::new("term", 1, 0)
}

/// Build two Rust `InvertedIndex<Full>` instances.
///
/// `first_pos` and `second_pos` are the (constant) positions used for every document.
///
/// NOTE: This function exists because intersection logic when using `max_slop` and `in_order`
/// need positional information, which `IdList` lacks.
fn make_rust_indexes(first_pos: u8, second_pos: u8) -> (InvertedIndex<Full>, InvertedIndex<Full>) {
    let mut first = InvertedIndex::<Full>::new(FLAGS);
    let mut second = InvertedIndex::<Full>::new(FLAGS);
    for doc_id in 1..=NUM_DOCS {
        first
            .add_record(
                &RSIndexResult::build_term()
                    .borrowed_record(
                        Some(RSQueryTerm::new("first", 1, 0)),
                        RSOffsetSlice::from_slice(&[first_pos]),
                    )
                    .doc_id(doc_id)
                    .field_mask(1u128)
                    .frequency(1)
                    .build(),
            )
            .unwrap();
        second
            .add_record(
                &RSIndexResult::build_term()
                    .borrowed_record(
                        Some(RSQueryTerm::new("second", 2, 0)),
                        RSOffsetSlice::from_slice(&[second_pos]),
                    )
                    .doc_id(doc_id)
                    .field_mask(1u128)
                    .frequency(1)
                    .build(),
            )
            .unwrap();
    }
    (first, second)
}

/// Build two C `InvertedIndex` instances.
///
/// `first_pos` and `second_pos` are the (constant) positions used for every document.
fn make_c_indexes(first_pos: u8, second_pos: u8) -> (CInvertedIndex, CInvertedIndex) {
    let first = CInvertedIndex::new(FLAGS);
    let second = CInvertedIndex::new(FLAGS);
    for doc_id in 1..=NUM_DOCS {
        first.write_term_entry(doc_id, 1, 1, None, &[first_pos]);
        second.write_term_entry(doc_id, 1, 1, None, &[second_pos]);
    }
    (first, second)
}

impl Bencher {
    const MEASUREMENT_TIME: Duration = Duration::from_millis(3000);
    const WARMUP_TIME: Duration = Duration::from_millis(200);

    pub fn bench(&self, c: &mut Criterion) {
        self.read_parametric(c);
        self.read_varying_sizes(c);
        self.skip_to_parametric(c);
        self.read_slop0_all_pass(c);
        self.read_slop100_all_pass(c);
        self.read_in_order_all_pass(c);
        self.read_in_order_slop100_all_pass(c);
        self.read_in_order_all_fail(c);
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

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

    /// Sweep `NUM_CHILDREN_CASES` × `OVERLAP_CASES`, building a fresh `Intersection` from
    /// `IdListSorted` children for each sample, then driving it with `routine`.
    fn bench_parametric<M, F>(&self, group: &mut BenchmarkGroup<'_, M>, routine: F)
    where
        M: Measurement,
        F: Fn(&mut Intersection<'static, IdListSorted<'static>>) + Copy,
    {
        for &num_children in NUM_CHILDREN_CASES {
            for &(label, pct) in OVERLAP_CASES {
                group.bench_function(format!("Rust/n={num_children}/{label}"), |b| {
                    b.iter_batched_ref(
                        || {
                            Intersection::new(
                                ids_to_rust_children(parametric_ids(num_children, pct)),
                                WEIGHT,
                                PRIORITIZE_UNION_CHILDREN,
                            )
                        },
                        routine,
                        criterion::BatchSize::SmallInput,
                    );
                });
            }
        }
    }

    fn bench_read<M, F>(&self, group: &mut BenchmarkGroup<'_, M>, make_ids: F)
    where
        M: Measurement,
        F: Fn() -> Vec<Vec<u64>>,
    {
        group.bench_function("Rust", |b| {
            b.iter_batched_ref(
                || {
                    Intersection::new(
                        ids_to_rust_children(make_ids()),
                        WEIGHT,
                        PRIORITIZE_UNION_CHILDREN,
                    )
                },
                |it| {
                    while let Ok(Some(current)) = it.read() {
                        black_box(current);
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }

    fn bench_read_slop<M: Measurement>(
        &self,
        group: &mut BenchmarkGroup<'_, M>,
        max_slop: Option<u32>,
        in_order: bool,
        first_pos: u8,
        second_pos: u8,
    ) {
        let (first_c, second_c) = make_c_indexes(first_pos, second_pos);

        group.bench_function("C", |b| {
            b.iter_batched_ref(
                || {
                    ffi::QueryIterator::new_intersection_from_term_its(
                        first_c.iterator_term(),
                        second_c.iterator_term(),
                        max_slop.map_or(-1, |v| v as i32),
                        in_order,
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

        let (first_rust, second_rust) = make_rust_indexes(first_pos, second_pos);
        let mock_ctx = MockContext::new(NUM_DOCS, NUM_DOCS as usize);

        group.bench_function("Rust", |b| {
            b.iter_batched_ref(
                || {
                    let first_reader = first_rust.reader();
                    let second_reader = second_rust.reader();
                    let first_iter = unsafe {
                        Term::new(
                            first_reader,
                            mock_ctx.sctx(),
                            new_query_term(),
                            1.0,
                            NoOpChecker,
                        )
                    };
                    let second_iter = unsafe {
                        Term::new(
                            second_reader,
                            mock_ctx.sctx(),
                            new_query_term(),
                            1.0,
                            NoOpChecker,
                        )
                    };
                    let children: Vec<Box<dyn RQEIterator<'_>>> =
                        vec![Box::new(first_iter), Box::new(second_iter)];
                    Intersection::new_with_slop_order(
                        children,
                        WEIGHT,
                        PRIORITIZE_UNION_CHILDREN,
                        max_slop,
                        in_order,
                    )
                },
                |it| {
                    while let Ok(Some(r)) = it.read() {
                        black_box(r);
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }

    // ── Benchmarks ───────────────────────────────────────────────────────────

    /// Parametric Read benchmark sweeping `num_children` × overlap percentage.
    fn read_parametric(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Iterator - Intersection - Read");
        self.bench_parametric(&mut group, |it| {
            while let Ok(Some(current)) = it.read() {
                black_box(current);
            }
        });
        group.finish();
    }

    fn read_varying_sizes(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Iterator - Intersection - Read Varying Sizes");
        self.bench_read(&mut group, varying_size_ids);
        group.finish();
    }

    /// Parametric SkipTo benchmark sweeping `num_children` × overlap percentage.
    fn skip_to_parametric(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Iterator - Intersection - SkipTo");
        self.bench_parametric(&mut group, |it| {
            while let Ok(Some(current)) = it.skip_to(it.last_doc_id() + STEP) {
                black_box(current);
            }
        });
        group.finish();
    }

    /// max_slop=0, in_order=false, adjacent in-order positions → all docs pass.
    fn read_slop0_all_pass(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Iterator - Intersection - Read Slop=0 All Pass");
        self.bench_read_slop(&mut group, Some(0), false, 1, 2);
        group.finish();
    }

    /// max_slop=100, in_order=false, positions with span=48 (first@1, second@50) → all docs pass.
    ///
    /// Represents a realistic wide-window phrase query. Comparable to `read_slop0_all_pass`
    /// to show how slop value affects proximity-check overhead.
    fn read_slop100_all_pass(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Iterator - Intersection - Read Slop=100 All Pass");
        self.bench_read_slop(&mut group, Some(100), false, 1, 50);
        group.finish();
    }

    /// max_slop=None, in_order=true, adjacent in-order positions (first@1, second@2) → all docs pass.
    ///
    /// No slop constraint; isolates the cost of pure ordering checks.
    fn read_in_order_all_pass(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Iterator - Intersection - Read In-Order All Pass");
        self.bench_read_slop(&mut group, None, true, 1, 2);
        group.finish();
    }

    /// max_slop=100, in_order=true, positions first@1, second@50 (span=48) → all docs pass.
    ///
    /// Combines a wide slop window with ordering constraint; both checks run but all pass.
    fn read_in_order_slop100_all_pass(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(
            c,
            "Iterator - Intersection - Read In-Order Slop=100 All Pass",
        );
        self.bench_read_slop(&mut group, Some(100), true, 1, 50);
        group.finish();
    }

    /// max_slop=0, in_order=true, adjacent reverse positions → all docs fail.
    ///
    /// The iterator must scan the full corpus without yielding any result,
    /// representing the worst-case cost of the proximity-check rejection path.
    fn read_in_order_all_fail(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Iterator - Intersection - Read In-Order All Fail");
        self.bench_read_slop(&mut group, Some(0), true, 2, 1);
        group.finish();
    }
}
