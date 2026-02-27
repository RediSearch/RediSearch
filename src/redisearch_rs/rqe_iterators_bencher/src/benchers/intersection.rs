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
use rqe_iterators::{Intersection, RQEIterator, id_list::IdListSorted};

use crate::ffi::{self, IteratorStatus_ITERATOR_OK};

#[derive(Default)]
pub struct Bencher;

/// Number of child iterators for intersection benchmarks.
const NUM_CHILDREN: usize = 5;
/// Size of each child iterator's ID list.
const CHILD_SIZE: u64 = 100_000;
/// Weight for intersection results.
const WEIGHT: f64 = 1.0;
/// Step size for skip_to benchmarks.
const STEP: u64 = 100;

/// Generate IDs for high overlap scenario (dense intersection results).
/// Each child contains IDs 1..CHILD_SIZE, so all documents appear in all children.
fn high_overlap_ids() -> Vec<Vec<u64>> {
    (0..NUM_CHILDREN)
        .map(|_| (1..=CHILD_SIZE).collect())
        .collect()
}

/// Generate IDs for low overlap scenario (sparse intersection results).
/// Children have staggered starting points, reducing intersection size.
fn low_overlap_ids() -> Vec<Vec<u64>> {
    (0..NUM_CHILDREN)
        .map(|i| {
            let offset = (i as u64) * (CHILD_SIZE / 10);
            (1..=CHILD_SIZE).map(|x| x + offset).collect()
        })
        .collect()
}

/// Generate IDs for varying sizes scenario (realistic workload).
/// First child is smallest (drives the intersection), others are progressively larger.
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

impl Bencher {
    const MEASUREMENT_TIME: Duration = Duration::from_millis(3000);
    const WARMUP_TIME: Duration = Duration::from_millis(200);

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
        self.read_high_overlap(c);
        self.read_low_overlap(c);
        self.read_varying_sizes(c);
        self.skip_to_high_overlap(c);
        self.skip_to_low_overlap(c);
        slop_and_order::Bencher::default().bench(c);
    }

    fn read_high_overlap(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Iterator - Intersection - Read High Overlap");
        self.bench_read(&mut group, high_overlap_ids);
        group.finish();
    }

    fn read_low_overlap(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Iterator - Intersection - Read Low Overlap");
        self.bench_read(&mut group, low_overlap_ids);
        group.finish();
    }

    fn read_varying_sizes(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Iterator - Intersection - Read Varying Sizes");
        self.bench_read(&mut group, varying_size_ids);
        group.finish();
    }

    fn skip_to_high_overlap(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Iterator - Intersection - SkipTo High Overlap");
        self.bench_skip_to(&mut group, high_overlap_ids);
        group.finish();
    }

    fn skip_to_low_overlap(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Iterator - Intersection - SkipTo Low Overlap");
        self.bench_skip_to(&mut group, low_overlap_ids);
        group.finish();
    }

    fn bench_read<M, F>(&self, group: &mut BenchmarkGroup<'_, M>, make_ids: F)
    where
        M: Measurement,
        F: Fn() -> Vec<Vec<u64>>,
    {
        // C implementation benchmark
        group.bench_function("C", |b| {
            b.iter_batched_ref(
                || ffi::QueryIterator::new_intersection(&make_ids(), WEIGHT),
                |it| {
                    while it.read() == IteratorStatus_ITERATOR_OK {
                        black_box(it.current());
                    }
                    it.free();
                },
                criterion::BatchSize::SmallInput,
            );
        });

        // Rust implementation benchmark
        group.bench_function("Rust", |b| {
            b.iter_batched_ref(
                || Intersection::new(ids_to_rust_children(make_ids()), -1, false),
                |it| {
                    while let Ok(Some(current)) = it.read() {
                        black_box(current);
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }

    fn bench_skip_to<M, F>(&self, group: &mut BenchmarkGroup<'_, M>, make_ids: F)
    where
        M: Measurement,
        F: Fn() -> Vec<Vec<u64>>,
    {
        // C implementation benchmark
        group.bench_function("C", |b| {
            b.iter_batched_ref(
                || ffi::QueryIterator::new_intersection(&make_ids(), WEIGHT),
                |it| {
                    while it.skip_to(it.last_doc_id() + STEP) == IteratorStatus_ITERATOR_OK {
                        black_box(it.current());
                    }
                    it.free();
                },
                criterion::BatchSize::SmallInput,
            );
        });

        // Rust implementation benchmark
        group.bench_function("Rust", |b| {
            b.iter_batched_ref(
                || Intersection::new(ids_to_rust_children(make_ids()), -1, false),
                |it| {
                    while let Ok(Some(current)) = it.skip_to(it.last_doc_id() + STEP) {
                        black_box(current);
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }
}

/// Benchmarks for the `max_slop` and `in_order` proximity-check path.
///
/// Uses real term inverted indexes with encoded position data so that `is_within_range`
/// performs actual work. Two position patterns cover the performance extremes:
///
/// - **adjacent_in_order** (foo@1, bar@2): all docs pass `max_slop=0` and `in_order=true`.
/// - **adjacent_reverse** (foo@2, bar@1): all docs fail `max_slop=0 + in_order=true`,
///   forcing the iterator to scan the full corpus without yielding any results.
pub mod slop_and_order {
    use std::{hint::black_box, time::Duration};

    use criterion::{
        BenchmarkGroup, Criterion,
        measurement::{Measurement, WallTime},
    };
    use ffi::{
        IndexFlags_Index_StoreByteOffsets, IndexFlags_Index_StoreFieldFlags,
        IndexFlags_Index_StoreFreqs, IndexFlags_Index_StoreTermOffsets,
    };
    use inverted_index::{InvertedIndex, RSIndexResult, RSOffsetSlice, full::Full};
    use query_term::RSQueryTerm;
    use rqe_iterators::{
        NoOpChecker, RQEIterator, intersection::Intersection, inverted_index::Term,
    };
    use rqe_iterators_test_utils::MockContext;

    use ::ffi::IndexFlags;
    use crate::ffi::{self, InvertedIndex as CInvertedIndex, IteratorStatus_ITERATOR_OK};

    const NUM_DOCS: u64 = 100_000;
    const FLAGS: IndexFlags = IndexFlags_Index_StoreFreqs
        | IndexFlags_Index_StoreTermOffsets
        | IndexFlags_Index_StoreFieldFlags
        | IndexFlags_Index_StoreByteOffsets;

    fn new_query_term() -> Box<RSQueryTerm> {
        RSQueryTerm::new(b"term", 1, 0)
    }

    /// Build two Rust `InvertedIndex<Full>` instances.
    ///
    /// `foo_pos` and `bar_pos` are the (constant) positions used for every document.
    fn make_rust_indexes(foo_pos: u8, bar_pos: u8) -> (InvertedIndex<Full>, InvertedIndex<Full>) {
        let mut foo = InvertedIndex::<Full>::new(FLAGS);
        let mut bar = InvertedIndex::<Full>::new(FLAGS);
        for doc_id in 1..=NUM_DOCS {
            foo.add_record(&RSIndexResult::with_term(
                None,
                RSOffsetSlice::from_slice(&[foo_pos]),
                doc_id,
                1u128,
                1,
            ))
            .unwrap();
            bar.add_record(&RSIndexResult::with_term(
                None,
                RSOffsetSlice::from_slice(&[bar_pos]),
                doc_id,
                1u128,
                1,
            ))
            .unwrap();
        }
        (foo, bar)
    }

    /// Build two C `InvertedIndex` instances.
    ///
    /// `foo_pos` and `bar_pos` are the (constant) positions used for every document.
    fn make_c_indexes(foo_pos: u8, bar_pos: u8) -> (CInvertedIndex, CInvertedIndex) {
        let foo = CInvertedIndex::new(FLAGS);
        let bar = CInvertedIndex::new(FLAGS);
        for doc_id in 1..=NUM_DOCS {
            foo.write_term_entry(doc_id, 1, 1, None, &[foo_pos]);
            bar.write_term_entry(doc_id, 1, 1, None, &[bar_pos]);
        }
        (foo, bar)
    }

    #[derive(Default)]
    pub struct Bencher;

    impl Bencher {
        const MEASUREMENT_TIME: Duration = Duration::from_millis(3000);
        const WARMUP_TIME: Duration = Duration::from_millis(200);

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
            self.read_slop0_all_pass(c);
            self.read_slop100_all_pass(c);
            self.read_in_order_all_pass(c);
            self.read_in_order_slop100_all_pass(c);
            self.read_in_order_all_fail(c);
        }

        /// max_slop=0, in_order=false, adjacent in-order positions → all docs pass.
        fn read_slop0_all_pass(&self, c: &mut Criterion) {
            let mut group =
                self.benchmark_group(c, "Iterator - Intersection - Read Slop=0 All Pass");
            self.bench_read(&mut group, 0, false, 1, 2);
            group.finish();
        }

        /// max_slop=100, in_order=false, positions with span=48 (foo@1, bar@50) → all docs pass.
        ///
        /// Represents a realistic wide-window phrase query. Comparable to `read_slop0_all_pass`
        /// to show how slop value affects proximity-check overhead.
        fn read_slop100_all_pass(&self, c: &mut Criterion) {
            let mut group =
                self.benchmark_group(c, "Iterator - Intersection - Read Slop=100 All Pass");
            self.bench_read(&mut group, 100, false, 1, 50);
            group.finish();
        }

        /// max_slop=0, in_order=true, adjacent in-order positions (foo@1, bar@2) → all docs pass.
        fn read_in_order_all_pass(&self, c: &mut Criterion) {
            let mut group =
                self.benchmark_group(c, "Iterator - Intersection - Read In-Order All Pass");
            self.bench_read(&mut group, 0, true, 1, 2);
            group.finish();
        }

        /// max_slop=100, in_order=true, positions foo@1, bar@50 (span=48) → all docs pass.
        ///
        /// Combines a wide slop window with ordering constraint; both checks run but all pass.
        fn read_in_order_slop100_all_pass(&self, c: &mut Criterion) {
            let mut group = self.benchmark_group(
                c,
                "Iterator - Intersection - Read In-Order Slop=100 All Pass",
            );
            self.bench_read(&mut group, 100, true, 1, 50);
            group.finish();
        }

        /// max_slop=0, in_order=true, adjacent reverse positions → all docs fail.
        ///
        /// The iterator must scan the full corpus without yielding any result,
        /// representing the worst-case cost of the proximity-check rejection path.
        fn read_in_order_all_fail(&self, c: &mut Criterion) {
            let mut group =
                self.benchmark_group(c, "Iterator - Intersection - Read In-Order All Fail");
            self.bench_read(&mut group, 0, true, 2, 1);
            group.finish();
        }

        fn bench_read<M: Measurement>(
            &self,
            group: &mut BenchmarkGroup<'_, M>,
            max_slop: i32,
            in_order: bool,
            foo_pos: u8,
            bar_pos: u8,
        ) {
            let (foo_c, bar_c) = make_c_indexes(foo_pos, bar_pos);

            group.bench_function("C", |b| {
                b.iter_batched_ref(
                    || {
                        ffi::QueryIterator::new_intersection_from_term_its(
                            foo_c.iterator_term(),
                            bar_c.iterator_term(),
                            max_slop,
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

            let (foo_rust, bar_rust) = make_rust_indexes(foo_pos, bar_pos);
            let mock_ctx = MockContext::new(NUM_DOCS, NUM_DOCS as usize);

            group.bench_function("Rust", |b| {
                b.iter_batched_ref(
                    || {
                        let foo_reader = foo_rust.reader();
                        let bar_reader = bar_rust.reader();
                        let foo_iter = unsafe {
                            Term::new(foo_reader, mock_ctx.sctx(), new_query_term(), 1.0, NoOpChecker)
                        };
                        let bar_iter = unsafe {
                            Term::new(bar_reader, mock_ctx.sctx(), new_query_term(), 1.0, NoOpChecker)
                        };
                        let children: Vec<Box<dyn RQEIterator<'_>>> =
                            vec![Box::new(foo_iter), Box::new(bar_iter)];
                        Intersection::new(children, max_slop, in_order)
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
    }
}
