/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Micro-benchmarks for [`TopKHeap`].
//!
//! These benchmarks use mock sources and synthetic data.  They are NOT
//! comparable to the C iterator numbers — their purpose is to provide a
//! regression guard for the shared skeleton before real sources are plugged in.

// Pull in the lib to ensure FFI stubs and mock allocator symbols are linked.
use top_k_bencher as _;

use std::hint::black_box;
use std::{cmp::Ordering, num::NonZeroUsize};

use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use rand::{SeedableRng as _, seq::SliceRandom as _};
use rqe_iterators::{BoxedRQEIterator, RQEIterator};
use top_k::{BatchStrategy, TopKHeap, TopKIterator, TopKMode, mock::MockScoreSource};

fn asc(a: f64, b: f64) -> Ordering {
    a.partial_cmp(&b).unwrap_or(Ordering::Equal)
}

// ── Heap benchmarks ───────────────────────────────────────────────────────────

fn bench_heap_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("heap");
    let (n, k) = (100_000, NonZeroUsize::new(1_000).unwrap());

    // Best case: scores arrive worst-first; after the heap fills every
    // subsequent element is immediately rejected without touching the heap.
    group.bench_with_input(
        BenchmarkId::new("insert_asc", format!("{n}→k{k}")),
        &(n, k),
        |b, &(n, k)| {
            b.iter(|| {
                let mut heap = TopKHeap::new(k, asc);
                for i in 0..n {
                    heap.push(i as u64, i as f64);
                }
                black_box(heap.len())
            })
        },
    );

    // Worst case: scores arrive best-first; every element after fill is
    // strictly better than the current worst, forcing a pop + push
    // (O(log k)) on each of the n-k remaining insertions.
    group.bench_with_input(
        BenchmarkId::new("insert_desc", format!("{n}→k{k}")),
        &(n, k),
        |b, &(n, k)| {
            b.iter(|| {
                let mut heap = TopKHeap::new(k, asc);
                for i in (0..n).rev() {
                    heap.push(i as u64, i as f64);
                }
                black_box(heap.len())
            })
        },
    );

    // Realistic case: shuffled scores give a mix of evictions and
    // rejections representative of real query-result streams.
    // The shuffle is done once outside the timed loop.
    let scores: Vec<u64> = {
        let mut v: Vec<u64> = (0..n as u64).collect();
        v.shuffle(&mut rand::rngs::SmallRng::seed_from_u64(42));
        v
    };
    group.bench_function(
        BenchmarkId::new("insert_rand", format!("{n}→k{k}")),
        |b| {
            b.iter(|| {
                let mut heap = TopKHeap::new(k, asc);
                for &i in &scores {
                    heap.push(i, i as f64);
                }
                black_box(heap.len())
            })
        },
    );

    group.finish();
}

fn bench_heap_pop_all(c: &mut Criterion) {
    let mut group = c.benchmark_group("heap");
    let k = NonZeroUsize::new(1_000).unwrap();
    group.bench_with_input(
        BenchmarkId::new("drain_sorted", format!("k{k}")),
        &k,
        |b, &k| {
            b.iter_batched(
                || {
                    let mut heap = TopKHeap::new(k, asc);
                    for i in 0..k.get() {
                        heap.push(i as u64, i as f64);
                    }
                    heap
                },
                |heap| black_box(heap.drain_sorted()),
                BatchSize::SmallInput,
            )
        },
    );
    group.finish();
}

// ── Iterator benchmarks (mock sources) ───────────────────────────────────────

fn bench_intersection_overlap(c: &mut Criterion) {
    let mut group = c.benchmark_group("iterator");
    let k = NonZeroUsize::new(100).unwrap();
    // 50% overlap: batch docs are even numbers, child docs are every other even.
    for n in [1_000usize, 10_000] {
        let batch: Vec<(u64, f64)> = (0..n as u64).map(|i| (i * 2, i as f64)).collect();
        let child_ids: Vec<u64> = (0..n as u64).step_by(2).map(|i| i * 2).collect();

        group.bench_with_input(BenchmarkId::new("batches_50pct_overlap", n), &n, |b, _| {
            b.iter_batched(
                || {
                    let source = MockScoreSource::new(vec![batch.clone()], vec![], |_, _| {
                        BatchStrategy::Continue
                    });
                    let child: Box<_> =
                        Box::new(rqe_iterators::IdList::<true>::new(child_ids.clone()));
                    (source, child)
                },
                |(source, child)| {
                    let mut it = TopKIterator::new(source, child, k, asc);
                    while black_box(it.read()).unwrap().is_some() {}
                },
                BatchSize::SmallInput,
            )
        });
    }
    group.finish();
}

fn bench_intersection_disjoint(c: &mut Criterion) {
    let mut group = c.benchmark_group("iterator");
    let k = NonZeroUsize::new(100).unwrap();
    // 0% overlap: batch has even IDs, child has odd IDs.
    for n in [1_000usize, 10_000] {
        let batch: Vec<(u64, f64)> = (0..n as u64).map(|i| (i * 2, i as f64)).collect();
        let child_ids: Vec<u64> = (0..n as u64).map(|i| i * 2 + 1).collect();

        group.bench_with_input(BenchmarkId::new("batches_0pct_overlap", n), &n, |b, _| {
            b.iter_batched(
                || {
                    let source = MockScoreSource::new(vec![batch.clone()], vec![], |_, _| {
                        BatchStrategy::Continue
                    });
                    let child: Box<_> =
                        Box::new(rqe_iterators::IdList::<true>::new(child_ids.clone()));
                    (source, child)
                },
                |(source, child)| {
                    let mut it = TopKIterator::new(source, child, k, asc);
                    while black_box(it.read()).unwrap().is_some() {}
                },
                BatchSize::SmallInput,
            )
        });
    }
    group.finish();
}

fn bench_adhoc_vs_batches(c: &mut Criterion) {
    // Compare Adhoc-BF and Batches on the same inputs as n_child varies.
    // Source: 100k scored docs. Child: n_child docs spread evenly across the source.
    // Adhoc-BF is expected to win for small n_child; Batches for large n_child.
    let mut group = c.benchmark_group("iterator/adhoc_vs_batches");
    let n_source = 100_000usize;
    let k = NonZeroUsize::new(10).unwrap();
    let source_docs: Vec<(u64, f64)> = (0..n_source as u64).map(|i| (i, i as f64)).collect();

    for n_child in [100usize, n_source / 2, n_source - 100usize] {
        let step = n_source / n_child;
        let child_ids: Vec<u64> = (0..n_source as u64).step_by(step).take(n_child).collect();

        group.bench_with_input(BenchmarkId::new("adhoc", n_child), &n_child, |b, _| {
            b.iter_batched(
                || {
                    let source = MockScoreSource::new(vec![], source_docs.clone(), |_, _| {
                        BatchStrategy::Continue
                    });
                    let child = BoxedRQEIterator::new(Box::new(
                        rqe_iterators::IdList::<true>::new(child_ids.clone()),
                    ));
                    (source, child)
                },
                |(source, child)| {
                    let mut it =
                        TopKIterator::new_with_mode(source, Some(child), k, asc, TopKMode::AdhocBF);
                    while it.read().unwrap().is_some() {}
                    black_box(it.at_eof())
                },
                BatchSize::SmallInput,
            )
        });

        group.bench_with_input(BenchmarkId::new("batches", n_child), &n_child, |b, _| {
            b.iter_batched(
                || {
                    let source = MockScoreSource::new(vec![source_docs.clone()], vec![], |_, _| {
                        BatchStrategy::Continue
                    });
                    let child = BoxedRQEIterator::new(Box::new(
                        rqe_iterators::IdList::<true>::new(child_ids.clone()),
                    ));
                    (source, child)
                },
                |(source, child)| {
                    let mut it =
                        TopKIterator::new_with_mode(source, Some(child), k, asc, TopKMode::Batches);
                    while it.read().unwrap().is_some() {}
                    black_box(it.at_eof())
                },
                BatchSize::SmallInput,
            )
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_heap_insert,
    bench_heap_pop_all,
    bench_intersection_overlap,
    bench_intersection_disjoint,
    bench_adhoc_vs_batches,
);
criterion_main!(benches);
