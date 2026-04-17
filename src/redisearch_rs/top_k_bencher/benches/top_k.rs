/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Micro-benchmarks for [`TopKHeap`] and [`TopKIterator`].
//!
//! These benchmarks use mock sources and synthetic data.  They are NOT
//! comparable to the C iterator numbers — their purpose is to provide a
//! regression guard for the shared skeleton before real sources are plugged in.

// Pull in the lib to ensure FFI stubs and mock allocator symbols are linked.
use top_k_bencher as _;

use std::hint::black_box;
use std::{cmp::Ordering, num::NonZeroUsize};

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use rqe_iterators::RQEIterator;
use top_k::{CollectionStrategy, TopKHeap, TopKIterator, TopKMode, mock::MockScoreSource};

// ── Comparators ───────────────────────────────────────────────────────────────

fn asc(a: f64, b: f64) -> Ordering {
    a.partial_cmp(&b).unwrap_or(Ordering::Equal)
}

// ── Heap benchmarks ───────────────────────────────────────────────────────────

fn bench_heap_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("heap");
    for (n, k) in [
        (1_000, NonZeroUsize::new(10).unwrap()),
        (10_000, NonZeroUsize::new(100).unwrap()),
        (100_000, NonZeroUsize::new(1_000).unwrap()),
    ] {
        group.bench_with_input(
            BenchmarkId::new("insert", format!("{n}→k{k}")),
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
    }
    group.finish();
}

fn bench_heap_pop_all(c: &mut Criterion) {
    let mut group = c.benchmark_group("heap");
    for k in [10usize, 100, 1_000].map(|k| NonZeroUsize::new(k).unwrap()) {
        group.bench_with_input(
            BenchmarkId::new("drain_sorted", format!("k{k}")),
            &k,
            |b, &k| {
                b.iter(|| {
                    let mut heap = TopKHeap::new(k, asc);
                    for i in 0..k.get() {
                        heap.push(i as u64, i as f64);
                    }
                    black_box(heap.drain_sorted())
                })
            },
        );
    }
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
            b.iter(|| {
                let source = MockScoreSource::new(vec![batch.clone()], vec![], |_, _| {
                    CollectionStrategy::Continue
                });
                let child: Box<dyn RQEIterator> =
                    Box::new(rqe_iterators::IdList::<true>::new(child_ids.clone()));
                let mut it = TopKIterator::new(source, Some(child), k, asc);
                while it.read().unwrap().is_some() {}
                black_box(it.metrics.total_comparisons)
            })
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
            b.iter(|| {
                let source = MockScoreSource::new(vec![batch.clone()], vec![], |_, _| {
                    CollectionStrategy::Continue
                });
                let child: Box<dyn RQEIterator> =
                    Box::new(rqe_iterators::IdList::<true>::new(child_ids.clone()));
                let mut it = TopKIterator::new(source, Some(child), k, asc);
                while it.read().unwrap().is_some() {}
                black_box(it.metrics.total_comparisons)
            })
        });
    }
    group.finish();
}

fn bench_adhoc_10k_child(c: &mut Criterion) {
    let n = 10_000usize;
    let k = NonZeroUsize::new(100).unwrap();
    // Every 10th document has a score.
    let scores: Vec<(u64, f64)> = (0..n as u64).step_by(10).map(|i| (i, i as f64)).collect();
    let child_ids: Vec<u64> = (0..n as u64).collect();

    c.bench_function("iterator/adhoc_10k_child", |b| {
        b.iter(|| {
            let source =
                MockScoreSource::new(vec![], scores.clone(), |_, _| CollectionStrategy::Continue);
            let child: Box<dyn RQEIterator> =
                Box::new(rqe_iterators::IdList::<true>::new(child_ids.clone()));
            let mut it =
                TopKIterator::new_with_mode(source, Some(child), k, asc, TopKMode::AdhocBF);
            while it.read().unwrap().is_some() {}
            black_box(it.at_eof())
        })
    });
}

criterion_group!(
    benches,
    bench_heap_insert,
    bench_heap_pop_all,
    bench_intersection_overlap,
    bench_intersection_disjoint,
    bench_adhoc_10k_child,
);
criterion_main!(benches);
