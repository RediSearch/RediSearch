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

use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use rand::{SeedableRng as _, seq::SliceRandom as _};
use top_k::TopKHeap;

// ── Comparators ───────────────────────────────────────────────────────────────

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

criterion_group!(benches, bench_heap_insert, bench_heap_pop_all);
criterion_main!(benches);
