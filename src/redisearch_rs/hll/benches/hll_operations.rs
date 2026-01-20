/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use hll::{AHasher, FxHasher, Hll, HllHasher, Murmur3Hasher, WyHasher, XxHash32Hasher};

type FnvHll = Hll<12, 4096, HllHasher>;
type Murmur3Hll = Hll<12, 4096, Murmur3Hasher>;
type XxHash32Hll = Hll<12, 4096, XxHash32Hasher>;
type AHashHll = Hll<12, 4096, AHasher>;
type FxHashHll = Hll<12, 4096, FxHasher>;
type WyHashHll = Hll<12, 4096, WyHasher>;

fn bench_add(c: &mut Criterion) {
    let mut group = c.benchmark_group("hll_add");
    group.throughput(Throughput::Elements(1));

    group.bench_function("fnv", |b| {
        let mut hll = FnvHll::new();
        let mut i = 0u32;
        b.iter(|| {
            hll.add(black_box(&i.to_le_bytes()));
            i = i.wrapping_add(1);
        });
    });

    group.bench_function("murmur3", |b| {
        let mut hll = Murmur3Hll::new();
        let mut i = 0u32;
        b.iter(|| {
            hll.add(black_box(&i.to_le_bytes()));
            i = i.wrapping_add(1);
        });
    });

    group.bench_function("xxhash32", |b| {
        let mut hll = XxHash32Hll::new();
        let mut i = 0u32;
        b.iter(|| {
            hll.add(black_box(&i.to_le_bytes()));
            i = i.wrapping_add(1);
        });
    });

    group.bench_function("ahash", |b| {
        let mut hll = AHashHll::new();
        let mut i = 0u32;
        b.iter(|| {
            hll.add(black_box(&i.to_le_bytes()));
            i = i.wrapping_add(1);
        });
    });

    group.bench_function("fxhash", |b| {
        let mut hll = FxHashHll::new();
        let mut i = 0u32;
        b.iter(|| {
            hll.add(black_box(&i.to_le_bytes()));
            i = i.wrapping_add(1);
        });
    });

    group.bench_function("wyhash", |b| {
        let mut hll = WyHashHll::new();
        let mut i = 0u32;
        b.iter(|| {
            hll.add(black_box(&i.to_le_bytes()));
            i = i.wrapping_add(1);
        });
    });

    group.finish();
}

fn bench_count(c: &mut Criterion) {
    let mut group = c.benchmark_group("hll_count");

    // Pre-populate HLLs
    let mut fnv_hll = FnvHll::new();
    let mut murmur3_hll = Murmur3Hll::new();
    let mut xxhash32_hll = XxHash32Hll::new();
    let mut ahash_hll = AHashHll::new();
    let mut fxhash_hll = FxHashHll::new();
    let mut wyhash_hll = WyHashHll::new();

    for i in 0..10000u32 {
        fnv_hll.add(&i.to_le_bytes());
        murmur3_hll.add(&i.to_le_bytes());
        xxhash32_hll.add(&i.to_le_bytes());
        ahash_hll.add(&i.to_le_bytes());
        fxhash_hll.add(&i.to_le_bytes());
        wyhash_hll.add(&i.to_le_bytes());
    }

    group.bench_function("fnv", |b| {
        b.iter(|| black_box(fnv_hll.count()));
    });

    group.bench_function("murmur3", |b| {
        b.iter(|| black_box(murmur3_hll.count()));
    });

    group.bench_function("xxhash32", |b| {
        b.iter(|| black_box(xxhash32_hll.count()));
    });

    group.bench_function("ahash", |b| {
        b.iter(|| black_box(ahash_hll.count()));
    });

    group.bench_function("fxhash", |b| {
        b.iter(|| black_box(fxhash_hll.count()));
    });

    group.bench_function("wyhash", |b| {
        b.iter(|| black_box(wyhash_hll.count()));
    });

    group.finish();
}

fn bench_merge(c: &mut Criterion) {
    let mut group = c.benchmark_group("hll_merge");

    group.bench_function("fnv", |b| {
        b.iter_batched(
            || {
                let mut hll1 = FnvHll::new();
                let mut hll2 = FnvHll::new();
                for i in 0..1000u32 {
                    hll1.add(&i.to_le_bytes());
                    hll2.add(&(i + 500).to_le_bytes());
                }
                (hll1, hll2)
            },
            |(mut hll1, hll2)| hll1.merge(black_box(&hll2)),
            criterion::BatchSize::SmallInput,
        );
    });

    group.bench_function("murmur3", |b| {
        b.iter_batched(
            || {
                let mut hll1 = Murmur3Hll::new();
                let mut hll2 = Murmur3Hll::new();
                for i in 0..1000u32 {
                    hll1.add(&i.to_le_bytes());
                    hll2.add(&(i + 500).to_le_bytes());
                }
                (hll1, hll2)
            },
            |(mut hll1, hll2)| hll1.merge(black_box(&hll2)),
            criterion::BatchSize::SmallInput,
        );
    });

    group.bench_function("xxhash32", |b| {
        b.iter_batched(
            || {
                let mut hll1 = XxHash32Hll::new();
                let mut hll2 = XxHash32Hll::new();
                for i in 0..1000u32 {
                    hll1.add(&i.to_le_bytes());
                    hll2.add(&(i + 500).to_le_bytes());
                }
                (hll1, hll2)
            },
            |(mut hll1, hll2)| hll1.merge(black_box(&hll2)),
            criterion::BatchSize::SmallInput,
        );
    });

    group.bench_function("ahash", |b| {
        b.iter_batched(
            || {
                let mut hll1 = AHashHll::new();
                let mut hll2 = AHashHll::new();
                for i in 0..1000u32 {
                    hll1.add(&i.to_le_bytes());
                    hll2.add(&(i + 500).to_le_bytes());
                }
                (hll1, hll2)
            },
            |(mut hll1, hll2)| hll1.merge(black_box(&hll2)),
            criterion::BatchSize::SmallInput,
        );
    });

    group.bench_function("fxhash", |b| {
        b.iter_batched(
            || {
                let mut hll1 = FxHashHll::new();
                let mut hll2 = FxHashHll::new();
                for i in 0..1000u32 {
                    hll1.add(&i.to_le_bytes());
                    hll2.add(&(i + 500).to_le_bytes());
                }
                (hll1, hll2)
            },
            |(mut hll1, hll2)| hll1.merge(black_box(&hll2)),
            criterion::BatchSize::SmallInput,
        );
    });

    group.bench_function("wyhash", |b| {
        b.iter_batched(
            || {
                let mut hll1 = WyHashHll::new();
                let mut hll2 = WyHashHll::new();
                for i in 0..1000u32 {
                    hll1.add(&i.to_le_bytes());
                    hll2.add(&(i + 500).to_le_bytes());
                }
                (hll1, hll2)
            },
            |(mut hll1, hll2)| hll1.merge(black_box(&hll2)),
            criterion::BatchSize::SmallInput,
        );
    });

    group.finish();
}

fn print_accuracy(_c: &mut Criterion) {
    println!("\n=== Accuracy Comparison ===");
    println!(
        "{:>8} | {:>16} | {:>16} | {:>16} | {:>16} | {:>16} | {:>16}",
        "n", "FNV", "Murmur3", "xxHash32", "AHash", "FxHash", "WyHash"
    );
    println!("{}", "-".repeat(120));

    for &n in &[100u32, 1000, 10000, 100000] {
        let mut fnv_hll = FnvHll::new();
        let mut murmur3_hll = Murmur3Hll::new();
        let mut xxhash32_hll = XxHash32Hll::new();
        let mut ahash_hll = AHashHll::new();
        let mut fxhash_hll = FxHashHll::new();
        let mut wyhash_hll = WyHashHll::new();

        for i in 0..n {
            fnv_hll.add(&i.to_le_bytes());
            murmur3_hll.add(&i.to_le_bytes());
            xxhash32_hll.add(&i.to_le_bytes());
            ahash_hll.add(&i.to_le_bytes());
            fxhash_hll.add(&i.to_le_bytes());
            wyhash_hll.add(&i.to_le_bytes());
        }

        let format_result = |count: usize| -> String {
            let err = (count as f64 - n as f64).abs() / n as f64 * 100.0;
            format!("{:>6} ({:>5.2}%)", count, err)
        };

        println!(
            "{:>8} | {:>16} | {:>16} | {:>16} | {:>16} | {:>16} | {:>16}",
            n,
            format_result(fnv_hll.count()),
            format_result(murmur3_hll.count()),
            format_result(xxhash32_hll.count()),
            format_result(ahash_hll.count()),
            format_result(fxhash_hll.count()),
            format_result(wyhash_hll.count()),
        );
    }
}

criterion_group!(benches, bench_add, bench_count, bench_merge, print_accuracy);
criterion_main!(benches);
