/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{hash::Hasher, hint::black_box};

use criterion::{
    BenchmarkGroup, Criterion, Throughput, criterion_group, criterion_main, measurement::WallTime,
};
use hyperloglog::{CFnvHasher, HyperLogLog, HyperLogLog10, Murmur3Hasher, WyHasher};

// Benchmark-only hasher wrappers (not used in production)

/// xxHash32 hasher wrapper.
struct XxHash32Hasher(xxhash_rust::xxh32::Xxh32);

impl Default for XxHash32Hasher {
    fn default() -> Self {
        Self(xxhash_rust::xxh32::Xxh32::new(0))
    }
}

impl Hasher for XxHash32Hasher {
    #[inline]
    fn finish(&self) -> u64 {
        u64::from(self.0.digest())
    }

    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        self.0.update(bytes);
    }
}

impl hash32::Hasher for XxHash32Hasher {
    #[inline]
    fn finish32(&self) -> u32 {
        self.0.digest()
    }
}

/// AHash hasher wrapper (truncated to 32 bits).
struct AHasher(ahash::AHasher);

impl Default for AHasher {
    fn default() -> Self {
        Self(ahash::AHasher::default())
    }
}

impl Hasher for AHasher {
    #[inline]
    fn finish(&self) -> u64 {
        self.0.finish()
    }

    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        self.0.write(bytes);
    }
}

impl hash32::Hasher for AHasher {
    #[inline]
    fn finish32(&self) -> u32 {
        self.0.finish() as u32
    }
}

/// FxHash hasher wrapper (truncated to 32 bits).
struct FxHasher(rustc_hash::FxHasher);

impl Default for FxHasher {
    fn default() -> Self {
        Self(rustc_hash::FxHasher::default())
    }
}

impl Hasher for FxHasher {
    #[inline]
    fn finish(&self) -> u64 {
        self.0.finish()
    }

    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        self.0.write(bytes);
    }
}

impl hash32::Hasher for FxHasher {
    #[inline]
    fn finish32(&self) -> u32 {
        self.0.finish() as u32
    }
}

const CARDINALITIES: &[u32] = &[100, 1000, 10000, 100000];
const HASHER_NAMES: &[&str] = &["fnv", "murmur3", "xxhash32", "ahash", "fxhash", "wyhash"];

fn measure_accuracy<H: hash32::Hasher + Default>(n: u32) -> (usize, f64) {
    let mut hll: HyperLogLog10<[u8; 4], H> = HyperLogLog::new();
    for i in 0..n {
        hll.add(&i.to_le_bytes());
    }
    let count = hll.count();
    let err = (count as f64 - n as f64).abs() / n as f64 * 100.0;
    (count, err)
}

fn bench_add(c: &mut Criterion) {
    fn _bench_add<H: hash32::Hasher + Default>(
        group: &mut BenchmarkGroup<'_, WallTime>,
        name: &str,
    ) {
        group.bench_function(name, |b| {
            let mut hll: HyperLogLog10<[u8; 4], H> = HyperLogLog::new();
            let mut i = 0u32;
            b.iter(|| {
                hll.add(black_box(&i.to_le_bytes()));
                i = i.wrapping_add(1);
            });
        });
    }

    let mut group = c.benchmark_group("hyperloglog_add");
    group.throughput(Throughput::Elements(1));
    _bench_add::<CFnvHasher>(&mut group, "fnv");
    _bench_add::<Murmur3Hasher>(&mut group, "murmur3");
    _bench_add::<XxHash32Hasher>(&mut group, "xxhash32");
    _bench_add::<AHasher>(&mut group, "ahash");
    _bench_add::<FxHasher>(&mut group, "fxhash");
    _bench_add::<WyHasher>(&mut group, "wyhash");
    group.finish();
}

fn bench_count(c: &mut Criterion) {
    let mut group = c.benchmark_group("hyperloglog_count");
    group.bench_function("fnv", |b| {
        b.iter_batched(
            || {
                // Setup: create HyperLogLog with data, cache not yet computed
                let mut hll: HyperLogLog10<[u8; 4], CFnvHasher> = HyperLogLog::new();
                for i in 0..10000u32 {
                    hll.add(&i.to_le_bytes());
                }
                hll
            },
            // The counting routine isn't sensitive to the hasher,
            // so it's a waste of time to bench it N times, once for each
            // hasher.
            |hll| black_box(hll.count()),
            criterion::BatchSize::SmallInput,
        );
    });
    group.finish();
}

fn bench_merge(c: &mut Criterion) {
    let mut group = c.benchmark_group("hyperloglog_merge");
    group.bench_function("fnv", |b| {
        b.iter_batched(
            || {
                let mut hll1: HyperLogLog10<[u8; 4], CFnvHasher> = HyperLogLog::new();
                let mut hll2: HyperLogLog10<[u8; 4], CFnvHasher> = HyperLogLog::new();
                for i in 0..1000u32 {
                    hll1.add(&i.to_le_bytes());
                    hll2.add(&(i + 500).to_le_bytes());
                }
                (hll1, hll2)
            },
            // The merging routine isn't sensitive to the hasher,
            // so it's a waste of time to bench it N times, once for each
            // hasher.
            |(mut hll1, hll2)| hll1.merge(black_box(&hll2)),
            criterion::BatchSize::SmallInput,
        );
    });
    group.finish();
}

fn print_accuracy(_c: &mut Criterion) {
    println!("\n=== Accuracy Comparison ===");
    print!("{:>8}", "n");
    for name in HASHER_NAMES {
        print!(" | {:>16}", name);
    }
    println!();
    println!("{}", "-".repeat(8 + HASHER_NAMES.len() * 20));

    for &n in CARDINALITIES {
        print!("{:>8}", n);

        // Macro needed here because we can't iterate over types at runtime
        macro_rules! print_accuracy {
            ($($hasher:ty),+) => {
                $(
                    let (count, err) = measure_accuracy::<$hasher>(n);
                    print!(" | {:>6} ({:>5.2}%)", count, err);
                )+
            };
        }
        print_accuracy!(
            CFnvHasher,
            Murmur3Hasher,
            XxHash32Hasher,
            AHasher,
            FxHasher,
            WyHasher
        );
        println!();
    }
}

criterion_group!(benches, bench_add, bench_count, bench_merge, print_accuracy);
criterion_main!(benches);
