/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::hash::Hasher;

use criterion::{
    black_box, criterion_group, criterion_main, measurement::WallTime, BenchmarkGroup, Criterion,
    Throughput,
};
use hll::{Hll, HllHasher, Murmur3Hasher};

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
struct FxHasher(fxhash::FxHasher);

impl Default for FxHasher {
    fn default() -> Self {
        Self(fxhash::FxHasher::default())
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

/// WyHash hasher wrapper (truncated to 32 bits).
struct WyHasher(wyhash::WyHash);

impl Default for WyHasher {
    fn default() -> Self {
        Self(wyhash::WyHash::with_seed(0))
    }
}

impl Hasher for WyHasher {
    #[inline]
    fn finish(&self) -> u64 {
        self.0.finish()
    }

    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        self.0.write(bytes);
    }
}

impl hash32::Hasher for WyHasher {
    #[inline]
    fn finish32(&self) -> u32 {
        self.0.finish() as u32
    }
}

const CARDINALITIES: &[u32] = &[100, 1000, 10000, 100000];
const HASHER_NAMES: &[&str] = &["fnv", "murmur3", "xxhash32", "ahash", "fxhash", "wyhash"];

// Generic benchmark implementations - all logic lives here

fn bench_add_impl<H: hash32::Hasher + Default>(group: &mut BenchmarkGroup<'_, WallTime>, name: &str) {
    group.bench_function(name, |b| {
        let mut hll: Hll<12, 4096, H> = Hll::new();
        let mut i = 0u32;
        b.iter(|| {
            hll.add(black_box(&i.to_le_bytes()));
            i = i.wrapping_add(1);
        });
    });
}

fn bench_count_impl<H: hash32::Hasher + Default>(group: &mut BenchmarkGroup<'_, WallTime>, name: &str) {
    let mut hll: Hll<12, 4096, H> = Hll::new();
    for i in 0..10000u32 {
        hll.add(&i.to_le_bytes());
    }
    group.bench_function(name, |b| {
        b.iter(|| black_box(hll.count()));
    });
}

fn bench_merge_impl<H: hash32::Hasher + Default>(group: &mut BenchmarkGroup<'_, WallTime>, name: &str) {
    group.bench_function(name, |b| {
        b.iter_batched(
            || {
                let mut hll1: Hll<12, 4096, H> = Hll::new();
                let mut hll2: Hll<12, 4096, H> = Hll::new();
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
}

fn measure_accuracy<H: hash32::Hasher + Default>(n: u32) -> (usize, f64) {
    let mut hll: Hll<12, 4096, H> = Hll::new();
    for i in 0..n {
        hll.add(&i.to_le_bytes());
    }
    let count = hll.count();
    let err = (count as f64 - n as f64).abs() / n as f64 * 100.0;
    (count, err)
}

/// Calls a benchmark function for each hasher type.
/// This macro only handles type dispatch - all logic is in the generic functions above.
macro_rules! bench_all_hashers {
    ($group:expr, $func:ident) => {
        $func::<HllHasher>($group, "fnv");
        $func::<Murmur3Hasher>($group, "murmur3");
        $func::<XxHash32Hasher>($group, "xxhash32");
        $func::<AHasher>($group, "ahash");
        $func::<FxHasher>($group, "fxhash");
        $func::<WyHasher>($group, "wyhash");
    };
}

fn bench_add(c: &mut Criterion) {
    let mut group = c.benchmark_group("hll_add");
    group.throughput(Throughput::Elements(1));
    bench_all_hashers!(&mut group, bench_add_impl);
    group.finish();
}

fn bench_count(c: &mut Criterion) {
    let mut group = c.benchmark_group("hll_count");
    bench_all_hashers!(&mut group, bench_count_impl);
    group.finish();
}

fn bench_merge(c: &mut Criterion) {
    let mut group = c.benchmark_group("hll_merge");
    bench_all_hashers!(&mut group, bench_merge_impl);
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
        print_accuracy!(HllHasher, Murmur3Hasher, XxHash32Hasher, AHasher, FxHasher, WyHasher);
        println!();
    }
}

criterion_group!(benches, bench_add, bench_count, bench_merge, print_accuracy);
criterion_main!(benches);
