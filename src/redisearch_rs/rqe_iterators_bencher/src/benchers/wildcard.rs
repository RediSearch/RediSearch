/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Benchmark Wildcard iterator.

use std::time::Duration;

use criterion::{
    BenchmarkGroup, BenchmarkId, Criterion,
    measurement::{Measurement, WallTime},
};
use rqe_iterators::{RQEIterator, wildcard::Wildcard};

use crate::ffi;

#[derive(Default)]
pub struct Bencher;

impl Bencher {
    const MEASUREMENT_TIME: Duration = Duration::from_millis(500);
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
        // Direct C benchmarks (eliminates FFI overhead on every function call)
        self.read_large_range_direct(c);
        self.read_medium_range_direct(c);
        self.skip_to_large_range_direct(c);
        self.skip_to_medium_range_direct(c);

        // Edge cases and patterns
        self.edge_cases(c);
        self.variable_steps(c);
        self.random_access(c);
    }

    fn rust_read_large_range<M: Measurement>(&self, group: &mut BenchmarkGroup<'_, M>) {
        group.bench_function("Rust", |b| {
            b.iter_batched_ref(
                || {
                    // Large range: 1M documents (1 to 1,000,000)
                    // Matches C large range benchmark exactly
                    Wildcard::new(1_000_000)
                },
                |it| {
                    while let Ok(Some(current)) = it.read() {
                        criterion::black_box(current);
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }

    fn rust_read_medium_range<M: Measurement>(&self, group: &mut BenchmarkGroup<'_, M>) {
        group.bench_function("Rust", |b| {
            b.iter_batched_ref(
                || {
                    // Medium range: 500K documents (1 to 500,000)
                    // Matches C medium range benchmark exactly
                    Wildcard::new(500_000)
                },
                |it| {
                    while let Ok(Some(current)) = it.read() {
                        criterion::black_box(current);
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }

    fn rust_skip_to_large_range<M: Measurement>(&self, group: &mut BenchmarkGroup<'_, M>) {
        group.bench_function("Rust", |b| {
            let step = 100;
            b.iter_batched_ref(
                || {
                    // Large range: skip through 1M documents with step=100
                    // Matches C large range benchmark exactly
                    Wildcard::new(1_000_000)
                },
                |it| {
                    while let Ok(Some(current)) = it.skip_to(it.last_doc_id() + step) {
                        criterion::black_box(current);
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }

    fn rust_skip_to_medium_range<M: Measurement>(&self, group: &mut BenchmarkGroup<'_, M>) {
        group.bench_function("Rust", |b| {
            let step = 100;
            b.iter_batched_ref(
                || {
                    // Medium range: skip through 500K documents with step=100
                    // Matches C medium range benchmark exactly
                    Wildcard::new(500_000)
                },
                |it| {
                    while let Ok(Some(current)) = it.skip_to(it.last_doc_id() + step) {
                        criterion::black_box(current);
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }

    fn edge_cases(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Iterator - Wildcard - Edge Cases");

        // Small range benchmark
        group.bench_function("Small Range (100)", |b| {
            b.iter_batched_ref(
                || Wildcard::new(100),
                |it| {
                    while let Ok(Some(current)) = it.read() {
                        criterion::black_box(current);
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });

        // Single document benchmark
        group.bench_function("Single Document", |b| {
            b.iter_batched_ref(
                || Wildcard::new(1),
                |it| {
                    while let Ok(Some(current)) = it.read() {
                        criterion::black_box(current);
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });

        // Large range skip benchmark
        group.bench_function("Large Range Skip", |b| {
            b.iter_batched_ref(
                || Wildcard::new(10_000_000),
                |it| {
                    // Skip to various positions in large range
                    let positions = [1, 1_000_000, 5_000_000, 9_999_999];
                    for &pos in &positions {
                        if let Ok(Some(current)) = it.skip_to(pos) {
                            criterion::black_box(current);
                        }
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });

        group.finish();
    }

    fn variable_steps(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Iterator - Wildcard - Variable Steps");
        let steps = [1, 10, 100, 1000];

        for &step in &steps {
            self.rust_variable_steps(&mut group, step);
        }

        group.finish();
    }

    fn rust_variable_steps<M: Measurement>(&self, group: &mut BenchmarkGroup<'_, M>, step: u64) {
        group.bench_function(BenchmarkId::new("Rust", format!("Step: {}", step)), |b| {
            b.iter_batched_ref(
                || Wildcard::new(100_000),
                |it| {
                    let mut count = 0;
                    while let Ok(Some(current)) = it.skip_to(it.last_doc_id() + step) {
                        criterion::black_box(current);
                        count += 1;
                        if count >= 1000 {
                            // Limit to avoid timeout
                            break;
                        }
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }

    fn random_access(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Iterator - Wildcard - Random Access");
        let targets = [1, 50000, 25000, 75000, 10000, 90000, 5000, 95000];

        for &target in &targets {
            self.rust_random_access(&mut group, target);
        }

        group.finish();
    }

    fn rust_random_access<M: Measurement>(&self, group: &mut BenchmarkGroup<'_, M>, target: u64) {
        group.bench_function(
            BenchmarkId::new("Rust", format!("Target: {}", target)),
            |b| {
                b.iter_batched_ref(
                    || Wildcard::new(100_000),
                    |it| {
                        if let Ok(Some(current)) = it.skip_to(target) {
                            criterion::black_box(current);
                        }
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }

    // ===== DIRECT C BENCHMARKS (NO FFI OVERHEAD) =====

    fn read_large_range_direct(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Iterator - Wildcard - Read Large Range");
        self.c_read_large_range_direct(&mut group);
        self.rust_read_large_range(&mut group); // Same Rust implementation
        group.finish();
    }

    fn read_medium_range_direct(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Iterator - Wildcard - Read Medium Range");
        self.c_read_medium_range_direct(&mut group);
        self.rust_read_medium_range(&mut group); // Same Rust implementation
        group.finish();
    }

    fn skip_to_large_range_direct(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Iterator - Wildcard - SkipTo Large Range");
        self.c_skip_to_large_range_direct(&mut group);
        self.rust_skip_to_large_range(&mut group); // Same Rust implementation
        group.finish();
    }

    fn skip_to_medium_range_direct(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Iterator - Wildcard - SkipTo Medium Range");
        self.c_skip_to_medium_range_direct(&mut group);
        self.rust_skip_to_medium_range(&mut group); // Same Rust implementation
        group.finish();
    }

    fn c_read_large_range_direct<M: Measurement>(&self, group: &mut BenchmarkGroup<'_, M>)
    where
        M::Value: From<Duration>,
    {
        group.bench_function("C", |b| {
            b.iter_custom(|iters| {
                let mut total_time = Duration::ZERO;
                for _ in 0..iters {
                    let result = ffi::QueryIterator::benchmark_wildcard_read_direct(1_000_000);
                    total_time += Duration::from_nanos(result.time_ns);
                    // Verify iteration count matches expected
                    assert_eq!(
                        result.iterations, 1_000_000,
                        "C direct benchmark iteration count mismatch"
                    );
                }
                total_time.into()
            });
        });
    }

    fn c_read_medium_range_direct<M: Measurement>(&self, group: &mut BenchmarkGroup<'_, M>)
    where
        M::Value: From<Duration>,
    {
        group.bench_function("C", |b| {
            b.iter_custom(|iters| {
                let mut total_time = Duration::ZERO;
                for _ in 0..iters {
                    let result = ffi::QueryIterator::benchmark_wildcard_read_direct(500_000);
                    total_time += Duration::from_nanos(result.time_ns);
                    // Verify iteration count matches expected
                    assert_eq!(
                        result.iterations, 500_000,
                        "C direct benchmark iteration count mismatch"
                    );
                }
                total_time.into()
            });
        });
    }

    fn c_skip_to_large_range_direct<M: Measurement>(&self, group: &mut BenchmarkGroup<'_, M>)
    where
        M::Value: From<Duration>,
    {
        group.bench_function("C", |b| {
            b.iter_custom(|iters| {
                let mut total_time = Duration::ZERO;
                for _ in 0..iters {
                    let result =
                        ffi::QueryIterator::benchmark_wildcard_skip_to_direct(1_000_000, 100);
                    total_time += Duration::from_nanos(result.time_ns);
                    // Expected iterations: 1_000_000 / 100 = 10_000
                    assert_eq!(
                        result.iterations, 10_000,
                        "C direct skip_to benchmark iteration count mismatch"
                    );
                }
                total_time.into()
            });
        });
    }

    fn c_skip_to_medium_range_direct<M: Measurement>(&self, group: &mut BenchmarkGroup<'_, M>)
    where
        M::Value: From<Duration>,
    {
        group.bench_function("C", |b| {
            b.iter_custom(|iters| {
                let mut total_time = Duration::ZERO;
                for _ in 0..iters {
                    let result =
                        ffi::QueryIterator::benchmark_wildcard_skip_to_direct(500_000, 100);
                    total_time += Duration::from_nanos(result.time_ns);
                    // Expected iterations: 500_000 / 100 = 5_000
                    assert_eq!(
                        result.iterations, 5_000,
                        "C direct skip_to benchmark iteration count mismatch"
                    );
                }
                total_time.into()
            });
        });
    }
}
