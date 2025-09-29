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
    BenchmarkGroup, Criterion,
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
