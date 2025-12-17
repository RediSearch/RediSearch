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

use criterion::{BenchmarkGroup, Criterion, measurement::WallTime};
use rqe_iterators::{RQEIterator, wildcard::Wildcard};

static WEIGHT: f64 = 1.0;

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
        self.read_large_range(c);
        self.read_medium_range(c);
        self.skip_to_large_range(c);
        self.skip_to_medium_range(c);
    }

    fn read_large_range(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Iterator - Wildcard - Read Large Range");
        group.bench_function("Rust", |b| {
            b.iter_batched_ref(
                || {
                    // Large range: 1M documents (1 to 1,000,000)
                    // Matches C large range benchmark exactly
                    Wildcard::new(1_000_000, WEIGHT)
                },
                |it| {
                    while let Ok(Some(current)) = it.read() {
                        criterion::black_box(current);
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });
        group.finish();
    }

    fn read_medium_range(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Iterator - Wildcard - Read Medium Range");
        group.bench_function("Rust", |b| {
            b.iter_batched_ref(
                || {
                    // Medium range: 500K documents (1 to 500,000)
                    // Matches C medium range benchmark exactly
                    Wildcard::new(500_000, WEIGHT)
                },
                |it| {
                    while let Ok(Some(current)) = it.read() {
                        criterion::black_box(current);
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });
        group.finish();
    }

    fn skip_to_large_range(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Iterator - Wildcard - SkipTo Large Range");
        group.bench_function("Rust", |b| {
            let step = 100;
            b.iter_batched_ref(
                || {
                    // Large range: skip through 1M documents with step=100
                    // Matches C large range benchmark exactly
                    Wildcard::new(1_000_000, WEIGHT)
                },
                |it| {
                    while let Ok(Some(current)) = it.skip_to(it.last_doc_id() + step) {
                        criterion::black_box(current);
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });
        group.finish();
    }

    fn skip_to_medium_range(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Iterator - Wildcard - SkipTo Medium Range");
        group.bench_function("Rust", |b| {
            let step = 100;
            b.iter_batched_ref(
                || {
                    // Medium range: skip through 500K documents with step=100
                    // Matches C medium range benchmark exactly
                    Wildcard::new(500_000, WEIGHT)
                },
                |it| {
                    while let Ok(Some(current)) = it.skip_to(it.last_doc_id() + step) {
                        criterion::black_box(current);
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });
        group.finish();
    }
}
