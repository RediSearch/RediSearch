/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Benchmark Empty iterator.
//!
//! This is more of an example than a proper benchmark as there is no
//! actual code to benchmark.

use std::time::Duration;

use criterion::{BenchmarkGroup, Criterion, measurement::WallTime};
use rqe_iterators::{RQEIterator, empty::Empty};

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
        self.read(c);
        self.skip_to(c);
    }

    fn read(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Iterator - Empty - Read");
        group.bench_function("Rust", |b| {
            b.iter(|| {
                let mut it = Empty;
                let _ = it.read();
            });
        });
        group.finish();
    }

    fn skip_to(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Iterator - Empty - SkipTo");
        group.bench_function("Rust", |b| {
            b.iter(|| {
                let mut it = Empty;
                let _ = it.skip_to(0);
            });
        });
        group.finish();
    }
}
