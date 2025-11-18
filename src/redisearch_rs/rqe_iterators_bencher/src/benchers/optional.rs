/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Benchmark Optional iterator.
//!
//! Dense = child covers the full range (all real results, weight applied)
//! Sparse = no child (all virtual results)

use std::time::Duration;

use criterion::{BenchmarkGroup, Criterion, measurement::WallTime};
use rqe_iterators::{RQEIterator, empty::Empty, optional::Optional, wildcard::Wildcard};

use crate::ffi;

#[derive(Default)]
pub struct Bencher;

impl Bencher {
    const MEASUREMENT_TIME: Duration = Duration::from_millis(1000);
    const WARMUP_TIME: Duration = Duration::from_millis(200);

    const LARGE_MAX: u64 = 1_000_000;
    const STEP: u64 = 100;
    const WEIGHT: f64 = 1.0;

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
        self.read_dense(c);
        self.read_sparse(c);
        self.skip_to_dense(c);
        self.skip_to_sparse(c);
    }

    fn read_dense(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Iterator - Optional - Read Dense");

        group.bench_function("C", |b| {
            b.iter_batched_ref(
                || ffi::QueryIterator::new_optional_full_child(Self::LARGE_MAX, Self::WEIGHT),
                |it| {
                    while it.read() == ::ffi::IteratorStatus_ITERATOR_OK {
                        criterion::black_box(it.current());
                    }
                    it.free();
                },
                criterion::BatchSize::SmallInput,
            );
        });

        group.bench_function("Rust", |b| {
            b.iter_batched_ref(
                || {
                    let child = Wildcard::new(Self::LARGE_MAX);
                    Optional::new(Self::LARGE_MAX, Self::WEIGHT, child)
                },
                |it| {
                    while let Ok(Some(current)) = it.read() {
                        // touch fields to avoid elision
                        criterion::black_box(current.doc_id);
                        criterion::black_box(current.weight);
                        criterion::black_box(current.freq);
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });

        group.finish();
    }

    fn read_sparse(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Iterator - Optional - Read Sparse");

        group.bench_function("C", |b| {
            b.iter_batched_ref(
                || ffi::QueryIterator::new_optional_virtual_only(Self::LARGE_MAX, Self::WEIGHT),
                |it| {
                    while it.read() == ::ffi::IteratorStatus_ITERATOR_OK {
                        criterion::black_box(it.current());
                    }
                    it.free();
                },
                criterion::BatchSize::SmallInput,
            );
        });

        group.bench_function("Rust", |b| {
            b.iter_batched_ref(
                || Optional::new(Self::LARGE_MAX, Self::WEIGHT, Empty),
                |it| {
                    while let Ok(Some(current)) = it.read() {
                        criterion::black_box(current.doc_id);
                        criterion::black_box(current.weight);
                        criterion::black_box(current.freq);
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });

        group.finish();
    }

    fn skip_to_dense(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Iterator - Optional - SkipTo Dense");

        group.bench_function("C", |b| {
            let step = Self::STEP;
            b.iter_batched_ref(
                || ffi::QueryIterator::new_optional_full_child(Self::LARGE_MAX, Self::WEIGHT),
                |it| {
                    while it.skip_to(it.last_doc_id() + step) != ::ffi::IteratorStatus_ITERATOR_EOF
                    {
                        criterion::black_box(it.current());
                    }
                    it.free();
                },
                criterion::BatchSize::SmallInput,
            );
        });

        group.bench_function("Rust", |b| {
            let step = Self::STEP;
            b.iter_batched_ref(
                || {
                    let child = Wildcard::new(Self::LARGE_MAX);
                    Optional::new(Self::LARGE_MAX, Self::WEIGHT, child)
                },
                |it| {
                    while let Ok(Some(outcome)) = it.skip_to(it.last_doc_id() + step) {
                        match outcome {
                            rqe_iterators::SkipToOutcome::Found(r)
                            | rqe_iterators::SkipToOutcome::NotFound(r) => {
                                criterion::black_box(r.doc_id);
                                criterion::black_box(r.weight);
                                criterion::black_box(r.freq);
                            }
                        }
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });

        group.finish();
    }

    fn skip_to_sparse(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Iterator - Optional - SkipTo Sparse");

        group.bench_function("C", |b| {
            let step = Self::STEP;
            b.iter_batched_ref(
                || ffi::QueryIterator::new_optional_virtual_only(Self::LARGE_MAX, Self::WEIGHT),
                |it| {
                    while it.skip_to(it.last_doc_id() + step) != ::ffi::IteratorStatus_ITERATOR_EOF
                    {
                        criterion::black_box(it.current());
                    }
                    it.free();
                },
                criterion::BatchSize::SmallInput,
            );
        });

        group.bench_function("Rust", |b| {
            let step = Self::STEP;
            b.iter_batched_ref(
                || Optional::new(Self::LARGE_MAX, Self::WEIGHT, Empty),
                |it| {
                    while let Ok(Some(outcome)) = it.skip_to(it.last_doc_id() + step) {
                        match outcome {
                            rqe_iterators::SkipToOutcome::Found(r)
                            | rqe_iterators::SkipToOutcome::NotFound(r) => {
                                criterion::black_box(r.doc_id);
                                criterion::black_box(r.weight);
                                criterion::black_box(r.freq);
                            }
                        }
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });

        group.finish();
    }
}
