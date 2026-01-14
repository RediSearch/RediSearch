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

use ::ffi::t_docId;
use criterion::{BenchmarkGroup, Criterion, measurement::WallTime};
use rand::{Rng as _, SeedableRng as _, rngs::StdRng};
use rqe_iterators::{IdList, RQEIterator, empty::Empty, optional::Optional, wildcard::Wildcard};

use crate::{RedisModule_Alloc, ffi};

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
        self.read_id_list_ratios(c);
    }

    fn read_dense(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Iterator - Optional - Read Dense");

        group.bench_function("C", |b| {
            b.iter_batched_ref(
                || {
                    ffi::QueryIterator::new_optional_full_child_wildcard(
                        Self::LARGE_MAX,
                        Self::WEIGHT,
                    )
                },
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
                    let child = Wildcard::new(Self::LARGE_MAX, 0.);
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
                || {
                    ffi::QueryIterator::new_optional_full_child_wildcard(
                        Self::LARGE_MAX,
                        Self::WEIGHT,
                    )
                },
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
                    let child = Wildcard::new(Self::LARGE_MAX, 1.);
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

    fn read_id_list_ratios(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Iterator - Optional - IdList");

        // 0, 10, 20, ..., 90 percent
        let child_ratios = [0_u64, 10, 20, 30, 40, 50, 60, 70, 80, 90];

        for &ratio in &child_ratios {
            let child_ratio_f = ratio as f64 / 100.0;

            group.bench_function(format!("Rust child_ratio={}", ratio), |b| {
                b.iter_batched(
                    || {
                        // setup
                        Self::make_optional_with_id_list(child_ratio_f)
                    },
                    |mut it| {
                        // measurement, full scan
                        while let Ok(Some(current)) = it.read() {
                            criterion::black_box(current.doc_id);
                            criterion::black_box(current.weight);
                            criterion::black_box(current.freq);
                        }
                    },
                    criterion::BatchSize::SmallInput,
                );
            });

            group.bench_function(format!("C child_ratio={}", ratio), |b| {
                b.iter_batched(
                    || Self::make_c_optional_with_id_list(child_ratio_f),
                    |it| {
                        while it.read() == ::ffi::IteratorStatus_ITERATOR_OK {
                            criterion::black_box(it.current());
                        }
                        it.free();
                    },
                    criterion::BatchSize::SmallInput,
                );
            });
        }

        group.finish();
    }

    fn make_child_doc_ids(child_ratio: f64) -> Vec<u64> {
        let mut rng = StdRng::seed_from_u64(42);
        let mut child_doc_ids = Vec::new();

        for doc_id in 1..=Self::LARGE_MAX {
            if rng.random::<f64>() < child_ratio {
                child_doc_ids.push(doc_id);
            }
        }

        child_doc_ids.sort();
        child_doc_ids
    }

    fn make_optional_with_id_list<'index>(
        child_ratio: f64,
    ) -> Optional<'index, IdList<'index, true>> {
        let child_doc_ids = Self::make_child_doc_ids(child_ratio);

        let child = IdList::new(child_doc_ids);
        Optional::new(Self::LARGE_MAX, Self::WEIGHT, child)
    }

    /// # Safety
    ///
    /// Callee needs to make sure to Free the memory
    unsafe fn make_c_child_doc_ids(child_ratio: f64) -> (*mut t_docId, u64) {
        let doc_id_vec = Self::make_child_doc_ids(child_ratio);
        let n = doc_id_vec.len();
        let out = unsafe { RedisModule_Alloc(std::mem::size_of::<t_docId>() * n) } as *mut t_docId;
        unsafe { out.copy_from(doc_id_vec.as_slice().as_ptr(), n) };
        (out, n as u64)
    }

    fn make_c_optional_with_id_list<'index>(child_ratio: f64) -> ffi::QueryIterator {
        let (child_doc_ids_array, ids_len) = unsafe { Self::make_c_child_doc_ids(child_ratio) };

        // SAFETY: our wrapper ensures to free the child doc ids array
        let child = unsafe {
            iterators_ffi::id_list::NewSortedIdListIterator(
                child_doc_ids_array,
                ids_len as u64,
                Self::WEIGHT,
            ) as *mut crate::ffi::QueryIterator
        };

        ffi::QueryIterator::new_optional_full_child(Self::LARGE_MAX, Self::WEIGHT, child)
    }
}
