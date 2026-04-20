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
//! Both `Read` and `SkipTo` are benchmarked across a range of child doc ratios (0–90%),
//! using an `IdList` child and `maxDocId=1_000_000`.

use std::{hint::black_box, time::Duration};

use criterion::{BenchmarkGroup, Criterion, measurement::WallTime};
use rand::{Rng as _, SeedableRng as _, rngs::StdRng};
use rqe_iterators::{IdList, RQEIterator, SkipToOutcome, optional::Optional};

#[derive(Default)]
pub struct Bencher;

impl Bencher {
    const MEASUREMENT_TIME: Duration = Duration::from_millis(1000);
    const WARMUP_TIME: Duration = Duration::from_millis(200);

    const MAX_DOC_ID: u64 = 1_000_000;
    const STEP: u64 = 10;
    const WEIGHT: f64 = 1.0;
    /// Child doc ratios (%), from 0% to 90% in steps of 10.
    const CHILD_RATIOS: [u64; 10] = [0, 10, 20, 30, 40, 50, 60, 70, 80, 90];

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
        let mut group = self.benchmark_group(c, "Iterator - Optional - Read");

        for &ratio in &Self::CHILD_RATIOS {
            let child_ratio_f = ratio as f64 / 100.0;

            group.bench_function(format!("Rust child_ratio={ratio}"), |b| {
                b.iter_batched(
                    || Self::make_optional(child_ratio_f),
                    |mut it| {
                        while let Ok(Some(current)) = it.read() {
                            black_box(current.doc_id);
                            black_box(current.weight);
                            black_box(current.freq);
                        }
                    },
                    criterion::BatchSize::SmallInput,
                );
            });
        }

        group.finish();
    }

    fn skip_to(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Iterator - Optional - SkipTo");

        for &ratio in &Self::CHILD_RATIOS {
            let child_ratio_f = ratio as f64 / 100.0;

            group.bench_function(format!("Rust child_ratio={ratio}"), |b| {
                b.iter_batched(
                    || Self::make_optional(child_ratio_f),
                    |mut it| {
                        while let Ok(Some(outcome)) = it.skip_to(it.last_doc_id() + Self::STEP) {
                            match outcome {
                                SkipToOutcome::Found(r) | SkipToOutcome::NotFound(r) => {
                                    black_box(r.doc_id);
                                    black_box(r.weight);
                                    black_box(r.freq);
                                }
                            }
                        }
                    },
                    criterion::BatchSize::SmallInput,
                );
            });
        }

        group.finish();
    }

    fn make_child_doc_ids(child_ratio: f64) -> Vec<u64> {
        let mut rng = StdRng::seed_from_u64(42);
        let mut ids = Vec::new();
        for doc_id in 1..=Self::MAX_DOC_ID {
            if rng.random::<f64>() < child_ratio {
                ids.push(doc_id);
            }
        }
        ids
    }

    fn make_optional<'index>(child_ratio: f64) -> Optional<'index, IdList<'index, true>> {
        let child_doc_ids = Self::make_child_doc_ids(child_ratio);
        let child = IdList::new(child_doc_ids);
        Optional::new(Self::MAX_DOC_ID, Self::WEIGHT, child)
    }
}
