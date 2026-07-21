/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Benchmark the geometry (GEOSHAPE) query iterator.

use std::{hint::black_box, time::Duration};

use criterion::{BenchmarkGroup, Criterion, measurement::WallTime};
use rqe_core::DocId;
use rqe_iterators::{NoOpChecker, NoTracker, RQEIterator, geo_shape::GeoShape, utils::NoTimeoutChecker};

/// The Rust [`GeoShape`] configured the way the sorted-id-list benchmark needs:
/// no timeout, no field-expiration, no memory tracking.
type RustGeoShape = GeoShape<'static, NoTimeoutChecker, NoOpChecker, NoTracker>;

#[derive(Default)]
pub struct Bencher;

impl Bencher {
    const MEASUREMENT_TIME: Duration = Duration::from_millis(500);
    const WARMUP_TIME: Duration = Duration::from_millis(200);

    /// Number of matched documents the iterator walks over.
    const NUM_DOCS: DocId = 1_000_000;
    /// Stride between `skip_to` targets.
    const SKIP_TO_STEP: DocId = 100;
    /// Spacing between document IDs in the sparse data set.
    const SPARSE_STEP: DocId = 1000;

    fn benchmark_group<'a>(
        &self,
        c: &'a mut Criterion,
        label: &str,
    ) -> BenchmarkGroup<'a, WallTime> {
        super::group(c, label, Self::MEASUREMENT_TIME, Self::WARMUP_TIME)
    }

    /// Contiguous matches: every document ID from 1 to `NUM_DOCS`.
    fn dense_ids() -> Vec<DocId> {
        (1..=Self::NUM_DOCS).collect()
    }

    /// Spread-out matches: `NUM_DOCS` IDs spaced `SPARSE_STEP` apart, so most
    /// `skip_to` targets land between entries (a `NOTFOUND` result).
    fn sparse_ids() -> Vec<DocId> {
        (1..=Self::NUM_DOCS)
            .map(|x| x * Self::SPARSE_STEP)
            .collect()
    }

    /// Build a Rust [`GeoShape`] over `ids` (no timeout/expiration/tracking).
    fn rust_iter(ids: Vec<DocId>) -> RustGeoShape {
        GeoShape::new(ids, NoTimeoutChecker, NoOpChecker, NoTracker)
    }

    pub fn bench(&self, c: &mut Criterion) {
        self.read(c);
        self.skip_to_dense(c);
        self.skip_to_sparse(c);
    }

    /// Benchmark reading every matched document in order.
    fn read(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Iterator - GeoShape - Read");

        group.bench_function("Rust", |b| {
            let ids = Self::dense_ids();
            b.iter_batched_ref(
                || Self::rust_iter(ids.clone()),
                |it| {
                    while let Ok(Some(current)) = it.read() {
                        black_box(current);
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });

        group.finish();
    }

    /// Benchmark `skip_to` over contiguous matches (every target hits exactly).
    fn skip_to_dense(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Iterator - GeoShape - SkipTo Dense");

        group.bench_function("Rust", |b| {
            let ids = Self::dense_ids();
            b.iter_batched_ref(
                || Self::rust_iter(ids.clone()),
                |it| {
                    while let Ok(Some(current)) = it.skip_to(it.last_doc_id() + Self::SKIP_TO_STEP)
                    {
                        black_box(current);
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });

        group.finish();
    }

    /// Benchmark `skip_to` over spread-out matches (most targets miss).
    fn skip_to_sparse(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Iterator - GeoShape - SkipTo Sparse");

        group.bench_function("Rust", |b| {
            let ids = Self::sparse_ids();
            b.iter_batched_ref(
                || Self::rust_iter(ids.clone()),
                |it| {
                    while let Ok(Some(current)) = it.skip_to(it.last_doc_id() + Self::SKIP_TO_STEP)
                    {
                        black_box(current);
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });

        group.finish();
    }
}
