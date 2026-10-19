/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Drop-in wrappers around criterion's [`criterion::BenchmarkGroup`] and
//! [`criterion::Bencher`] that report each benchmark routine to hotpath.
//!
//! Routine calls run in a [`hotpath::measure_block!`] labeled
//! `<group>/<id>`, so a hotpath report gets one row per benchmark. Setup
//! closures run outside the block: their allocations are not attributed to
//! the benchmark. Calls to very fast routines are sampled (see `measured`),
//! so a report's call counts are of measured calls only. Without the
//! `hotpath` feature routines run unwrapped.

// TODO: move this wrapper to hotpath? https://github.com/pawurb/hotpath-rs/issues/708

use criterion::{
    BatchSize,
    measurement::{Measurement, WallTime},
};

/// A [`criterion::BenchmarkGroup`] whose benchmarks are reported to hotpath.
pub struct BenchmarkGroup<'a, M: Measurement = WallTime> {
    inner: criterion::BenchmarkGroup<'a, M>,
    name: String,
}

impl<'a, M: Measurement> BenchmarkGroup<'a, M> {
    pub fn new(inner: criterion::BenchmarkGroup<'a, M>, name: impl Into<String>) -> Self {
        Self {
            inner,
            name: name.into(),
        }
    }

    pub fn measurement_time(&mut self, dur: std::time::Duration) -> &mut Self {
        self.inner.measurement_time(dur);
        self
    }

    pub fn warm_up_time(&mut self, dur: std::time::Duration) -> &mut Self {
        self.inner.warm_up_time(dur);
        self
    }

    /// See [`criterion::BenchmarkGroup::bench_function`].
    pub fn bench_function<F>(&mut self, id: impl Into<String>, mut f: F) -> &mut Self
    where
        F: FnMut(&mut Bencher<'_, '_, M>),
    {
        let id = id.into();
        // hotpath labels are `&'static str`; leaking one short string per
        // benchmark is negligible in a bench binary.
        let label: &'static str = format!("{}/{id}", self.name).leak();
        self.inner.bench_function(id, |b| {
            f(&mut Bencher { inner: b, label });
        });
        self
    }

    pub fn finish(self) {
        self.inner.finish();
    }
}

/// A [`criterion::Bencher`] that runs each routine call in a hotpath block.
pub struct Bencher<'b, 'a, M: Measurement = WallTime> {
    inner: &'b mut criterion::Bencher<'a, M>,
    label: &'static str,
}

impl<M: Measurement> Bencher<'_, '_, M> {
    /// See [`criterion::Bencher::iter`].
    pub fn iter<O, R>(&mut self, mut routine: R)
    where
        R: FnMut() -> O,
    {
        let label = self.label;
        self.inner.iter(|| measured(label, &mut routine));
    }

    /// See [`criterion::Bencher::iter_batched`].
    pub fn iter_batched<I, O, S, R>(&mut self, setup: S, mut routine: R, size: BatchSize)
    where
        S: FnMut() -> I,
        R: FnMut(I) -> O,
    {
        let label = self.label;
        self.inner
            .iter_batched(setup, |input| measured(label, || routine(input)), size);
    }

    /// See [`criterion::Bencher::iter_batched_ref`].
    pub fn iter_batched_ref<I, O, S, R>(&mut self, setup: S, mut routine: R, size: BatchSize)
    where
        S: FnMut() -> I,
        R: FnMut(&mut I) -> O,
    {
        let label = self.label;
        self.inner.iter_batched_ref(
            setup,
            |input: &mut I| measured(label, || routine(input)),
            size,
        );
    }
}

/// Minimum time between the starts of two measured routine calls on a thread.
///
/// hotpath queues one event per measured call, and its background thread
/// cannot drain the tens of millions per second a nanosecond-scale routine
/// produces: the queue then grows until the process runs out of memory.
/// Routines slower than this are still measured on every call.
#[cfg(feature = "hotpath")]
const MIN_MEASURE_INTERVAL: std::time::Duration = std::time::Duration::from_micros(10);

/// Runs `routine`, in a hotpath block when [`MIN_MEASURE_INTERVAL`] has
/// elapsed since the last measured call.
#[cfg(feature = "hotpath")]
#[inline(always)]
fn measured<O>(label: &'static str, routine: impl FnOnce() -> O) -> O {
    use std::{cell::Cell, time::Instant};

    thread_local! {
        static NEXT_MEASURE: Cell<Option<Instant>> = const { Cell::new(None) };
    }

    let now = Instant::now();
    if NEXT_MEASURE.get().is_some_and(|next| now < next) {
        return routine();
    }
    NEXT_MEASURE.set(Some(now + MIN_MEASURE_INTERVAL));
    hotpath::measure_block!(label, routine())
}

#[cfg(not(feature = "hotpath"))]
#[inline(always)]
fn measured<O>(_label: &'static str, routine: impl FnOnce() -> O) -> O {
    routine()
}
