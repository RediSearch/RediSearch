/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Buffered rank summaries for approximate streaming quantiles.

use std::num::NonZeroUsize;

#[derive(Clone, Copy)]
struct Sample {
    value: f64,
    gap: f32,
    delta: f32,
}

/// An approximate quantile summary with a relative rank error envelope.
///
/// Observations are sorted in batches, merged into the summary, then compressed
/// from largest to smallest. Queries flush pending values without compression.
/// NaN observations are ignored; signed zeros are normalized to positive zero.
pub struct QuantStream {
    buffer: Vec<f64>,
    capacity: NonZeroUsize,
    samples: Vec<Sample>,
    scratch: Vec<Sample>,
    count: usize,
    quantiles: Vec<f64>,
}

impl QuantStream {
    /// Creates a stream, copying `quantiles`. An empty slice selects the
    /// untargeted envelope; otherwise each target contributes an error bound.
    pub fn new(quantiles: &[f64], capacity: NonZeroUsize) -> Self {
        Self {
            buffer: Vec::with_capacity(capacity.get()),
            capacity,
            samples: Vec::new(),
            scratch: Vec::new(),
            count: 0,
            quantiles: quantiles.to_vec(),
        }
    }

    /// Adds an observation, flushing and compressing when the buffer fills.
    pub fn insert(&mut self, value: f64) {
        if value.is_nan() {
            return;
        }
        self.buffer.push(if value == 0.0 { 0.0 } else { value });
        if self.buffer.len() == self.capacity.get() {
            self.flush();
            self.compress();
        }
    }

    /// Returns the number of flushed observations, excluding pending values.
    pub const fn count(&self) -> usize {
        self.count
    }

    /// Flushes pending values and returns the estimate at quantile `q`.
    ///
    /// Empty streams return [`f64::NAN`]. The intended quantile range is
    /// inclusive zero to one; other values follow the same rank arithmetic.
    pub fn query(&mut self, q: f64) -> f64 {
        self.flush();
        let mut target = (q * self.count as f64).ceil();
        target += (self.max_rank_error(target) / 2.0).floor();
        let Some(mut previous) = self.samples.first() else {
            return f64::NAN;
        };
        let mut rank = 0.0;
        for current in &self.samples[1..] {
            if rank + f64::from(current.gap) + f64::from(current.delta) >= target {
                break;
            }
            rank += f64::from(current.gap);
            previous = current;
        }
        previous.value
    }

    fn max_rank_error(&self, rank: f64) -> f64 {
        if self.quantiles.is_empty() {
            return 0.02 * rank;
        }
        self.quantiles.iter().fold(f64::MAX, |bound, &q| {
            let error = if q * self.count as f64 <= rank {
                0.02 * rank / q
            } else {
                0.02 * (self.count as f64 - rank) / (1.0 - q)
            };
            bound.min(error)
        })
    }

    fn flush(&mut self) {
        if self.buffer.is_empty() {
            return;
        }
        self.buffer.sort_unstable_by(f64::total_cmp);
        let mut samples = self.samples.iter().peekable();
        let mut rank = 0.0;
        for &value in &self.buffer {
            while let Some(sample) = samples.next_if(|sample| sample.value <= value) {
                rank += f64::from(sample.gap);
                self.scratch.push(*sample);
            }
            let delta = if samples.peek().is_some() {
                (self.max_rank_error(rank).floor() - 1.0) as f32
            } else {
                0.0
            };
            self.scratch.push(Sample {
                value,
                gap: 1.0,
                delta,
            });
            self.count += 1;
        }
        self.scratch.extend(samples.copied());
        std::mem::swap(&mut self.samples, &mut self.scratch);
        self.scratch.clear();
        self.buffer.clear();
    }

    fn compress(&mut self) {
        let mut parent = self.samples.len() - 1;
        // Rank fields and this initial subtraction round at single precision.
        let mut rank = f64::from((self.count - 1) as f32 - self.samples[parent].gap);
        for index in (0..parent).rev() {
            let current = self.samples[index];
            let next = self.samples[parent];
            if f64::from(current.gap + next.gap + next.delta) <= self.max_rank_error(rank) {
                self.samples[parent].gap += current.gap;
            } else {
                parent -= 1;
                self.samples[parent] = current;
            }
            rank -= f64::from(current.gap);
        }
        self.samples.drain(..parent);
    }
}
