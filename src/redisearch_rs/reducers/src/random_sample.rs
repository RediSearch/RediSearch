/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! The `RANDOM_SAMPLE` reducer.

use rlookup::{RLookupKey, RLookupRow};
use value::SharedValue;

use crate::accumulator::Accumulator;

/// `RANDOM_SAMPLE` of a property: up to `size` of its values in the group, each
/// equally likely to be kept (reservoir sampling). Rows where the property is
/// missing are skipped and not counted.
pub struct RandomSample<'a> {
    key: &'a RLookupKey<'a>,
    size: usize,
}

impl<'a> RandomSample<'a> {
    pub const fn new(key: &'a RLookupKey<'a>, size: usize) -> Self {
        Self { key, size }
    }
}

/// The per-group state of [`RandomSample`].
#[derive(Default)]
pub struct RandomSampleState {
    /// The number of values seen, kept or not.
    seen: usize,
    /// The sample: the first `size` values seen, then replaced at random.
    samples: Vec<SharedValue>,
}

impl Accumulator for RandomSample<'_> {
    type State = RandomSampleState;

    fn init(&self) -> RandomSampleState {
        RandomSampleState::default()
    }

    fn add(&self, state: &mut RandomSampleState, row: &RLookupRow<'_>) {
        let Some(value) = row.get(self.key) else {
            return;
        };
        if state.samples.len() < self.size {
            state.samples.push(value.clone());
        } else {
            // The new value replaces a sampled one with probability `size / (seen + 1)`.
            let slot = random_below(state.seen + 1);
            if let Some(sample) = state.samples.get_mut(slot) {
                *sample = value.clone();
            }
        }
        state.seen += 1;
    }

    fn finalize(&self, state: &RandomSampleState) -> SharedValue {
        SharedValue::new_array(state.samples.iter().cloned())
    }
}

/// A pseudo-random number in `0..bound`, from the C library's generator, which
/// the C reducer used.
fn random_below(bound: usize) -> usize {
    // SAFETY: `rand` has no preconditions.
    let random = unsafe { libc::rand() };
    // `rand` returns a non-negative `int`.
    random as usize % bound
}
