/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! The `RANDOM_SAMPLE` reducer.

use bumpalo::Bump;
use bumpalo::collections::Vec;
use rlookup::{RLookupKey, RLookupRow};
use value::SharedValue;

use crate::accumulator::Accumulator;

/// `RANDOM_SAMPLE` of a property: up to `size` of its values in the group, chosen
/// at random by reservoir sampling over the C library's `rand` (so with its small
/// modulo bias). Rows where the property is missing are skipped and not counted.
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
pub struct RandomSampleState<'arena> {
    /// The number of values seen, kept or not.
    seen: usize,
    /// The sample, with room for all of it reserved in the arena when the group
    /// starts, as the C reducer did, so a group never allocates on the heap.
    samples: Vec<'arena, SharedValue>,
}

impl Accumulator for RandomSample<'_> {
    type State<'arena> = RandomSampleState<'arena>;

    fn init<'arena>(&self, arena: &'arena Bump) -> RandomSampleState<'arena> {
        RandomSampleState {
            seen: 0,
            samples: Vec::with_capacity_in(self.size, arena),
        }
    }

    fn add(&self, state: &mut RandomSampleState<'_>, row: &RLookupRow<'_>) {
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

    fn finalize(&self, state: &RandomSampleState<'_>) -> SharedValue {
        SharedValue::new_array(state.samples.iter().cloned())
    }
}

/// A pseudo-random number in `0..bound`, from the C library's generator, which
/// the C reducer used.
fn random_below(bound: usize) -> usize {
    // TODO: replace `rand` with a thread-safe generator.
    // SAFETY: `rand` has no preconditions. Its thread safety depends on the libc.
    let random = unsafe { libc::rand() };
    // `rand` returns a non-negative `int`.
    random as usize % bound
}
