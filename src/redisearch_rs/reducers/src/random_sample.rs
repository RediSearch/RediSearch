/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! The `RANDOM_SAMPLE` reducer.

use std::mem::MaybeUninit;

use bumpalo::Bump;
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
    /// One slot per value the sample can hold, allocated in the arena when the group
    /// starts, as the C reducer did, so a group never allocates on the heap. Only
    /// the first `len` slots are initialized.
    slots: &'arena mut [MaybeUninit<SharedValue>],
    len: usize,
}

impl<'arena> RandomSampleState<'arena> {
    fn new(arena: &'arena Bump, size: usize) -> Self {
        Self {
            seen: 0,
            slots: arena.alloc_slice_fill_with(size, |_| MaybeUninit::uninit()),
            len: 0,
        }
    }

    /// Keeps `value` in the next free slot, if any is left.
    fn push(&mut self, value: &SharedValue) -> bool {
        let Some(slot) = self.slots.get_mut(self.len) else {
            return false;
        };
        slot.write(value.clone());
        self.len += 1;
        true
    }

    const fn samples(&self) -> &[SharedValue] {
        // SAFETY: the first `len` slots are initialized, and `MaybeUninit<T>` has
        // the layout of `T`.
        unsafe { std::slice::from_raw_parts(self.slots.as_ptr().cast(), self.len) }
    }

    const fn samples_mut(&mut self) -> &mut [SharedValue] {
        // SAFETY: as in `samples`.
        unsafe { std::slice::from_raw_parts_mut(self.slots.as_mut_ptr().cast(), self.len) }
    }
}

impl Drop for RandomSampleState<'_> {
    fn drop(&mut self) {
        // SAFETY: the samples are initialized, and the arena never drops them, so
        // this is the only place they are dropped.
        unsafe { std::ptr::drop_in_place(self.samples_mut()) }
    }
}

impl Accumulator for RandomSample<'_> {
    type State<'arena> = RandomSampleState<'arena>;

    fn init<'arena>(&self, arena: &'arena Bump) -> RandomSampleState<'arena> {
        RandomSampleState::new(arena, self.size)
    }

    fn add(&self, state: &mut RandomSampleState<'_>, row: &RLookupRow<'_>) {
        let Some(value) = row.get(self.key) else {
            return;
        };
        if !state.push(value) {
            // The new value replaces a sampled one with probability `size / (seen + 1)`.
            let slot = random_below(state.seen + 1);
            if let Some(sample) = state.samples_mut().get_mut(slot) {
                *sample = value.clone();
            }
        }
        state.seen += 1;
    }

    fn finalize(&self, state: &RandomSampleState<'_>) -> SharedValue {
        SharedValue::new_array(state.samples().iter().cloned())
    }
}

/// A pseudo-random number in `0..bound`, from the C library's generator, which
/// the C reducer used.
fn random_below(bound: usize) -> usize {
    // SAFETY: `rand` has no preconditions. It is not required to be thread-safe:
    // glibc and macOS lock around its seed, while on a libc that does not, concurrent
    // queries race on it, as they did with the C reducer this replaces.
    let random = unsafe { libc::rand() };
    // `rand` returns a non-negative `int`.
    random as usize % bound
}
