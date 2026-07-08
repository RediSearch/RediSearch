/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! RANDOM_SAMPLE reducer: keep a uniform reservoir sample of up to `len`
//! values of a source key across the rows of a group, emitted as an array.
//!
//! C parses the reducer arguments (including the `MAX_SAMPLE_SIZE` bound)
//! and constructs the reducer via `reducers_ffi`.

use bumpalo::Bump;
use rand::RngExt;
use rlookup::{RLookupKey, RLookupRow};
use value::SharedValue;

use crate::Reducer;

/// Group-independent state of RANDOM_SAMPLE.
///
/// Must remain `#[repr(C)]` with [`Reducer`] at offset 0 so the C layer
/// can downcast this struct to `ffi::Reducer*` and read the vtable directly.
#[repr(C)]
pub struct RandomSampleReducer<'a> {
    /// C-visible vtable header. Pinned to offset 0 by `#[repr(C)]` so the
    /// C layer can downcast `RandomSampleReducer*` to `ffi::Reducer*`.
    reducer: Reducer,
    /// Arena for per-group contexts. Because [`RandomSampleCtx`] is
    /// arena-allocated ([`Bump`] does not run destructors),
    /// [`drop_in_place`][std::ptr::drop_in_place] must be called to
    /// decrement the sampled [`SharedValue`] refcounts.
    arena: Bump,
    srckey: &'a RLookupKey<'a>,
    /// Reservoir capacity (the requested sample size).
    len: usize,
}

const _: () = assert!(core::mem::offset_of!(RandomSampleReducer<'_>, reducer) == 0);

/// Per-group reservoir of [`RandomSampleReducer`].
#[derive(Default)]
pub struct RandomSampleCtx {
    /// How many candidate values have been seen (sampled or not).
    seen: u64,
    samples: Vec<SharedValue>,
}

impl<'a> RandomSampleReducer<'a> {
    pub fn new(srckey: &'a RLookupKey<'a>, len: usize) -> Self {
        Self {
            reducer: Reducer::new(),
            arena: Bump::new(),
            srckey,
            len,
        }
    }

    pub const fn reducer_mut(&mut self) -> &mut Reducer {
        &mut self.reducer
    }

    /// Allocate an empty per-group reservoir in the arena.
    pub fn alloc_instance(&self) -> &mut RandomSampleCtx {
        self.arena.alloc(RandomSampleCtx {
            seen: 0,
            samples: Vec::with_capacity(self.len),
        })
    }
}

impl RandomSampleCtx {
    /// Consider the source key's value of `row` for the reservoir
    /// ([Algorithm R](https://en.wikipedia.org/wiki/Reservoir_sampling)).
    /// Rows without the key are skipped. Unlike C's `rand() %`, the
    /// replacement index is drawn without modulo bias.
    pub fn add(&mut self, r: &RandomSampleReducer, row: &RLookupRow) {
        let Some(v) = row.get(r.srckey) else {
            return;
        };
        if self.samples.len() < r.len {
            self.samples.push(v.clone());
        } else {
            let i = rand::rng().random_range(0..=self.seen);
            if (i as usize) < r.len {
                self.samples[i as usize] = v.clone();
            }
        }
        self.seen += 1;
    }

    /// Emit the sampled values as an array.
    pub fn finalize(&self) -> SharedValue {
        SharedValue::new_array(self.samples.iter().cloned())
    }
}
