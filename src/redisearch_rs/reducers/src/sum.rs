/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! SUM and AVG reducers: fold the numeric value of a source key across the
//! rows of a group and emit the total (SUM) or the mean (AVG).
//!
//! C parses the reducer arguments and constructs the reducer via
//! `reducers_ffi`.

use bumpalo::Bump;
use rlookup::{RLookupKey, RLookupRow};
use value::{SharedValue, comparison::try_value_as_number};

use crate::Reducer;

/// Group-independent state shared by SUM and AVG.
///
/// Must remain `#[repr(C)]` with [`Reducer`] at offset 0 so the C layer
/// can downcast this struct to `ffi::Reducer*` and read the vtable directly.
#[repr(C)]
pub struct SumReducer<'a> {
    /// C-visible vtable header. Pinned to offset 0 by `#[repr(C)]` so the
    /// C layer can downcast `SumReducer*` to `ffi::Reducer*`.
    reducer: Reducer,
    /// Arena for per-group contexts. [`SumCtx`] has no destructor, so
    /// instances need no explicit drop; the memory is freed when the
    /// reducer (and its [`Bump`]) is dropped.
    arena: Bump,
    srckey: &'a RLookupKey<'a>,
    is_avg: bool,
}

const _: () = assert!(core::mem::offset_of!(SumReducer<'_>, reducer) == 0);

/// Per-group accumulator of [`SumReducer`].
#[derive(Default)]
pub struct SumCtx {
    count: u64,
    total: f64,
}

impl<'a> SumReducer<'a> {
    pub fn new(srckey: &'a RLookupKey<'a>, is_avg: bool) -> Self {
        Self {
            reducer: Reducer::new(),
            arena: Bump::new(),
            srckey,
            is_avg,
        }
    }

    pub const fn reducer_mut(&mut self) -> &mut Reducer {
        &mut self.reducer
    }

    /// Allocate a zeroed per-group accumulator in the arena.
    pub fn alloc_instance(&self) -> &mut SumCtx {
        self.arena.alloc(SumCtx::default())
    }
}

impl SumCtx {
    /// Fold the source key's value of `row` into the accumulator.
    ///
    /// Rows where the key is missing or not coercible to a number are
    /// skipped entirely: they do not count towards the AVG divisor.
    pub fn add(&mut self, r: &SumReducer, row: &RLookupRow) {
        if let Some(d) = row.get(r.srckey).and_then(|v| try_value_as_number(v)) {
            self.total += d;
            self.count += 1;
        }
    }

    /// Emit the total (SUM) or mean (AVG). An empty group yields `NaN`,
    /// not 0.
    pub fn finalize(&self, r: &SumReducer) -> SharedValue {
        let v = if self.count == 0 {
            f64::NAN
        } else if r.is_avg {
            self.total / self.count as f64
        } else {
            self.total
        };
        SharedValue::new_num(v)
    }
}
