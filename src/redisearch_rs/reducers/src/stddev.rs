/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! STDDEV reducer: sample standard deviation of the numeric values of a
//! source key across the rows of a group, via Welford's online algorithm
//! (<https://www.johndcook.com/blog/standard_deviation/>).
//!
//! Array values are expanded element-wise: the distributed planner rewrites
//! `STDDEV` into a shard-side `RANDOM_SAMPLE` whose array output feeds the
//! merge-side `STDDEV` (see `distributeStdDev` in `dist_plan.cpp`).
//!
//! C parses the reducer arguments and constructs the reducer via
//! `reducers_ffi`.

use bumpalo::Bump;
use rlookup::{RLookupKey, RLookupRow};
use value::{SharedValue, Value, comparison::try_value_as_number};

use crate::Reducer;

/// Group-independent state of STDDEV.
///
/// Must remain `#[repr(C)]` with [`Reducer`] at offset 0 so the C layer
/// can downcast this struct to `ffi::Reducer*` and read the vtable directly.
#[repr(C)]
pub struct StdDevReducer<'a> {
    /// C-visible vtable header. Pinned to offset 0 by `#[repr(C)]` so the
    /// C layer can downcast `StdDevReducer*` to `ffi::Reducer*`.
    reducer: Reducer,
    /// Arena for per-group contexts. [`StdDevCtx`] has no destructor, so
    /// instances need no explicit drop; the memory is freed when the
    /// reducer (and its [`Bump`]) is dropped.
    arena: Bump,
    srckey: &'a RLookupKey<'a>,
}

const _: () = assert!(core::mem::offset_of!(StdDevReducer<'_>, reducer) == 0);

/// Per-group accumulator of [`StdDevReducer`]: Welford's running count,
/// mean, and sum of squared distances from the mean.
#[derive(Default)]
pub struct StdDevCtx {
    n: u64,
    m: f64,
    s: f64,
}

impl<'a> StdDevReducer<'a> {
    pub fn new(srckey: &'a RLookupKey<'a>) -> Self {
        Self {
            reducer: Reducer::new(),
            arena: Bump::new(),
            srckey,
        }
    }

    pub const fn reducer_mut(&mut self) -> &mut Reducer {
        &mut self.reducer
    }

    /// Allocate a zeroed per-group accumulator in the arena.
    pub fn alloc_instance(&self) -> &mut StdDevCtx {
        self.arena.alloc(StdDevCtx::default())
    }
}

impl StdDevCtx {
    fn push(&mut self, d: f64) {
        self.n += 1;
        if self.n == 1 {
            self.m = d;
            self.s = 0.0;
        } else {
            let new_m = self.m + (d - self.m) / self.n as f64;
            self.s += (d - self.m) * (d - new_m);
            self.m = new_m;
        }
    }

    /// Fold the source key's value of `row` into the accumulator; arrays
    /// are expanded element-wise. Non-numeric values are skipped.
    pub fn add(&mut self, r: &StdDevReducer, row: &RLookupRow) {
        let Some(v) = row.get(r.srckey) else {
            return;
        };
        match &**v {
            Value::Array(items) => {
                for item in items.iter() {
                    if let Some(d) = try_value_as_number(item) {
                        self.push(d);
                    }
                }
            }
            other => {
                if let Some(d) = try_value_as_number(other) {
                    self.push(d);
                }
            }
        }
    }

    /// Emit the sample standard deviation; groups with fewer than two
    /// values yield 0.
    pub fn finalize(&self) -> SharedValue {
        let variance = if self.n > 1 {
            self.s / (self.n - 1) as f64
        } else {
            0.0
        };
        SharedValue::new_num(variance.sqrt())
    }
}
