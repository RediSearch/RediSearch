/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! MIN and MAX reducers: fold the numeric value of a source key across the
//! rows of a group, keeping the smallest (MIN) or largest (MAX) value.
//!
//! C parses the reducer arguments and constructs the reducer via
//! `reducers_ffi`.

use bumpalo::Bump;
use rlookup::{RLookupKey, RLookupRow};
use value::{SharedValue, comparison::try_value_as_number};

use crate::Reducer;

/// Group-independent state shared by MIN and MAX.
///
/// Must remain `#[repr(C)]` with [`Reducer`] at offset 0 so the C layer
/// can downcast this struct to `ffi::Reducer*` and read the vtable directly.
#[repr(C)]
pub struct MinMaxReducer<'a> {
    /// C-visible vtable header. Pinned to offset 0 by `#[repr(C)]` so the
    /// C layer can downcast `MinMaxReducer*` to `ffi::Reducer*`.
    reducer: Reducer,
    /// Arena for per-group contexts. [`MinMaxCtx`] has no destructor, so
    /// instances need no explicit drop; the memory is freed when the
    /// reducer (and its [`Bump`]) is dropped.
    arena: Bump,
    srckey: &'a RLookupKey<'a>,
    is_max: bool,
}

const _: () = assert!(core::mem::offset_of!(MinMaxReducer<'_>, reducer) == 0);

/// Per-group accumulator of [`MinMaxReducer`].
pub struct MinMaxCtx {
    val: f64,
}

impl<'a> MinMaxReducer<'a> {
    pub fn new(srckey: &'a RLookupKey<'a>, is_max: bool) -> Self {
        Self {
            reducer: Reducer::new(),
            arena: Bump::new(),
            srckey,
            is_max,
        }
    }

    pub const fn reducer_mut(&mut self) -> &mut Reducer {
        &mut self.reducer
    }

    /// Allocate a per-group accumulator in the arena, seeded so that any
    /// folded value replaces it. An empty group therefore finalizes to
    /// `INFINITY` (MIN) / `-INFINITY` (MAX), matching C.
    pub fn alloc_instance(&self) -> &mut MinMaxCtx {
        self.arena.alloc(MinMaxCtx {
            val: if self.is_max {
                f64::NEG_INFINITY
            } else {
                f64::INFINITY
            },
        })
    }
}

impl MinMaxCtx {
    /// Fold the source key's value of `row` into the accumulator. The
    /// explicit comparisons (rather than `f64::min`/`f64::max`) replicate
    /// C's `MIN`/`MAX` macros, including their NaN behavior.
    pub fn add(&mut self, r: &MinMaxReducer, row: &RLookupRow) {
        if let Some(d) = row.get(r.srckey).and_then(|v| try_value_as_number(v)) {
            let keep_current = if r.is_max { self.val > d } else { self.val < d };
            if !keep_current {
                self.val = d;
            }
        }
    }

    pub fn finalize(&self) -> SharedValue {
        SharedValue::new_num(self.val)
    }
}
