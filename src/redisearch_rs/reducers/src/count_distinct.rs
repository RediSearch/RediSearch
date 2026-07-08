/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! COUNT_DISTINCT reducer: exact distinct count of a source key's values
//! across the rows of a group, deduplicated by their 64-bit value hash
//! (so hash collisions count once, matching C).
//!
//! C parses the reducer arguments and constructs the reducer via
//! `reducers_ffi`.

use std::collections::HashSet;

use bumpalo::Bump;
use rlookup::{RLookupKey, RLookupRow};
use value::{SharedValue, Value, hash::hash};

use crate::Reducer;

/// Group-independent state of COUNT_DISTINCT.
///
/// Must remain `#[repr(C)]` with [`Reducer`] at offset 0 so the C layer
/// can downcast this struct to `ffi::Reducer*` and read the vtable directly.
#[repr(C)]
pub struct CountDistinctReducer<'a> {
    /// C-visible vtable header. Pinned to offset 0 by `#[repr(C)]` so the
    /// C layer can downcast `CountDistinctReducer*` to `ffi::Reducer*`.
    reducer: Reducer,
    /// Arena for per-group contexts. Because [`CountDistinctCtx`] is
    /// arena-allocated ([`Bump`] does not run destructors),
    /// [`drop_in_place`][std::ptr::drop_in_place] must be called to free
    /// the dedup set.
    arena: Bump,
    srckey: &'a RLookupKey<'a>,
}

const _: () = assert!(core::mem::offset_of!(CountDistinctReducer<'_>, reducer) == 0);

/// Per-group dedup set of [`CountDistinctReducer`].
#[derive(Default)]
pub struct CountDistinctCtx {
    dedup: HashSet<u64>,
}

impl<'a> CountDistinctReducer<'a> {
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

    /// Allocate an empty per-group dedup set in the arena.
    pub fn alloc_instance(&self) -> &mut CountDistinctCtx {
        self.arena.alloc(CountDistinctCtx::default())
    }
}

impl CountDistinctCtx {
    /// Record the source key's value of `row` in the dedup set. Missing
    /// keys and null values are skipped.
    pub fn add(&mut self, r: &CountDistinctReducer, row: &RLookupRow) {
        let Some(v) = row.get(r.srckey) else {
            return;
        };
        if matches!(&**v, Value::Null) {
            return;
        }
        self.dedup.insert(hash(v, 0));
    }

    /// Emit the number of distinct values seen.
    pub fn finalize(&self) -> SharedValue {
        SharedValue::new_num(self.dedup.len() as f64)
    }
}
