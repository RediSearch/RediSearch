/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! TOLIST reducer: collect the distinct values of a source key across the
//! rows of a group into an array. Array values contribute each of their
//! elements rather than the array itself.
//!
//! C parses the reducer arguments and constructs the reducer via
//! `reducers_ffi`.

use std::hash::{Hash, Hasher};

use bumpalo::Bump;
use indexmap::IndexSet;
use rlookup::{RLookupKey, RLookupRow};
use value::{SharedValue, Value, comparison::compare_on_equality_only, hash::hash_value};

use crate::Reducer;

/// A [`SharedValue`] deduplicated by value semantics: hashing via
/// [`hash_value`] and equality via [`compare_on_equality_only`], the same
/// pair C's `dict` uses (`RSValue_Hash` / `RSValue_Equal`).
struct DedupValue(SharedValue);

impl Hash for DedupValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        hash_value(&self.0, state);
    }
}

impl PartialEq for DedupValue {
    fn eq(&self, other: &Self) -> bool {
        compare_on_equality_only(&self.0, &other.0)
    }
}

impl Eq for DedupValue {}

/// Group-independent state of TOLIST.
///
/// Must remain `#[repr(C)]` with [`Reducer`] at offset 0 so the C layer
/// can downcast this struct to `ffi::Reducer*` and read the vtable directly.
#[repr(C)]
pub struct ToListReducer<'a> {
    /// C-visible vtable header. Pinned to offset 0 by `#[repr(C)]` so the
    /// C layer can downcast `ToListReducer*` to `ffi::Reducer*`.
    reducer: Reducer,
    /// Arena for per-group contexts. Because [`ToListCtx`] is
    /// arena-allocated ([`Bump`] does not run destructors),
    /// [`drop_in_place`][std::ptr::drop_in_place] must be called to
    /// decrement the collected [`SharedValue`] refcounts.
    arena: Bump,
    srckey: &'a RLookupKey<'a>,
}

const _: () = assert!(core::mem::offset_of!(ToListReducer<'_>, reducer) == 0);

/// Per-group set of distinct values, in insertion order.
#[derive(Default)]
pub struct ToListCtx {
    values: IndexSet<DedupValue>,
}

impl<'a> ToListReducer<'a> {
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

    /// Allocate an empty per-group set in the arena.
    pub fn alloc_instance(&self) -> &mut ToListCtx {
        self.arena.alloc(ToListCtx::default())
    }
}

impl ToListCtx {
    /// Insert the source key's value of `row` (or, for an array value, each
    /// of its elements) into the distinct set. Rows without the key are
    /// skipped.
    pub fn add(&mut self, r: &ToListReducer, row: &RLookupRow) {
        let Some(v) = row.get(r.srckey) else {
            return;
        };
        match &**v {
            Value::Array(items) => {
                for item in items.iter() {
                    self.values.insert(DedupValue(item.clone()));
                }
            }
            _ => {
                self.values.insert(DedupValue(v.clone()));
            }
        }
    }

    /// Emit the distinct values as an array, in insertion order. (C's dict
    /// iterates in unspecified hash order; the output order of TOLIST is
    /// unordered either way.)
    pub fn finalize(&self) -> SharedValue {
        SharedValue::new_array(self.values.iter().map(|v| v.0.clone()))
    }
}
