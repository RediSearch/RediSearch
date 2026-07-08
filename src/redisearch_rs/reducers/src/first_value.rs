/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! FIRST_VALUE reducer: return the value of a source key from the first row
//! of a group — or, with `BY <key> [ASC|DESC]`, from the row ranking best by
//! that sort key.
//!
//! C parses the reducer arguments and constructs the reducer via
//! `reducers_ffi`.

use std::cmp::Ordering;

use bumpalo::Bump;
use rlookup::{RLookupKey, RLookupRow};
use value::{SharedValue, Value, comparison::compare_with_query_error};

use crate::Reducer;

/// Group-independent state of FIRST_VALUE.
///
/// Must remain `#[repr(C)]` with [`Reducer`] at offset 0 so the C layer
/// can downcast this struct to `ffi::Reducer*` and read the vtable directly.
#[repr(C)]
pub struct FirstValueReducer<'a> {
    /// C-visible vtable header. Pinned to offset 0 by `#[repr(C)]` so the
    /// C layer can downcast `FirstValueReducer*` to `ffi::Reducer*`.
    reducer: Reducer,
    /// Arena for per-group contexts. Because [`FirstValueCtx`] is
    /// arena-allocated ([`Bump`] does not run destructors),
    /// [`drop_in_place`][std::ptr::drop_in_place] must be called to
    /// decrement the retained [`SharedValue`] refcounts.
    arena: Bump,
    /// The key whose value is returned.
    retkey: &'a RLookupKey<'a>,
    /// The `BY` sort key, if any.
    sortkey: Option<&'a RLookupKey<'a>>,
    ascending: bool,
}

const _: () = assert!(core::mem::offset_of!(FirstValueReducer<'_>, reducer) == 0);

/// Per-group state of [`FirstValueReducer`]: the best value seen so far and,
/// in `BY` mode, the sort value it won with.
#[derive(Default)]
pub struct FirstValueCtx {
    value: Option<SharedValue>,
    sortval: Option<SharedValue>,
}

impl<'a> FirstValueReducer<'a> {
    pub fn new(
        retkey: &'a RLookupKey<'a>,
        sortkey: Option<&'a RLookupKey<'a>>,
        ascending: bool,
    ) -> Self {
        Self {
            reducer: Reducer::new(),
            arena: Bump::new(),
            retkey,
            sortkey,
            ascending,
        }
    }

    pub const fn reducer_mut(&mut self) -> &mut Reducer {
        &mut self.reducer
    }

    /// Allocate an empty per-group context in the arena.
    pub fn alloc_instance(&self) -> &mut FirstValueCtx {
        self.arena.alloc(FirstValueCtx::default())
    }
}

fn is_null(v: &SharedValue) -> bool {
    matches!(v.fully_dereferenced_ref(), Value::Null)
}

impl FirstValueCtx {
    /// Consider `row` for the group. A missing key reads as the null value,
    /// so in no-`BY` mode the very first row always locks in the result —
    /// even as null.
    pub fn add(&mut self, r: &FirstValueReducer, row: &RLookupRow) {
        let Some(sortkey) = r.sortkey else {
            if self.value.is_none() {
                self.value = Some(get_or_null(row, r.retkey));
            }
            return;
        };

        let val = get_or_null(row, r.retkey);
        let cur_sortval = get_or_null(row, sortkey);

        match &self.sortval {
            None => {
                // First row seen: take it unconditionally.
                self.value = Some(val);
                self.sortval = Some(cur_sortval);
            }
            Some(_) if is_null(&cur_sortval) => {
                // A null sort value never wins.
            }
            Some(best) if is_null(best) => {
                // A null best is replaced by any non-null sort value — but
                // only the sort value, not the returned value (C parity).
                self.sortval = Some(cur_sortval);
            }
            Some(best) => {
                let ord = compare_with_query_error(&cur_sortval, best, None);
                let wins = if r.ascending {
                    ord == Ordering::Less
                } else {
                    ord == Ordering::Greater
                };
                if wins {
                    self.sortval = Some(cur_sortval);
                    self.value = Some(val);
                }
            }
        }
    }

    /// Emit the winning value; an empty group yields null.
    pub fn finalize(&self) -> SharedValue {
        self.value.clone().unwrap_or_else(SharedValue::null_static)
    }
}

fn get_or_null(row: &RLookupRow, key: &RLookupKey) -> SharedValue {
    row.get(key)
        .cloned()
        .unwrap_or_else(SharedValue::null_static)
}
