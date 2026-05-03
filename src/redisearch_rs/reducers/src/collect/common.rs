/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Shared COLLECT reducer state and utilities.

use bumpalo::Bump;
use rlookup::{RLookup, RLookupKey, RLookupKeyFlag, RLookupRow};
use value::SharedValue;

use crate::Reducer;

/// State shared by every COLLECT reducer variant, plus its FFI-layout prefix.
///
/// `#[repr(C)]` plus embedding `CollectCommon` as the first field of each
/// variant pins the C-visible [`Reducer`] vtable header to byte 0, so the C
/// layer can downcast any variant to `ffi::Reducer*`. Each variant asserts the
/// invariant with its own `const _` offset check.
#[repr(C)]
pub struct CollectCommon {
    pub(super) reducer: Reducer,
    /// Arena for per-group contexts; destructors still need explicit calls.
    pub(super) arena: Bump,
    /// Bit `i` is 0 for DESC and 1 for ASC, matching `SORTASCMAP_INIT`.
    pub(super) sort_asc_map: u64,
    pub(super) limit: Option<(u64, u64)>,
}

impl CollectCommon {
    pub fn new(sort_asc_map: u64, limit: Option<(u64, u64)>) -> Self {
        Self {
            reducer: Reducer::new(),
            arena: Bump::new(),
            sort_asc_map,
            limit,
        }
    }
}

/// Walk `lookup` in insertion order, invoking `f` for every *visible* key
/// that has a value in `row`.
///
/// "Visible" means [`RLookupKey::is_tombstone`] is `false` and the key
/// is not flagged [`RLookupKeyFlag::Hidden`] — the `FIELDS *` projection
/// rule.
pub(super) fn for_each_visible_value<'a, F>(lookup: &RLookup<'a>, row: &RLookupRow<'_>, mut f: F)
where
    F: FnMut(&RLookupKey<'a>, &SharedValue),
{
    let mut cursor = lookup.cursor();
    while let Some(key) = cursor.current() {
        if !key.is_tombstone()
            && !key.flags.contains(RLookupKeyFlag::Hidden)
            && let Some(v) = row.get(key)
        {
            f(key, v);
        }
        cursor.move_next();
    }
}
