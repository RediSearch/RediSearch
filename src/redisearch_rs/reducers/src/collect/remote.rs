/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Remote COLLECT reducer.
//!
//! This reducer consumes ordinary input rows: each [`RLookupRow`] represents one
//! item/document flowing through the aggregation pipeline. In distributed
//! execution it is used for the shard-side half of the innermost split
//! `GROUPBY`, where it serializes collected items into a payload for the
//! coordinator-side COLLECT reducer.
//!
//! Coordinator-side outer `GROUPBY` steps also see ordinary rows/items; they are
//! not this reducer's special merge input.

use std::collections::HashSet;
use std::mem;

use rlookup::{RLookupKey, RLookupRow};
use value::SharedValue;

use crate::Reducer;
use crate::collect::common::CollectCommon;

/// Remote COLLECT reducer.
///
/// Must remain `#[repr(C)]` with [`CollectCommon`] at offset 0 so the C layer
/// can downcast this struct to `ffi::Reducer*` and read the vtable directly.
#[repr(C)]
pub struct RemoteCollectReducer<'a> {
    common: CollectCommon,
    field_keys: Box<[&'a RLookupKey<'a>]>,
    load_all: bool,
    /// Raw sort-key references, including keys not present in `FIELDS`.
    sort_keys: Box<[&'a RLookupKey<'a>]>,
    /// `true` for shard replies dispatched by the coordinator: extra sort-key
    /// columns are emitted alongside the requested fields so the coordinator
    /// can re-order shard rows during merge. `false` for direct execution.
    include_sort_keys: bool,
}

// Chain through `CollectCommon::reducer` so the assertion still catches a
// reordering inside `CollectCommon`, not just inside the outer struct.
const _: () = assert!(
    core::mem::offset_of!(RemoteCollectReducer<'_>, common)
        + core::mem::offset_of!(CollectCommon, reducer)
        == 0
);

/// Per-group instance of [`RemoteCollectReducer`].
///
/// Arena-allocated under [`Bump`][bumpalo::Bump], which does not run
/// destructors — [`ptr::drop_in_place`][std::ptr::drop_in_place] must be
/// called to release the stored [`RLookupRow`]s and decrement
/// [`SharedValue`] refcounts.
pub struct RemoteCollectCtx<'a> {
    rows: Vec<RLookupRow<'a>>,
}

impl<'a> RemoteCollectReducer<'a> {
    /// Create a reducer from C-parsed configuration.
    pub fn new(
        field_keys: Box<[&'a RLookupKey<'a>]>,
        load_all: bool,
        sort_keys: Box<[&'a RLookupKey<'a>]>,
        sort_asc_map: u64,
        limit: Option<(u64, u64)>,
        include_sort_keys: bool,
    ) -> Self {
        Self {
            common: CollectCommon::new(sort_asc_map, limit),
            field_keys,
            load_all,
            sort_keys,
            include_sort_keys,
        }
    }

    pub const fn reducer_mut(&mut self) -> &mut Reducer {
        &mut self.common.reducer
    }

    pub fn alloc_instance(&self) -> &mut RemoteCollectCtx<'a> {
        self.common.arena.alloc(RemoteCollectCtx::new(self))
    }

    // Temporary C++ parser-test accessors, exposed through `reducers_ffi`.

    pub const fn field_keys_len(&self) -> usize {
        self.field_keys.len()
    }

    pub const fn load_all(&self) -> bool {
        self.load_all
    }

    pub const fn sort_keys_len(&self) -> usize {
        self.sort_keys.len()
    }

    pub const fn sort_asc_map(&self) -> u64 {
        self.common.sort_asc_map
    }

    pub const fn has_limit(&self) -> bool {
        self.common.limit.is_some()
    }

    pub const fn limit_offset(&self) -> u64 {
        match self.common.limit {
            Some((offset, _)) => offset,
            None => 0,
        }
    }

    pub const fn limit_count(&self) -> u64 {
        match self.common.limit {
            Some((_, count)) => count,
            None => 0,
        }
    }
}

impl<'a> RemoteCollectCtx<'a> {
    pub const fn new(_r: &RemoteCollectReducer<'a>) -> Self {
        Self { rows: Vec::new() }
    }

    /// Project the source row's field-key and sort-key values into a stored
    /// [`RLookupRow`].
    pub fn add(&mut self, r: &RemoteCollectReducer<'a>, row: &RLookupRow<'_>) {
        let mut dst = RLookupRow::new();
        for key in r.field_keys.iter() {
            if let Some(v) = row.get(key) {
                dst.write_key(key, v.clone());
            }
        }
        for key in r.sort_keys.iter() {
            if let Some(v) = row.get(key) {
                dst.write_key(key, v.clone());
            }
        }
        self.rows.push(dst);
    }

    /// Serialize the buffered rows into a Map.
    pub fn finalize(&mut self, r: &RemoteCollectReducer<'a>) -> SharedValue {
        let rows = mem::take(&mut self.rows);

        let sort_extras: &[&RLookupKey<'a>] = if r.include_sort_keys {
            &r.sort_keys
        } else {
            &[]
        };
        let mut seen: HashSet<u16> = HashSet::with_capacity(r.field_keys.len() + sort_extras.len());
        let template: Vec<(&RLookupKey<'a>, SharedValue)> = r
            .field_keys
            .iter()
            .chain(sort_extras)
            .filter(|key| seen.insert(key.dstidx))
            .map(|key| {
                (
                    *key,
                    SharedValue::new_string(key.name().to_bytes().to_vec()),
                )
            })
            .collect();

        SharedValue::new_array(rows.into_iter().map(|row| {
            let entries: Vec<_> = template
                .iter()
                .map(|(key, name)| {
                    let val = row
                        .get(key)
                        .cloned()
                        .unwrap_or_else(SharedValue::null_static);
                    (name.clone(), val)
                })
                .collect();
            SharedValue::new_map(entries)
        }))
    }
}
