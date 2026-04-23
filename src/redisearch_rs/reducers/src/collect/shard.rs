/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Shard-side COLLECT reducer.
//!
//! Runs on every data shard and projects the configured field keys out of each
//! row, storing the (refcounted) values per group. [`ShardCollectCtx::finalize`]
//! serializes the collected rows as an array of maps that the coordinator will
//! later unpack.

use rlookup::{RLookupKey, RLookupRow};
use value::{Array, Map, SharedValue, Value};

use crate::Reducer;
use crate::collect::common::CollectCommon;

/// Shard-side COLLECT reducer.
///
/// Configuration (field keys, sort keys, limits) is parsed in C and passed to
/// Rust via [`ShardCollectReducer::new`]. The [`RLookupKey`][ffi::RLookupKey]
/// pointers are borrowed from the [`RLookup`][ffi::RLookup] infrastructure and
/// outlive this reducer.
///
/// Must remain `#[repr(C)]` with [`CollectCommon`] at offset 0 so the C layer
/// can downcast this struct to `ffi::Reducer*` and read the vtable directly.
#[repr(C)]
pub struct ShardCollectReducer<'a> {
    /// Shared base state: vtable, arena, SORTBY/LIMIT configuration. Must be
    /// the first field so `&ShardCollectReducer as *const ffi::Reducer` is
    /// valid.
    common: CollectCommon,
    /// Projected field keys. Empty when only a wildcard is used.
    field_keys: Box<[&'a RLookupKey<'a>]>,
    /// Whether the wildcard `*` was specified in the FIELDS clause.
    has_wildcard: bool,
    /// Sort keys for in-group ordering. Empty when SORTBY is omitted.
    ///
    /// Stored as raw `RLookupKey` references (not resolved indices) so that
    /// sort keys absent from `FIELDS` still survive parsing. How they are
    /// consumed at finalize time is decided by the follow-up SORTBY/LIMIT PR.
    sort_keys: Box<[&'a RLookupKey<'a>]>,
}

// `CollectCommon` must live at offset 0 so the C layer can downcast to
// `ffi::Reducer`. Guard against accidental reordering of the struct fields.
const _: () = assert!(core::mem::offset_of!(ShardCollectReducer<'_>, common) == 0);

/// Per-group instance of [`ShardCollectReducer`].
///
/// Each call to [`ShardCollectCtx::add`] projects the configured field keys
/// from the source row and stores the cloned values. [`ShardCollectCtx::finalize`]
/// serializes all collected rows as an array of maps.
///
/// Because `ShardCollectCtx` is arena-allocated ([`Bump`][bumpalo::Bump] does
/// not run destructors), `ptr::drop_in_place` must be called to run
/// destructors for the inner heap-allocated `Vec`s and decrement
/// `SharedValue` refcounts.
pub struct ShardCollectCtx {
    rows: Vec<Vec<SharedValue>>,
}

impl<'a> ShardCollectReducer<'a> {
    /// Create a new `ShardCollectReducer` with the given pre-parsed configuration.
    ///
    /// The raw pointers in `field_keys` and `sort_keys` are stored but not
    /// dereferenced here; they are only dereferenced (unsafely) in
    /// [`ShardCollectCtx::add`] and [`ShardCollectCtx::finalize`].
    pub fn new(
        field_keys: Box<[&'a RLookupKey<'a>]>,
        has_wildcard: bool,
        sort_keys: Box<[&'a RLookupKey<'a>]>,
        sort_asc_map: u64,
        limit: Option<(u64, u64)>,
    ) -> Self {
        Self {
            common: CollectCommon::new(sort_asc_map, limit),
            field_keys,
            has_wildcard,
            sort_keys,
        }
    }

    /// Get a mutable reference to the base reducer.
    pub const fn reducer_mut(&mut self) -> &mut Reducer {
        &mut self.common.reducer
    }

    /// Allocate a new [`ShardCollectCtx`] instance from the arena.
    pub fn alloc_instance(&self) -> &mut ShardCollectCtx {
        self.common.arena.alloc(ShardCollectCtx::new(self))
    }

    // The accessors below exist only for the C++ parser tests
    // (`test_cpp_collect.cpp`) via `reducers_ffi`. Remove them once those
    // tests are migrated to Python flow tests.

    /// Number of explicitly listed field keys (excludes the wildcard).
    pub const fn field_keys_len(&self) -> usize {
        self.field_keys.len()
    }

    /// Whether the wildcard `*` was specified in the FIELDS clause.
    pub const fn has_wildcard(&self) -> bool {
        self.has_wildcard
    }

    /// Number of sort keys.
    pub const fn sort_keys_len(&self) -> usize {
        self.sort_keys.len()
    }

    /// The ASC/DESC bitmask for sort keys.
    pub const fn sort_asc_map(&self) -> u64 {
        self.common.sort_asc_map
    }

    /// Whether a LIMIT clause was specified.
    pub const fn has_limit(&self) -> bool {
        self.common.limit.is_some()
    }

    /// The LIMIT offset value (0 if no limit).
    pub const fn limit_offset(&self) -> u64 {
        match self.common.limit {
            Some((offset, _)) => offset,
            None => 0,
        }
    }

    /// The LIMIT count value (0 if no limit).
    pub const fn limit_count(&self) -> u64 {
        match self.common.limit {
            Some((_, count)) => count,
            None => 0,
        }
    }
}

impl ShardCollectCtx {
    /// Create a new per-group shard collect reducer instance.
    pub const fn new(_r: &ShardCollectReducer) -> Self {
        Self { rows: Vec::new() }
    }

    /// Project field values from `row` and store them for later
    /// serialization in [`Self::finalize`].
    ///
    /// For each configured field key the value is looked up in the row
    /// and cloned (incrementing its refcount). Missing values are stored as
    /// [`SharedValue::null_static`].
    pub fn add(&mut self, r: &ShardCollectReducer, row: &RLookupRow) {
        let values = r
            .field_keys
            .iter()
            .map(|key| {
                row.get(key)
                    .cloned()
                    .unwrap_or_else(SharedValue::null_static)
            })
            .collect();
        self.rows.push(values);
    }

    /// Serialize all collected rows as an array of maps, consuming the stored
    /// rows.
    ///
    /// Each map contains `{field_name: value}` entries keyed by the
    /// [`RLookupKey`] name. The outer array has one element per collected row.
    pub fn finalize(&mut self, r: &ShardCollectReducer) -> SharedValue {
        let row_maps: Vec<SharedValue> = self
            .rows
            .drain(..)
            .map(|row_values| {
                let entries: Box<[_]> = row_values
                    .into_iter()
                    .zip(r.field_keys.iter())
                    .map(|(val, key)| {
                        let name_val = SharedValue::new_string(key.name().to_bytes().to_vec());
                        (name_val, val)
                    })
                    .collect();
                SharedValue::new(Value::Map(Map::new(entries)))
            })
            .collect();
        SharedValue::new(Value::Array(Array::new(row_maps.into_boxed_slice())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // End-to-end `add`/`finalize` coverage requires the Redis module
    // allocator (`RedisModule_Alloc`/`_Free`) to be linked because
    // `SharedValue` invokes it during drop. Those tests live in
    // `tests/test_cpp_collect.cpp` today and will migrate to Python flow
    // tests alongside the SORTBY/LIMIT follow-up PR. The unit tests here
    // only cover pure-Rust configuration round-tripping.

    /// Every accessor on a reducer built with no configuration reports the
    /// defaults expected by the C-side factory.
    #[test]
    fn new_with_no_fields_exposes_empty_configuration() {
        let r = ShardCollectReducer::new(Box::new([]), false, Box::new([]), 0, None);
        assert_eq!(r.field_keys_len(), 0);
        assert!(!r.has_wildcard());
        assert_eq!(r.sort_keys_len(), 0);
        assert_eq!(r.sort_asc_map(), 0);
        assert!(!r.has_limit());
        assert_eq!(r.limit_offset(), 0);
        assert_eq!(r.limit_count(), 0);
    }

    /// LIMIT and SORTBY configuration round-trips through the accessors.
    #[test]
    fn new_with_limit_and_sort_exposes_configuration() {
        let r = ShardCollectReducer::new(Box::new([]), true, Box::new([]), 0b101, Some((5, 10)));
        assert!(r.has_wildcard());
        assert_eq!(r.sort_asc_map(), 0b101);
        assert!(r.has_limit());
        assert_eq!(r.limit_offset(), 5);
        assert_eq!(r.limit_count(), 10);
    }
}
