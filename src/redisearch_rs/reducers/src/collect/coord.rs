/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Coordinator-side COLLECT reducer.
//!
//! Runs on the coordinator node and consumes the per-shard `Array<Map>`
//! payloads produced by [`super::shard::ShardCollectCtx::finalize`]. Incoming
//! maps are stored under the `__SOURCE__` key of the merged row;
//! [`CoordCollectCtx::finalize`] rebuilds each output row from the accumulated
//! shard-provided entries.
//!
//! ## Serialization contract
//!
//! The shard emits per-row entries as `Map` (RESP3) or a flat `[k, v, ...]`
//! `Array` (RESP2). In internal mode (shard responding to a
//! coordinator-dispatched `_FT.*` command) the shard additionally includes
//! sort-key values in each entry alongside the projected field values.
//!
//! `finalize` uses [`value::Map::get`] / [`value::Array::map_get`] to fish
//! each `field_name` out of the per-row entry regardless of the RESP flavour,
//! then constructs a fresh `Map` keyed only by the client-visible field names.
//! Sort-key values are silently ignored — the coordinator never asks for them
//! in this step (ordering is deferred to the follow-up SORTBY/LIMIT PR).
//!
//! A missing field in a shard entry becomes [`SharedValue::null_static`],
//! matching the shard's own behaviour for rows where a field has no value.

use rlookup::{RLookupKey, RLookupRow};
use value::{Array, Map, SharedValue, Value};

use crate::Reducer;
use crate::collect::common::CollectCommon;

/// Coordinator-side COLLECT reducer.
///
/// Must remain `#[repr(C)]` with [`CollectCommon`] at offset 0 so the C layer
/// can downcast this struct to `ffi::Reducer*` and read the vtable directly.
#[repr(C)]
pub struct CoordCollectReducer<'a> {
    /// Shared base state: vtable, arena, SORTBY/LIMIT configuration. Must be
    /// the first field so `&CoordCollectReducer as *const ffi::Reducer` is
    /// valid.
    common: CollectCommon,
    /// Lookup key for the `__SOURCE__` payload carrying the per-shard
    /// `Array<Map>`.
    source_key: &'a RLookupKey<'a>,
    /// Field names parsed from the `FIELDS` clause, preserved as raw bytes
    /// (no leading `@`). Consumed by the follow-up SORTBY/LIMIT PR to
    /// project out a subset of the shard-side maps.
    field_names: Box<[Box<[u8]>]>,
    /// Sort-key names parsed from the `SORTBY` clause, preserved as raw
    /// bytes (no leading `@`). Consumed by the follow-up SORTBY/LIMIT PR.
    sort_key_names: Box<[Box<[u8]>]>,
}

// `CollectCommon` must live at offset 0 so the C layer can downcast to
// `ffi::Reducer`. Guard against accidental reordering of the struct fields.
const _: () = assert!(core::mem::offset_of!(CoordCollectReducer<'_>, common) == 0);

/// Per-group instance of [`CoordCollectReducer`].
///
/// Each call to [`CoordCollectCtx::add`] unpacks the shard-emitted
/// `Array<Map>` payload at the `__SOURCE__` key and accumulates the
/// individual maps. [`CoordCollectCtx::finalize`] returns all maps wrapped
/// in a single outer [`Array`].
///
/// Because `CoordCollectCtx` is arena-allocated ([`Bump`][bumpalo::Bump] does
/// not run destructors), `ptr::drop_in_place` must be called to run
/// destructors for the inner heap-allocated `Vec` and decrement
/// `SharedValue` refcounts.
pub struct CoordCollectCtx {
    maps: Vec<SharedValue>,
}

impl<'a> CoordCollectReducer<'a> {
    /// Create a new `CoordCollectReducer` with the given pre-parsed configuration.
    ///
    /// `field_names` and `sort_key_names` are owned raw-byte copies of the
    /// names supplied by the client — the coordinator does not have access
    /// to the originating shards' [`RLookup`][ffi::RLookup] tables, so it
    /// cannot keep `RLookupKey` references for them.
    pub fn new(
        source_key: &'a RLookupKey<'a>,
        field_names: Box<[Box<[u8]>]>,
        sort_key_names: Box<[Box<[u8]>]>,
        sort_asc_map: u64,
        limit: Option<(u64, u64)>,
    ) -> Self {
        Self {
            common: CollectCommon::new(sort_asc_map, limit),
            source_key,
            field_names,
            sort_key_names,
        }
    }

    /// Get a mutable reference to the base reducer.
    pub const fn reducer_mut(&mut self) -> &mut Reducer {
        &mut self.common.reducer
    }

    /// Allocate a new [`CoordCollectCtx`] instance from the arena.
    pub fn alloc_instance(&self) -> &mut CoordCollectCtx {
        self.common.arena.alloc(CoordCollectCtx::new(self))
    }
}

impl CoordCollectCtx {
    /// Create a new per-group coordinator collect reducer instance.
    pub const fn new(_r: &CoordCollectReducer) -> Self {
        Self { maps: Vec::new() }
    }

    /// Consume the per-shard `Array<Map>` payload stored under the
    /// `__SOURCE__` key and append each map to this context.
    ///
    /// A row without a `__SOURCE__` value is silently skipped — this
    /// matches the shard-side behavior where a group with zero collected
    /// rows simply yields an empty payload.
    ///
    /// A row whose `__SOURCE__` value is not a [`Value::Array`] is also
    /// skipped defensively: this should never happen in practice because
    /// the shard unconditionally emits an array, but falling through keeps
    /// the coordinator robust against malformed inputs rather than
    /// panicking in production.
    pub fn add(&mut self, r: &CoordCollectReducer, row: &RLookupRow) {
        let Some(payload) = row.get(r.source_key) else {
            return;
        };
        if let Value::Array(array) = &**payload {
            self.maps.reserve(array.len());
            for entry in array.iter() {
                self.maps.push(entry.clone());
            }
        }
    }

    /// Rebuild each shard-provided row as a client-facing `Map`, then wrap
    /// all rows in a single outer [`Array`] and return it.
    ///
    /// For each accumulated entry the coordinator looks up each configured
    /// `field_name` using the RESP-flavour-agnostic getters:
    ///
    /// - [`value::Map::get`] for RESP3 entries.
    /// - [`value::Array::map_get`] for RESP2 flat `[k, v, ...]` entries.
    ///
    /// Extra keys that the shard included in the entry (e.g. sort-field
    /// values emitted in internal mode) are never fetched and therefore
    /// never appear in the output, achieving client-output parity with the
    /// standalone shard path.
    ///
    /// Entries that are neither a `Map` nor an `Array` are silently skipped —
    /// this should never happen in practice but preserves the defensive
    /// posture of [`Self::add`] rather than panicking on malformed input.
    ///
    /// A missing field within an entry becomes [`SharedValue::null_static`],
    /// matching the shard's own behaviour for rows where a field has no value.
    ///
    /// Consumes the internal storage: subsequent calls on the same instance
    /// will observe an empty outer array.
    pub fn finalize(&mut self, r: &CoordCollectReducer) -> SharedValue {
        let rebuilt: Vec<SharedValue> = std::mem::take(&mut self.maps)
            .into_iter()
            .filter_map(|entry| {
                // Entries that are neither Map nor Array are malformed shard
                // payloads; skip them defensively rather than panicking.
                let is_valid = matches!(&*entry, Value::Map(_) | Value::Array(_));
                if !is_valid {
                    return None;
                }

                let row_entries: Box<[_]> = r
                    .field_names
                    .iter()
                    .map(|name| {
                        let val = match &*entry {
                            Value::Map(m) => m.get(name).cloned(),
                            Value::Array(a) => a.map_get(name).cloned(),
                            // SAFETY: checked above; this arm is unreachable.
                            _ => unreachable!(),
                        }
                        .unwrap_or_else(SharedValue::null_static);
                        (SharedValue::new_string(name.to_vec()), val)
                    })
                    .collect();
                Some(SharedValue::new(Value::Map(Map::new(row_entries))))
            })
            .collect();
        SharedValue::new(Value::Array(Array::new(rebuilt.into_boxed_slice())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rlookup::{RLookupKey, RLookupKeyFlags};

    // End-to-end `finalize` coverage requires the Redis module allocator to
    // be linked (because `SharedValue` invokes it during drop). Those paths
    // are exercised by the Python flow tests (`test_collect_internal_*`).
    // The unit tests here only cover pure-Rust configuration round-tripping.

    /// A coordinator reducer without FIELDS or SORTBY reports defaults on
    /// every shared-state accessor and keeps a stable pointer to its
    /// `__SOURCE__` key.
    #[test]
    fn new_with_no_fields_uses_common_defaults() {
        let source_key = RLookupKey::new(c"__SOURCE__", RLookupKeyFlags::empty());
        let r = CoordCollectReducer::new(&source_key, Box::new([]), Box::new([]), 0, None);
        assert_eq!(r.common.sort_asc_map, 0);
        assert!(r.common.limit.is_none());
        assert!(std::ptr::eq(r.source_key, &source_key));
    }

    /// LIMIT and SORTBY configuration is reflected in the shared state.
    #[test]
    fn new_with_limit_and_sort_stores_configuration() {
        let source_key = RLookupKey::new(c"__SOURCE__", RLookupKeyFlags::empty());
        let r = CoordCollectReducer::new(
            &source_key,
            Box::new([b"a".to_vec().into_boxed_slice()]),
            Box::new([b"b".to_vec().into_boxed_slice()]),
            0b11,
            Some((2, 7)),
        );
        assert_eq!(r.common.sort_asc_map, 0b11);
        assert_eq!(r.common.limit, Some((2, 7)));
    }
}
