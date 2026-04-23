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
//! [`CoordCollectCtx::finalize`] flattens the maps across shards into a
//! single outer array.
//!
//! Field and sort-key names are stored as raw bytes because the coordinator
//! has no access to the shards' `RLookup` schema: ordering and projection at
//! finalize time happen through the map keys embedded in the shard payload.
//! Their actual use — projection, SORTBY, LIMIT — is deferred to the
//! follow-up SORTBY/LIMIT PR.

use rlookup::{RLookupKey, RLookupRow};
use value::{Array, SharedValue, Value};

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

    /// Wrap all collected maps in a single outer [`Array`] and return it.
    ///
    /// Consumes the internal storage: subsequent calls on the same
    /// instance (which would indicate reuse across groups — not something
    /// the reducer pipeline does today) will observe an empty vector.
    pub fn finalize(&mut self, _r: &CoordCollectReducer) -> SharedValue {
        let maps: Vec<SharedValue> = std::mem::take(&mut self.maps);
        SharedValue::new(Value::Array(Array::new(maps.into_boxed_slice())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rlookup::{RLookupKey, RLookupKeyFlags};

    // End-to-end `add`/`finalize` coverage requires the Redis module
    // allocator to be linked (because `SharedValue` invokes it during
    // drop). Those paths are exercised by the Python flow tests once the
    // coordinator path is wired up. The unit tests here only cover pure
    // configuration round-tripping.

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
