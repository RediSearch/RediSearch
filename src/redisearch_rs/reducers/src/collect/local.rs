/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Local COLLECT reducer.
//!
//! This reducer consumes merge rows produced by the distributed `GROUPBY` split:
//! each input row represents an already-collected shard group, and the collected
//! items arrive as an [`Value`] payload under the planner-provided input key.
//! It flattens those shard payloads and rebuilds the client-facing COLLECT
//! result.
//!
//! Only the coordinator-side reducer for the innermost split `GROUPBY` has this
//! shape. Shard-side COLLECT and outer coordinator `GROUPBY` reducers consume
//! ordinary rows/items.
//!
//! ## Serialization contract
//!
//! Remote reducers emit each row as `Map` (RESP3) or flat `[k, v, ...]` `Array`
//! (RESP2). The local reducer projects only requested field names; extra
//! internal sort-key values are ignored, and missing fields are omitted.

use std::mem;

use rlookup::{RLookupKey, RLookupRow};
use value::{SharedValue, Value};

use crate::Reducer;
use crate::collect::common::CollectCommon;

/// Local COLLECT reducer.
///
/// Must remain `#[repr(C)]` with [`CollectCommon`] at offset 0 so the C layer
/// can downcast this struct to `ffi::Reducer*` and read the vtable directly.
#[repr(C)]
pub struct LocalCollectReducer<'a> {
    common: CollectCommon,
    /// Lookup key for the per-remote payload.
    input_key: &'a RLookupKey<'a>,
    field_names: Box<[Box<[u8]>]>,
    load_all: bool,
    sort_key_names: Box<[Box<[u8]>]>,
}

// Chain through `CollectCommon::reducer` so the assertion still catches a
// reordering inside `CollectCommon`, not just inside the outer struct.
const _: () = assert!(
    core::mem::offset_of!(LocalCollectReducer<'_>, common)
        + core::mem::offset_of!(CollectCommon, reducer)
        == 0
);

/// Per-group instance of [`LocalCollectReducer`].
///
/// Because `LocalCollectCtx` is arena-allocated ([`Bump`][bumpalo::Bump] does
/// not run destructors), `ptr::drop_in_place` must be called to run
/// destructors for the inner `Vec` and decrement `SharedValue` refcounts.
pub struct LocalCollectCtx {
    maps: Vec<SharedValue>,
}

impl<'a> LocalCollectReducer<'a> {
    /// Create a reducer from C-parsed configuration. Names are owned byte
    /// copies because the local reducer cannot borrow remote `RLookupKey`s.
    pub fn new(
        input_key: &'a RLookupKey<'a>,
        field_names: Box<[Box<[u8]>]>,
        load_all: bool,
        sort_key_names: Box<[Box<[u8]>]>,
        sort_asc_map: u64,
        limit: Option<(u64, u64)>,
    ) -> Self {
        Self {
            common: CollectCommon::new(sort_asc_map, limit),
            input_key,
            field_names,
            load_all,
            sort_key_names,
        }
    }

    pub const fn reducer_mut(&mut self) -> &mut Reducer {
        &mut self.common.reducer
    }

    pub fn alloc_instance(&self) -> &mut LocalCollectCtx {
        self.common.arena.alloc(LocalCollectCtx::new(self))
    }
}

impl LocalCollectCtx {
    pub const fn new(_r: &LocalCollectReducer) -> Self {
        Self { maps: Vec::new() }
    }

    /// Append maps from the remote payload; missing or malformed payloads are skipped.
    pub fn add(&mut self, r: &LocalCollectReducer, row: &RLookupRow) {
        if let Some(payload) = row.get(r.input_key)
            && let Value::Array(array) = &**payload
        {
            self.maps.extend(array.iter().cloned());
        }
    }

    /// Rebuild remote rows as client-facing maps, accepting RESP3 maps and
    /// RESP2 flat arrays while ignoring internal-only extra keys.
    pub fn finalize(&mut self, r: &LocalCollectReducer) -> SharedValue {
        let rebuilt = mem::take(&mut self.maps)
            .into_iter()
            // Malformed remote payloads are skipped defensively.
            .filter(|entry| matches!(&**entry, Value::Map(_) | Value::Array(_)))
            .map(|entry| {
                let row_entries: Vec<_> = r
                    .field_names
                    .iter()
                    .filter_map(|name| {
                        let val = match &*entry {
                            Value::Map(m) => m.get(name).cloned(),
                            Value::Array(a) => a.map_get(name).cloned(),
                            // Filtered above; only `Map` and `Array` reach here.
                            _ => unreachable!(),
                        }?;
                        Some((SharedValue::new_string(name.to_vec()), val))
                    })
                    .collect();
                SharedValue::new_map(row_entries)
            })
            .collect::<Vec<_>>();
        SharedValue::new_array(rebuilt)
    }
}
