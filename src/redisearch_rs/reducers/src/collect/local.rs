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

use rlookup::{RLookupKey, RLookupRow};
use value::{SharedValue, Value};

use crate::Reducer;
use crate::collect::common::CollectCommon;
use crate::collect::storage::Storage;

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
    limit: Option<(u64, u64)>,
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
    storage: Storage<Box<[SharedValue]>>,
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
            common: CollectCommon::new(sort_asc_map),
            input_key,
            field_names,
            load_all,
            sort_key_names,
            limit,
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
    pub fn new(r: &LocalCollectReducer) -> Self {
        Self {
            storage: Storage::new(!r.sort_key_names.is_empty(), r.limit),
        }
    }

    /// Push every entry from the merge row's payload through
    /// [`Storage::insert_entry`].
    ///
    /// Remote rows arrive as [`Value::Map`] under RESP3 and as a flat
    /// `[k, v, k, v, ...]` [`Value::Array`] under RESP2; any other shape is
    /// treated as "not present". Missing or malformed payloads are skipped
    /// defensively rather than aborting the merge — one bad shard reply must
    /// not poison the rest.
    pub fn add(&mut self, r: &LocalCollectReducer, row: &RLookupRow) {
        if let Some(payload) = row.get(r.input_key)
            && let Value::Array(array) = &**payload
        {
            for entry in array.iter() {
                if !matches!(&**entry, Value::Map(_) | Value::Array(_)) {
                    tracing_assert::debug_assert_warn!(
                        false,
                        "LocalCollectReducer payload entry must be a Map or Array"
                    );
                    continue;
                }
                self.storage.insert_entry(|| {
                    r.field_names
                        .iter()
                        .map(|name| {
                            match &**entry {
                                Value::Map(m) => m.get(name),
                                Value::Array(a) => a.map_get(name),
                                _ => None,
                            }
                            .cloned()
                            .unwrap_or_else(SharedValue::null_static)
                        })
                        .collect::<Vec<_>>()
                        .into_boxed_slice()
                });
            }
        } else {
            tracing_assert::debug_assert_warn!(
                false,
                "LocalCollectReducer requires an array payload"
            );
        }
    }

    /// Apply the user's global `LIMIT offset count`. The local reducer is the
    /// client-facing terminus, so it is the single point where `OFFSET` is
    /// honoured in distributed mode — see
    /// [`super::remote::RemoteCollectReducer::is_internal`].
    pub fn finalize(&mut self, r: &LocalCollectReducer) -> SharedValue {
        let field_names: Vec<SharedValue> = r
            .field_names
            .iter()
            .map(|name| SharedValue::new_string(name.to_vec()))
            .collect();

        let rows: Vec<SharedValue> = self
            .storage
            .drain(true)
            .map(|projected| {
                let entries: Vec<(SharedValue, SharedValue)> =
                    field_names.iter().cloned().zip(projected).collect();
                SharedValue::new_map(entries)
            })
            .collect();
        SharedValue::new_array(rows)
    }
}
