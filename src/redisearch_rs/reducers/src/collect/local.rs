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
//! items arrive as a [`Value`] payload under the planner-provided input key.
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
//! (RESP2). The local reducer filters fields at ingestion time; missing fields
//! are omitted from the output.

use std::ffi::CString;
use std::mem;

use rlookup::{RLookup, RLookupKey, RLookupKeyFlag, RLookupRow};
use value::{SharedValue, Value};

use crate::Reducer;
use crate::collect::common::CollectCommon;

/// Look up `name` in a shard-payload item (`Map` or flat `Array`).
///
/// Unexpected shapes are a remote-side contract violation.
fn get_field<'a>(item: &'a Value, name: &[u8]) -> Option<&'a SharedValue> {
    match item {
        Value::Map(m) => m.get(name),
        Value::Array(a) => a.map_get(name),
        _ => {
            // TODO: replace with `debug_assert_warn!` once that macro is accessible from this crate.
            debug_assert!(
                false,
                "local COLLECT: shard payload item must be a Map or Array"
            );
            tracing::warn!("local COLLECT: shard payload item is not a Map or Array; skipping");
            None
        }
    }
}

/// Build a [`RLookupRow`] from a single shard-payload item.
///
/// When `requested` is `Some`, only the listed fields are written (explicit
/// field mode). When `None`, every field in `item` is written (LOADALL mode).
fn prepare_row(
    lookup: &mut RLookup<'static>,
    requested: Option<&[CString]>,
    item: &Value,
) -> RLookupRow<'static> {
    let mut dst = RLookupRow::new();
    match requested {
        Some(fields) => {
            for cname in fields {
                if let Some(v) = get_field(item, cname.to_bytes()) {
                    dst.write_key_by_name(lookup, cname.clone(), v.clone());
                }
            }
        }
        None => write_item_to_row(&mut dst, lookup, item),
    }
    dst
}

/// Write every field from `item` into `dst`, interning new names into `lookup`.
///
/// Counterpart to [`get_field`]: used in LOADALL mode where every field is
/// written rather than only those listed in [`requested`][LocalCollectReducer::requested].
fn write_item_to_row(dst: &mut RLookupRow<'static>, lookup: &mut RLookup<'static>, item: &Value) {
    match item {
        Value::Map(m) => {
            for (k, v) in m.iter() {
                let Some(name) = k.as_str_bytes() else {
                    continue;
                };
                let Ok(cname) = CString::new(name) else {
                    continue;
                };
                dst.write_key_by_name(lookup, cname, v.clone());
            }
        }
        Value::Array(a) => {
            let (pairs, remainder) = a.as_chunks::<2>();
            debug_assert!(remainder.is_empty(), "odd-length RESP2 payload");
            for [k, v] in pairs {
                let Some(name) = k.as_str_bytes() else {
                    continue;
                };
                let Ok(cname) = CString::new(name) else {
                    continue;
                };
                dst.write_key_by_name(lookup, cname, v.clone());
            }
        }
        _ => {
            // TODO: replace with `debug_assert_warn!` once that macro is accessible from this crate.
            debug_assert!(
                false,
                "local COLLECT: shard payload item must be a Map or Array"
            );
            tracing::warn!("local COLLECT: shard payload item is not a Map or Array; skipping");
        }
    }
}

/// Local COLLECT reducer.
///
/// Must remain `#[repr(C)]` with [`CollectCommon`] at offset 0 so the C layer
/// can downcast this struct to `ffi::Reducer*` and read the vtable directly.
#[repr(C)]
pub struct LocalCollectReducer<'a> {
    common: CollectCommon,
    /// Lookup key for the per-remote payload.
    input_key: &'a RLookupKey<'a>,
    /// Requested field names in declaration order.
    ///
    /// `Some` for an explicit field list; `None` when the user wrote `FIELDS *`.
    /// Controls which fields [`LocalCollectCtx::add`] writes into the lookup.
    requested: Option<Box<[CString]>>,
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
/// not run destructors), [`drop_in_place`][std::ptr::drop_in_place] must be
/// called to run destructors for the inner `Vec`s and decrement
/// [`SharedValue`] refcounts.
pub struct LocalCollectCtx {
    lookup: RLookup<'static>,
    rows: Vec<RLookupRow<'static>>,
}

impl<'a> LocalCollectReducer<'a> {
    /// Create a reducer from C-parsed configuration.
    pub fn new(
        input_key: &'a RLookupKey<'a>,
        field_names: &[Box<[u8]>],
        is_load_all: bool,
        sort_asc_map: u64,
        limit: Option<(u64, u64)>,
    ) -> Self {
        let requested = if is_load_all {
            None
        } else {
            Some(
                field_names
                    .iter()
                    .filter_map(|name| CString::new(name.as_ref()).ok())
                    .collect::<Box<[_]>>(),
            )
        };
        Self {
            common: CollectCommon::new(sort_asc_map, limit),
            input_key,
            requested,
        }
    }

    pub const fn reducer_mut(&mut self) -> &mut Reducer {
        &mut self.common.reducer
    }

    pub fn alloc_instance(&self) -> &mut LocalCollectCtx {
        self.common.arena.alloc(LocalCollectCtx::new(self))
    }

    /// Exposed via `CollectReducer_IsLocalLoadAll` for C++ parser tests.
    pub const fn is_load_all(&self) -> bool {
        self.requested.is_none()
    }
}

impl LocalCollectCtx {
    pub fn new(_r: &LocalCollectReducer) -> Self {
        Self {
            lookup: RLookup::new(),
            rows: Vec::new(),
        }
    }

    /// Deserialize the shard payload carried by `row` into [`RLookupRow`]s.
    ///
    /// When [`requested`][LocalCollectReducer::requested] is `Some`, only
    /// those fields are written; extra fields (e.g. sort keys) are ignored.
    pub fn add(&mut self, r: &LocalCollectReducer, row: &RLookupRow) {
        // TODO: replace with `debug_assert_warn!` once that macro is accessible from this crate.
        let Some(payload) = row.get(r.input_key) else {
            debug_assert!(
                false,
                "local COLLECT: input_key must be present in every merge row"
            );
            tracing::warn!("local COLLECT: input_key missing from merge row; skipping row");
            return;
        };
        // TODO: replace with `debug_assert_warn!` once that macro is accessible from this crate.
        let Value::Array(items) = &**payload else {
            debug_assert!(false, "local COLLECT: input_key payload must be an Array");
            tracing::warn!("local COLLECT: input_key payload is not an Array; skipping row");
            return;
        };

        for item in items.iter() {
            self.rows
                .push(prepare_row(&mut self.lookup, r.requested.as_deref(), item));
        }
    }

    /// Emit buffered rows as a client-facing `[Map, …]` array.
    ///
    /// [`RLookupKeyFlag::Hidden`] keys are excluded, matching the remote
    /// `FIELDS *` projection rule.
    pub fn finalize(&mut self, _r: &LocalCollectReducer) -> SharedValue {
        let rows = mem::take(&mut self.rows);
        let template: Vec<(&RLookupKey, SharedValue)> = self
            .lookup
            .iter()
            .filter(|k| !k.flags.contains(RLookupKeyFlag::Hidden))
            .map(|k| (k, SharedValue::new_string(k.name().to_bytes().to_vec())))
            .collect();

        SharedValue::new_array(rows.into_iter().map(|row| {
            let entries: Vec<_> = template
                .iter()
                .filter_map(|(key, name)| row.get(key).map(|v| (name.clone(), v.clone())))
                .collect();
            SharedValue::new_map(entries)
        }))
    }
}
