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

use bumpalo::Bump;
use itertools::Either;
use rlookup::{RLookup, RLookupKey, RLookupKeyFlag, RLookupRow};
use value::{SharedValue, Value};

use crate::Reducer;
use crate::collect::storage::{ProjectedRow, Storage};

/// Look up `name` in a shard-payload item (`Map` or flat `Array`).
///
/// # Panics
///
/// Panics in debug builds if `item` is not a `Map` or `Array`. Callers must
/// pre-validate the shape (e.g. with a `matches!` guard) before calling this.
fn get_field<'a>(item: &'a Value, name: &[u8]) -> Option<&'a SharedValue> {
    match item {
        Value::Map(m) => m.get(name),
        Value::Array(a) => a.map_get(name),
        _ => unreachable!("shard payload item must be a Map or Array"),
    }
}

/// Field-selection state: `FIELDS *` vs explicit list.
///
/// Both variants carry `sort_key_names`; the names feed the ranking
/// comparator and the per-item sort snapshot regardless of mode.
enum Fields {
    /// `FIELDS *` mode. Every key observed in a shard payload is written
    /// into the per-group lookup at ingestion time.
    All { sort_key_names: Box<[CString]> },
    /// Explicit field list. Only `requested` names are written into the
    /// lookup; extra payload fields are ignored.
    Specific {
        requested: Box<[CString]>,
        sort_key_names: Box<[CString]>,
    },
}

impl Fields {
    fn sort_key_names(&self) -> &[CString] {
        match self {
            Self::All { sort_key_names } | Self::Specific { sort_key_names, .. } => sort_key_names,
        }
    }

    /// Build a row from one shard-payload item; consumed by
    /// [`LocalCollectCtx::add`].
    ///
    /// Callers must pre-validate that `item` is a [`Value::Map`] or
    /// [`Value::Array`].
    fn prepare_row(&self, item: &Value, lookup: &mut RLookup<'static>) -> RLookupRow<'static> {
        let mut dst = RLookupRow::new();
        match self {
            Self::All { .. } => match item {
                Value::Map(m) => {
                    for (k, v) in m.iter() {
                        write_named_field(&mut dst, lookup, k, v);
                    }
                }
                Value::Array(a) => {
                    let (pairs, remainder) = a.as_chunks::<2>();
                    tracing_assert::debug_assert_warn!(
                        remainder.is_empty(),
                        "odd-length RESP2 payload"
                    );
                    for [k, v] in pairs {
                        write_named_field(&mut dst, lookup, k, v);
                    }
                }
                _ => unreachable!("shard payload item must be a Map or Array"),
            },
            Self::Specific { requested, .. } => {
                for name in requested {
                    if let Some(v) = get_field(item, name.to_bytes()) {
                        dst.write_key_by_name(lookup, name.clone(), v.clone());
                    }
                }
            }
        }
        dst
    }
}

/// Materialize `(k, v)` as a typed [`RLookupRow`] entry.
///
/// Terminates the wire-side `BString → CString` check; a non-string or
/// interior-NUL key is a remote-side contract bug and skipped.
fn write_named_field(
    dst: &mut RLookupRow<'static>,
    lookup: &mut RLookup<'static>,
    k: &SharedValue,
    v: &SharedValue,
) {
    if let Some(name) = k.as_str_bytes()
        && let Ok(cname) = CString::new(name)
    {
        dst.write_key_by_name(lookup, cname, v.clone());
    } else {
        tracing_assert::debug_assert_warn!(
            false,
            "local COLLECT: shard payload field name must be a NUL-free string"
        );
    }
}

/// Local COLLECT reducer.
///
/// Must remain `#[repr(C)]` with [`Reducer`] at offset 0 so the C layer
/// can downcast this struct to `ffi::Reducer*` and read the vtable directly.
#[repr(C)]
pub struct LocalCollectReducer<'a> {
    /// C-visible vtable header. Pinned to offset 0 by `#[repr(C)]` so the
    /// C layer can downcast `LocalCollectReducer*` to `ffi::Reducer*`.
    reducer: Reducer,
    /// Arena for per-group contexts; destructors still need explicit calls.
    arena: Bump,
    /// Bit `i` is 0 for DESC and 1 for ASC, matching `SORTASCMAP_INIT`.
    sort_asc_map: u64,
    /// Lookup key for the per-remote payload.
    input_key: &'a RLookupKey<'a>,
    fields: Fields,
    limit: Option<(u64, u64)>,
    distinct: bool,
}

const _: () = assert!(core::mem::offset_of!(LocalCollectReducer<'_>, reducer) == 0);

/// Per-group instance of [`LocalCollectReducer`].
///
/// Because `LocalCollectCtx` is arena-allocated ([`Bump`] does not run
/// destructors), [`drop_in_place`][std::ptr::drop_in_place] must be
/// called to run destructors for the inner storage and decrement
/// [`SharedValue`] refcounts.
pub struct LocalCollectCtx {
    lookup: RLookup<'static>,
    storage: Storage<()>,
}

impl<'a> LocalCollectReducer<'a> {
    /// Create a reducer from C-parsed configuration.
    ///
    /// [`CString`]-typed names move the NUL/encoding check to the FFI
    /// boundary, where C strings are NUL-terminated by contract.
    pub fn new(
        input_key: &'a RLookupKey<'a>,
        requested: Option<Box<[CString]>>,
        sort_key_names: Box<[CString]>,
        sort_asc_map: u64,
        limit: Option<(u64, u64)>,
        distinct: bool,
    ) -> Self {
        let fields = match requested {
            Some(requested) => Fields::Specific {
                requested,
                sort_key_names,
            },
            None => Fields::All { sort_key_names },
        };
        Self {
            reducer: Reducer::new(),
            arena: Bump::new(),
            sort_asc_map,
            input_key,
            fields,
            limit,
            distinct,
        }
    }

    pub const fn reducer_mut(&mut self) -> &mut Reducer {
        &mut self.reducer
    }

    pub fn alloc_instance(&self) -> &mut LocalCollectCtx {
        self.arena.alloc(LocalCollectCtx::new(self))
    }

    /// Exposed via `CollectReducer_IsLocalLoadAll` for C++ parser tests.
    pub const fn is_load_all(&self) -> bool {
        matches!(self.fields, Fields::All { .. })
    }
}

impl LocalCollectCtx {
    pub fn new(r: &LocalCollectReducer) -> Self {
        Self {
            lookup: RLookup::new(),
            storage: Storage::new(
                !r.fields.sort_key_names().is_empty(),
                r.distinct,
                r.limit,
                r.sort_asc_map,
            ),
        }
    }

    /// Add each item of the shard payload carried by `row` (a serialized array
    /// of per-row maps) to the storage.
    pub fn add(&mut self, r: &LocalCollectReducer, row: &RLookupRow) {
        let Some(Value::Array(items)) = row.get(r.input_key).map(|p| &**p) else {
            tracing_assert::debug_assert_warn!(
                false,
                "local COLLECT: input_key must be present and contain an Array"
            );
            return;
        };

        for item in items.iter() {
            if !matches!(&**item, Value::Map(_) | Value::Array(_)) {
                tracing_assert::debug_assert_warn!(
                    false,
                    "local COLLECT: shard payload item must be a Map or Array"
                );
                continue;
            }
            match &mut self.storage {
                Storage::Unranked(unranked) => unranked
                    .push(|| ProjectedRow::new(r.fields.prepare_row(item, &mut self.lookup))),
                Storage::Ranked(ranked) => {
                    let item = &**item;
                    let sort_vals = r
                        .fields
                        .sort_key_names()
                        .iter()
                        .map(|name| get_field(item, name.to_bytes()));
                    ranked.consider(sort_vals, (), || {
                        ProjectedRow::new(r.fields.prepare_row(item, &mut self.lookup))
                    })
                }
            }
        }
    }

    /// Emit buffered rows as a client-facing `[Map, …]` array, applying the
    /// `LIMIT offset count` slice. The local reducer is the client-facing
    /// terminus, so it is the single point where `OFFSET` is honoured in
    /// distributed mode — see
    /// [`super::remote::RemoteCollectReducer::is_internal`].
    ///
    /// [`RLookupKeyFlag::Hidden`] keys are excluded, matching the remote
    /// `FIELDS *` projection rule.
    pub fn finalize(&mut self, _r: &LocalCollectReducer) -> SharedValue {
        let template: Vec<(&RLookupKey, SharedValue)> = self
            .lookup
            .iter()
            .filter(|k| !k.flags.contains(RLookupKeyFlag::Hidden))
            .map(|k| (k, SharedValue::new_string(k.name().to_bytes().to_vec())))
            .collect();

        let rows = match &mut self.storage {
            Storage::Unranked(u) => Either::Left(u.drain()),
            Storage::Ranked(r) => Either::Right(r.drain()),
        };
        SharedValue::new_array(rows.map(|projected| {
            let row = projected.row();
            SharedValue::new_map(
                template
                    .iter()
                    .filter_map(|(key, name)| row.get(key).map(|v| (name.clone(), v.clone())))
                    .collect::<Vec<_>>(),
            )
        }))
    }
}
