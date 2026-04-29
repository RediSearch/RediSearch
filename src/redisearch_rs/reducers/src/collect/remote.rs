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

use rlookup::{RLookupKey, RLookupRow};
use value::SharedValue;

use crate::Reducer;
use crate::collect::common::CollectCommon;
use crate::collect::storage::Storage;

/// Remote COLLECT reducer. See [`CollectCommon`] for the FFI-layout invariant
/// the trailing `const _` assertion enforces.
#[repr(C)]
pub struct RemoteCollectReducer<'a> {
    common: CollectCommon,
    field_keys: Box<[&'a RLookupKey<'a>]>,
    has_wildcard: bool,
    /// Raw sort-key references, including keys not present in `FIELDS`.
    sort_keys: Box<[&'a RLookupKey<'a>]>,
    /// Set by `RDCRCollect_New` from `ReducerOpts_IsInternal(options)`:
    /// `true` for shard replies that the coordinator dispatched with
    /// `_FT.AGGREGATE`, `false` for direct standalone or coordinator
    /// execution.
    ///
    /// In its current shape this single bit gates **two** behaviours that
    /// the distributed COLLECT pipeline needs from the shard side:
    ///
    /// 1. **Sort-key column emission.** When `true`, the per-row map serialised
    ///    by [`RemoteCollectCtx::finalize`] includes the raw sort-key values
    ///    alongside the requested fields, so the coordinator's
    ///    [`super::local::LocalCollectReducer`] can re-rank shard rows during
    ///    merge.
    /// 2. **LIMIT offset application policy.** The coordinator forwards the
    ///    user's `LIMIT offset count` to shards verbatim (via
    ///    `buildCollectArgs`'s `memcpy`) instead of rewriting it to
    ///    `LIMIT 0 (offset+count)` like
    ///    [`crate::collect::storage`]'s sister patterns do (see
    ///    `serializeArrange` in `aggregate_plan.c`). To honour that wire
    ///    contract, when `is_internal == true` the shard sizes its top-K
    ///    storage to `offset + count` but **does not** apply `skip(offset)`
    ///    locally — the coordinator owns the global offset. When `false`
    ///    (standalone), the shard applies the full
    ///    `skip(offset).take(count)` itself.
    ///
    /// ## Architectural debt — `TODO(<TICKET>)`
    ///
    /// Conflating these two concerns into one bit reflects a missing piece
    /// of the COLLECT planner integration: unlike the outer pipeline's
    /// `PLN_ArrangeStep`, COLLECT's `PLN_Reducer` does not have `LIMIT` as
    /// a semantic field that the C planner can rewrite for the shard wire
    /// (`(offset, count) → (0, offset+count)`). Once `LIMIT` is lifted into
    /// `PLN_Reducer` the C side can perform the canonical rewrite once,
    /// at which point this flag should split back into a single-purpose
    /// "emit sort-key columns" boolean and the local-offset gate disappears
    /// — the coordinator-to-shard wire would already carry `(0,
    /// offset+count)` like every other distributed pipeline step.
    ///
    /// Until then, the implicit contract is: **whoever changes
    /// `distributeCollect` (or any other coordinator-side rewriter) to drop
    /// `offset` on the shard wire must also flip this gate to always apply
    /// `skip(offset).take(count)`**, otherwise the offset gets dropped
    /// twice. The `remote_internal_mode_does_not_apply_limit_offset_locally`
    /// integration regression test in `reducers/tests/collect.rs` codifies
    /// the contract.
    is_internal: bool,
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
/// `RemoteCollectCtx` is arena-allocated ([`Bump`][bumpalo::Bump] does not run
/// destructors), so the FFI side must call `ptr::drop_in_place` to decrement
/// the buffered [`SharedValue`] refcounts.
pub struct RemoteCollectCtx {
    storage: Storage,
}

impl<'a> RemoteCollectReducer<'a> {
    pub fn new(
        field_keys: Box<[&'a RLookupKey<'a>]>,
        has_wildcard: bool,
        sort_keys: Box<[&'a RLookupKey<'a>]>,
        sort_asc_map: u64,
        limit: Option<(u64, u64)>,
        is_internal: bool,
    ) -> Self {
        let is_array = sort_keys.is_empty();
        Self {
            common: CollectCommon::new(is_array, sort_asc_map, limit),
            field_keys,
            has_wildcard,
            sort_keys,
            is_internal,
        }
    }

    pub const fn reducer_mut(&mut self) -> &mut Reducer {
        &mut self.common.reducer
    }

    pub fn alloc_instance(&self) -> &mut RemoteCollectCtx {
        self.common.arena.alloc(RemoteCollectCtx::new(self))
    }

    // Temporary C++ parser-test accessors, exposed through `reducers_ffi`.

    pub const fn field_keys_len(&self) -> usize {
        self.field_keys.len()
    }

    pub const fn has_wildcard(&self) -> bool {
        self.has_wildcard
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

impl RemoteCollectCtx {
    pub fn new(r: &RemoteCollectReducer) -> Self {
        Self {
            storage: Storage::new(r.sort_keys.is_empty(), r.common.cap),
        }
    }

    /// Buffer one input row through [`Storage::insert_entry`]. Sort values are
    /// eager (the comparator reads them on every heap decision); field values
    /// are deferred inside the `project` closure. Missing values are
    /// materialised as the static null sentinel.
    pub fn add(&mut self, r: &RemoteCollectReducer, row: &RLookupRow) {
        let sort_vals: Vec<SharedValue> = r
            .sort_keys
            .iter()
            .map(|key| {
                row.get(key)
                    .cloned()
                    .unwrap_or_else(SharedValue::null_static)
            })
            .collect();
        let project = || {
            r.field_keys
                .iter()
                .map(|key| {
                    row.get(key)
                        .cloned()
                        .unwrap_or_else(SharedValue::null_static)
                })
                .collect::<Vec<_>>()
                .into_boxed_slice()
        };
        self.storage
            .insert_entry(r.common.cap, r.common.sort_asc_map, &sort_vals, project);
    }

    /// Serialise the buffered rows as `[Map, ...]`. The two `is_internal`
    /// behaviours (LIMIT-offset gating, sort-key column emission) are
    /// documented on [`RemoteCollectReducer::is_internal`].
    pub fn finalize(&mut self, r: &RemoteCollectReducer) -> SharedValue {
        let drain_limit = if r.is_internal { None } else { r.common.limit };
        let drained = self.storage.drain_with_limit(drain_limit);

        let field_names: Vec<SharedValue> = r
            .field_keys
            .iter()
            .map(|k| SharedValue::new_string(k.name().to_bytes().to_vec()))
            .collect();
        let sort_names: Vec<SharedValue> = if r.is_internal {
            r.sort_keys
                .iter()
                .map(|k| SharedValue::new_string(k.name().to_bytes().to_vec()))
                .collect()
        } else {
            Vec::new()
        };

        let rows: Vec<SharedValue> = drained
            .into_iter()
            .map(|(projected, sort_vals)| {
                let mut entries: Vec<(SharedValue, SharedValue)> = field_names
                    .iter()
                    .cloned()
                    .zip(projected.into_vec())
                    .collect();
                if r.is_internal
                    && let Some(sort_vals) = sort_vals
                {
                    entries.extend(sort_names.iter().cloned().zip(sort_vals.into_vec()));
                }
                SharedValue::new_map(entries)
            })
            .collect();
        SharedValue::new_array(rows)
    }
}
