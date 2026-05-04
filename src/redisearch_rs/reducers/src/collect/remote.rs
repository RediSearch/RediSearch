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

use std::collections::HashSet;
use std::mem;

use rlookup::{RLookup, RLookupKey, RLookupRow};
use value::SharedValue;

use crate::Reducer;
use crate::collect::common::{CollectCommon, visible_keys};

/// Remote COLLECT reducer.
///
/// Must remain `#[repr(C)]` with [`CollectCommon`] at offset 0 so the C layer
/// can downcast this struct to `ffi::Reducer*` and read the vtable directly.
#[expect(rustdoc::private_intra_doc_links)]
#[repr(C)]
pub struct RemoteCollectReducer<'a> {
    common: CollectCommon,
    field_keys: Box<[&'a RLookupKey<'a>]>,
    /// Source lookup for `FIELDS *` mode (a.k.a. *load-all*).
    ///
    /// `Some` when the user wrote `FIELDS *`; the projection then emits
    /// every visible key present on the row (see [`visible_keys`]), walked
    /// per call.
    ///
    /// The walk runs per `add()` call rather than once at construction because an
    /// upstream `LOAD *` may append keys mid-pipeline.
    srclookup: Option<&'a RLookup<'a>>,
    /// Raw sort-key references, including keys not present in `FIELDS`.
    sort_keys: Box<[&'a RLookupKey<'a>]>,
    /// `true` for shard replies dispatched by the coordinator: extra sort-key
    /// columns are emitted alongside the requested fields so the coordinator
    /// can re-order shard rows during merge. `false` for direct execution.
    /// No-op when [`srclookup`][Self::srclookup] is `Some`.
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
    ///
    /// `srclookup` is `Some` when the user wrote `FIELDS *`; see
    /// [`Self::srclookup`] for the load-all-mode policy.
    #[expect(
        rustdoc::private_intra_doc_links,
        reason = "links to the private srclookup field"
    )]
    pub fn new(
        field_keys: Box<[&'a RLookupKey<'a>]>,
        srclookup: Option<&'a RLookup<'a>>,
        sort_keys: Box<[&'a RLookupKey<'a>]>,
        sort_asc_map: u64,
        limit: Option<(u64, u64)>,
        include_sort_keys: bool,
    ) -> Self {
        Self {
            common: CollectCommon::new(sort_asc_map, limit),
            field_keys,
            srclookup,
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

    pub const fn is_load_all(&self) -> bool {
        self.srclookup.is_some()
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

/// Deduplicate `field_keys ++ sort_extras` by [`RLookupKey::dstidx`],
/// preserving the chained order. A field referenced by `SORTBY` lands in
/// both inputs but must be emitted only once.
fn dedup_by_dstidx<'a>(
    field_keys: &[&'a RLookupKey<'a>],
    sort_extras: &[&'a RLookupKey<'a>],
) -> Vec<&'a RLookupKey<'a>> {
    let mut seen: HashSet<u16> = HashSet::with_capacity(field_keys.len() + sort_extras.len());
    field_keys
        .iter()
        .copied()
        .chain(sort_extras.iter().copied())
        .filter(|k| seen.insert(k.dstidx))
        .collect()
}

/// Pair each `key` with its name as a reusable [`SharedValue`], hoisting the
/// allocation out of the per-row loop in [`RemoteCollectCtx::finalize`].
fn build_template_for_finalize<'a, I>(keys: I) -> Vec<(&'a RLookupKey<'a>, SharedValue)>
where
    I: IntoIterator<Item = &'a RLookupKey<'a>>,
{
    keys.into_iter()
        .map(|k| (k, SharedValue::new_string(k.name().to_bytes().to_vec())))
        .collect()
}

impl<'a> RemoteCollectCtx<'a> {
    pub const fn new(_r: &RemoteCollectReducer<'a>) -> Self {
        Self { rows: Vec::new() }
    }

    /// Project the source row's field-key and sort-key values into a stored
    /// [`RLookupRow`].
    pub fn add(&mut self, r: &RemoteCollectReducer<'a>, row: &RLookupRow<'_>) {
        let mut dst = RLookupRow::new();
        let mut write_if_present = |key: &RLookupKey<'a>| {
            if let Some(v) = row.get(key) {
                dst.write_key(key, v.clone());
            }
        };
        if let Some(lookup) = r.srclookup {
            visible_keys(lookup).for_each(&mut write_if_present);
        } else {
            r.field_keys
                .iter()
                .copied()
                .chain(r.sort_keys.iter().copied())
                .for_each(&mut write_if_present);
        }
        self.rows.push(dst);
    }

    /// Serialize the buffered rows into an array of maps. Keys absent from a
    /// row are omitted; on the cluster path
    /// [`LocalCollectCtx::finalize`][crate::collect::local::LocalCollectCtx::finalize]
    /// null-fills missing requested fields when reconstructing the
    /// client-facing result.
    pub fn finalize(&mut self, r: &RemoteCollectReducer<'a>) -> SharedValue {
        let rows = mem::take(&mut self.rows);
        let keys: Vec<&RLookupKey<'a>> = if let Some(lookup) = r.srclookup {
            visible_keys(lookup).collect()
        } else {
            let sort_extras: &[&RLookupKey<'a>] = if r.include_sort_keys {
                &r.sort_keys
            } else {
                &[]
            };
            dedup_by_dstidx(&r.field_keys, sort_extras)
        };
        let template = build_template_for_finalize(keys);
        SharedValue::new_array(rows.into_iter().map(|row| {
            let entries: Vec<_> = template
                .iter()
                .filter_map(|(key, name)| row.get(key).map(|v| (name.clone(), v.clone())))
                .collect();
            SharedValue::new_map(entries)
        }))
    }
}
