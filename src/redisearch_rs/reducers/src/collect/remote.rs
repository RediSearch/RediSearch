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

use itertools::Either;
use rlookup::{RLookup, RLookupKey, RLookupKeyFlag, RLookupRow};
use value::SharedValue;

use crate::Reducer;
use crate::collect::common::CollectCommon;
use crate::collect::storage::Storage;

/// Remote COLLECT reducer.
///
/// Must remain `#[repr(C)]` with [`CollectCommon`] at offset 0 so the C layer
/// can downcast this struct to `ffi::Reducer*` and read the vtable directly.
#[repr(C)]
pub struct RemoteCollectReducer<'a> {
    common: CollectCommon,
    field_keys: Box<[&'a RLookupKey<'a>]>,
    /// Source lookup for `FIELDS *` mode.
    ///
    /// `Some` when the user wrote `FIELDS *`; every key not flagged
    /// [`RLookupKeyFlag::Hidden`] is emitted. Walked per [`RemoteCollectCtx::add`]
    /// call rather than once at construction so that keys appended by an
    /// upstream `LOAD *` mid-pipeline are included.
    srclookup: Option<&'a RLookup<'a>>,
    /// Raw sort-key references, including keys not present in `FIELDS`.
    sort_keys: Box<[&'a RLookupKey<'a>]>,
    limit: Option<(u64, u64)>,
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
/// Arena-allocated under [`Bump`][bumpalo::Bump], which does not run
/// destructors — [`ptr::drop_in_place`][std::ptr::drop_in_place] must be
/// called to release the stored [`RLookupRow`]s and decrement
/// [`SharedValue`] refcounts.
pub struct RemoteCollectCtx {
    storage: Storage,
}

impl<'a> RemoteCollectReducer<'a> {
    /// Create a reducer from C-parsed configuration.
    ///
    /// `srclookup` is `Some` when the user wrote `FIELDS *`.
    pub fn new(
        field_keys: Box<[&'a RLookupKey<'a>]>,
        srclookup: Option<&'a RLookup<'a>>,
        sort_keys: Box<[&'a RLookupKey<'a>]>,
        sort_asc_map: u64,
        limit: Option<(u64, u64)>,
        is_internal: bool,
    ) -> Self {
        Self {
            common: CollectCommon::new(sort_asc_map),
            field_keys,
            srclookup,
            sort_keys,
            limit,
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
        self.limit.is_some()
    }

    pub const fn limit_offset(&self) -> u64 {
        match self.limit {
            Some((offset, _)) => offset,
            None => 0,
        }
    }

    pub const fn limit_count(&self) -> u64 {
        match self.limit {
            Some((_, count)) => count,
            None => 0,
        }
    }
}

/// Deduplicate `field_keys ++ sort_extras` by `dstidx`,
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

/// Builds the key→name template once per group so [`RemoteCollectCtx::finalize`]
/// can clone pre-allocated name [`SharedValue`]s per row rather than re-allocating.
fn build_finalize_template<'a>(
    r: &RemoteCollectReducer<'a>,
) -> Vec<(&'a RLookupKey<'a>, SharedValue)> {
    let keys: Vec<&RLookupKey<'a>> = if let Some(lookup) = r.srclookup {
        lookup
            .iter()
            .filter(|k| !k.flags.contains(RLookupKeyFlag::Hidden))
            .collect()
    } else {
        let sort_extras: &[&RLookupKey<'a>] = if r.is_internal { &r.sort_keys } else { &[] };
        dedup_by_dstidx(&r.field_keys, sort_extras)
    };
    keys.into_iter()
        .map(|k| (k, SharedValue::new_string(k.name().to_bytes().to_vec())))
        .collect()
}

impl RemoteCollectCtx {
    pub fn new(r: &RemoteCollectReducer<'_>) -> Self {
        Self {
            storage: Storage::new(!r.sort_keys.is_empty(), r.limit, r.common.sort_asc_map),
        }
    }

    /// Project the source row's field values into a stored [`RLookupRow`]
    /// and snapshot the sort-key values for the heap comparator.
    ///
    /// The array path ignores the snapshot closure entirely. The heap path
    /// uses the snapshot to drive comparisons, dropping doomed candidates
    /// without paying the row-projection cost.
    pub fn add(&mut self, r: &RemoteCollectReducer<'_>, row: &RLookupRow<'_>) {
        let sort_vals = || -> Box<[SharedValue]> {
            r.sort_keys
                .iter()
                .map(|key| {
                    row.get(key)
                        .cloned()
                        .unwrap_or_else(SharedValue::null_static)
                })
                .collect()
        };
        let project = || {
            let mut dst = RLookupRow::new();
            let keys = if let Some(lookup) = r.srclookup {
                Either::Left(
                    lookup
                        .iter()
                        .filter(|k| !k.flags.contains(RLookupKeyFlag::Hidden)),
                )
            } else {
                Either::Right(
                    r.field_keys
                        .iter()
                        .copied()
                        .chain(r.sort_keys.iter().copied()),
                )
            };
            for key in keys {
                if let Some(v) = row.get(key) {
                    dst.write_key(key, v.clone());
                }
            }
            dst
        };
        self.storage.insert_entry(sort_vals, project);
    }

    /// Serialize the buffered rows into an array of maps. Keys absent from a
    /// row are omitted; on the cluster path
    /// [`LocalCollectCtx::finalize`][crate::collect::local::LocalCollectCtx::finalize]
    /// reconstructs the client-facing result from the emitted payload.
    pub fn finalize(&mut self, r: &RemoteCollectReducer<'_>) -> SharedValue {
        // TODO: drop `limit` and the `apply_limit` argument to `drain` once
        // `distributeCollect` switches to the `LIMIT 0 (offset+count)`
        // rewrite that other `distribute*` paths use; the shard would no
        // longer need LIMIT context and `drain` could be called
        // unconditionally.
        let rows = self.storage.drain(!r.is_internal);
        let template = build_finalize_template(r);
        SharedValue::new_array(rows.map(|row| {
            let entries: Vec<_> = template
                .iter()
                .filter_map(|(key, name)| row.get(key).map(|v| (name.clone(), v.clone())))
                .collect();
            SharedValue::new_map(entries)
        }))
    }
}
