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

use bumpalo::Bump;
use itertools::Either;
use rlookup::{RLookup, RLookupKey, RLookupKeyFlag, RLookupRow};
use value::SharedValue;

use crate::Reducer;
use crate::collect::ranking::{RankedEntry, RankingKey};
use crate::collect::storage::{ProjectedRow, Storage};

/// Field-selection state: `FIELDS *` vs explicit list.
///
/// Sort keys feed the ranking comparator in both variants. [`Fields::Specific`]
/// deliberately excludes them from the per-row projection and merges them back at
/// finalize (see [`Fields::build_template`]); [`Fields::All`] carries them only
/// because they are non-hidden lookup fields it already projects, not by any
/// special handling.
enum Fields<'a> {
    /// `FIELDS *` mode. Non-[`RLookupKeyFlag::Hidden`] keys in `src_lookup`
    /// are emitted; the lookup is re-walked per call so an upstream
    /// `LOAD *` mid-pipeline is picked up.
    All {
        src_lookup: &'a RLookup<'a>,
        sort_keys: Box<[&'a RLookupKey<'a>]>,
    },
    /// Explicit field list. `field_keys` may overlap with `sort_keys`;
    /// dedup is the consumer's job (see [`dedup_by_dstidx`]).
    Specific {
        field_keys: Box<[&'a RLookupKey<'a>]>,
        sort_keys: Box<[&'a RLookupKey<'a>]>,
    },
}

impl<'a> Fields<'a> {
    fn sort_keys(&self) -> &[&'a RLookupKey<'a>] {
        match self {
            Self::All { sort_keys, .. } | Self::Specific { sort_keys, .. } => sort_keys,
        }
    }

    /// Keys to project per row in [`RemoteCollectCtx::add`]. [`Fields::Specific`]
    /// projects only `field_keys`; its sort columns are merged back at finalize
    /// (see [`Fields::build_template`]).
    fn get_keys_add(&self) -> impl Iterator<Item = &'a RLookupKey<'a>> + '_ {
        match self {
            Self::All { src_lookup, .. } => Either::Left(
                src_lookup
                    .iter()
                    .filter(|k| !k.flags.contains(RLookupKeyFlag::Hidden)),
            ),
            Self::Specific { field_keys, .. } => Either::Right(field_keys.iter().copied()),
        }
    }

    /// The `(key→name template, sort keys to merge)` pair for
    /// [`RemoteCollectCtx::finalize`].
    ///
    /// When `include_sort_extras` (the shard path), the [`Fields::Specific`]
    /// template gains its SORTBY keys and they are returned as the merge set so
    /// `finalize` re-attaches them from the ranking-key snapshot; otherwise the
    /// merge set is empty. [`Fields::All`] already projects every field.
    fn build_template(
        &self,
        include_sort_extras: bool,
    ) -> (
        Vec<(&'a RLookupKey<'a>, SharedValue)>,
        &[&'a RLookupKey<'a>],
    ) {
        let (keys, sort_extras): (Vec<&RLookupKey<'a>>, &[&RLookupKey<'a>]) = match self {
            Self::All { src_lookup, .. } => (
                src_lookup
                    .iter()
                    .filter(|k| !k.flags.contains(RLookupKeyFlag::Hidden))
                    .collect(),
                &[],
            ),
            Self::Specific {
                field_keys,
                sort_keys,
            } => {
                let extras: &[&RLookupKey<'a>] = if include_sort_extras { sort_keys } else { &[] };
                (dedup_by_dstidx(field_keys, extras), extras)
            }
        };
        let template = keys
            .into_iter()
            .map(|k| (k, SharedValue::new_string(k.name().to_bytes().to_vec())))
            .collect();
        (template, sort_extras)
    }
}

/// Remote COLLECT reducer.
///
/// Must remain `#[repr(C)]` with [`Reducer`] at offset 0 so the C layer
/// can downcast this struct to `ffi::Reducer*` and read the vtable directly.
#[repr(C)]
pub struct RemoteCollectReducer<'a> {
    /// C-visible vtable header. Pinned to offset 0 by `#[repr(C)]` so the
    /// C layer can downcast `RemoteCollectReducer*` to `ffi::Reducer*`.
    reducer: Reducer,
    /// Arena for per-group contexts; destructors still need explicit calls.
    arena: Bump,
    /// Bit `i` is 0 for DESC and 1 for ASC, matching `SORTASCMAP_INIT`.
    sort_asc_map: u64,
    fields: Fields<'a>,
    limit: Option<(u64, u64)>,
    is_internal: bool,
    distinct: bool,
}

const _: () = assert!(core::mem::offset_of!(RemoteCollectReducer<'_>, reducer) == 0);

/// Per-group instance of [`RemoteCollectReducer`].
///
/// Arena-allocated under [`Bump`], which does not run destructors —
/// [`ptr::drop_in_place`][std::ptr::drop_in_place] must be called to release
/// the stored [`RLookupRow`]s and decrement [`SharedValue`] refcounts.
pub struct RemoteCollectCtx {
    storage: Storage<ffi::t_docId>,
}

impl<'a> RemoteCollectReducer<'a> {
    /// Create a reducer from C-parsed configuration.
    ///
    /// `srclookup` is `Some` for `FIELDS *`; when both `srclookup` and
    /// `field_keys` are supplied, `srclookup` wins.
    pub fn new(
        field_keys: Box<[&'a RLookupKey<'a>]>,
        srclookup: Option<&'a RLookup<'a>>,
        sort_keys: Box<[&'a RLookupKey<'a>]>,
        sort_asc_map: u64,
        limit: Option<(u64, u64)>,
        is_internal: bool,
        distinct: bool,
    ) -> Self {
        // `distributeCollect` rewrites the shard wire's LIMIT to
        // `(0, offset+count)`, so an internal shard never sees a non-zero
        // offset.
        debug_assert!(!(is_internal && limit.is_some_and(|(offset, _)| offset != 0)));

        let fields = match srclookup {
            Some(src_lookup) => Fields::All {
                src_lookup,
                sort_keys,
            },
            None => Fields::Specific {
                field_keys,
                sort_keys,
            },
        };
        Self {
            reducer: Reducer::new(),
            arena: Bump::new(),
            sort_asc_map,
            fields,
            limit,
            is_internal,
            distinct,
        }
    }

    pub const fn reducer_mut(&mut self) -> &mut Reducer {
        &mut self.reducer
    }

    pub fn alloc_instance(&self) -> &mut RemoteCollectCtx {
        self.arena.alloc(RemoteCollectCtx::new(self))
    }

    // Temporary C++ parser-test accessors, exposed through `reducers_ffi`.

    pub const fn field_keys_len(&self) -> usize {
        match &self.fields {
            Fields::Specific { field_keys, .. } => field_keys.len(),
            Fields::All { .. } => 0,
        }
    }

    pub const fn is_load_all(&self) -> bool {
        matches!(self.fields, Fields::All { .. })
    }

    pub const fn sort_keys_len(&self) -> usize {
        match &self.fields {
            Fields::All { sort_keys, .. } | Fields::Specific { sort_keys, .. } => sort_keys.len(),
        }
    }

    pub const fn sort_asc_map(&self) -> u64 {
        self.sort_asc_map
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

impl RemoteCollectCtx {
    pub fn new(r: &RemoteCollectReducer<'_>) -> Self {
        Self {
            storage: Storage::new(
                !r.fields.sort_keys().is_empty(),
                r.distinct,
                r.limit,
                r.sort_asc_map,
            ),
        }
    }

    /// Add `row` to the storage. The sort-key snapshot is built only on the
    /// ranked arm, so the unranked path pays nothing for sorting.
    pub fn add(
        &mut self,
        r: &RemoteCollectReducer<'_>,
        row: &RLookupRow<'_>,
        doc_id: ffi::t_docId,
    ) {
        let project = || {
            let mut dst = RLookupRow::new();
            for key in r.fields.get_keys_add() {
                if let Some(v) = row.get(key) {
                    dst.write_key(key, v.clone());
                }
            }
            ProjectedRow::new(dst)
        };
        match &mut self.storage {
            Storage::Unranked(unranked) => unranked.push(project),
            Storage::Ranked(ranked) => {
                let sort_vals = r
                    .fields
                    .sort_keys()
                    .iter()
                    .map(|key| row.get(key).cloned())
                    .collect();
                ranked.consider(sort_vals, doc_id, project);
            }
        }
    }

    /// Serialize the buffered rows into an array of maps. Keys absent from a
    /// row are omitted; on the cluster path
    /// [`LocalCollectCtx::finalize`][crate::collect::local::LocalCollectCtx::finalize]
    /// reconstructs the client-facing result from the emitted payload.
    ///
    /// On the shard (`is_internal`) ranked path the SORTBY columns are re-attached
    /// from each entry's ranking-key snapshot; the client path emits only the
    /// requested fields.
    pub fn finalize(&mut self, r: &RemoteCollectReducer<'_>) -> SharedValue {
        let (template, sort_extras) = r.fields.build_template(r.is_internal);
        let to_map = |row: &RLookupRow<'static>| {
            SharedValue::new_map(
                template
                    .iter()
                    .filter_map(|(key, name)| row.get(key).map(|v| (name.clone(), v.clone())))
                    .collect::<Vec<_>>(),
            )
        };
        // Rebuild a ranked entry's wire row: projected fields plus the deferred
        // sort columns merged back from the ranking-key snapshot.
        let map_entry = |entry: RankedEntry<RankingKey<ffi::t_docId>, ProjectedRow>| {
            let (ranking_key, projected) = entry.into_parts();
            let mut row = projected.into_row();
            // `sort_extras` (names) and the snapshot (values) share SORTBY order.
            for (key, val) in sort_extras.iter().zip(ranking_key.sort_vals()) {
                if let Some(val) = val {
                    row.write_key(key, val.clone());
                }
            }
            to_map(&row)
        };
        let rows = match &mut self.storage {
            Storage::Unranked(unranked) => Either::Left(unranked.drain().map(|p| to_map(p.row()))),
            Storage::Ranked(ranked) => Either::Right(ranked.drain().map(map_entry)),
        };
        SharedValue::new_array(rows)
    }
}
