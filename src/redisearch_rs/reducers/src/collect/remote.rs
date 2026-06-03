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
use crate::collect::UNDERSCORE_KEY;
use crate::collect::distinct::encode_value_refs;
use crate::collect::storage::Storage;

/// Whether `key` projects the document key (`@__key`), comparing the resolved
/// path (falling back to the name) like the C `RLookupKey_GetPath` check.
fn is_key_field(key: &RLookupKey<'_>) -> bool {
    let path = key
        .path()
        .as_ref()
        .map_or_else(|| key.name().as_ref(), |p| p.as_ref());
    path == UNDERSCORE_KEY
}

/// Field-selection state: `FIELDS *` vs explicit list.
///
/// Sort keys live in both variants; they feed the heap comparator and, in
/// [`Fields::Specific`] mode, append to the per-row projection.
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

    /// Keys to project per row in [`RemoteCollectCtx::add`].
    fn get_keys_add(&self) -> impl Iterator<Item = &'a RLookupKey<'a>> + '_ {
        match self {
            Self::All { src_lookup, .. } => Either::Left(
                src_lookup
                    .iter()
                    .filter(|k| !k.flags.contains(RLookupKeyFlag::Hidden)),
            ),
            Self::Specific {
                field_keys,
                sort_keys,
            } => Either::Right(field_keys.iter().copied().chain(sort_keys.iter().copied())),
        }
    }

    /// Keys forming the DISTINCT dedup identity: the **projected fields only**,
    /// excluding the SORTBY keys that [`Fields::Specific`] appends to each row
    /// for the coordinator's re-sort.
    /// For [`Fields::All`] the projection *is* the non-hidden lookup, so this
    /// matches [`Fields::get_keys_add`].
    fn dedup_keys(&self) -> impl Iterator<Item = &'a RLookupKey<'a>> + '_ {
        match self {
            Self::All { src_lookup, .. } => Either::Left(
                src_lookup
                    .iter()
                    .filter(|k| !k.flags.contains(RLookupKeyFlag::Hidden)),
            ),
            Self::Specific { field_keys, .. } => Either::Right(field_keys.iter().copied()),
        }
    }

    /// Whether `@__key` is among the projected fields, which makes DISTINCT a
    /// no-op. Scans the same keys [`Fields::dedup_keys`] uses.
    fn projects_key_field(&self) -> bool {
        match self {
            Self::All { src_lookup, .. } => src_lookup
                .iter()
                .any(|k| !k.flags.contains(RLookupKeyFlag::Hidden) && is_key_field(k)),
            Self::Specific { field_keys, .. } => field_keys.iter().any(|k| is_key_field(k)),
        }
    }

    /// Key→name template for [`RemoteCollectCtx::finalize`].
    ///
    /// `include_sort_extras` extends the [`Fields::Specific`] projection
    /// with sort keys (shard path: `true`; client-facing: `false`).
    fn build_template(&self, include_sort_extras: bool) -> Vec<(&'a RLookupKey<'a>, SharedValue)> {
        let keys: Vec<&RLookupKey<'a>> = match self {
            Self::All { src_lookup, .. } => src_lookup
                .iter()
                .filter(|k| !k.flags.contains(RLookupKeyFlag::Hidden))
                .collect(),
            Self::Specific {
                field_keys,
                sort_keys,
            } => {
                let extras: &[&RLookupKey<'a>] = if include_sort_extras { sort_keys } else { &[] };
                dedup_by_dstidx(field_keys, extras)
            }
        };
        keys.into_iter()
            .map(|k| (k, SharedValue::new_string(k.name().to_bytes().to_vec())))
            .collect()
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

    pub const fn is_distinct(&self) -> bool {
        self.distinct
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
        let sortby = !r.fields.sort_keys().is_empty();
        // The `@__key`: DISTINCT needs SORTBY, and when the document key is
        // projected every tuple is unique, so dedup is a no-op and we keep the
        // cheaper plain `Heap`.
        let distinct = r.distinct && sortby && !r.fields.projects_key_field();
        let storage = if distinct {
            Storage::new_distinct(r.limit, r.sort_asc_map)
        } else {
            Storage::new(sortby, r.limit, r.sort_asc_map)
        };
        Self { storage }
    }

    /// Project the source row's field values into a stored [`RLookupRow`]
    /// and snapshot the sort-key values for the heap comparator.
    ///
    /// The array path ignores the snapshot closure entirely. The heap path
    /// uses the snapshot to drive comparisons, dropping doomed candidates
    /// without paying the row-projection cost.
    pub fn add(
        &mut self,
        r: &RemoteCollectReducer<'_>,
        row: &RLookupRow<'_>,
        doc_id: ffi::t_docId,
    ) {
        let sort_vals = || -> Box<[Option<SharedValue>]> {
            r.fields
                .sort_keys()
                .iter()
                .map(|key| row.get(key).cloned())
                .collect()
        };
        let project = || {
            let mut dst = RLookupRow::new();
            for key in r.fields.get_keys_add() {
                if let Some(v) = row.get(key) {
                    dst.write_key(key, v.clone());
                }
            }
            dst
        };
        // DISTINCT dedup identity: the projected fields only.
        let dedup_from_row = |projected: &RLookupRow<'static>| {
            encode_value_refs(
                r.fields
                    .dedup_keys()
                    .map(|k| projected.get(k).map(|v| &**v)),
            )
        };
        self.storage
            .insert_entry_with_dedup(sort_vals, doc_id, project, dedup_from_row);
    }

    /// Serialize the buffered rows into an array of maps. Keys absent from a
    /// row are omitted; on the cluster path
    /// [`LocalCollectCtx::finalize`][crate::collect::local::LocalCollectCtx::finalize]
    /// reconstructs the client-facing result from the emitted payload.
    pub fn finalize(&mut self, r: &RemoteCollectReducer<'_>) -> SharedValue {
        let rows = self.storage.drain();
        let template = r.fields.build_template(r.is_internal);
        SharedValue::new_array(rows.map(|row| {
            let entries: Vec<_> = template
                .iter()
                .filter_map(|(key, name)| row.get(key).map(|v| (name.clone(), v.clone())))
                .collect();
            SharedValue::new_map(entries)
        }))
    }
}
