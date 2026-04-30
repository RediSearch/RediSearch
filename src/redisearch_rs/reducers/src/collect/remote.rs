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

use std::collections::HashSet;
use std::mem;

use rlookup::{RLookup, RLookupKey, RLookupKeyFlag, RLookupRow};
use value::SharedValue;

use crate::Reducer;
use crate::collect::common::CollectCommon;

/// Remote COLLECT reducer.
///
/// Must remain `#[repr(C)]` with [`CollectCommon`] at offset 0 so the C layer
/// can downcast this struct to `ffi::Reducer*` and read the vtable directly.
#[expect(rustdoc::private_intra_doc_links)]
#[repr(C)]
pub struct RemoteCollectReducer<'a> {
    common: CollectCommon,
    field_keys: Box<[&'a RLookupKey<'a>]>,
    /// [`Some`] when `FIELDS *` was specified at parse time.
    ///
    /// In wildcard mode both [`RemoteCollectCtx::add`] and
    /// [`RemoteCollectCtx::finalize`] walk this lookup *live* on every
    /// invocation, filtering tombstones and keys flagged
    /// [`RLookupKeyFlag::Hidden`]. The
    /// per-call walk is required because an upstream `LOAD *` may append
    /// keys mid-pipeline; caching the iteration result would silently lose
    /// them. This mirrors the per-row `RLOOKUP_FOREACH` pattern in
    /// `aggregate_exec.c`'s wildcard reply path.
    ///
    /// The borrow lives for the reducer's full lifetime; both the source
    /// [`RLookup`] and the reducer outlive every per-group
    /// [`RemoteCollectCtx`].
    srclookup: Option<&'a RLookup<'a>>,
    /// Raw sort-key references, including keys not present in `FIELDS`.
    sort_keys: Box<[&'a RLookupKey<'a>]>,
    /// `true` for shard replies dispatched by the coordinator: extra sort-key
    /// columns are emitted alongside the requested fields so the coordinator
    /// can re-order shard rows during merge. `false` for direct execution.
    /// No-op in wildcard mode — the [`srclookup`][Self::srclookup] live walk
    /// emits whatever is currently in the lookup regardless of this flag,
    /// since sort keys are always registered in the source lookup at parse
    /// time.
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
///
/// The `<'a>` parameter ties the row's slot indices back to the source
/// [`RLookup`]: every value sits at the slot keyed by
/// `dstidx` on its [`RLookupKey`]. Stored values are owned [`SharedValue`]
/// clones; no row data borrows from the source lookup.
pub struct RemoteCollectCtx<'a> {
    rows: Vec<RLookupRow<'a>>,
}

impl<'a> RemoteCollectReducer<'a> {
    /// Create a reducer from C-parsed configuration.
    ///
    /// `srclookup` is [`Some`] when the user wrote `FIELDS *`;
    /// see [`Self::srclookup`] for the wildcard-mode policy. The borrow ties
    /// the reducer to the request's source lookup, whose stable address is
    /// guaranteed by the parser holding `options->srclookup` for the
    /// reducer's entire lifetime.
    #[expect(
        rustdoc::private_intra_doc_links,
        reason = "links to the private srclookup field per docs guidelines"
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

    pub const fn has_wildcard(&self) -> bool {
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

impl<'a> RemoteCollectCtx<'a> {
    pub const fn new(_r: &RemoteCollectReducer<'a>) -> Self {
        Self { rows: Vec::new() }
    }

    /// Project the source row's values into a stored [`RLookupRow`].
    ///
    /// Branches on `r.srclookup`:
    ///
    /// - [`None`]: iterates `r.field_keys` and `r.sort_keys`,
    ///   `clone`-projecting each present value. Sort-key values are stored
    ///   unconditionally; the `include_sort_keys` flag gates *emission* in
    ///   [`Self::finalize`], not storage here.
    /// - [`Some`]: follows the live-walk policy on
    ///   [`RemoteCollectReducer::srclookup`].
    #[expect(
        rustdoc::private_intra_doc_links,
        reason = "cross-type link to the private srclookup field per docs guidelines"
    )]
    pub fn add(&mut self, r: &RemoteCollectReducer<'a>, row: &RLookupRow<'_>) {
        let mut dst = RLookupRow::new();
        if let Some(lookup) = r.srclookup {
            let mut cursor = lookup.cursor();
            while let Some(key) = cursor.current() {
                if !key.is_tombstone()
                    && !key.flags.contains(RLookupKeyFlag::Hidden)
                    && let Some(v) = row.get(key)
                {
                    dst.write_key(key, v.clone());
                }
                cursor.move_next();
            }
        } else {
            for key in r.field_keys.iter() {
                if let Some(v) = row.get(key) {
                    dst.write_key(key, v.clone());
                }
            }
            for key in r.sort_keys.iter() {
                if let Some(v) = row.get(key) {
                    dst.write_key(key, v.clone());
                }
            }
        }
        self.rows.push(dst);
    }

    /// Serialize the buffered rows into a [`Map`][value::Map]-of-fields array.
    ///
    /// Branches on `r.srclookup`:
    ///
    /// - [`None`]: builds a name-allocation-hoisted,
    ///   dedup-by-`dstidx` template across `r.field_keys` and (when
    ///   `include_sort_keys`) `r.sort_keys`. Per-row, each template entry
    ///   emits `(name, value)` and falls back to
    ///   [`SharedValue::null_static`] for absent values, keeping the output
    ///   map shape uniform across rows.
    /// - [`Some`]: follows the live-walk policy on
    ///   [`RemoteCollectReducer::srclookup`]. Per-row, an entry is emitted
    ///   **only** if `row.get(key)` is [`Some`] — there is no
    ///   [`SharedValue::null_static`] padding for absent values, so two rows
    ///   in the same group may emit maps with different key sets. Dedup is
    ///   automatic since each key appears at most once in the lookup's
    ///   key list.
    #[expect(
        rustdoc::private_intra_doc_links,
        reason = "cross-type link to the private srclookup field per docs guidelines"
    )]
    pub fn finalize(&mut self, r: &RemoteCollectReducer<'a>) -> SharedValue {
        let rows = mem::take(&mut self.rows);

        if let Some(lookup) = r.srclookup {
            // Each row walks the lookup freshly. The cursor hands out
            // references borrowed from itself, so the explicit-fields path's
            // pre-built template (with hoisted name allocations) cannot be
            // expressed without unsafe — and hoisting matters less here
            // anyway, since the wildcard key set varies per row (a hoisted
            // name might not even be emitted for some rows).
            SharedValue::new_array(rows.into_iter().map(|row| {
                let mut entries: Vec<(SharedValue, SharedValue)> = Vec::new();
                let mut cursor = lookup.cursor();
                while let Some(key) = cursor.current() {
                    if !key.is_tombstone()
                        && !key.flags.contains(RLookupKeyFlag::Hidden)
                        && let Some(v) = row.get(key)
                    {
                        entries.push((
                            SharedValue::new_string(key.name().to_bytes().to_vec()),
                            v.clone(),
                        ));
                    }
                    cursor.move_next();
                }
                SharedValue::new_map(entries)
            }))
        } else {
            let sort_extras: &[&RLookupKey<'a>] = if r.include_sort_keys {
                &r.sort_keys
            } else {
                &[]
            };
            let mut seen: HashSet<u16> =
                HashSet::with_capacity(r.field_keys.len() + sort_extras.len());
            let template: Vec<(&RLookupKey<'a>, SharedValue)> = r
                .field_keys
                .iter()
                .chain(sort_extras)
                .filter(|key| seen.insert(key.dstidx))
                .map(|key| {
                    (
                        *key,
                        SharedValue::new_string(key.name().to_bytes().to_vec()),
                    )
                })
                .collect();

            SharedValue::new_array(rows.into_iter().map(|row| {
                let entries: Vec<_> = template
                    .iter()
                    .map(|(key, name)| {
                        let val = row
                            .get(key)
                            .cloned()
                            .unwrap_or_else(SharedValue::null_static);
                        (name.clone(), val)
                    })
                    .collect();
                SharedValue::new_map(entries)
            }))
        }
    }
}
