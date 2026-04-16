/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ops::Deref;

use bumpalo::Bump;
use rlookup::{RLookupKey, RLookupRow};

use crate::Reducer;
use value::{Array, Map, RsValue, SharedRsValue};

/// The COLLECT reducer aggregates rows within each group, with optional field
/// projection, sorting, and limiting.
///
/// Configuration is parsed in C and passed to Rust via
/// [`CollectReducer::new_shard`] or [`CollectReducer::new_coord`], depending
/// on whether the reducer runs on a shard or on the coordinator. The
/// [`CollectMode`] enum encodes this distinction at the type level, avoiding
/// runtime `is_internal` checks throughout `add` / `finalize`.
///
/// This struct must be `#[repr(C)]` and its first field must be a [`Reducer`]
/// because it is downcast in C to `ffi::Reducer`, which reads vtable pointers
/// directly.
#[repr(C)]
pub struct CollectReducer<'a> {
    reducer: Reducer,
    /// Arena allocator for [`CollectCtx`] instances, matching the `BlkAlloc`
    /// pattern used by C reducers. All instances are freed at once when the
    /// reducer is dropped.
    arena: Bump,
    /// Bitmask where bit `i` is 0 for DESC and 1 for ASC (matching
    /// `SORTASCMAP_INIT`). Only meaningful for the first N sort-key bits.
    sort_asc_map: u64,
    /// Optional LIMIT clause: `(offset, count)`.
    limit: Option<(u64, u64)>,
    /// Shard-vs-coordinator configuration. Determines which data path
    /// [`CollectCtx::add`] and [`CollectCtx::finalize`] take.
    mode: CollectMode<'a>,
}

/// Shard-vs-coordinator configuration for a [`CollectReducer`].
///
/// In **shard** mode the reducer projects fields from each
/// [`RLookupRow`] via resolved [`RLookupKey`] references.
///
/// In **coordinator** mode the reducer unpacks an already-finalized
/// `Array<Map>` produced by a shard COLLECT. Field and sort key names
/// are stored as raw byte slices because the corresponding
/// [`RLookupKey`]s do not exist in the coordinator's lookup table.
#[expect(dead_code, reason = "field_names / sort_key_names prepared for follow-up sorting/limiting work")]
#[allow(rustdoc::private_intra_doc_links)]
enum CollectMode<'a> {
    Shard {
        /// Projected field keys for O(1) [`RLookupRow`] access.
        field_keys: Box<[&'a RLookupKey<'a>]>,
        /// Pre-built string [`SharedRsValue`]s matching `field_keys`, used
        /// as Map keys when constructing output maps. Cloning these is a
        /// refcount bump, avoiding per-row string allocation.
        field_names: Box<[SharedRsValue]>,
        /// Whether the wildcard `*` was specified in the FIELDS clause.
        has_wildcard: bool,
        /// Sort keys for in-group ordering.
        sort_keys: Box<[&'a RLookupKey<'a>]>,
    },
    Coord {
        /// Lookup key pointing at the shard's pre-collected `Array<Map>` column.
        source_key: &'a RLookupKey<'a>,
        /// Raw field names for coordinator-side use in follow-up work.
        field_names: Box<[Box<[u8]>]>,
        /// Raw sort key names for `Map::get()` in follow-up sorting work.
        sort_key_names: Box<[Box<[u8]>]>,
    },
}

/// Per-group instance of the [`CollectReducer`].
///
/// Each call to [`CollectCtx::add`] projects the configured field keys from
/// the source row and stores the cloned values. [`CollectCtx::finalize`]
/// serializes all collected rows as an array of maps.
///
/// Because `CollectCtx` is arena-allocated ([`Bump`] does not run destructors),
/// [`CollectCtx::free`] must be called to release the heap-allocated `Vec`s
/// and decrement `SharedRsValue` refcounts.
pub struct CollectCtx {
    /// Rows collected in standalone (shard) mode: each inner `Vec` holds one
    /// projected value per field key.
    rows: Vec<Vec<SharedRsValue>>,
    /// Pre-collected maps from shard COLLECT output, used in coordinator mode.
    pre_collected: Vec<SharedRsValue>,
}

impl<'a> CollectReducer<'a> {
    /// Create a shard-mode `CollectReducer`.
    ///
    /// `field_keys` and `sort_keys` are resolved [`RLookupKey`] references
    /// from the shard's lookup table. `field_names` are pre-built from
    /// `field_keys[i].name()` for efficient Map key construction.
    pub fn new_shard(
        field_keys: Box<[&'a RLookupKey<'a>]>,
        has_wildcard: bool,
        sort_keys: Box<[&'a RLookupKey<'a>]>,
        sort_asc_map: u64,
        limit: Option<(u64, u64)>,
    ) -> Self {
        let field_names: Box<[SharedRsValue]> = field_keys
            .iter()
            .map(|key| SharedRsValue::new_string(key.name().to_bytes().to_vec()))
            .collect();
        Self {
            reducer: Reducer::new(),
            arena: Bump::new(),
            sort_asc_map,
            limit,
            mode: CollectMode::Shard {
                field_keys,
                field_names,
                has_wildcard,
                sort_keys,
            },
        }
    }

    /// Create a coordinator-mode `CollectReducer`.
    ///
    /// `source_key` is the resolved lookup key that holds the shard's
    /// pre-collected `Array<Map>` column. `field_names` and `sort_key_names`
    /// are raw byte slices because the corresponding [`RLookupKey`]s do not
    /// exist in the coordinator's lookup table.
    pub fn new_coord(
        source_key: &'a RLookupKey<'a>,
        field_names: Box<[Box<[u8]>]>,
        sort_key_names: Box<[Box<[u8]>]>,
        sort_asc_map: u64,
        limit: Option<(u64, u64)>,
    ) -> Self {
        Self {
            reducer: Reducer::new(),
            arena: Bump::new(),
            sort_asc_map,
            limit,
            mode: CollectMode::Coord {
                source_key,
                field_names,
                sort_key_names,
            },
        }
    }

    /// Get a mutable reference to the base reducer.
    pub const fn reducer_mut(&mut self) -> &mut Reducer {
        &mut self.reducer
    }

    /// Allocate a new [`CollectCtx`] instance from the arena.
    pub fn alloc_instance(&self) -> &mut CollectCtx {
        self.arena.alloc(CollectCtx::new(self))
    }

    // The accessors below exist only for the C++ parser tests
    // (`test_cpp_collect.cpp`) via `reducers_ffi`. They assume shard mode.
    // Remove them once those tests are migrated to Python flow tests.

    /// Number of explicitly listed field keys (shard mode only).
    pub fn field_keys_len(&self) -> usize {
        match &self.mode {
            CollectMode::Shard { field_keys, .. } => field_keys.len(),
            CollectMode::Coord { .. } => 0,
        }
    }

    /// Whether the wildcard `*` was specified in the FIELDS clause (shard mode only).
    pub fn has_wildcard(&self) -> bool {
        match &self.mode {
            CollectMode::Shard { has_wildcard, .. } => *has_wildcard,
            CollectMode::Coord { .. } => false,
        }
    }

    /// Number of sort keys (shard mode only; coord sort keys are stored as raw names).
    pub fn sort_keys_len(&self) -> usize {
        match &self.mode {
            CollectMode::Shard { sort_keys, .. } => sort_keys.len(),
            CollectMode::Coord { sort_key_names, .. } => sort_key_names.len(),
        }
    }

    /// The ASC/DESC bitmask for sort keys.
    pub const fn sort_asc_map(&self) -> u64 {
        self.sort_asc_map
    }

    /// Whether a LIMIT clause was specified.
    pub const fn has_limit(&self) -> bool {
        self.limit.is_some()
    }

    /// The LIMIT offset value (0 if no limit).
    pub const fn limit_offset(&self) -> u64 {
        match self.limit {
            Some((offset, _)) => offset,
            None => 0,
        }
    }

    /// The LIMIT count value (0 if no limit).
    pub const fn limit_count(&self) -> u64 {
        match self.limit {
            Some((_, count)) => count,
            None => 0,
        }
    }
}

impl CollectCtx {
    /// Create a new per-group collect reducer instance.
    pub const fn new(_r: &CollectReducer) -> Self {
        Self {
            rows: Vec::new(),
            pre_collected: Vec::new(),
        }
    }

    /// Project field values from `row` and store them for later
    /// serialization in [`Self::finalize`].
    ///
    /// In [`CollectMode::Coord`] the source key points at an
    /// already-finalized `Array<Map>` from a shard COLLECT; we unpack
    /// that array and store each map directly.
    ///
    /// In [`CollectMode::Shard`] each configured field key is looked up in
    /// the row and cloned (incrementing its refcount). Missing values are
    /// stored as [`SharedRsValue::null_static`].
    #[allow(rustdoc::private_intra_doc_links)]
    pub fn add(&mut self, r: &CollectReducer, row: &RLookupRow) {
        match &r.mode {
            CollectMode::Coord { source_key, .. } => {
                if let Some(shard_val) = row.get(source_key) {
                    if let RsValue::Array(arr) = shard_val.deref() {
                        self.pre_collected.extend(arr.iter().cloned());
                    }
                }
            }
            CollectMode::Shard { field_keys, .. } => {
                let mut values = Vec::with_capacity(field_keys.len());
                for key in field_keys.iter() {
                    let value = row
                        .get(key)
                        .cloned()
                        .unwrap_or_else(SharedRsValue::null_static);
                    values.push(value);
                }
                self.rows.push(values);
            }
        }
    }

    /// Serialize all collected rows as an array of maps.
    ///
    /// In [`CollectMode::Coord`] the maps were already built by the shard;
    /// we simply concatenate them into a single array.
    ///
    /// In [`CollectMode::Shard`] each map contains `{field_name: value}`
    /// entries keyed by the [`RLookupKey`] name. The outer array has one
    /// element per collected row.
    #[allow(rustdoc::private_intra_doc_links)]
    pub fn finalize(&self, r: &CollectReducer) -> SharedRsValue {
        match &r.mode {
            CollectMode::Coord { .. } => {
                let maps: Box<[SharedRsValue]> = self.pre_collected.iter().cloned().collect();
                SharedRsValue::new(RsValue::Array(Array::new(maps)))
            }
            CollectMode::Shard { field_keys, .. } => {
                let row_maps: Vec<SharedRsValue> = self
                    .rows
                    .iter()
                    .map(|row_values| {
                        let entries: Box<[_]> = row_values
                            .iter()
                            .zip(field_keys.iter())
                            .map(|(val, key)| {
                                let name_val =
                                    SharedRsValue::new_string(key.name().to_bytes().to_vec());
                                (name_val, val.clone())
                            })
                            .collect();
                        SharedRsValue::new(RsValue::Map(Map::new(entries)))
                    })
                    .collect();
                SharedRsValue::new(RsValue::Array(Array::new(row_maps.into_boxed_slice())))
            }
        }
    }

    /// Release heap-allocated storage and decrement `SharedRsValue` refcounts.
    ///
    /// Must be called before the arena drops this instance, since [`Bump`]
    /// does not run destructors.
    pub fn free(&mut self, _r: &CollectReducer) {
        drop(std::mem::take(&mut self.rows));
        drop(std::mem::take(&mut self.pre_collected));
    }
}
