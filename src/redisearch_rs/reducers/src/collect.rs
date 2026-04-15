/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use bumpalo::Bump;
use rlookup::{RLookupKey, RLookupRow};

use crate::Reducer;
use value::RSValueFFI;

/// The COLLECT reducer aggregates rows within each group, with optional field
/// projection, sorting, and limiting.
///
/// Configuration (field keys, sort keys, limits) is parsed in C and passed to
/// Rust via [`CollectReducer::new`]. The [`RLookupKey`][ffi::RLookupKey]
/// pointers are borrowed from the [`RLookup`][ffi::RLookup] infrastructure and
/// outlive this reducer.
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
    /// Projected field keys. Empty when only a wildcard is used.
    field_keys: Box<[&'a RLookupKey<'a>]>,
    /// Whether the wildcard `*` was specified in the FIELDS clause.
    has_wildcard: bool,
    /// Sort keys for in-group ordering. Empty when SORTBY is omitted.
    sort_keys: Box<[&'a RLookupKey<'a>]>,
    /// Bitmask where bit `i` is 0 for DESC and 1 for ASC (matching
    /// `SORTASCMAP_INIT`). Only meaningful for the first
    /// `sort_keys.len()` bits.
    sort_asc_map: u64,
    /// Optional LIMIT clause: `(offset, count)`.
    limit: Option<(u64, u64)>,
}

/// Per-group instance of the [`CollectReducer`].
///
/// Each call to [`CollectCtx::add`] projects the configured field keys from
/// the source row and stores the cloned values. [`CollectCtx::finalize`]
/// serializes all collected rows as an array of maps.
///
/// Because `CollectCtx` is arena-allocated ([`Bump`] does not run destructors),
/// [`CollectCtx::free`] must be called to release the heap-allocated `Vec`s
/// and decrement `RSValueFFI` refcounts.
pub struct CollectCtx {
    rows: Vec<Vec<RSValueFFI>>,
}

impl<'a> CollectReducer<'a> {
    /// Create a new `CollectReducer` with the given pre-parsed configuration.
    ///
    /// The raw pointers in `field_keys` and `sort_keys` are stored but not
    /// dereferenced here; they are only dereferenced (unsafely) in
    /// [`CollectCtx::add`] and [`CollectCtx::finalize`].
    pub fn new(
        field_keys: Box<[&'a RLookupKey<'a>]>,
        has_wildcard: bool,
        sort_keys: Box<[&'a RLookupKey<'a>]>,
        sort_asc_map: u64,
        limit: Option<(u64, u64)>,
    ) -> Self {
        Self {
            reducer: Reducer::new(),
            arena: Bump::new(),
            field_keys: field_keys,
            has_wildcard,
            sort_keys: sort_keys,
            sort_asc_map,
            limit,
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
    // (`test_cpp_collect.cpp`) via `reducers_ffi`. Remove them once those
    // tests are migrated to Python flow tests.

    /// Number of explicitly listed field keys (excludes the wildcard).
    pub const fn field_keys_len(&self) -> usize {
        self.field_keys.len()
    }

    /// Whether the wildcard `*` was specified in the FIELDS clause.
    pub const fn has_wildcard(&self) -> bool {
        self.has_wildcard
    }

    /// Number of sort keys.
    pub const fn sort_keys_len(&self) -> usize {
        self.sort_keys.len()
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
        Self { rows: Vec::new() }
    }

    /// Project field values from `row` and store them for later
    /// serialization in [`Self::finalize`].
    ///
    /// For each configured field key the value is looked up in the row
    /// and cloned (incrementing its refcount). Missing values are stored as
    /// [`RSValueFFI::null_static`].
    pub fn add(&mut self, r: &CollectReducer, row: &RLookupRow) {
        let mut values = Vec::with_capacity(r.field_keys.len());
        for key in &r.field_keys {
            let value = row
                .get(key)
                .cloned()
                .unwrap_or_else(RSValueFFI::null_static);
            values.push(value);
        }
        self.rows.push(values);
    }

    /// Serialize all collected rows as an array of maps.
    ///
    /// Each map contains `{field_name: value}` entries keyed by the
    /// [`RLookupKey`] name. The outer array has one element per collected row.
    pub fn finalize(&self, r: &CollectReducer) -> RSValueFFI {
        let row_maps: Vec<RSValueFFI> = self
            .rows
            .iter()
            .map(|row_values| {
                let entries = row_values
                    .iter()
                    .zip(r.field_keys.iter())
                    .map(|(val, key)| {
                        let name_val = RSValueFFI::new_string(key.name().to_bytes().to_vec());
                        (name_val, val.clone())
                    });
                RSValueFFI::new_map(entries)
            })
            .collect();
        RSValueFFI::new_array(row_maps)
    }

    /// Release heap-allocated storage and decrement `RSValueFFI` refcounts.
    ///
    /// Must be called before the arena drops this instance, since [`Bump`]
    /// does not run destructors.
    pub fn free(&mut self, _r: &CollectReducer) {
        drop(std::mem::take(&mut self.rows));
    }
}
