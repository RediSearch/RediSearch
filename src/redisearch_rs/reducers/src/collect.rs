/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use bumpalo::Bump;

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
pub struct CollectReducer {
    reducer: Reducer,
    /// Arena allocator for [`CollectCtx`] instances, matching the `BlkAlloc`
    /// pattern used by C reducers. All instances are freed at once when the
    /// reducer is dropped.
    arena: Bump,
    /// Projected field keys. Empty when only a wildcard is used.
    field_keys: Vec<*const ffi::RLookupKey>,
    /// Whether the wildcard `*` was specified in the FIELDS clause.
    has_wildcard: bool,
    /// Sort keys for in-group ordering. Empty when SORTBY is omitted.
    sort_keys: Vec<*const ffi::RLookupKey>,
    /// Bitmask where bit `i` is 0 for DESC and 1 for ASC (matching
    /// `SORTASCMAP_INIT`). Only meaningful for the first
    /// `sort_keys.len()` bits.
    sort_asc_map: u64,
    /// Whether a LIMIT clause was specified.
    has_limit: bool,
    /// Number of rows to skip (only meaningful when `has_limit` is true).
    limit_offset: u64,
    /// Maximum number of rows to return (only meaningful when `has_limit` is
    /// true).
    limit_count: u64,
}

/// Per-group instance of the [`CollectReducer`].
///
/// TODO: This is a placeholder. The actual per-group collection logic (field
/// projection, sorting, limiting) will be implemented in a follow-up task.
pub struct CollectCtx {
    _private: (),
}

impl CollectReducer {
    /// Create a new `CollectReducer` with the given pre-parsed configuration.
    ///
    /// # Safety
    ///
    /// Every pointer in `field_keys` and `sort_keys` must remain [valid] for
    /// the lifetime of this reducer (guaranteed by the `RLookup` infrastructure).
    ///
    /// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
    pub unsafe fn new(
        field_keys: Vec<*const ffi::RLookupKey>,
        has_wildcard: bool,
        sort_keys: Vec<*const ffi::RLookupKey>,
        sort_asc_map: u64,
        has_limit: bool,
        limit_offset: u64,
        limit_count: u64,
    ) -> Self {
        Self {
            reducer: Reducer::new(),
            arena: Bump::new(),
            field_keys,
            has_wildcard,
            sort_keys,
            sort_asc_map,
            has_limit,
            limit_offset,
            limit_count,
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
}

impl CollectReducer {
    /// Number of explicitly listed field keys (excludes the wildcard).
    pub fn field_keys_len(&self) -> usize {
        self.field_keys.len()
    }

    /// Whether the wildcard `*` was specified in the FIELDS clause.
    pub fn has_wildcard(&self) -> bool {
        self.has_wildcard
    }

    /// Number of sort keys.
    pub fn sort_keys_len(&self) -> usize {
        self.sort_keys.len()
    }

    /// The ASC/DESC bitmask for sort keys.
    pub fn sort_asc_map(&self) -> u64 {
        self.sort_asc_map
    }

    /// Whether a LIMIT clause was specified.
    pub fn has_limit(&self) -> bool {
        self.has_limit
    }

    /// The LIMIT offset value.
    pub fn limit_offset(&self) -> u64 {
        self.limit_offset
    }

    /// The LIMIT count value.
    pub fn limit_count(&self) -> u64 {
        self.limit_count
    }
}

impl CollectCtx {
    /// Create a new per-group collect reducer instance.
    pub const fn new(_r: &CollectReducer) -> Self {
        Self { _private: () }
    }

    /// Process the provided [`ffi::RLookupRow`] with the collect reducer
    /// instance.
    ///
    /// TODO: Implement field projection and row collection.
    pub fn add(&mut self, _r: &CollectReducer, _srcrow: *const ffi::RLookupRow) {
        unimplemented!("COLLECT reducer add is not yet implemented");
    }

    /// Finalize the collect reducer instance result into an [`RSValueFFI`].
    ///
    /// TODO: Implement sorting, limiting, and result construction.
    pub fn finalize(&self, _r: &CollectReducer) -> RSValueFFI {
        unimplemented!("COLLECT reducer finalize is not yet implemented");
    }

    /// Free the per-group collect reducer instance resources.
    pub fn free(&mut self, _r: &CollectReducer) {}
}
