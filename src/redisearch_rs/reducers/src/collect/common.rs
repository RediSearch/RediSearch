/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! State shared by every COLLECT reducer variant (shard and coordinator).
//!
//! [`CollectCommon`] owns the base [`Reducer`] vtable, the per-group [`Bump`]
//! arena, and the SORTBY/LIMIT configuration. Both variants embed it as their
//! first field, so the C side can treat any `CollectReducer*` as the
//! underlying `ffi::Reducer*` regardless of mode.

use bumpalo::Bump;

use crate::Reducer;

/// Configuration and state shared by every COLLECT reducer variant.
///
/// Must remain `#[repr(C)]` and always sit at offset 0 of the embedding
/// struct: the C layer downcasts `CollectReducer*` to `ffi::Reducer*` by
/// reading the vtable through [`CollectCommon::reducer`].
#[repr(C)]
pub struct CollectCommon {
    /// Base reducer with its C-visible vtable.
    pub reducer: Reducer,
    /// Arena allocator for per-group `CollectCtx` instances, matching the
    /// `BlkAlloc` pattern used by C reducers. All instances are freed at
    /// once when the reducer is dropped.
    pub arena: Bump,
    /// Bitmask where bit `i` is 0 for DESC and 1 for ASC (matching
    /// `SORTASCMAP_INIT`). Only meaningful for the first `n` bits where `n`
    /// is the number of configured sort keys on the embedding struct.
    pub sort_asc_map: u64,
    /// Optional LIMIT clause: `(offset, count)`.
    pub limit: Option<(u64, u64)>,
}

impl CollectCommon {
    /// Create a new [`CollectCommon`] with a fresh [`Reducer`] vtable and arena.
    pub fn new(sort_asc_map: u64, limit: Option<(u64, u64)>) -> Self {
        Self {
            reducer: Reducer::new(),
            arena: Bump::new(),
            sort_asc_map,
            limit,
        }
    }
}
