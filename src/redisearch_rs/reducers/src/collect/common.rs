/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Shared COLLECT reducer state.

use bumpalo::Bump;

use crate::Reducer;

/// Shared state embedded at offset 0 so C can view every COLLECT variant as
/// an `ffi::Reducer*`.
#[repr(C)]
pub struct CollectCommon {
    pub(super) reducer: Reducer,
    /// Arena for per-group contexts; destructors still need explicit calls.
    pub(super) arena: Bump,
    /// Bit `i` is 0 for DESC and 1 for ASC, matching `SORTASCMAP_INIT`.
    pub(super) sort_asc_map: u64,
    pub(super) limit: Option<(u64, u64)>,
}

impl CollectCommon {
    pub fn new(sort_asc_map: u64, limit: Option<(u64, u64)>) -> Self {
        Self {
            reducer: Reducer::new(),
            arena: Bump::new(),
            sort_asc_map,
            limit,
        }
    }
}
