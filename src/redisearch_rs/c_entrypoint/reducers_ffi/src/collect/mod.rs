/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! FFI layer for shard and coordinator COLLECT reducers.

pub mod coord;
pub mod shard;

/// # Safety
///
/// `r` must point to a valid `Box<T>` that was created by the matching
/// `CollectReducer_Create*` factory and has not yet been freed.
pub(crate) unsafe fn collect_free_generic<T>(r: *mut ffi::Reducer) {
    // SAFETY: ensured by caller; `r` originates from `Box::into_raw` of a `Box<T>`
    // and is still owned by C, so we can reclaim it here.
    drop(unsafe { Box::from_raw(r.cast::<T>()) });
}
