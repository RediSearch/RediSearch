/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! C entry points for the doc table to own and feed a [`DeletedDocs`] set.
//! The GC reads and prunes the set through the `index_spec` wrappers, not here.

use std::ffi::c_void;

use deleted_docs::DeletedDocs;
use rqe_core::DocId;

/// A new, empty set. Free with [`DeletedDocs_Free`].
#[unsafe(no_mangle)]
pub extern "C" fn DeletedDocs_New() -> *mut DeletedDocs {
    Box::into_raw(Box::new(DeletedDocs::new()))
}

/// Free a set made by [`DeletedDocs_New`]. A null `docs` is a no-op.
///
/// # Safety
///
/// 1. `docs` is null, or came from [`DeletedDocs_New`] and has not been freed.
/// 2. No other reference to the set is live, and it is not used after this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn DeletedDocs_Free(docs: *mut DeletedDocs) {
    if !docs.is_null() {
        // SAFETY: (1) `docs` is a live allocation from `Box::into_raw` in
        // `DeletedDocs_New`; (2) nothing else refers to it, so it is freed exactly once.
        drop(unsafe { Box::from_raw(docs) });
    }
}

/// Record `id` as deleted. Called under the spec write lock.
///
/// # Safety
///
/// 1. `docs` came from [`DeletedDocs_New`] and has not been freed.
/// 2. No other reference to the set is live for the duration of the call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn DeletedDocs_Record(docs: *mut DeletedDocs, id: DocId) {
    debug_assert!(!docs.is_null(), "DeletedDocs_Record needs a set");
    // SAFETY: (1) `docs` points to a live set; (2) this is its only reference.
    unsafe { &mut *docs }.record(id);
}

/// Call `cb` with `ctx` once for every ID in the set, in increasing order. Called
/// under the spec read lock, so no ID is recorded or pruned meanwhile.
///
/// # Safety
///
/// 1. `docs` came from [`DeletedDocs_New`] and is not freed, and nothing changes
///    it during the call.
/// 2. `cb` is a non-null function that may be called with `ctx` any number of
///    times during the call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn DeletedDocs_ForEach(
    docs: *const DeletedDocs,
    cb: unsafe extern "C" fn(ctx: *mut c_void, id: DocId),
    ctx: *mut c_void,
) {
    debug_assert!(!docs.is_null(), "DeletedDocs_ForEach needs a set");
    // SAFETY: (1).
    for id in unsafe { &*docs }.iter() {
        // SAFETY: (2).
        unsafe { cb(ctx, id) };
    }
}

/// How many IDs the set holds.
///
/// # Safety
///
/// As for [`DeletedDocs_ForEach`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn DeletedDocs_Len(docs: *const DeletedDocs) -> u64 {
    debug_assert!(!docs.is_null(), "DeletedDocs_Len needs a set");
    // SAFETY: the caller guarantees `docs` is a live set nothing changes meanwhile.
    unsafe { &*docs }.len()
}

/// An estimate of the bytes the set holds; see [`DeletedDocs::memory_usage`].
///
/// # Safety
///
/// As for [`DeletedDocs_ForEach`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn DeletedDocs_MemoryUsage(docs: *const DeletedDocs) -> usize {
    debug_assert!(!docs.is_null(), "DeletedDocs_MemoryUsage needs a set");
    // SAFETY: the caller guarantees `docs` is a live set nothing changes meanwhile.
    unsafe { &*docs }.memory_usage()
}
