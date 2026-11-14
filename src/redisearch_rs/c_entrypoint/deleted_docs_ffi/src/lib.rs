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
