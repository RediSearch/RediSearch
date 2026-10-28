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
/// `docs` is null, or a live set from [`DeletedDocs_New`] that this call consumes:
/// it has not been freed before, no other reference to it is live, and it is not
/// used afterwards.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn DeletedDocs_Free(docs: *mut DeletedDocs) {
    if !docs.is_null() {
        // SAFETY: the caller guarantees `docs` is a live, exclusively owned allocation from
        // `Box::into_raw` in `DeletedDocs_New`, freed exactly once, here.
        drop(unsafe { Box::from_raw(docs) });
    }
}

/// Record `id` as deleted. Called under the spec write lock.
///
/// # Safety
///
/// `docs` came from [`DeletedDocs_New`], is not freed, and no other reference to it is live.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn DeletedDocs_Record(docs: *mut DeletedDocs, id: DocId) {
    debug_assert!(!docs.is_null(), "DeletedDocs_Record needs a set");
    // SAFETY: the caller guarantees `docs` is a live, uniquely referenced set.
    unsafe { &mut *docs }.record(id);
}
