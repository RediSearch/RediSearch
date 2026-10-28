/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! C ABI for the doc table's [`DeletedDocs`] set. The set is owned by the C
//! `DocTable`, which feeds and frees it through these functions; the in-process
//! GC creates it and reads it from Rust.

use deleted_docs::DeletedDocs;

/// Free the set the in-process GC boxed for a doc table. A null `docs` is a no-op.
///
/// # Safety
///
/// `docs` must be null or a pointer `Box::into_raw` produced for a
/// [`DeletedDocs`] (as `IndexSpecWriteGuard::ensure_deleted_docs` does), not
/// freed before.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn DeletedDocs_Free(docs: *mut DeletedDocs) {
    if !docs.is_null() {
        // SAFETY: the caller guarantees `docs` came from `Box::into_raw` and is freed once.
        drop(unsafe { Box::from_raw(docs) });
    }
}

/// Record `id` as deleted; see [`DeletedDocs::record`].
///
/// # Safety
///
/// `docs` must be the live pointer a doc table holds, with no other reference to
/// the set alive: the spec write lock provides that, since every reader borrows
/// it under the read lock.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn DeletedDocs_Record(docs: *mut DeletedDocs, id: u64) {
    debug_assert!(!docs.is_null(), "DeletedDocs_Record needs a set");
    // SAFETY: the caller guarantees `docs` is valid and exclusively accessed.
    unsafe { &mut *docs }.record(id);
}
