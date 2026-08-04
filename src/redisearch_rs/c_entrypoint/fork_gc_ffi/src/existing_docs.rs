/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use fork_gc::{
    ForkGC, HandleError, HandleOutcome,
    existing_docs::{ExistingDocsDeleted, collect_existing_docs, handle_existing_docs},
    io_result_ext::IoResultExt,
};
use index_spec::IndexSpecReadGuard;

use crate::FGCError;

/// Collect GC delta data for the spec's `existingDocs` inverted index and
/// send it to the parent process over the pipe.
///
/// If the spec has no existing-docs index, or the scan produces no delta,
/// only the terminator is sent.  Otherwise an empty header followed by the
/// serialised GC delta is sent before the terminator.
///
/// # Panic
///
/// Panics if `pipe_write_fd` on `gc` is an invalid or closed writable file descriptor.
///
/// # Safety
///
/// 1. `gc` must point to a valid [`ffi::ForkGC`].
/// 2. `sctx` must point to a valid [`ffi::RedisSearchCtx`].
/// 3. `sctx.spec` must be a non-null pointer to a valid [`ffi::IndexSpec`].
/// 4. This function should only be called when it has exclusive access to the [`ffi::IndexSpec`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn FGC_childCollectExistingDocs(
    gc: *mut ffi::ForkGC,
    sctx: *mut ffi::RedisSearchCtx,
) {
    // SAFETY: caller guarantees (1).
    let fgc = unsafe { ForkGC::from_ptr_mut(gc) };
    // SAFETY: caller guarantees (2).
    let spec_ptr = unsafe { (*sctx).spec };
    // SAFETY: caller guarantees (3).
    let spec = unsafe { &*spec_ptr };

    // SAFETY: caller guarantees (4). We don't actually hold a read lock, but when the Fork GC code
    // runs it holds the Redis GIL (so no other thread would be touching any shared Redis state),
    // then forks and the child has only one thread with exclusive access to the index spec.
    let guard = unsafe { IndexSpecReadGuard::from_locked(spec) };

    collect_existing_docs(&mut fgc.writer(), &guard).unwrap_or_exit();
}

/// Receive and apply the GC delta for the spec's `existingDocs` inverted index.
///
/// Reads one protocol frame from the pipe. Returns [`FGCError::Done`] when
/// the child sent no data (index absent or nothing to collect),
/// [`FGCError::Collected`] after successfully applying a delta, or an
/// error variant on pipe or spec failure.
///
/// # Panic
///
/// Panics if `pipe_write_fd` on `gc` is an invalid or closed writable file descriptor.
///
/// # Safety
///
/// 1. `gc` must point to a valid [`ffi::ForkGC`], with no other reference to it
///    alive for the duration of this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn FGC_parentHandleExistingDocs(gc: *mut ffi::ForkGC) -> FGCError {
    // SAFETY: caller guarantees (1).
    let fgc = unsafe { ForkGC::from_ptr_mut(gc) };

    match handle_existing_docs(fgc) {
        Ok(HandleOutcome::Collected) => FGCError::Collected,
        Ok(HandleOutcome::Done) => FGCError::Done,
        Err(HandleError::Codec { .. }) => FGCError::ChildError,
        Err(HandleError::SpecDeleted) => FGCError::SpecDeleted,
        Err(HandleError::Custom(ExistingDocsDeleted)) => FGCError::ParentError,
    }
}
