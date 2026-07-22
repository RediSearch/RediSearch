/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use fork_gc::{
    ForkGC,
    io_result_ext::IoResultExt,
    missing_docs::{HandleError, HandleOutcome, collect_missing_docs, handle_missing_docs},
};
use index_spec::IndexSpecReadGuard;

use crate::FGCError;

/// Collect GC delta data for every entry in the spec's `missingFieldDict` and
/// send it to the parent process over the pipe.
///
/// Iterates the `missingFieldDict`, and for each entry with a non-null
/// `InvertedIndex` calls `scan_gc` which sends the field name header
/// followed by the serialised GC delta. Sends a terminator once all
/// entries are processed.
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
pub unsafe extern "C" fn FGC_childCollectMissingDocs(
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

    collect_missing_docs(&mut fgc.writer(), &guard).unwrap_or_exit();
}

/// Receive and apply the GC delta for one field in the spec's `missingFieldDict`.
///
/// Reads one protocol frame from the pipe. Returns [`FGCError::Done`] when the
/// child sent a terminator (all fields processed), [`FGCError::Collected`] after
/// successfully applying a delta, or an error variant on pipe or spec failure.
///
/// Called in a loop (via `COLLECT_FROM_CHILD`) until it returns something other
/// than [`FGCError::Collected`].
///
/// # Panic
///
/// Panics if `pipe_write_fd` on `gc` is an invalid or closed writable file descriptor.
///
/// # Safety
///
/// 1. `gc` must point to a valid [`ffi::ForkGC`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn FGC_parentHandleMissingDocs(gc: *mut ffi::ForkGC) -> FGCError {
    // SAFETY: caller guarantees (1).
    let fgc = unsafe { ForkGC::from_ptr_mut(gc) };

    match handle_missing_docs(fgc) {
        Ok(HandleOutcome::Collected) => FGCError::Collected,
        Ok(HandleOutcome::Done) => FGCError::Done,
        Err(HandleError::PipeReadError(_)) => FGCError::ChildError,
        Err(HandleError::DeserializationFailed(_)) => FGCError::ChildError,
        Err(HandleError::UnexpectedFrame) => FGCError::ChildError,
        Err(HandleError::SpecDeleted) => FGCError::SpecDeleted,
        Err(HandleError::FieldNotFound) => FGCError::ParentError,
    }
}
