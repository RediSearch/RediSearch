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
    tags::{collect_tags, handle_tags},
};
use index_spec::IndexSpecReadGuard;

use crate::{FGCError, util::into_fgc_error};

/// Collect GC delta data for every tag of every TAG field in the spec and send
/// it to the parent process over the pipe.
///
/// Walks each TAG field's tag index and, for every tag whose posting list has
/// GC work, sends the field name, the tag index's unique id, the tag, and the
/// serialised GC delta. Tags that produce no delta, and fields whose postings
/// live on disk, are skipped. A terminator is sent once every field has been
/// walked.
///
/// Any write failure, such as a closed fd or a broken pipe, terminates the
/// child process via `RedisModule_ExitFromChild`.
///
/// # Safety
///
/// 1. `gc` must point to a valid [`ffi::ForkGC`], with no other reference to it
///    alive for the duration of this call.
/// 2. `sctx` must point to a valid [`ffi::RedisSearchCtx`].
/// 3. `sctx.spec` must be a non-null pointer to a valid [`ffi::IndexSpec`].
/// 4. This function should only be called when it has exclusive access to the [`ffi::IndexSpec`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn FGC_childCollectTags(
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

    collect_tags(&mut fgc.writer(), &guard).unwrap_or_exit();
}

/// Receive and apply the GC delta for one tag value.
///
/// Reads one message from the pipe. Returns [`FGCError::Collected`] after
/// successfully applying a delta, [`FGCError::Done`] when the child sent the
/// terminator (all tags processed), or an error variant on pipe, spec, or
/// tag-index lookup failure.
///
/// Called in a loop (via `COLLECT_FROM_CHILD`) until it returns something other
/// than [`FGCError::Collected`].
///
/// # Safety
///
/// 1. `gc` must point to a valid [`ffi::ForkGC`], with no other reference to it
///    alive for the duration of this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn FGC_parentHandleTags(gc: *mut ffi::ForkGC) -> FGCError {
    // SAFETY: caller guarantees (1).
    let fgc = unsafe { ForkGC::from_ptr_mut(gc) };

    into_fgc_error(handle_tags(fgc), "tags")
}
