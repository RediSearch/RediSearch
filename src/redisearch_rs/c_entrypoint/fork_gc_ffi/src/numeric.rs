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
    numeric::{collect_numeric, handle_numeric},
};
use index_spec::IndexSpecReadGuard;

use crate::{FGCError, util::into_fgc_error};

/// Collect GC delta data for every numeric and geo field in the spec and send
/// it to the parent process over the pipe.
///
/// For each NUMERIC or GEO field whose tree has been initialised, sends the
/// field name and unique ID as a header, followed by one entry per tree node
/// with GC work, then a per-field terminator. A final terminator is sent once
/// all fields have been processed.
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
pub unsafe extern "C" fn FGC_childCollectNumeric(
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

    collect_numeric(&mut fgc.writer(), &guard).unwrap_or_exit();
}

/// Receive and apply the GC deltas for one numeric or geo field.
///
/// Reads a field header from the pipe followed by that field's per-node
/// deltas, applying each to the field's numeric tree under the write lock and
/// updating statistics. Returns [`FGCError::Done`] when the child sent the
/// global terminator instead of a field header (all fields processed),
/// [`FGCError::Collected`] after a field's deltas were applied, or an error
/// variant on pipe, spec, or tree-lookup failure.
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
pub unsafe extern "C" fn FGC_parentHandleNumeric(gc: *mut ffi::ForkGC) -> FGCError {
    // SAFETY: caller guarantees (1).
    let fgc = unsafe { ForkGC::from_ptr_mut(gc) };

    into_fgc_error(handle_numeric(fgc), "numeric")
}
