/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use fork_gc::{ForkGC, io_result_ext::IoResultExt, numeric::collect_numeric};
use index_spec::IndexSpecReadGuard;

/// Collect GC delta data for every numeric and geo field in the spec and send
/// it to the parent process over the pipe.
///
/// For each NUMERIC or GEO field whose tree has been initialised, sends the
/// field name and unique ID as a header, followed by one entry per tree node
/// with GC work, then a per-field terminator. A final terminator is sent once
/// all fields have been processed.
///
/// # Safety
///
/// 1. `gc` must point to a valid [`ffi::ForkGC`] whose `pipe_write_fd` is an
///    open, writable file descriptor.
/// 2. `sctx` must point to a valid [`ffi::RedisSearchCtx`] whose `spec` field
///    is a non-null, properly initialised `IndexSpec`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn FGC_childCollectNumeric(
    gc: *mut ffi::ForkGC,
    sctx: *mut ffi::RedisSearchCtx,
) {
    // SAFETY: caller guarantees (1).
    let fgc = unsafe { ForkGC::from_ptr_mut(gc) };

    // SAFETY: caller guarantees (2); sctx is a valid non-null pointer.
    let spec_ptr = unsafe { (*sctx).spec };
    // SAFETY: caller guarantees (2); sctx.spec is a valid non-null IndexSpec.
    let spec = unsafe { &*spec_ptr };

    // SAFETY: We don't actually hold a read lock, but when the Fork GC code runs it holds the
    // Redis GIL (so no other thread would be touching any shared Redis state), then forks and
    // the child has only one thread with exclusive access to the index spec.
    let guard = unsafe { IndexSpecReadGuard::from_locked(spec) };

    collect_numeric(&mut fgc.writer(), &guard).unwrap_or_exit();
}
