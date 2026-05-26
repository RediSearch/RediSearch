/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use fork_gc::{ForkGC, existing_docs::collect_existing_docs, io_result_ext::IoResultExt};
use index_spec::IndexSpecReadGuard;

/// Collect GC delta data for the spec's `existingDocs` inverted index and
/// send it to the parent process over the pipe.
///
/// If the spec has no existing-docs index, or the scan produces no delta,
/// only the terminator is sent.  Otherwise an empty header followed by the
/// serialised GC delta is sent before the terminator.
///
/// # Safety
///
/// 1. `gc` must point to a valid [`ffi::ForkGC`] whose `pipe_write_fd` is an open,
///    writable file descriptor.
/// 2. `sctx` must point to a valid [`ffi::RedisSearchCtx`] whose `spec` field is
///    a non-null `IndexSpec`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn FGC_childCollectExistingDocs(
    gc: *mut ffi::ForkGC,
    sctx: *mut ffi::RedisSearchCtx,
) {
    // SAFETY: caller guarantees (1).
    let fgc = unsafe { ForkGC::from_ptr_mut(gc) };

    // SAFETY: caller guarantees (2); sctx.spec is a valid non-null IndexSpec.
    // We don't actually hold a read lock, but when the Fork GC code runs it holds the Redis GIL
    // (so no other thread would be touching any shared Redis state), then forks and the child has
    // only one thread with exclusive access to the index spec.
    let guard = unsafe { IndexSpecReadGuard::from_locked(&*(*sctx).spec) };

    collect_existing_docs(&mut fgc.writer(), &*guard).unwrap_or_exit();
}
