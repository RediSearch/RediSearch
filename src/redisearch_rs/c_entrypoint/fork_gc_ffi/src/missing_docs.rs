/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use fork_gc::{ForkGC, io_result_ext::IoResultExt, missing_docs::collect_missing_docs};
use index_spec::IndexSpecReadGuard;

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
