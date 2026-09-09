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
    terms::{collect_terms, handle_terms},
};
use index_spec::IndexSpecReadGuard;

use crate::{FGCError, util::into_fgc_error};

/// Collect GC delta data for every term in the spec's terms trie and send it
/// to the parent process over the pipe.
///
/// Walks the terms trie, and for each term with a non-null `InvertedIndex`
/// attempts a GC scan. When a scan produces a delta the term header (its raw
/// bytes) followed by the serialised GC delta is sent. Terms that produce no
/// delta or fail the scan are skipped. A terminator is sent once every term
/// has been processed.
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
pub unsafe extern "C" fn FGC_childCollectTerms(
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

    collect_terms(&mut fgc.writer(), &guard).unwrap_or_exit();
}

/// Receive and apply the GC delta for one term in the spec's terms trie.
///
/// Reads one protocol frame from the pipe. Returns [`FGCError::Collected`] after
/// successfully applying a delta, [`FGCError::Done`] when the child sent a
/// terminator (all terms processed), or an error variant on pipe or spec failure.
///
/// Called in a loop (via `COLLECT_FROM_CHILD`) until it returns something other
/// than [`FGCError::Collected`].
///
/// # Safety
///
/// 1. `gc` must point to a valid [`ffi::ForkGC`], with no other reference to it
///    alive for the duration of this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn FGC_parentHandleTerms(gc: *mut ffi::ForkGC) -> FGCError {
    // SAFETY: caller guarantees (1).
    let fgc = unsafe { ForkGC::from_ptr_mut(gc) };

    into_fgc_error(handle_terms(fgc), "terms")
}
