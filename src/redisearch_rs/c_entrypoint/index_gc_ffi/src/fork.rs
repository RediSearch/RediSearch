/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! C ABI entrypoints for the Fork GC child and parent orchestration.

use index_gc::{
    DocExistence, ForkPipe, GcError, HandleOutcome,
    fork::cycle::{collect_scanners, handle_scanners},
};
use index_spec::IndexSpecReadGuard;
use tracing::Level;
use tracing_log_error::log_error;

use crate::ForkGCError;

/// Scan every Fork GC index kind and send its deltas to the parent process.
///
/// # Safety
///
/// 1. `gc` must point to a [valid] [`ffi::IndexGC`], with no other reference to it
///    alive for the duration of this call.
/// 2. `spec` must be a non-null pointer to a [valid] [`ffi::IndexSpec`].
/// 3. This function should only be called when it has exclusive access to the [`ffi::IndexSpec`].
/// 4. This function must only be called in a forked GC child, where the spec's
///    document table never changes again: the scanners answer existence checks
///    from a [`DocExistence::snapshot`] of it.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ForkGC_childScanIndexes(gc: *mut ffi::IndexGC, spec: *mut ffi::IndexSpec) {
    // SAFETY: caller guarantees (1).
    let fgc = unsafe { ForkPipe::from_ptr_mut(gc) };
    // SAFETY: caller guarantees (2).
    let spec = unsafe { &*spec };
    // SAFETY: caller guarantees (3). We don't actually hold a read lock, but when the Fork GC code
    // runs it holds the Redis GIL (so no other thread would be touching any shared Redis state),
    // then forks and the child has only one thread with exclusive access to the index spec.
    let guard = unsafe { IndexSpecReadGuard::from_locked(spec) };

    // Taken once for all scanners: (4) keeps it right for the whole scan.
    let docs = DocExistence::snapshot(&guard);
    collect_scanners(fgc, &guard, &docs);
}

/// Consume every Fork GC scanner stream and apply its deltas in the parent.
///
/// # Safety
///
/// 1. `gc` must point to a [valid] [`ffi::IndexGC`], with no other reference to it
///    alive for the duration of this call.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ForkGC_parentHandleFromChild(gc: *mut ffi::IndexGC) -> ForkGCError {
    // SAFETY: caller guarantees (1).
    let fgc = unsafe { ForkPipe::from_ptr_mut(gc) };

    match fgc.with_read_ahead(handle_scanners) {
        Ok(HandleOutcome::Collected) => ForkGCError::Collected,
        Ok(HandleOutcome::Done) => ForkGCError::Done,
        Err((scanner, error @ GcError::Codec { .. })) => {
            log_error!(error, level: Level::WARN, "ForkPipe: {scanner}: codec error");
            ForkGCError::ChildError
        }
        Err((scanner, error @ GcError::SpecDeleted)) => {
            log_error!(error, level: Level::WARN, "ForkPipe: {scanner}: index spec deleted");
            ForkGCError::SpecDeleted
        }
        Err((scanner, error @ GcError::ApplyError(_))) => {
            log_error!(error, level: Level::WARN, "ForkPipe: {scanner}: apply error");
            ForkGCError::ParentError
        }
    }
}
