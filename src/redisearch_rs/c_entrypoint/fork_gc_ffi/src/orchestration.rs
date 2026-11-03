/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! C ABI entrypoints for the Fork GC child and parent orchestration.

use fork_gc::{
    DocExistence, ForkGC, HandleError, HandleOutcome,
    orchestration::{collect_scanners, handle_scanners},
};
use index_spec::IndexSpecReadGuard;
use tracing::Level;
use tracing_log_error::log_error;

use crate::FGCError;
use std::ptr::NonNull;

/// Scan every Fork GC index kind and send its deltas to the parent process.
///
/// # Safety
///
/// 1. `gc` must point to a valid [`ffi::ForkGC`], with no other reference to it
///    alive for the duration of this call.
/// 2. `spec` must be a non-null pointer to a valid [`ffi::IndexSpec`].
/// 3. This function should only be called when it has exclusive access to the [`ffi::IndexSpec`].
/// 4. This function must only be called in a forked GC child, where the spec's
///    document table never changes again: the scanners answer existence checks
///    from a [`DocExistence::snapshot`] of it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn FGC_childScanIndexes(gc: *mut ffi::ForkGC, spec: *mut ffi::IndexSpec) {
    debug_assert!(!gc.is_null(), "`gc` must not be null");
    // SAFETY: caller guarantees (1), which implies `gc` is non-null.
    let fgc = unsafe { NonNull::new_unchecked(gc) };
    // SAFETY: caller guarantees (1).
    let fgc = unsafe { ForkGC::from_ptr_mut(fgc) };
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
/// 1. `gc` must point to a valid [`ffi::ForkGC`], with no other reference to it
///    alive for the duration of this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn FGC_parentHandleFromChild(gc: *mut ffi::ForkGC) -> FGCError {
    debug_assert!(!gc.is_null(), "`gc` must not be null");
    // SAFETY: caller guarantees (1), which implies `gc` is non-null.
    let fgc = unsafe { NonNull::new_unchecked(gc) };
    // SAFETY: caller guarantees (1).
    let fgc = unsafe { ForkGC::from_ptr_mut(fgc) };

    match handle_scanners(fgc) {
        Ok(HandleOutcome::Collected) => FGCError::Collected,
        Ok(HandleOutcome::Done) => FGCError::Done,
        Err((scanner, error @ HandleError::Codec { .. })) => {
            log_error!(error, level: Level::WARN, "ForkGC: {scanner}: codec error");
            FGCError::ChildError
        }
        Err((scanner, error @ HandleError::SpecDeleted)) => {
            log_error!(error, level: Level::WARN, "ForkGC: {scanner}: index spec deleted");
            FGCError::SpecDeleted
        }
        Err((scanner, error @ HandleError::ApplyError(_))) => {
            log_error!(error, level: Level::WARN, "ForkGC: {scanner}: apply error");
            FGCError::ParentError
        }
    }
}
