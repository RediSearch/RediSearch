/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! C ABI entrypoint for the in-process GC driver.

use index_gc::{ForkPipe, HandleError, thread::run_cycle};
use tracing::Level;
use tracing_log_error::log_error;

use crate::ForkGCError;

/// Run every GC scanner over `gc`'s index on the calling thread, under the
/// spec's read/write lock, a bounded number of keys per hold.
///
/// Returns [`ForkGCError::Done`] when the cycle completed,
/// [`ForkGCError::SpecDeleted`] when the index was dropped during it, and
/// [`ForkGCError::ParentError`] for any other failure. Stats of the chunks applied
/// before an early end are kept.
///
/// # Safety
///
/// 1. `gc` must point to a valid [`ffi::IndexGC`] that nothing else references for
///    the duration of this call: in particular no other GC cycle runs on it. The
///    main thread's deletion counter is an atomic the C side updates in place, not
///    a reference to the struct.
/// 2. The calling thread must not hold any spec lock.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ThreadGC_runScanners(gc: *mut ffi::IndexGC) -> ForkGCError {
    debug_assert!(!gc.is_null(), "ThreadGC_runScanners needs a GC");
    // SAFETY: caller guarantees (1).
    let fgc = unsafe { ForkPipe::from_ptr_mut(gc) };
    let mut spec = fgc.index_spec();
    let clean_numeric_empty_nodes = fgc.clean_numeric_empty_nodes();

    match run_cycle(&mut spec, fgc, clean_numeric_empty_nodes) {
        Ok(report) => {
            tracing::debug!(
                read_locks = report.read_locks,
                write_locks = report.write_locks,
                max_read_hold_us = report.max_read_hold.as_micros() as u64,
                max_write_hold_us = report.max_write_hold.as_micros() as u64,
                "thread GC cycle done"
            );
            ForkGCError::Done
        }
        Err((scanner, HandleError::SpecDeleted)) => {
            tracing::debug!("thread GC: {scanner}: index spec deleted");
            ForkGCError::SpecDeleted
        }
        Err((scanner, error)) => {
            log_error!(error, level: Level::WARN, "thread GC: {scanner}: cycle failed");
            ForkGCError::ParentError
        }
    }
}
