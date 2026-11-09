/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! C ABI entrypoint for the in-process GC driver.

use index_gc::{GcError, stats::GcCounters, thread::run_cycle};
use index_spec::IndexSpecWeakRef;
use tracing::Level;
use tracing_log_error::log_error;

/// How an in-process GC cycle ended.
#[cheadergen::config(export)]
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThreadGCResult {
    /// The cycle ran to the end.
    #[cheadergen(rename = "THREAD_GC_DONE")]
    Done = 0,
    /// The index was dropped during the cycle.
    #[cheadergen(rename = "THREAD_GC_SPEC_DELETED")]
    SpecDeleted = 1,
    /// Any other failure.
    #[cheadergen(rename = "THREAD_GC_FAILED")]
    Failed = 2,
}

/// Run every GC scanner over `gc`'s index on the calling thread, under the
/// spec's read/write lock, a bounded number of keys per hold.
///
/// Returns how the cycle ended; see [`ThreadGCResult`]. Stats of the chunks applied
/// before an early end are kept.
///
/// # Safety
///
/// 1. `gc` must point to a valid [`ffi::IndexGC`] on which no other GC cycle runs
///    for the duration of this call. The main thread may update its deletion
///    counter and read its stats meanwhile, so this call borrows no part of it.
/// 2. The calling thread must not hold any spec lock.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ThreadGC_runScanners(gc: *mut ffi::IndexGC) -> ThreadGCResult {
    debug_assert!(!gc.is_null(), "ThreadGC_runScanners needs a GC");
    // SAFETY: (1) `gc` is valid, and the field is read in place, without a reference
    // to the struct.
    let index = unsafe { (*gc).index };
    // SAFETY: `index` is a valid WeakRef for as long as the GC exists.
    let mut spec = unsafe { IndexSpecWeakRef::from_raw(index) };
    // SAFETY: as for `index`.
    let clean_numeric_empty_nodes = unsafe { (*gc).cleanNumericEmptyNodes } != 0;
    // SAFETY: (1) `gc` is valid; this only takes the counters' address.
    let counters = unsafe { &raw mut (*gc).stats };
    // SAFETY: `GcCounters::new`'s 1. the counters are valid for the call, and 2.
    // the cycle records into them only under the spec's write lock.
    let mut stats = unsafe { GcCounters::new(counters) };

    match run_cycle(&mut spec, &mut stats, clean_numeric_empty_nodes) {
        Ok(report) => {
            tracing::debug!(
                read_locks = report.read_locks,
                write_locks = report.write_locks,
                max_read_hold_us = report.max_read_hold.as_micros() as u64,
                max_write_hold_us = report.max_write_hold.as_micros() as u64,
                "thread GC cycle done"
            );
            ThreadGCResult::Done
        }
        Err((scanner, GcError::SpecDeleted)) => {
            tracing::debug!("thread GC: {scanner}: index spec deleted");
            ThreadGCResult::SpecDeleted
        }
        Err((scanner, error)) => {
            log_error!(error, level: Level::WARN, "thread GC: {scanner}: cycle failed");
            ThreadGCResult::Failed
        }
    }
}
