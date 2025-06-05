/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
/*
use result_processor::{counter::Counter, ffi};

/// Crate a new heap-allocated `Counter` result processor
///
/// # Safety
///
/// - The caller must never move the allocated result processor from its original allocation.
/// - The caller must ensure to call the `Free` VTable function to properly destroy the type.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RPCounter_New() -> *mut ffi::Header {
    let rp = Box::pin(ffi::ResultProcessor::new(Counter::new()));

    // Safety: The safety contract requires the caller to treat the returned pointer as pinned
    unsafe { ffi::ResultProcessor::into_ptr(rp) }.cast()
}
*/
