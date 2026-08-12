/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use result_processor::{ResultProcessorWrapper, pager::Pager};

/// Create a new heap-allocated `Pager` result processor. `offset` and `limit` are taken from the
/// user's `LIMIT` clause.
///
/// # Safety
///
/// - The caller must never move the allocated result processor from its original allocation.
/// - The caller must ensure to call the `Free` VTable function to properly destroy the type.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RPPager_New(offset: usize, limit: usize) -> *mut ffi::ResultProcessor {
    // The C pager stored both in `uint32_t` fields, truncating the same way.
    let rp = Box::pin(ResultProcessorWrapper::new(Pager::new(
        offset as u32,
        limit as u32,
    )));

    // Safety: The safety contract requires the caller to treat the returned pointer as pinned
    unsafe { ResultProcessorWrapper::into_ptr(rp) }
        .cast()
        .as_ptr()
}
