/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! This module contains pure Rust types that we want to expose to C code.

use inverted_index::{RSAggregateResult, RSIndexResult};

/// Append a child to an aggregate result.
///
/// Note, `parent` will not take ownership of the `child` and the caller is still responsible for
/// freeing the `child` correctly.
///
/// If the `parent` is also not an aggregate type, then this is a no-op.
///
/// # Safety
/// The following must be upheld by the caller when calling this function:
/// - `parent` must be a valid `RSIndexResult` instance created from calling [`AggregateResult_New`].
/// - `child` must be a valid `RSIndexResult` instance created from calling [`AggregateResult_New`].
#[unsafe(no_mangle)]
pub extern "C" fn AggregateResult_AddChild(parent: *mut RSIndexResult, child: *mut RSIndexResult) {
    debug_assert!(!parent.is_null(), "parent cannot be NULL");
    debug_assert!(!child.is_null(), "child cannot be NULL");

    // SAFETY: we just checked that `parent` is a valid pointer and the caller is to ensure it is a
    // valid `RSIndexResult`
    let parent = unsafe { &mut *parent };

    // SAFETY: we just checked that `child` is a valid pointer and the caller is to ensure it is a
    // valid `RSIndexResult`
    let child = unsafe { &mut *child };

    parent.push(child);
}

/// Create and allocate a new `RSAggregateResult` instance with the specified capacity.
///
/// To free the aggregate, use [`AggregateResult_Free`].
#[unsafe(no_mangle)]
pub extern "C" fn AggregateResult_New(cap: usize) -> *mut RSAggregateResult {
    let agg = RSAggregateResult::new(cap);
    let boxed_agg = Box::new(agg);

    Box::into_raw(boxed_agg)
}

/// Free the children array allocated for a `RSAggregateResult` which was previously created with [`AggregateResult_New`].
///
/// # Safety
/// `agg` should be a valid `RSAggregateResult` that was created with [`AggregateResult_New`].
#[unsafe(no_mangle)]
pub extern "C" fn AggregateResult_FreeChildren(agg: *mut RSAggregateResult) {
    if agg.is_null() {
        return;
    }

    // SAFETY: we just checked the pointer is not NULL, and it is up to the caller to ensure the
    // pointer is a valid `RSAggregateResult`
    let agg = unsafe { &mut *agg };
    agg.free_children();
}
