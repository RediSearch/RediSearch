/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! This module contains pure Rust types that we want to expose to C code.

pub use inverted_index::RSIndexResult;

/// Append a child to an aggregate result
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
