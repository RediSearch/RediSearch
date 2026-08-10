/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Shared helpers for the query_eval integration tests.

/// Create an SDS string from `s`. The caller owns the result and must free
/// it with [`ffi::sdsfree`].
///
/// Gated out under Miri: [`ffi::sdsnewlen`] calls into the C library, which
/// Miri cannot execute.
#[cfg(not(miri))]
pub fn new_sds(s: &str) -> ffi::sds {
    // SAFETY: `s` points to `s.len()` valid bytes; `sdsnewlen` copies them
    // into a freshly allocated SDS string.
    unsafe { ffi::sdsnewlen(s.as_ptr().cast(), s.len()) }
}
