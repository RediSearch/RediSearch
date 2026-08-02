/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Shared helpers for the query_eval integration tests.

/// A borrowed (pointer, length) view of `s`, as consumed by the id-filter
/// node. `'static` keeps the borrow trivially valid for the test's lifetime.
pub fn key_view(s: &'static str) -> ffi::RSStringView {
    ffi::RSStringView {
        data: s.as_ptr().cast(),
        len: s.len(),
    }
}

/// A NULL key view, for tests that never read the keys (pre-resolved doc ids).
pub fn null_view() -> ffi::RSStringView {
    ffi::RSStringView {
        data: std::ptr::null(),
        len: 0,
    }
}
