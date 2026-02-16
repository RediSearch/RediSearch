/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! FFI bridge for [`query_term::RSQueryTerm`].
//!
//! Provides C-callable lifecycle functions (`NewQueryTerm`, `Term_Free`) and
//! generates the `query_term.h` header via cbindgen.

use std::ffi::c_int;
use std::ptr;

use query_term::RSQueryTerm;

/// Allocate a new [`RSQueryTerm`] from an [`RSToken`](ffi::RSToken).
///
/// The term string is copied into a Rust-owned allocation (`Box<[u8]>`).
/// The returned pointer must be freed with [`Term_Free`].
///
/// # Safety
///
/// - `tok` must point to a valid `RSToken` and cannot be NULL.
/// - `tok->str` may be NULL, in which case the resulting term will have a
///   NULL `str` field.
/// - If not NULL, tok->str should be a valid byte slice of tok->len bytes.
/// - The returned pointer is heap-allocated and must be freed with
///   [`Term_Free`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NewQueryTerm(tok: *const ffi::RSToken, id: c_int) -> *mut RSQueryTerm {
    debug_assert!(!tok.is_null(), "tok cannot be NULL");

    // SAFETY: caller guarantees `tok` is a valid, non-null `RSToken`.
    let tok = unsafe { &*tok };

    let tok_str = tok.str_;
    let tok_len = tok.len;
    let tok_flags = tok.flags();

    if tok_str.is_null() {
        let term = Box::new(RSQueryTerm {
            str_: ptr::null_mut(),
            len: 0,
            idf: 1.0,
            id,
            flags: tok_flags,
            bm25_idf: 0.0,
        });
        return Box::into_raw(term);
    }

    // SAFETY: caller guarantees `tok_str` is valid for `tok_len` bytes.
    let slice = unsafe { std::slice::from_raw_parts(tok_str as *const u8, tok_len) };
    Box::into_raw(RSQueryTerm::new(slice, id, tok_flags))
}

/// Free an [`RSQueryTerm`] previously allocated by [`NewQueryTerm`].
///
/// # Safety
///
/// - `t` may be NULL (in which case this is a no-op).
/// - If non-NULL, `t` must have been allocated by [`NewQueryTerm`].
/// - After this call, `t` is dangling and must not be used.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn Term_Free(t: *mut RSQueryTerm) {
    if t.is_null() {
        return;
    }

    // SAFETY: caller guarantees `t` was allocated by `NewQueryTerm`
    // (i.e. via `Box::into_raw`). `RSQueryTerm::Drop` frees the string.
    let _ = unsafe { Box::from_raw(t) };
}
