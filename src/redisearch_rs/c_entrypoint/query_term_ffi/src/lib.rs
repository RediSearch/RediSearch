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
        return Box::into_raw(RSQueryTerm::new_null_str(id, tok_flags));
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

/// Get the IDF (inverse document frequency) value from a query term.
///
/// # Safety
///
/// `term` must be a valid, non-null pointer to an [`RSQueryTerm`] previously
/// allocated by [`NewQueryTerm`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn QueryTerm_GetIDF(term: *const RSQueryTerm) -> f64 {
    debug_assert!(!term.is_null(), "term cannot be NULL");
    // SAFETY: caller guarantees `term` is valid and non-null
    unsafe { (*term).idf() }
}

/// Get the BM25 IDF value from a query term.
///
/// # Safety
///
/// `term` must be a valid, non-null pointer to an [`RSQueryTerm`] previously
/// allocated by [`NewQueryTerm`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn QueryTerm_GetBM25_IDF(term: *const RSQueryTerm) -> f64 {
    debug_assert!(!term.is_null(), "term cannot be NULL");
    // SAFETY: caller guarantees `term` is valid and non-null
    unsafe { (*term).bm25_idf() }
}

/// Set both IDF values (TF-IDF and BM25) on a query term.
///
/// This is a convenience function for setting both values at once.
///
/// # Safety
///
/// `term` must be a valid, non-null pointer to an [`RSQueryTerm`] previously
/// allocated by [`NewQueryTerm`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn QueryTerm_SetIDFs(term: *mut RSQueryTerm, idf: f64, bm25_idf: f64) {
    debug_assert!(!term.is_null(), "term cannot be NULL");
    // SAFETY: caller guarantees `term` is valid and non-null
    unsafe { (*term).set_idf(idf) };
    // SAFETY: caller guarantees `term` is valid and non-null
    unsafe { (*term).set_bm25_idf(bm25_idf) };
}

/// Get the term ID.
///
/// Each term in the query gets an incremental ID assigned during parsing.
///
/// # Safety
///
/// `term` must be a valid, non-null pointer to an [`RSQueryTerm`] previously
/// allocated by [`NewQueryTerm`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn QueryTerm_GetID(term: *const RSQueryTerm) -> c_int {
    debug_assert!(!term.is_null(), "term cannot be NULL");
    // SAFETY: caller guarantees `term` is valid and non-null
    unsafe { (*term).id() }
}

/// Get the term string length in bytes (excluding null terminator).
///
/// # Safety
///
/// `term` must be a valid, non-null pointer to an [`RSQueryTerm`] previously
/// allocated by [`NewQueryTerm`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn QueryTerm_GetLen(term: *const RSQueryTerm) -> usize {
    debug_assert!(!term.is_null(), "term cannot be NULL");
    // SAFETY: caller guarantees `term` is valid and non-null
    unsafe { (*term).len() }
}

/// Get the string pointer from a query term.
///
/// Returns a pointer to the null-terminated byte string. The string may not be valid UTF-8.
///
/// # Safety
///
/// `term` must be valid and non-null. Returned pointer is valid for the lifetime of the term.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn QueryTerm_GetStr(term: *const RSQueryTerm) -> *const std::ffi::c_char {
    debug_assert!(!term.is_null(), "term cannot be NULL");
    // SAFETY: caller guarantees `term` is valid
    unsafe { (*term).str_ptr() }
}

/// Get both the string pointer and length from a query term.
///
/// This is useful for C code that needs to work with the byte slice directly.
/// The string may not be valid UTF-8.
///
/// # Safety
///
/// - `term` must be valid and non-null
/// - `out_len` must be a valid pointer to write the length to
#[unsafe(no_mangle)]
pub unsafe extern "C" fn QueryTerm_GetStrAndLen(
    term: *const RSQueryTerm,
    out_len: *mut usize,
) -> *const std::ffi::c_char {
    debug_assert!(!term.is_null(), "term cannot be NULL");
    debug_assert!(!out_len.is_null(), "out_len cannot be NULL");
    // SAFETY: caller guarantees `term` is valid and non-null
    let len = unsafe { (*term).len() };
    // SAFETY: caller guarantees `out_len` is valid and writable
    unsafe { *out_len = len };
    // SAFETY: caller guarantees `term` is valid and non-null
    unsafe { (*term).str_ptr() }
}
