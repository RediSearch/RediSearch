/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Integration tests for `TermDict_GetNumDocs`.
//!
//! Backs the `Trie_GetNode(...)->numDocs` reads in `src/query.c`. The C
//! path collapses "node missing" into a zero count via
//! `trienode ? trienode->numDocs : 0`; the FFI does the same, plus
//! mirrors that collapse for NULL `d` and non-UTF-8 `term`.

use std::ffi::c_char;
use std::ptr;

use libc::size_t;
use term_dict_ffi::*;

unsafe fn add(d: *mut TermDict, term: &[u8], num_docs: size_t) {
    // SAFETY: `d` is a live `TermDict`; `term` is a borrowed byte slice.
    let _ = unsafe {
        TermDict_AddTerm(
            d,
            term.as_ptr().cast::<c_char>(),
            term.len() as size_t,
            1.0,
            num_docs,
        )
    };
}

unsafe fn get(d: *const TermDict, term: &[u8]) -> size_t {
    // SAFETY: `d` is a live `TermDict` or NULL; `term` is a borrowed slice.
    unsafe { TermDict_GetNumDocs(d, term.as_ptr().cast::<c_char>(), term.len() as size_t) }
}

#[test]
fn null_dict_returns_zero() {
    // SAFETY: NULL is explicitly allowed by the FFI contract.
    let n = unsafe { get(ptr::null(), b"alpha") };
    assert_eq!(n, 0, "NULL must collapse to zero docs");
}

#[test]
fn missing_term_returns_zero() {
    let d = TermDict_New();
    // SAFETY: `d` is a live `TermDict`.
    let n = unsafe { get(d, b"never-inserted") };
    assert_eq!(n, 0);
    // SAFETY: `d` is a live pointer.
    unsafe { TermDict_Free(d) };
}

#[test]
fn present_term_returns_count() {
    let d = TermDict_New();
    // SAFETY: `d` is a live `TermDict`.
    unsafe { add(d, b"alpha", 7) };
    // SAFETY: `d` is a live `TermDict`.
    let n = unsafe { get(d, b"alpha") };
    assert_eq!(n, 7);
    // SAFETY: `d` is a live pointer.
    unsafe { TermDict_Free(d) };
}

#[test]
fn accumulated_count_after_multiple_adds() {
    let d = TermDict_New();
    // SAFETY: `d` is a live `TermDict`.
    unsafe { add(d, b"alpha", 3) };
    // SAFETY: `d` is a live `TermDict`.
    unsafe { add(d, b"alpha", 4) };
    // ADD_INCR accumulates per `TermDict_AddTerm`'s contract.
    // SAFETY: `d` is a live `TermDict`.
    let n = unsafe { get(d, b"alpha") };
    assert_eq!(n, 7);
    // SAFETY: `d` is a live pointer.
    unsafe { TermDict_Free(d) };
}

#[test]
fn case_insensitive_lookup() {
    let d = TermDict_New();
    // SAFETY: `d` is a live `TermDict`.
    unsafe { add(d, b"Alpha", 5) };
    // Lookup with a different case must hit the folded entry — production
    // query paths receive lowercased terms via `runeBufFill`, but the
    // case-folding contract is worth pinning explicitly.
    // SAFETY: `d` is a live `TermDict`.
    let n = unsafe { get(d, b"ALPHA") };
    assert_eq!(n, 5);
    // SAFETY: `d` is a live pointer.
    unsafe { TermDict_Free(d) };
}

#[test]
fn invalid_utf8_returns_zero() {
    let d = TermDict_New();
    // SAFETY: `d` is a live `TermDict`.
    let n = unsafe { get(d, &[0xFFu8]) };
    assert_eq!(n, 0, "non-UTF-8 must collapse to zero docs");
    // SAFETY: `d` is a live pointer.
    unsafe { TermDict_Free(d) };
}
