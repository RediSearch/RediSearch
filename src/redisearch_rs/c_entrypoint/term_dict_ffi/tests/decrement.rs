/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Integration tests for `TermDict_DecrementNumDocs` and the
//! `TermDictDecrResult` ABI mirror.
//!
//! Exercises the three branches read at `src/spec.c:4862-4864`
//! (the assert checks `!= NotFound`; the caller distinguishes `Deleted`
//! from `Updated`) plus the NULL contract from `src/trie/trie.c:132-135`.
//!
//! The ABI-mirror test pins the discriminants to `0, 1, 2` — reordering
//! variants in `term_dict_ffi/src/lib.rs` without updating the matching
//! `_Static_assert` in `spec.c` would silently miscast at runtime.

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

unsafe fn decr(d: *mut TermDict, term: &[u8], delta: size_t) -> TermDictDecrResult {
    // SAFETY: `d` is a live `TermDict` or NULL; `term` is a borrowed slice.
    unsafe {
        TermDict_DecrementNumDocs(
            d,
            term.as_ptr().cast::<c_char>(),
            term.len() as size_t,
            delta,
        )
    }
}

#[test]
fn null_dict_returns_not_found() {
    // SAFETY: NULL is explicitly allowed by the FFI contract.
    let r = unsafe { decr(ptr::null_mut(), b"alpha", 1) };
    assert!(
        matches!(r, TermDictDecrResult::NotFound),
        "NULL must mirror Trie_DecrementNumDocs(NULL, …) == TRIE_DECR_NOT_FOUND"
    );
}

#[test]
fn never_inserted_term_returns_not_found() {
    let d = TermDict_New();
    // SAFETY: `d` is a live `TermDict`.
    let r = unsafe { decr(d, b"alpha", 1) };
    assert!(matches!(r, TermDictDecrResult::NotFound));
    // SAFETY: `d` is a live `TermDict`.
    assert_eq!(unsafe { TermDict_Len(d) }, 0);
    // SAFETY: `d` is a live pointer.
    unsafe { TermDict_Free(d) };
}

#[test]
fn delta_less_than_count_returns_updated() {
    let d = TermDict_New();
    // SAFETY: `d` is a live `TermDict`.
    unsafe { add(d, b"alpha", 5) };
    // SAFETY: `d` is a live `TermDict`.
    let r = unsafe { decr(d, b"alpha", 2) };
    assert!(matches!(r, TermDictDecrResult::Updated));
    // Term must still exist — `Updated` means "decremented, > 0".
    // SAFETY: `d` is a live `TermDict`.
    assert_eq!(unsafe { TermDict_Len(d) }, 1);
    // SAFETY: `d` is a live pointer.
    unsafe { TermDict_Free(d) };
}

#[test]
fn delta_equal_to_count_returns_deleted() {
    let d = TermDict_New();
    // SAFETY: `d` is a live `TermDict`.
    unsafe { add(d, b"alpha", 3) };
    // SAFETY: `d` is a live `TermDict`.
    let r = unsafe { decr(d, b"alpha", 3) };
    assert!(matches!(r, TermDictDecrResult::Deleted));
    // SAFETY: `d` is a live `TermDict`.
    assert_eq!(
        unsafe { TermDict_Len(d) },
        0,
        "Deleted must remove the entry"
    );
    // SAFETY: `d` is a live pointer.
    unsafe { TermDict_Free(d) };
}

#[test]
fn delta_greater_than_count_returns_deleted() {
    let d = TermDict_New();
    // SAFETY: `d` is a live `TermDict`.
    unsafe { add(d, b"alpha", 2) };
    // SAFETY: `d` is a live `TermDict`.
    let r = unsafe { decr(d, b"alpha", 99) };
    assert!(
        matches!(r, TermDictDecrResult::Deleted),
        "delta >= num_docs must saturate to Deleted, not panic"
    );
    // SAFETY: `d` is a live `TermDict`.
    assert_eq!(unsafe { TermDict_Len(d) }, 0);
    // SAFETY: `d` is a live pointer.
    unsafe { TermDict_Free(d) };
}

#[test]
fn invalid_utf8_returns_not_found() {
    let d = TermDict_New();
    // SAFETY: `d` is a live `TermDict`.
    let r = unsafe { decr(d, &[0xFFu8], 1) };
    assert!(
        matches!(r, TermDictDecrResult::NotFound),
        "non-UTF-8 must mirror the C path's runeBufFill-failure NotFound"
    );
    // SAFETY: `d` is a live pointer.
    unsafe { TermDict_Free(d) };
}

#[test]
fn abi_mirror_discriminants() {
    // Pinning the C-side ABI. If these change, the matching
    // `_Static_assert` in `src/spec.c`'s `terms_decrement` wrapper will
    // also fire — both sides must move together.
    assert_eq!(TermDictDecrResult::NotFound as u32, 0);
    assert_eq!(TermDictDecrResult::Updated as u32, 1);
    assert_eq!(TermDictDecrResult::Deleted as u32, 2);
}
