/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Integration tests for `TermDict_Delete`.
//!
//! Mirrors `Trie_Delete` in `src/trie/trie.c:89`, which backs the fork-GC
//! delete site at `src/fork_gc/terms.c:118`. The C path collapses
//! "term not present" and "internal rune-fill failure" into a single `0`
//! return; the Rust FFI does the same, so the boolean is "deleted" vs
//! "not deleted" rather than a finer-grained outcome.

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

unsafe fn del(d: *mut TermDict, term: &[u8]) -> bool {
    // SAFETY: `d` is a live `TermDict` or NULL; `term` is a borrowed slice.
    unsafe { TermDict_Delete(d, term.as_ptr().cast::<c_char>(), term.len() as size_t) }
}

#[test]
fn null_dict_returns_false() {
    // SAFETY: NULL is explicitly allowed by the FFI contract.
    let removed = unsafe { del(ptr::null_mut(), b"alpha") };
    assert!(!removed, "NULL dict must return false, not panic");
}

#[test]
fn missing_term_returns_false() {
    let d = TermDict_New();
    // SAFETY: `d` is a live `TermDict`.
    let removed = unsafe { del(d, b"never-inserted") };
    assert!(!removed);
    // SAFETY: `d` is a live pointer.
    unsafe { TermDict_Free(d) };
}

#[test]
fn existing_term_returns_true_and_shrinks_len() {
    let d = TermDict_New();
    // SAFETY: `d` is a live `TermDict`.
    unsafe { add(d, b"alpha", 1) };
    // SAFETY: `d` is a live `TermDict`.
    unsafe { add(d, b"beta", 1) };
    // SAFETY: `d` is a live `TermDict`.
    assert_eq!(unsafe { TermDict_Len(d) }, 2);

    // SAFETY: `d` is a live `TermDict`.
    let removed = unsafe { del(d, b"alpha") };
    assert!(removed);
    // SAFETY: `d` is a live `TermDict`.
    assert_eq!(unsafe { TermDict_Len(d) }, 1);

    // Second delete of the same term is a no-op miss.
    // SAFETY: `d` is a live `TermDict`.
    let removed_again = unsafe { del(d, b"alpha") };
    assert!(!removed_again);
    // SAFETY: `d` is a live `TermDict`.
    assert_eq!(unsafe { TermDict_Len(d) }, 1);

    // SAFETY: `d` is a live pointer.
    unsafe { TermDict_Free(d) };
}

#[test]
fn case_insensitive_lookup() {
    let d = TermDict_New();
    // SAFETY: `d` is a live `TermDict`.
    unsafe { add(d, b"Alpha", 1) };
    // Delete with a different case must hit the folded entry — production
    // GC paths receive lowercased terms, but the case-folding contract is
    // worth pinning explicitly so the FFI matches `TermDict_AddTerm`.
    // SAFETY: `d` is a live `TermDict`.
    let removed = unsafe { del(d, b"ALPHA") };
    assert!(removed);
    // SAFETY: `d` is a live `TermDict`.
    assert_eq!(unsafe { TermDict_Len(d) }, 0);
    // SAFETY: `d` is a live pointer.
    unsafe { TermDict_Free(d) };
}

#[test]
fn invalid_utf8_returns_false() {
    let d = TermDict_New();
    // SAFETY: `d` is a live `TermDict`.
    let removed = unsafe { del(d, &[0xFFu8]) };
    assert!(
        !removed,
        "non-UTF-8 must collapse to the C path's runeBufFill-failure miss"
    );
    // SAFETY: `d` is a live pointer.
    unsafe { TermDict_Free(d) };
}
