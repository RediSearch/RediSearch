/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Integration tests for `TermDict_MemUsage` — the FFI shim feeding
//! `FT.INFO`'s per-spec terms-memory line.
//!
//! Exercises the three NULL-and-growth contracts the C call site relies
//! on at `src/spec.c:504`:
//! - NULL dict returns `0` (matches `TrieType_MemUsage(NULL)`),
//! - an empty live dict reports non-zero heap bookkeeping,
//! - usage grows monotonically as terms are inserted.

use std::ffi::c_char;
use std::ptr;

use libc::size_t;
use term_dict_ffi::*;

unsafe fn insert(d: *mut TermDict, term: &[u8]) {
    // SAFETY: `d` is a live `TermDict`; `term` is a borrowed byte slice.
    let ok = unsafe {
        TermDict_InsertRaw(
            d,
            term.as_ptr().cast::<c_char>(),
            term.len() as size_t,
            1.0,
            1,
        )
    };
    assert!(ok);
}

#[test]
fn null_returns_zero() {
    // SAFETY: NULL is explicitly allowed by the FFI contract.
    let bytes = unsafe { TermDict_MemUsage(ptr::null()) };
    assert_eq!(bytes, 0, "NULL must mirror TrieType_MemUsage(NULL) == 0");
}

#[test]
fn empty_dict_reports_nonzero_bookkeeping() {
    let d = TermDict_New();
    // SAFETY: `d` is a live `TermDict`.
    let bytes = unsafe { TermDict_MemUsage(d) };
    assert!(
        bytes > 0,
        "empty TermDict must account for its own heap bookkeeping; got {bytes}"
    );
    // SAFETY: `d` is a live pointer.
    unsafe { TermDict_Free(d) };
}

#[test]
fn usage_grows_with_inserts() {
    let d = TermDict_New();
    // SAFETY: `d` is a live `TermDict`.
    let empty_bytes = unsafe { TermDict_MemUsage(d) };

    // SAFETY: `d` is a live `TermDict`.
    unsafe { insert(d, b"alpha") };
    // SAFETY: `d` is a live `TermDict`.
    let one_term = unsafe { TermDict_MemUsage(d) };
    assert!(
        one_term > empty_bytes,
        "inserting a term must grow heap usage; empty={empty_bytes} one_term={one_term}"
    );

    // SAFETY: `d` is a live `TermDict`.
    unsafe { insert(d, b"beta") };
    // SAFETY: `d` is a live `TermDict`.
    unsafe { insert(d, b"gamma") };
    // SAFETY: `d` is a live `TermDict`.
    let three_terms = unsafe { TermDict_MemUsage(d) };
    assert!(
        three_terms > one_term,
        "further inserts must grow heap usage; one_term={one_term} three_terms={three_terms}"
    );

    // SAFETY: `d` is a live pointer.
    unsafe { TermDict_Free(d) };
}
