/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Integration tests for `TermDict_AddTerm` — the ADD_INCR FFI shim
//! backing `Trie_InsertStringBuffer(sp->terms, …, incr=1)` at
//! `src/spec.c:1971`.
//!
//! Exercises:
//! - `true` on a fresh term (mirrors `TRIE_OK_NEW`),
//! - `false` on a second insert for the same key (mirrors `TRIE_OK_UPDATED`),
//! - case-folding happens inside the dictionary — mixed-case inserts
//!   collapse to one lowercased terminal,
//! - `num_docs` accumulates across calls (ADD_INCR semantics), proving
//!   that a `false` return is "updated" and not "rejected".

use std::ffi::c_char;
use std::ptr;

use libc::size_t;
use term_dict_ffi::*;

unsafe fn add(d: *mut TermDict, term: &[u8], score: f64, num_docs: size_t) -> bool {
    // SAFETY: `d` is a live `TermDict`; `term` is a borrowed byte slice
    // with matching pointer/length.
    unsafe {
        TermDict_AddTerm(
            d,
            term.as_ptr().cast::<c_char>(),
            term.len() as size_t,
            score,
            num_docs,
        )
    }
}

struct Entry {
    term: Vec<u8>,
    num_docs: size_t,
}

unsafe fn collect(d: *mut TermDict) -> Vec<Entry> {
    // SAFETY: `d` is a live `TermDict`.
    let it = unsafe { TermDict_IterNew(d) };
    let mut out = Vec::new();
    loop {
        let mut term: *const c_char = ptr::null();
        let mut term_len: size_t = 0;
        let mut _score: f64 = 0.0;
        let mut num_docs: size_t = 0;
        // SAFETY: `it` is a live iterator; all out-pointers are writable.
        let ok =
            unsafe { TermDict_IterNext(it, &mut term, &mut term_len, &mut _score, &mut num_docs) };
        if !ok {
            break;
        }
        // SAFETY: On `true`, FFI guarantees `term` points to `term_len`
        // readable bytes valid until the next step.
        let bytes =
            unsafe { std::slice::from_raw_parts(term.cast::<u8>(), term_len as usize) }.to_vec();
        out.push(Entry {
            term: bytes,
            num_docs,
        });
    }
    // SAFETY: `it` is a live pointer.
    unsafe { TermDict_IterFree(it) };
    out
}

#[test]
fn fresh_term_returns_true() {
    let d = TermDict_New();
    // SAFETY: `d` is a live `TermDict`.
    let is_new = unsafe { add(d, b"alpha", 1.0, 1) };
    assert!(is_new, "first insert must report new (mirrors TRIE_OK_NEW)");
    // SAFETY: `d` is a live `TermDict`.
    assert_eq!(unsafe { TermDict_Len(d) }, 1);
    // SAFETY: `d` is a live pointer.
    unsafe { TermDict_Free(d) };
}

#[test]
fn repeated_term_returns_false() {
    let d = TermDict_New();
    // SAFETY: `d` is a live `TermDict`.
    assert!(unsafe { add(d, b"alpha", 1.0, 1) });
    // SAFETY: `d` is a live `TermDict`.
    let is_new = unsafe { add(d, b"alpha", 1.0, 1) };
    assert!(
        !is_new,
        "second insert for same key must report updated (mirrors TRIE_OK_UPDATED)"
    );
    // SAFETY: `d` is a live `TermDict`.
    assert_eq!(unsafe { TermDict_Len(d) }, 1, "len must not grow on update");
    // SAFETY: `d` is a live pointer.
    unsafe { TermDict_Free(d) };
}

#[test]
fn case_mixed_inserts_collapse_to_lowercased_key() {
    let d = TermDict_New();
    // SAFETY: `d` is a live `TermDict`.
    assert!(unsafe { add(d, b"Foo", 1.0, 1) });
    // Different case must hit the same terminal.
    // SAFETY: `d` is a live `TermDict`.
    let is_new = unsafe { add(d, b"FOO", 1.0, 1) };
    assert!(
        !is_new,
        "case-fold contract: mixed-case inserts must hit the same terminal"
    );
    // SAFETY: `d` is a live `TermDict`.
    let got = unsafe { collect(d) };
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].term, b"foo", "stored key must be lowercased");
    // SAFETY: `d` is a live pointer.
    unsafe { TermDict_Free(d) };
}

#[test]
fn num_docs_accumulates_across_calls() {
    let d = TermDict_New();
    // SAFETY: `d` is a live `TermDict`.
    assert!(unsafe { add(d, b"alpha", 1.0, 3) });
    // SAFETY: `d` is a live `TermDict`.
    assert!(!unsafe { add(d, b"alpha", 1.0, 4) });
    // SAFETY: `d` is a live `TermDict`.
    let got = unsafe { collect(d) };
    assert_eq!(got.len(), 1);
    assert_eq!(
        got[0].num_docs, 7,
        "ADD_INCR: num_docs of two calls must be the sum"
    );
    // SAFETY: `d` is a live pointer.
    unsafe { TermDict_Free(d) };
}

#[test]
fn invalid_utf8_is_rejected_without_inserting() {
    let d = TermDict_New();
    // SAFETY: `d` is a live `TermDict`.
    let is_new = unsafe { add(d, &[0xFFu8], 1.0, 1) };
    assert!(
        !is_new,
        "invalid UTF-8 must return false (same shape as updated existing — caller validates upstream)"
    );
    // SAFETY: `d` is a live `TermDict`.
    assert_eq!(
        unsafe { TermDict_Len(d) },
        0,
        "rejected insert must not bump len"
    );
    // SAFETY: `d` is a live pointer.
    unsafe { TermDict_Free(d) };
}
