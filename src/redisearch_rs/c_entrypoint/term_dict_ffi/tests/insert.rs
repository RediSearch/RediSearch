/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Integration tests for `TermDict_InsertRaw` — the RDB-load sink primitive.
//!
//! Exercises the three contracts the C codec relies on:
//! - successful round-trip of a known map,
//! - "raw" overwrite semantics: a second insert for the same key replaces
//!   the prior `(score, num_docs)` rather than merging,
//! - case-folding happens at the Rust boundary, mirroring the legacy C
//!   trie's libnu ASCII fold so RDBs written by the C path load identically.

use std::ffi::c_char;
use std::ptr;

use libc::size_t;
use term_dict_ffi::*;

struct Entry {
    term: Vec<u8>,
    score: f64,
    num_docs: size_t,
}

unsafe fn collect(d: *mut TermDict) -> Vec<Entry> {
    // SAFETY: `d` is a live `TermDict`.
    let it = unsafe { TermDict_IterNew(d) };
    let mut out = Vec::new();
    loop {
        let mut term: *const c_char = ptr::null();
        let mut term_len: size_t = 0;
        let mut score: f64 = 0.0;
        let mut num_docs: size_t = 0;
        // SAFETY: `it` is a live iterator; all out-pointers are writable.
        let ok =
            unsafe { TermDict_IterNext(it, &mut term, &mut term_len, &mut score, &mut num_docs) };
        if !ok {
            break;
        }
        // SAFETY: On `true`, the FFI guarantees `term` points to `term_len`
        // readable bytes that stay valid until the next step.
        let bytes =
            unsafe { std::slice::from_raw_parts(term.cast::<u8>(), term_len as usize) }.to_vec();
        out.push(Entry {
            term: bytes,
            score,
            num_docs,
        });
    }
    // SAFETY: `it` is a live pointer.
    unsafe { TermDict_IterFree(it) };
    out
}

unsafe fn insert(d: *mut TermDict, term: &[u8], score: f64, num_docs: size_t) -> bool {
    // SAFETY: `d` is a live `TermDict`; `term` is a borrowed byte slice
    // with matching pointer/length.
    unsafe {
        TermDict_InsertRaw(
            d,
            term.as_ptr().cast::<c_char>(),
            term.len() as size_t,
            score,
            num_docs,
        )
    }
}

#[test]
fn known_map_round_trips() {
    let d = TermDict_New();
    let inserts: &[(&str, f64, size_t)] = &[("alpha", 0.5, 1), ("beta", 1.0, 2), ("gamma", 1.5, 3)];
    for &(t, s, n) in inserts {
        // SAFETY: `d` is a live `TermDict`.
        assert!(unsafe { insert(d, t.as_bytes(), s, n) });
    }

    // SAFETY: `d` is a live `TermDict`.
    let got = unsafe { collect(d) };

    assert_eq!(got.len(), inserts.len());
    for (actual, &(t, s, n)) in got.iter().zip(inserts.iter()) {
        assert_eq!(actual.term, t.as_bytes());
        assert_eq!(actual.score, s);
        assert_eq!(actual.num_docs, n);
    }
    // SAFETY: `d` is a live pointer.
    unsafe { TermDict_Free(d) };
}

#[test]
fn repeated_insert_overwrites() {
    let d = TermDict_New();
    // SAFETY: `d` is a live `TermDict`.
    assert!(unsafe { insert(d, b"foo", 1.0, 1) });
    // SAFETY: `d` is a live `TermDict`.
    assert!(unsafe { insert(d, b"foo", 2.0, 2) });

    // SAFETY: `d` is a live `TermDict`.
    let len = unsafe { TermDict_Len(d) };
    assert_eq!(len, 1, "second insert must not append");

    // SAFETY: `d` is a live `TermDict`.
    let got = unsafe { collect(d) };
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].term, b"foo");
    assert_eq!(got[0].score, 2.0, "second insert wins (raw overwrite)");
    assert_eq!(got[0].num_docs, 2);

    // SAFETY: `d` is a live pointer.
    unsafe { TermDict_Free(d) };
}

#[test]
fn ascii_uppercase_reads_back_lowercased() {
    let d = TermDict_New();
    // SAFETY: `d` is a live `TermDict`.
    assert!(unsafe { insert(d, b"FOO", 3.0, 4) });

    // SAFETY: `d` is a live `TermDict`.
    let got = unsafe { collect(d) };
    assert_eq!(got.len(), 1);
    assert_eq!(
        got[0].term, b"foo",
        "case-fold at Rust boundary mirrors legacy C trie's libnu ASCII fold"
    );
    assert_eq!(got[0].score, 3.0);
    assert_eq!(got[0].num_docs, 4);

    // SAFETY: `d` is a live pointer.
    unsafe { TermDict_Free(d) };
}

#[test]
fn invalid_utf8_is_rejected() {
    let d = TermDict_New();
    // 0xFF is never valid UTF-8; insert must refuse.
    // SAFETY: `d` is a live `TermDict`.
    assert!(!unsafe { insert(d, &[0xFFu8], 0.0, 0) });
    // SAFETY: `d` is a live `TermDict`.
    let len = unsafe { TermDict_Len(d) };
    assert_eq!(len, 0, "rejected insert must not bump len");
    // SAFETY: `d` is a live pointer.
    unsafe { TermDict_Free(d) };
}

#[test]
fn empty_term_is_accepted() {
    // The legacy C trie tolerates an empty key; the codec must too,
    // since RDB load can encounter "" if a prior writer produced one.
    let d = TermDict_New();
    // SAFETY: `d` is a live `TermDict`. `term_len == 0` allows a NULL
    // pointer per `TermDict_InsertRaw`'s docstring.
    let ok = unsafe { TermDict_InsertRaw(d, ptr::null(), 0, 0.0, 0) };
    assert!(ok);
    // SAFETY: `d` is a live `TermDict`.
    assert_eq!(unsafe { TermDict_Len(d) }, 1);
    // SAFETY: `d` is a live pointer.
    unsafe { TermDict_Free(d) };
}
