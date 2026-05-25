/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Integration tests for the iterator half of the FFI surface
//! (`TermDict_IterNew` / `TermDict_IterNext` / `TermDict_IterFree`).
//!
//! Each test drives the FFI through its `unsafe extern "C"` signatures so
//! the exercised path matches what the C codec
//! (`src/trie/term_stream_codec_rust_backend.c`) will call.

use std::ffi::c_char;
use std::ptr;

use libc::size_t;
use term_dict_ffi::*;

/// One step of `TermDict_IterNext`, materialized into owned bytes so the
/// caller can keep it across subsequent iterator advances.
struct Step {
    term: Vec<u8>,
    score: f64,
    num_docs: size_t,
}

/// Drive `TermDict_IterNext` once. Returns `None` when the stream is
/// exhausted; otherwise copies the borrowed term bytes into a `Vec<u8>` so
/// the result survives the next step (the per-step `String` slot inside
/// the iterator is dropped on the next call).
unsafe fn step(it: *mut TermDictIter<'_>) -> Option<Step> {
    let mut term: *const c_char = ptr::null();
    let mut term_len: size_t = 0;
    let mut score: f64 = 0.0;
    let mut num_docs: size_t = 0;
    // SAFETY: `it` is a live pointer from `TermDict_IterNew` and all
    // out-pointers are writable stack slots.
    let ok = unsafe { TermDict_IterNext(it, &mut term, &mut term_len, &mut score, &mut num_docs) };
    if !ok {
        return None;
    }
    // SAFETY: On `true`, the FFI guarantees `term` points to `term_len`
    // readable bytes that stay valid until the next step.
    let bytes =
        unsafe { std::slice::from_raw_parts(term.cast::<u8>(), term_len as usize) }.to_vec();
    Some(Step {
        term: bytes,
        score,
        num_docs,
    })
}

/// Insert `(term, score, num_docs)` via the FFI raw-insert primitive.
unsafe fn insert(d: *mut TermDict, term: &str, score: f64, num_docs: size_t) {
    // SAFETY: `d` is a live `TermDict`; `term` is a borrowed slice of
    // valid UTF-8 with matching pointer/length.
    let ok = unsafe {
        TermDict_InsertRaw(
            d,
            term.as_ptr().cast::<c_char>(),
            term.len() as size_t,
            score,
            num_docs,
        )
    };
    assert!(ok, "TermDict_InsertRaw rejected valid UTF-8 input");
}

#[test]
fn empty_iter_returns_false() {
    let d = TermDict_New();
    // SAFETY: `d` is a freshly-allocated valid handle.
    let it = unsafe { TermDict_IterNew(d) };
    // SAFETY: `it` is a freshly-allocated valid iterator.
    let exhausted = unsafe { step(it) }.is_none();
    assert!(exhausted, "empty iter should yield None");
    // SAFETY: `it` is a live pointer.
    unsafe { TermDict_IterFree(it) };
    // SAFETY: `d` is a live pointer and no iterators are still borrowing it.
    unsafe { TermDict_Free(d) };
}

#[test]
fn single_element_yields_once_then_false() {
    let d = TermDict_New();
    // SAFETY: `d` is a live `TermDict`.
    unsafe { insert(d, "foo", 1.5, 7) };

    // SAFETY: `d` is a live `TermDict`.
    let it = unsafe { TermDict_IterNew(d) };
    // SAFETY: `it` is a live iterator borrowing `d`.
    let first = unsafe { step(it) }.expect("first step should yield");
    assert_eq!(first.term, b"foo");
    assert_eq!(first.score, 1.5);
    assert_eq!(first.num_docs, 7);
    // SAFETY: `it` is a live iterator.
    assert!(unsafe { step(it) }.is_none(), "second step should exhaust");

    // SAFETY: `it` is a live pointer.
    unsafe { TermDict_IterFree(it) };
    // SAFETY: `d` is a live pointer and `it` has been freed.
    unsafe { TermDict_Free(d) };
}

#[test]
fn multi_element_advance_preserves_each_term() {
    let d = TermDict_New();
    let inserts: &[(&str, f64, size_t)] =
        &[("apple", 1.0, 1), ("banana", 2.0, 2), ("cherry", 3.0, 3)];
    for &(t, s, n) in inserts {
        // SAFETY: `d` is a live `TermDict`.
        unsafe { insert(d, t, s, n) };
    }

    // SAFETY: `d` is a live `TermDict`.
    let it = unsafe { TermDict_IterNew(d) };

    let mut got: Vec<Step> = Vec::new();
    // SAFETY: `it` is a live iterator; the loop bails out on exhaustion.
    while let Some(s) = unsafe { step(it) } {
        got.push(s);
    }

    assert_eq!(got.len(), inserts.len(), "should see every inserted term");
    // The underlying trie yields entries in key order — keys here are
    // already sorted alphabetically, so the expected order matches the
    // insertion order.
    for (actual, &(t, s, n)) in got.iter().zip(inserts.iter()) {
        assert_eq!(actual.term, t.as_bytes());
        assert_eq!(actual.score, s);
        assert_eq!(actual.num_docs, n);
    }

    // SAFETY: `it` is a live pointer.
    unsafe { TermDict_IterFree(it) };
    // SAFETY: `d` is a live pointer and `it` has been freed.
    unsafe { TermDict_Free(d) };
}

#[test]
fn len_reflects_inserts() {
    let d = TermDict_New();
    // SAFETY: `d` is a freshly-allocated valid handle.
    assert_eq!(unsafe { TermDict_Len(d) }, 0);
    // SAFETY: `d` is a live `TermDict`.
    unsafe { insert(d, "x", 0.0, 0) };
    // SAFETY: `d` is a live `TermDict`.
    unsafe { insert(d, "y", 0.0, 0) };
    // SAFETY: `d` is a live `TermDict`.
    assert_eq!(unsafe { TermDict_Len(d) }, 2);
    // SAFETY: `d` is a live pointer with no outstanding iterators.
    unsafe { TermDict_Free(d) };
}
