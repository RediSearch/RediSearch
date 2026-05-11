/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Integration tests for `trie_lex_ffi`.
//!
//! These exercise the FFI surface directly (no Redis runtime). RDB save/load
//! is covered by separate test (skipped when the Redis allocator isn't set
//! up).

#![allow(non_snake_case)]

use std::ffi::c_int;
use std::ptr;
use trie_lex_ffi::{
    TrieLex, TrieLex_Delete, TrieLex_Free, TrieLex_InsertStringBuffer, TrieLex_IterateAll,
    TrieLex_New, TrieLex_Size, TrieLexIterator_Free, TrieLexIterator_Next, trie_lex_rune,
    trie_lex_t_len,
};

fn insert(t: *mut TrieLex, key: &str) -> c_int {
    // SAFETY: `t` is a valid trie and `key.as_bytes()` is a live slice.
    unsafe {
        TrieLex_InsertStringBuffer(
            t,
            key.as_ptr().cast(),
            key.len(),
            1.0,
            1,
        )
    }
}

fn delete(t: *mut TrieLex, key: &str) -> c_int {
    // SAFETY: `t` is a valid trie and `key.as_bytes()` is a live slice.
    unsafe { TrieLex_Delete(t, key.as_ptr().cast(), key.len()) }
}

fn collect_lex_order(t: *mut TrieLex) -> Vec<String> {
    // SAFETY: `t` is a valid trie.
    let it = unsafe { TrieLex_IterateAll(t) };
    let mut out = Vec::new();
    let mut rstr: *mut trie_lex_rune = ptr::null_mut();
    let mut slen: trie_lex_t_len = 0;
    // SAFETY: out-pointers are valid for writes; iterator is live.
    while unsafe { TrieLexIterator_Next(it, &mut rstr, &mut slen, ptr::null_mut()) } != 0 {
        // SAFETY: `rstr` borrowed from the iterator's buffer, valid until
        // the next call.
        let runes = unsafe { std::slice::from_raw_parts(rstr, slen as usize) };
        // Decode runes back to UTF-8 (each rune is a Unicode codepoint).
        let s: String = runes
            .iter()
            .map(|&r| char::from_u32(r as u32).unwrap_or('\u{FFFD}'))
            .collect();
        out.push(s);
    }
    // SAFETY: `it` not yet freed.
    unsafe { TrieLexIterator_Free(it) };
    out
}

#[test]
fn insert_find_remove_basic() {
    let t = TrieLex_New();
    assert_eq!(insert(t, "apple"), 1);
    assert_eq!(insert(t, "apple"), 0, "duplicate insert returns 0");
    assert_eq!(insert(t, "banana"), 1);
    // SAFETY: `t` valid.
    assert_eq!(unsafe { TrieLex_Size(t) }, 2);
    assert_eq!(delete(t, "apple"), 1);
    assert_eq!(delete(t, "apple"), 0, "second delete returns 0");
    // SAFETY: `t` valid.
    assert_eq!(unsafe { TrieLex_Size(t) }, 1);
    // SAFETY: `t` valid.
    unsafe { TrieLex_Free(t) };
}

#[test]
fn iter_yields_in_lex_order_ascii() {
    let t = TrieLex_New();
    for k in &["banana", "apple", "cherry", "ant", "apricot"] {
        insert(t, k);
    }
    let got = collect_lex_order(t);
    assert_eq!(
        got,
        vec!["ant", "apple", "apricot", "banana", "cherry"],
        "iteration must be lex-sorted"
    );
    // SAFETY: `t` valid.
    unsafe { TrieLex_Free(t) };
}

#[test]
fn iter_yields_in_lex_order_non_ascii() {
    // Use codepoints that straddle byte boundaries to catch any LE/BE
    // confusion. ÿ (U+00FF) < Ā (U+0100) < ā (U+0101).
    let t = TrieLex_New();
    for k in &["Ā", "ÿ", "ā", "a", "z"] {
        insert(t, k);
    }
    let got = collect_lex_order(t);
    assert_eq!(got, vec!["a", "z", "ÿ", "Ā", "ā"]);
    // SAFETY: `t` valid.
    unsafe { TrieLex_Free(t) };
}

#[test]
fn empty_key_is_rejected() {
    let t = TrieLex_New();
    let rc = unsafe { TrieLex_InsertStringBuffer(t, ptr::null(), 0, 1.0, 1) };
    assert_eq!(rc, 0);
    // SAFETY: `t` valid.
    assert_eq!(unsafe { TrieLex_Size(t) }, 0);
    // SAFETY: `t` valid.
    unsafe { TrieLex_Free(t) };
}

#[test]
fn oversize_key_is_rejected() {
    let t = TrieLex_New();
    // 256 ASCII runes is the boundary (must be < 256).
    let big = "a".repeat(256);
    assert_eq!(insert(t, &big), 0);
    let just_fits = "a".repeat(255);
    assert_eq!(insert(t, &just_fits), 1);
    // SAFETY: `t` valid.
    unsafe { TrieLex_Free(t) };
}

#[test]
fn iterating_empty_trie_yields_nothing() {
    let t = TrieLex_New();
    let got = collect_lex_order(t);
    assert!(got.is_empty());
    // SAFETY: `t` valid.
    unsafe { TrieLex_Free(t) };
}

#[test]
fn delete_unknown_returns_zero() {
    let t = TrieLex_New();
    insert(t, "alpha");
    assert_eq!(delete(t, "beta"), 0);
    // SAFETY: `t` valid.
    assert_eq!(unsafe { TrieLex_Size(t) }, 1);
    // SAFETY: `t` valid.
    unsafe { TrieLex_Free(t) };
}
