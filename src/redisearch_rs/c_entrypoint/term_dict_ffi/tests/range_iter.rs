/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Integration tests for `TermDict_IterateRange`.
//!
//! Mirrors `Trie_IterateRange` in `src/trie/trie.c`, which backs the lex
//! range query path at `src/query.c:912`. NULL bound pointers map to the
//! C `(NULL, -1)` "unbounded" sentinel via `Option<&str>::None`.

use std::ffi::{c_char, c_int, c_void};
use std::ptr;

use libc::size_t;
use term_dict_ffi::*;

struct Collector {
    matches: Vec<Vec<u8>>,
}

unsafe extern "C" fn collect_cb(
    term: *const c_char,
    term_len: size_t,
    ctx: *mut c_void,
    _num_docs: size_t,
) -> c_int {
    // SAFETY: `ctx` is the `Collector` we passed in.
    let collector = unsafe { &mut *ctx.cast::<Collector>() };
    let bytes = unsafe { std::slice::from_raw_parts(term.cast::<u8>(), term_len) }.to_vec();
    collector.matches.push(bytes);
    0
}

unsafe fn add(d: *mut TermDict, term: &[u8]) {
    // SAFETY: `d` is live; term is a borrowed byte slice.
    let _ = unsafe {
        TermDict_AddTerm(
            d,
            term.as_ptr().cast::<c_char>(),
            term.len() as size_t,
            1.0,
            1,
        )
    };
}

fn build_fixture() -> *mut TermDict {
    let d = TermDict_New();
    // SAFETY: `d` is freshly allocated.
    unsafe {
        for t in [b"apple" as &[u8], b"banana", b"cherry", b"date", b"elderberry"] {
            add(d, t);
        }
    }
    d
}

#[allow(clippy::too_many_arguments)]
fn run_range(
    d: *const TermDict,
    min: Option<&[u8]>,
    min_inclusive: bool,
    max: Option<&[u8]>,
    max_inclusive: bool,
) -> Vec<Vec<u8>> {
    let mut collector = Collector {
        matches: Vec::new(),
    };
    let (min_ptr, min_len) = match min {
        Some(s) => (s.as_ptr().cast::<c_char>(), s.len() as size_t),
        None => (ptr::null(), 0),
    };
    let (max_ptr, max_len) = match max {
        Some(s) => (s.as_ptr().cast::<c_char>(), s.len() as size_t),
        None => (ptr::null(), 0),
    };
    // SAFETY: `d` is live; bound buffers (if any) outlive this call.
    unsafe {
        TermDict_IterateRange(
            d,
            min_ptr,
            min_len,
            min_inclusive,
            max_ptr,
            max_len,
            max_inclusive,
            Some(collect_cb),
            (&mut collector as *mut Collector).cast::<c_void>(),
        );
    }
    let mut out = collector.matches;
    out.sort();
    out
}

#[test]
fn inclusive_both_ends() {
    let d = build_fixture();
    let got = run_range(d, Some(b"b"), true, Some(b"d"), true);
    assert_eq!(got, vec![b"banana".to_vec(), b"cherry".to_vec()]);
    // SAFETY: `d` is a live pointer.
    unsafe { TermDict_Free(d) };
}

#[test]
fn unbounded_min() {
    let d = build_fixture();
    let got = run_range(d, None, true, Some(b"c"), false);
    assert_eq!(got, vec![b"apple".to_vec(), b"banana".to_vec()]);
    // SAFETY: `d` is a live pointer.
    unsafe { TermDict_Free(d) };
}

#[test]
fn unbounded_max() {
    let d = build_fixture();
    let got = run_range(d, Some(b"d"), true, None, true);
    assert_eq!(got, vec![b"date".to_vec(), b"elderberry".to_vec()]);
    // SAFETY: `d` is a live pointer.
    unsafe { TermDict_Free(d) };
}

#[test]
fn unbounded_both() {
    let d = build_fixture();
    let got = run_range(d, None, true, None, true);
    assert_eq!(got.len(), 5);
    // SAFETY: `d` is a live pointer.
    unsafe { TermDict_Free(d) };
}

#[test]
fn exclusive_min() {
    let d = build_fixture();
    // banana excluded → first match is cherry.
    let got = run_range(d, Some(b"banana"), false, Some(b"date"), true);
    assert_eq!(got, vec![b"cherry".to_vec(), b"date".to_vec()]);
    // SAFETY: `d` is a live pointer.
    unsafe { TermDict_Free(d) };
}

#[test]
fn null_dict_is_noop() {
    // SAFETY: NULL is explicitly allowed by the FFI contract.
    let got = run_range(ptr::null(), Some(b"a"), true, Some(b"z"), true);
    assert!(got.is_empty());
}

#[test]
fn invalid_utf8_bound_is_noop() {
    let d = build_fixture();
    let mut collector = Collector {
        matches: Vec::new(),
    };
    // SAFETY: `d` is live; non-UTF-8 min → collapses to no-op (no callback fired).
    unsafe {
        TermDict_IterateRange(
            d,
            [0xFFu8].as_ptr().cast::<c_char>(),
            1,
            true,
            ptr::null(),
            0,
            true,
            Some(collect_cb),
            (&mut collector as *mut Collector).cast::<c_void>(),
        );
    }
    assert!(collector.matches.is_empty());
    // SAFETY: `d` is a live pointer.
    unsafe { TermDict_Free(d) };
}
