/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Integration tests for `TermDict_IterateWildcard`.
//!
//! Mirrors `Trie_IterateWildcard` in `src/trie/trie.c`, which backs the
//! wildcard query path at `src/query.c:800`. `?` matches one byte and `*`
//! matches zero-or-more bytes (byte-wise, not codepoint-wise — production
//! patterns are ASCII).

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

fn run_wildcard(d: *const TermDict, pattern: &[u8]) -> Vec<Vec<u8>> {
    let mut collector = Collector {
        matches: Vec::new(),
    };
    // SAFETY: `d` is live; `pattern` outlives this call; `cb` matches signature.
    unsafe {
        TermDict_IterateWildcard(
            d,
            pattern.as_ptr().cast::<c_char>(),
            pattern.len() as size_t,
            Some(collect_cb),
            (&mut collector as *mut Collector).cast::<c_void>(),
        );
    }
    let mut out = collector.matches;
    out.sort();
    out
}

#[test]
fn question_mark_matches_one_byte() {
    let d = TermDict_New();
    // SAFETY: `d` is live.
    unsafe {
        add(d, b"cat");
        add(d, b"car");
        add(d, b"cab");
        add(d, b"cart");
    }
    let got = run_wildcard(d, b"ca?");
    assert_eq!(got, vec![b"cab".to_vec(), b"car".to_vec(), b"cat".to_vec()]);
    // SAFETY: `d` is a live pointer.
    unsafe { TermDict_Free(d) };
}

#[test]
fn star_matches_zero_or_more() {
    let d = TermDict_New();
    // SAFETY: `d` is live.
    unsafe {
        add(d, b"foo");
        add(d, b"foobar");
        add(d, b"foobaz");
        add(d, b"qux");
    }
    let got = run_wildcard(d, b"foo*");
    assert_eq!(
        got,
        vec![b"foo".to_vec(), b"foobar".to_vec(), b"foobaz".to_vec()]
    );
    // SAFETY: `d` is a live pointer.
    unsafe { TermDict_Free(d) };
}

#[test]
fn null_dict_is_noop() {
    let mut collector = Collector {
        matches: Vec::new(),
    };
    let pattern = b"x*";
    // SAFETY: NULL is explicitly allowed by the FFI contract.
    unsafe {
        TermDict_IterateWildcard(
            ptr::null(),
            pattern.as_ptr().cast::<c_char>(),
            pattern.len() as size_t,
            Some(collect_cb),
            (&mut collector as *mut Collector).cast::<c_void>(),
        );
    }
    assert!(collector.matches.is_empty());
}

#[test]
fn invalid_utf8_is_noop() {
    let d = TermDict_New();
    // SAFETY: `d` is live.
    unsafe { add(d, b"alpha") };
    let mut collector = Collector {
        matches: Vec::new(),
    };
    // SAFETY: `d` is live; non-UTF-8 pattern → no-op.
    unsafe {
        TermDict_IterateWildcard(
            d,
            [0xFFu8].as_ptr().cast::<c_char>(),
            1,
            Some(collect_cb),
            (&mut collector as *mut Collector).cast::<c_void>(),
        );
    }
    assert!(collector.matches.is_empty());
    // SAFETY: `d` is a live pointer.
    unsafe { TermDict_Free(d) };
}
