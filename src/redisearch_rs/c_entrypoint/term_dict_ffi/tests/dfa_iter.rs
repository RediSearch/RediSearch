/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Integration tests for `TermDict_IterateDfa`.
//!
//! Mirrors the `Trie_Iterate` / `TrieIterator_Next` loop in
//! `src/trie/trie.c`, which backs `iterateExpandedTerms` at
//! `src/query.c:617`. The callback receives the full
//! `(term, score, num_docs, dist)` shape that `TrieIterator_Next` writes.

use std::ffi::{c_char, c_int, c_void};
use std::ptr;

use libc::size_t;
use term_dict_ffi::*;

#[derive(Debug, PartialEq)]
struct Hit {
    term: Vec<u8>,
    score: f64,
    num_docs: size_t,
    dist: u32,
}

struct Collector {
    hits: Vec<Hit>,
    cap: Option<usize>,
}

unsafe extern "C" fn collect_cb(
    term: *const c_char,
    term_len: size_t,
    ctx: *mut c_void,
    score: f64,
    num_docs: size_t,
    dist: u32,
) -> c_int {
    // SAFETY: `ctx` is the `Collector` we passed in.
    let collector = unsafe { &mut *ctx.cast::<Collector>() };
    let bytes = unsafe { std::slice::from_raw_parts(term.cast::<u8>(), term_len) }.to_vec();
    collector.hits.push(Hit {
        term: bytes,
        score,
        num_docs,
        dist,
    });
    if let Some(cap) = collector.cap
        && collector.hits.len() >= cap
    {
        return 1;
    }
    0
}

unsafe fn add(d: *mut TermDict, term: &[u8], score: f64, num_docs: size_t) {
    // SAFETY: `d` is live.
    let _ = unsafe {
        TermDict_AddTerm(
            d,
            term.as_ptr().cast::<c_char>(),
            term.len() as size_t,
            score,
            num_docs,
        )
    };
}

#[test]
fn exact_dist_zero_finds_term() {
    let d = TermDict_New();
    // SAFETY: `d` is live.
    unsafe {
        add(d, b"hello", 2.5, 7);
        add(d, b"world", 1.0, 3);
    }
    let mut collector = Collector {
        hits: Vec::new(),
        cap: None,
    };
    let prefix = b"hello";
    // SAFETY: `d` and `prefix` are live across this call.
    unsafe {
        TermDict_IterateDfa(
            d,
            prefix.as_ptr().cast::<c_char>(),
            prefix.len() as size_t,
            0,
            false,
            Some(collect_cb),
            (&mut collector as *mut Collector).cast::<c_void>(),
        );
    }
    assert_eq!(collector.hits.len(), 1);
    let h = &collector.hits[0];
    assert_eq!(h.term, b"hello");
    assert_eq!(h.num_docs, 7);
    assert_eq!(h.dist, 0);
    // Score widens 2.5_f32 → 2.5_f64 — exact in this case.
    assert_eq!(h.score, 2.5);
    // SAFETY: `d` is a live pointer.
    unsafe { TermDict_Free(d) };
}

#[test]
fn fuzzy_dist_one_finds_neighbours() {
    let d = TermDict_New();
    // SAFETY: `d` is live.
    unsafe {
        add(d, b"hello", 1.0, 1);
        add(d, b"hallo", 1.0, 1);
        add(d, b"hellp", 1.0, 1);
        add(d, b"world", 1.0, 1);
    }
    let mut collector = Collector {
        hits: Vec::new(),
        cap: None,
    };
    let prefix = b"hello";
    // SAFETY: `d` and `prefix` are live.
    unsafe {
        TermDict_IterateDfa(
            d,
            prefix.as_ptr().cast::<c_char>(),
            prefix.len() as size_t,
            1,
            false,
            Some(collect_cb),
            (&mut collector as *mut Collector).cast::<c_void>(),
        );
    }
    let mut got: Vec<&[u8]> = collector.hits.iter().map(|h| h.term.as_slice()).collect();
    got.sort();
    assert_eq!(got, vec![&b"hallo"[..], &b"hello"[..], &b"hellp"[..]]);
    // SAFETY: `d` is a live pointer.
    unsafe { TermDict_Free(d) };
}

#[test]
fn prefix_mode_admits_any_suffix() {
    let d = TermDict_New();
    // SAFETY: `d` is live.
    unsafe {
        add(d, b"helloworld", 1.0, 1);
        add(d, b"hellothere", 1.0, 1);
        add(d, b"helping", 1.0, 1);
    }
    let mut collector = Collector {
        hits: Vec::new(),
        cap: None,
    };
    let prefix = b"hello";
    // SAFETY: `d` and `prefix` are live.
    unsafe {
        TermDict_IterateDfa(
            d,
            prefix.as_ptr().cast::<c_char>(),
            prefix.len() as size_t,
            0,
            true,
            Some(collect_cb),
            (&mut collector as *mut Collector).cast::<c_void>(),
        );
    }
    let mut got: Vec<&[u8]> = collector.hits.iter().map(|h| h.term.as_slice()).collect();
    got.sort();
    assert_eq!(got, vec![&b"hellothere"[..], &b"helloworld"[..]]);
    // SAFETY: `d` is a live pointer.
    unsafe { TermDict_Free(d) };
}

#[test]
fn callback_break_stops_iteration() {
    let d = TermDict_New();
    // SAFETY: `d` is live.
    unsafe {
        for term in [b"app" as &[u8], b"apple", b"applet", b"apply"] {
            add(d, term, 1.0, 1);
        }
    }
    let mut collector = Collector {
        hits: Vec::new(),
        cap: Some(2),
    };
    let prefix = b"app";
    // SAFETY: `d` and `prefix` are live.
    unsafe {
        TermDict_IterateDfa(
            d,
            prefix.as_ptr().cast::<c_char>(),
            prefix.len() as size_t,
            0,
            true,
            Some(collect_cb),
            (&mut collector as *mut Collector).cast::<c_void>(),
        );
    }
    assert_eq!(collector.hits.len(), 2);
    // SAFETY: `d` is a live pointer.
    unsafe { TermDict_Free(d) };
}

#[test]
fn null_dict_is_noop() {
    let mut collector = Collector {
        hits: Vec::new(),
        cap: None,
    };
    let prefix = b"x";
    // SAFETY: NULL is explicitly allowed by the FFI contract.
    unsafe {
        TermDict_IterateDfa(
            ptr::null(),
            prefix.as_ptr().cast::<c_char>(),
            prefix.len() as size_t,
            1,
            false,
            Some(collect_cb),
            (&mut collector as *mut Collector).cast::<c_void>(),
        );
    }
    assert!(collector.hits.is_empty());
}

#[test]
fn invalid_utf8_prefix_is_noop() {
    let d = TermDict_New();
    // SAFETY: `d` is live.
    unsafe { add(d, b"alpha", 1.0, 1) };
    let mut collector = Collector {
        hits: Vec::new(),
        cap: None,
    };
    // SAFETY: `d` is live; non-UTF-8 prefix → no-op.
    unsafe {
        TermDict_IterateDfa(
            d,
            [0xFFu8].as_ptr().cast::<c_char>(),
            1,
            0,
            false,
            Some(collect_cb),
            (&mut collector as *mut Collector).cast::<c_void>(),
        );
    }
    assert!(collector.hits.is_empty());
    // SAFETY: `d` is a live pointer.
    unsafe { TermDict_Free(d) };
}
