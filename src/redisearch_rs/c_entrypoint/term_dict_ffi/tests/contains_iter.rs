/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Integration tests for `TermDict_IterateContains`.
//!
//! Mirrors `Trie_IterateContains` in `src/trie/trie.c`, which backs the
//! prefix / suffix / contains query paths at `src/query.c:735`. The C-side
//! `runeIterCb` returns `REDISEARCH_ERR` (non-zero) to honour the
//! `maxPrefixExpansions` cap; the early-break semantics are pinned here.

use std::ffi::{c_char, c_int, c_void};
use std::ptr;

use libc::size_t;
use term_dict_ffi::*;

struct Collector {
    matches: Vec<(Vec<u8>, size_t)>,
    cap: Option<usize>,
}

unsafe extern "C" fn collect_cb(
    term: *const c_char,
    term_len: size_t,
    ctx: *mut c_void,
    num_docs: size_t,
) -> c_int {
    // SAFETY: `ctx` is the `Collector` we passed in; `term`/`term_len`
    // describe a borrowed byte slice valid for the duration of this call.
    let collector = unsafe { &mut *ctx.cast::<Collector>() };
    let bytes = unsafe { std::slice::from_raw_parts(term.cast::<u8>(), term_len) }.to_vec();
    collector.matches.push((bytes, num_docs));
    if let Some(cap) = collector.cap
        && collector.matches.len() >= cap
    {
        return 1;
    }
    0
}

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

fn build_fixture() -> *mut TermDict {
    let d = TermDict_New();
    // SAFETY: `d` is a freshly-allocated valid handle.
    unsafe {
        add(d, b"apple", 1);
        add(d, b"applepie", 2);
        add(d, b"banana", 3);
        add(d, b"pineapple", 4);
        add(d, b"orange", 5);
    }
    d
}

#[test]
fn prefix_match() {
    let d = build_fixture();
    let mut collector = Collector {
        matches: Vec::new(),
        cap: None,
    };

    let pattern = b"app";
    // SAFETY: `d` is live; `pattern` outlives this call; `cb` matches signature.
    unsafe {
        TermDict_IterateContains(
            d,
            pattern.as_ptr().cast::<c_char>(),
            pattern.len() as size_t,
            true,
            false,
            Some(collect_cb),
            (&mut collector as *mut Collector).cast::<c_void>(),
        );
    }

    let mut got: Vec<&[u8]> = collector.matches.iter().map(|(b, _)| b.as_slice()).collect();
    got.sort();
    assert_eq!(got, vec![&b"apple"[..], &b"applepie"[..]]);
    // SAFETY: `d` is a live pointer.
    unsafe { TermDict_Free(d) };
}

#[test]
fn suffix_match() {
    let d = build_fixture();
    let mut collector = Collector {
        matches: Vec::new(),
        cap: None,
    };

    let pattern = b"apple";
    // SAFETY: see prefix test.
    unsafe {
        TermDict_IterateContains(
            d,
            pattern.as_ptr().cast::<c_char>(),
            pattern.len() as size_t,
            false,
            true,
            Some(collect_cb),
            (&mut collector as *mut Collector).cast::<c_void>(),
        );
    }

    let mut got: Vec<&[u8]> = collector.matches.iter().map(|(b, _)| b.as_slice()).collect();
    got.sort();
    assert_eq!(got, vec![&b"apple"[..], &b"pineapple"[..]]);
    // SAFETY: `d` is a live pointer.
    unsafe { TermDict_Free(d) };
}

#[test]
fn contains_match() {
    let d = build_fixture();
    let mut collector = Collector {
        matches: Vec::new(),
        cap: None,
    };

    let pattern = b"apple";
    // SAFETY: see prefix test.
    unsafe {
        TermDict_IterateContains(
            d,
            pattern.as_ptr().cast::<c_char>(),
            pattern.len() as size_t,
            true,
            true,
            Some(collect_cb),
            (&mut collector as *mut Collector).cast::<c_void>(),
        );
    }

    let mut got: Vec<&[u8]> = collector.matches.iter().map(|(b, _)| b.as_slice()).collect();
    got.sort();
    assert_eq!(got, vec![&b"apple"[..], &b"applepie"[..], &b"pineapple"[..]]);
    // SAFETY: `d` is a live pointer.
    unsafe { TermDict_Free(d) };
}

#[test]
fn callback_break_stops_iteration() {
    let d = build_fixture();
    let mut collector = Collector {
        matches: Vec::new(),
        cap: Some(1),
    };

    let pattern = b"apple";
    // SAFETY: see prefix test.
    unsafe {
        TermDict_IterateContains(
            d,
            pattern.as_ptr().cast::<c_char>(),
            pattern.len() as size_t,
            true,
            true,
            Some(collect_cb),
            (&mut collector as *mut Collector).cast::<c_void>(),
        );
    }

    // The break is honoured after the cap-th match is delivered, so the
    // total count must be exactly the cap.
    assert_eq!(collector.matches.len(), 1);
    // SAFETY: `d` is a live pointer.
    unsafe { TermDict_Free(d) };
}

#[test]
fn case_insensitive_pattern() {
    let d = TermDict_New();
    // SAFETY: `d` is live.
    unsafe {
        add(d, b"Apple", 1);
        add(d, b"APPLEPIE", 2);
    }

    let mut collector = Collector {
        matches: Vec::new(),
        cap: None,
    };
    let pattern = b"APP";
    // SAFETY: see prefix test.
    unsafe {
        TermDict_IterateContains(
            d,
            pattern.as_ptr().cast::<c_char>(),
            pattern.len() as size_t,
            true,
            false,
            Some(collect_cb),
            (&mut collector as *mut Collector).cast::<c_void>(),
        );
    }

    // Stored terms are folded to lowercase at insert; the iterator pattern
    // is folded too, so the uppercase `APP` query hits the lowered keys.
    assert_eq!(collector.matches.len(), 2);
    // SAFETY: `d` is a live pointer.
    unsafe { TermDict_Free(d) };
}

#[test]
fn null_dict_is_noop() {
    let mut collector = Collector {
        matches: Vec::new(),
        cap: None,
    };
    let pattern = b"x";
    // SAFETY: NULL is explicitly allowed by the FFI contract.
    unsafe {
        TermDict_IterateContains(
            ptr::null(),
            pattern.as_ptr().cast::<c_char>(),
            pattern.len() as size_t,
            true,
            false,
            Some(collect_cb),
            (&mut collector as *mut Collector).cast::<c_void>(),
        );
    }
    assert!(collector.matches.is_empty());
}

#[test]
fn null_callback_is_noop() {
    let d = build_fixture();
    let pattern = b"app";
    // SAFETY: `d` is live; NULL callback collapses to a no-op.
    unsafe {
        TermDict_IterateContains(
            d,
            pattern.as_ptr().cast::<c_char>(),
            pattern.len() as size_t,
            true,
            false,
            None,
            ptr::null_mut(),
        );
    }
    // SAFETY: `d` is a live pointer.
    unsafe { TermDict_Free(d) };
}

#[test]
fn invalid_utf8_is_noop() {
    let d = build_fixture();
    let mut collector = Collector {
        matches: Vec::new(),
        cap: None,
    };
    // SAFETY: `d` is live; pattern is non-UTF-8 → collapses to no-op.
    unsafe {
        TermDict_IterateContains(
            d,
            [0xFFu8].as_ptr().cast::<c_char>(),
            1,
            true,
            false,
            Some(collect_cb),
            (&mut collector as *mut Collector).cast::<c_void>(),
        );
    }
    assert!(collector.matches.is_empty());
    // SAFETY: `d` is a live pointer.
    unsafe { TermDict_Free(d) };
}
