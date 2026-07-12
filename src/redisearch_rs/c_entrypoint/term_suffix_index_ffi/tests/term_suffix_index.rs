/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use redis_mock::mock_or_stub_missing_redis_c_symbols;
use std::collections::HashSet;
use std::ffi::{c_char, c_int, c_void};
use term_suffix_index_ffi::*;

// Link both Rust-provided and C-provided symbols
extern crate redisearch_rs;
// Mock or stub the ones that aren't provided by the line above
mock_or_stub_missing_redis_c_symbols!();

#[test]
fn iterate_contains_reports_terms_containing_needle() {
    with_index(&["bike", "biker", "trike", "cool"], |t| {
        let actual = collect(t, "ike", TermSuffixIndex_IterateContains);

        assert_eq!(actual, to_set(&["bike", "biker", "trike"]));
    });
}

#[test]
fn nonzero_callback_return_stops_iteration() {
    with_index(&["bike", "biker", "trike"], |t| {
        unsafe extern "C" fn stop_after_first(
            _term: *const c_char,
            _len: usize,
            ctx: *mut c_void,
            _payload: *mut c_void,
        ) -> c_int {
            // Safety: `ctx` points at the local counter below.
            let count = unsafe { &mut *ctx.cast::<usize>() };
            *count += 1;
            1
        }

        let needle = "ike";
        let mut count = 0_usize;
        // Safety: `t` is valid, `needle` points to valid UTF-8 bytes,
        // and `ctx` points at a live counter.
        unsafe {
            TermSuffixIndex_IterateContains(
                t,
                needle.as_ptr().cast(),
                needle.len(),
                Some(stop_after_first),
                (&raw mut count).cast(),
            )
        };

        assert_eq!(count, 1, "iteration must stop on the first non-zero return");
    });
}

#[test]
fn remove_drops_term_from_results() {
    with_index(&["bike", "biker"], |t| {
        let term = "biker";
        // Safety: `term` points to valid UTF-8 bytes and no iteration
        // on `t` is in progress.
        unsafe { TermSuffixIndex_Remove(t, term.as_ptr().cast(), term.len()) };

        let actual = collect(t, "ike", TermSuffixIndex_IterateContains);

        assert_eq!(actual, to_set(&["bike"]));
    });
}

#[test]
fn iterate_all_yields_terms_and_proper_suffixes() {
    with_index(&["bike"], |t| {
        // Safety: `t` is valid and outlives the iterator.
        let it = unsafe { TermSuffixIndex_IterateAll(t) };
        let actual = drain(it);

        // Full term plus every proper suffix down to the final codepoint.
        assert_eq!(actual, to_set(&["bike", "ike", "ke", "e"]));
    });
}

#[test]
fn multibyte_terms_roundtrip() {
    with_index(&["żółć", "köln"], |t| {
        let actual = collect(t, "ółć", TermSuffixIndex_IterateSuffix);

        assert_eq!(actual, to_set(&["żółć"]));
    });
}

#[test]
fn mem_usage_grows_with_content() {
    with_index(&[], |t| {
        // Safety: `t` is a valid index pointer.
        let empty_usage = unsafe { TermSuffixIndex_MemUsage(t) };

        let term = "bicycle";
        // Safety: `term` points to valid UTF-8 bytes and no iterator
        // on `t` is alive.
        unsafe { TermSuffixIndex_Add(t, term.as_ptr().cast(), term.len()) };

        // Safety: `t` is a valid index pointer.
        let populated_usage = unsafe { TermSuffixIndex_MemUsage(t) };
        assert!(populated_usage > empty_usage);
    });
}

#[test]
fn iterate_wildcard_yields_matching_terms() {
    with_index(&["bike", "biker", "trike", "cool"], |t| {
        let (rc, actual) = collect_wildcard(t, "*ike*");

        assert_eq!(rc, 1, "'ike' anchors the search");
        assert_eq!(actual, to_set(&["bike", "biker", "trike"]));
    });
}

#[test]
fn iterate_wildcard_without_anchor_returns_zero() {
    with_index(&["bike"], |t| {
        // Every `*`-separated token contains `?`, so none can seed a
        // subtree or exact lookup — the search has no anchor.
        let (rc, actual) = collect_wildcard(t, "?*?e");

        assert_eq!(rc, 0, "caller must fall back to a full scan");
        assert!(actual.is_empty());
    });
}

/// Create a [`TermSuffixIndex`], add `terms` to it, call the callback
/// with the index pointer, and free the index.
fn with_index<F>(terms: &[&str], f: F)
where
    F: FnOnce(*mut TermSuffixIndex),
{
    let t = TermSuffixIndex_New();
    for term in terms {
        // Safety: `term` points to `term.len()` valid UTF-8 bytes and
        // no iterator on `t` is alive.
        unsafe { TermSuffixIndex_Add(t, term.as_ptr().cast::<c_char>(), term.len()) };
    }

    f(t);

    // Safety: `t` was obtained from `TermSuffixIndex_New` and all
    // iterators have been freed by the callback.
    unsafe { TermSuffixIndex_Free(t) };
}

/// Callback that inserts each reported term into the `HashSet<String>`
/// pointed to by `ctx`.
unsafe extern "C" fn collect_cb(
    term: *const c_char,
    len: usize,
    ctx: *mut c_void,
    _payload: *mut c_void,
) -> c_int {
    // Safety: `term` points to `len` valid UTF-8 bytes for the
    // duration of this call.
    let bytes = unsafe { std::slice::from_raw_parts(term.cast::<u8>(), len) };
    // Safety: `ctx` points at the live set below.
    let yielded = unsafe { &mut *ctx.cast::<HashSet<String>>() };
    yielded.insert(String::from_utf8(bytes.to_vec()).unwrap());
    0
}

/// Drive a callback-based iterate function with `needle` and collect
/// every reported term into a set.
fn collect(
    t: *mut TermSuffixIndex,
    needle: &str,
    iterate: unsafe extern "C" fn(
        *const TermSuffixIndex,
        *const c_char,
        usize,
        TermSuffixIterateCallback,
        *mut c_void,
    ),
) -> HashSet<String> {
    let mut yielded = HashSet::new();
    // Safety: `t` is valid, `needle` points to valid UTF-8 bytes, and
    // `ctx` points at a live set.
    unsafe {
        iterate(
            t,
            needle.as_ptr().cast(),
            needle.len(),
            Some(collect_cb),
            (&raw mut yielded).cast(),
        )
    };
    yielded
}

/// Drive [`TermSuffixIndex_IterateWildcard`] and collect every reported
/// term into a set, returning the function's status code alongside it.
fn collect_wildcard(t: *mut TermSuffixIndex, pattern: &str) -> (c_int, HashSet<String>) {
    let mut yielded = HashSet::new();
    // Safety: `t` is valid, `pattern` points to valid UTF-8 bytes, and
    // `ctx` points at a live set.
    let rc = unsafe {
        TermSuffixIndex_IterateWildcard(
            t,
            pattern.as_ptr().cast(),
            pattern.len(),
            Some(collect_cb),
            (&raw mut yielded).cast(),
        )
    };
    (rc, yielded)
}

/// Drain an iterator into the set of strings it yields, then free it.
fn drain(it: *mut TermSuffixIndexIterator) -> HashSet<String> {
    let mut yielded = HashSet::new();
    let mut str = std::ptr::null();
    let mut len = 0;
    // Safety: `it` is a live iterator and `str`/`len` point to
    // writable locals; the yielded bytes are copied before the next
    // advance invalidates them.
    while unsafe { TermSuffixIndexIterator_Next(it, &mut str, &mut len) } == 1 {
        // Safety: `str`/`len` were just written by a successful advance and
        // describe `len` valid bytes, copied before the next advance.
        let bytes = unsafe { std::slice::from_raw_parts(str.cast::<u8>(), len) };
        yielded.insert(String::from_utf8(bytes.to_vec()).unwrap());
    }
    // Safety: `it` is a live iterator, not used after this call.
    unsafe { TermSuffixIndexIterator_Free(it) };
    yielded
}

fn to_set(terms: &[&str]) -> HashSet<String> {
    terms.iter().map(|s| s.to_string()).collect()
}
