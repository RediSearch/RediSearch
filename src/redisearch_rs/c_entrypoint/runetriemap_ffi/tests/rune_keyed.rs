/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! FFI integration tests for the rune-keyed (suffix-path) entry points.
//!
//! UTF-8 and RDB entry points rely on C symbols (`strToRunesN`,
//! `runesToStr`, `RedisModule_*`) that aren't linked in `cargo test`;
//! those are exercised via C integration tests once the migration
//! steps wire the FFI into `librediseach`.

// FFI tests work in raw pointers throughout. The block-level safety
// invariants are uniform: every test holds a unique pointer obtained
// from `RuneTrieMap_New` and frees it before returning.
#![allow(
    clippy::undocumented_unsafe_blocks,
    clippy::multiple_unsafe_ops_per_block
)]

use redis_mock::mock_or_stub_missing_redis_c_symbols;
use runetriemap_ffi::*;
use std::ffi::c_void;
use std::sync::atomic::{AtomicUsize, Ordering};

mock_or_stub_missing_redis_c_symbols!();

static FREE_CALLS: AtomicUsize = AtomicUsize::new(0);

unsafe extern "C" fn count_free(_payload: *mut c_void) {
    FREE_CALLS.fetch_add(1, Ordering::SeqCst);
}

fn reset_free_counter() {
    FREE_CALLS.store(0, Ordering::SeqCst);
}

fn r(s: &str) -> Vec<u16> {
    s.encode_utf16().collect()
}

unsafe fn insert(t: *mut RuneTrieMap, runes: &[u16], payload: usize) -> i32 {
    unsafe { RuneTrieMap_InsertRune(t, runes.as_ptr(), runes.len(), payload as *mut c_void, None) }
}

unsafe fn find(t: *const RuneTrieMap, runes: &[u16]) -> *mut c_void {
    unsafe { RuneTrieMap_FindRune(t, runes.as_ptr(), runes.len()) }
}

#[test]
fn new_and_free_empty() {
    let t = RuneTrieMap_New();
    assert!(!t.is_null());
    unsafe {
        assert_eq!(RuneTrieMap_Size(t), 0);
        RuneTrieMap_Free(t, None);
    }
}

#[test]
fn free_null_is_noop() {
    unsafe { RuneTrieMap_Free(std::ptr::null_mut(), None) };
}

#[test]
fn insert_find_delete_basic() {
    let t = RuneTrieMap_New();
    let k = r("hello");

    unsafe {
        assert_eq!(insert(t, &k, 42), 1);
        assert_eq!(RuneTrieMap_Size(t), 1);

        let p = find(t, &k);
        assert_eq!(p as usize, 42);

        let missing = find(t, &r("missing"));
        let nf = RUNETRIEMAP_NOTFOUND;
        assert_eq!(missing, nf);

        assert_eq!(RuneTrieMap_DeleteRune(t, k.as_ptr(), k.len(), None), 1);
        assert_eq!(RuneTrieMap_Size(t), 0);

        RuneTrieMap_Free(t, None);
    }
}

#[test]
fn insert_replace_invokes_free_callback() {
    reset_free_counter();
    let t = RuneTrieMap_New();
    let k = r("a");

    let p1 = std::ptr::without_provenance_mut::<c_void>(1);
    let p2 = std::ptr::without_provenance_mut::<c_void>(2);
    unsafe {
        assert_eq!(
            RuneTrieMap_InsertRune(t, k.as_ptr(), k.len(), p1, Some(count_free)),
            1
        );
        assert_eq!(
            RuneTrieMap_InsertRune(t, k.as_ptr(), k.len(), p2, Some(count_free)),
            0
        );
        assert_eq!(FREE_CALLS.load(Ordering::SeqCst), 1);
        let p = find(t, &k);
        assert_eq!(p as usize, 2);
        RuneTrieMap_Free(t, None);
    }
}

#[test]
fn delete_missing_returns_zero() {
    let t = RuneTrieMap_New();
    let k = r("absent");
    unsafe {
        assert_eq!(RuneTrieMap_DeleteRune(t, k.as_ptr(), k.len(), None), 0);
        RuneTrieMap_Free(t, None);
    }
}

#[test]
fn free_invokes_callback_per_remaining_payload() {
    reset_free_counter();
    let t = RuneTrieMap_New();
    unsafe {
        insert(t, &r("alpha"), 1);
        insert(t, &r("beta"), 2);
        insert(t, &r("gamma"), 3);
        RuneTrieMap_Free(t, Some(count_free));
    }
    assert_eq!(FREE_CALLS.load(Ordering::SeqCst), 3);
}

#[test]
fn find_returns_stored_null_payload() {
    let t = RuneTrieMap_New();
    let k = r("nullable");
    unsafe {
        insert(t, &k, 0);
        let p = find(t, &k);
        assert!(p.is_null());
        let nf = RUNETRIEMAP_NOTFOUND;
        assert_ne!(p, nf, "stored NULL must be distinguishable from not-found");
        RuneTrieMap_Free(t, None);
    }
}

#[test]
fn mem_usage_grows_and_shrinks() {
    let t = RuneTrieMap_New();
    unsafe {
        let empty = RuneTrieMap_MemUsage(t);
        let k = r("growing");
        insert(t, &k, 1);
        assert!(RuneTrieMap_MemUsage(t) > empty);
        RuneTrieMap_DeleteRune(t, k.as_ptr(), k.len(), None);
        assert_eq!(RuneTrieMap_MemUsage(t), empty);
        RuneTrieMap_Free(t, None);
    }
}

// ----- iteration --------------------------------------------------------

struct CollectCtx {
    out: Vec<(Vec<u16>, usize)>,
    stop_after: Option<usize>,
}

unsafe extern "C" fn collect_cb(
    runes: *const u16,
    len: usize,
    payload: *mut c_void,
    ctx: *mut c_void,
) -> i32 {
    let cx = unsafe { &mut *(ctx as *mut CollectCtx) };
    let key = unsafe { std::slice::from_raw_parts(runes, len) }.to_vec();
    cx.out.push((key, payload as usize));
    if let Some(limit) = cx.stop_after
        && cx.out.len() >= limit
    {
        return 1;
    }
    0
}

#[test]
fn iterate_prefixed_visits_matching_keys() {
    let t = RuneTrieMap_New();
    unsafe {
        insert(t, &r("apple"), 1);
        insert(t, &r("apricot"), 2);
        insert(t, &r("banana"), 3);

        let mut cx = CollectCtx {
            out: Vec::new(),
            stop_after: None,
        };
        let prefix = r("ap");
        RuneTrieMap_IteratePrefixedRune(
            t,
            prefix.as_ptr(),
            prefix.len(),
            Some(collect_cb),
            (&mut cx) as *mut CollectCtx as *mut c_void,
        );
        cx.out.sort();
        let mut expected = vec![(r("apple"), 1usize), (r("apricot"), 2)];
        expected.sort();
        assert_eq!(cx.out, expected);

        RuneTrieMap_Free(t, None);
    }
}

#[test]
fn iterate_prefixed_empty_prefix_visits_all() {
    let t = RuneTrieMap_New();
    unsafe {
        insert(t, &r("a"), 1);
        insert(t, &r("b"), 2);
        insert(t, &r("c"), 3);

        let mut cx = CollectCtx {
            out: Vec::new(),
            stop_after: None,
        };
        RuneTrieMap_IteratePrefixedRune(
            t,
            std::ptr::null(),
            0,
            Some(collect_cb),
            (&mut cx) as *mut CollectCtx as *mut c_void,
        );
        assert_eq!(cx.out.len(), 3);
        RuneTrieMap_Free(t, None);
    }
}

#[test]
fn iterate_prefixed_stops_on_nonzero_callback() {
    let t = RuneTrieMap_New();
    unsafe {
        for (i, w) in ["a", "b", "c", "d", "e"].iter().enumerate() {
            insert(t, &r(w), i + 1);
        }
        let mut cx = CollectCtx {
            out: Vec::new(),
            stop_after: Some(2),
        };
        RuneTrieMap_IteratePrefixedRune(
            t,
            std::ptr::null(),
            0,
            Some(collect_cb),
            (&mut cx) as *mut CollectCtx as *mut c_void,
        );
        assert_eq!(cx.out.len(), 2);
        RuneTrieMap_Free(t, None);
    }
}

#[test]
fn iterate_wildcard_no_timeout_visits_all_matches() {
    let t = RuneTrieMap_New();
    unsafe {
        insert(t, &r("cat"), 1);
        insert(t, &r("car"), 2);
        insert(t, &r("dog"), 3);

        let mut cx = CollectCtx {
            out: Vec::new(),
            stop_after: None,
        };
        let pattern = r("ca?");
        RuneTrieMap_IterateWildcardRune(
            t,
            pattern.as_ptr(),
            pattern.len(),
            Some(collect_cb),
            (&mut cx) as *mut CollectCtx as *mut c_void,
            std::ptr::null(),
            true,
        );
        cx.out.sort();
        let mut expected = vec![(r("car"), 2usize), (r("cat"), 1)];
        expected.sort();
        assert_eq!(cx.out, expected);
        RuneTrieMap_Free(t, None);
    }
}

#[test]
fn iterate_wildcard_callback_can_short_circuit() {
    let t = RuneTrieMap_New();
    unsafe {
        for (i, w) in ["aa", "ab", "ac", "ad", "ae"].iter().enumerate() {
            insert(t, &r(w), i + 1);
        }
        let mut cx = CollectCtx {
            out: Vec::new(),
            stop_after: Some(3),
        };
        let pattern = r("a*");
        RuneTrieMap_IterateWildcardRune(
            t,
            pattern.as_ptr(),
            pattern.len(),
            Some(collect_cb),
            (&mut cx) as *mut CollectCtx as *mut c_void,
            std::ptr::null(),
            true,
        );
        assert_eq!(cx.out.len(), 3);
        RuneTrieMap_Free(t, None);
    }
}

#[test]
fn null_callback_is_safe() {
    let t = RuneTrieMap_New();
    unsafe {
        insert(t, &r("a"), 1);
        RuneTrieMap_IteratePrefixedRune(t, std::ptr::null(), 0, None, std::ptr::null_mut());
        RuneTrieMap_IterateWildcardRune(
            t,
            std::ptr::null(),
            0,
            None,
            std::ptr::null_mut(),
            std::ptr::null(),
            true,
        );
        RuneTrieMap_Free(t, None);
    }
}
