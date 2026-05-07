/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Iteration FFI for [`RuneTrieMap`].
//!
//! Three iteration shapes are exposed:
//!
//! * [`RuneTrieMap_IteratePrefixedRune`] — visit every entry whose key
//!   starts with the given rune prefix (replaces the suffix path's
//!   `Trie_GetNode` + `recursiveAdd` walk).
//! * [`RuneTrieMap_IterateWildcardRune`] — visit every entry whose key
//!   matches the given rune-level wildcard pattern, with optional
//!   timeout (replaces `Trie_IterateWildcard`).
//! * [`RuneTrieMap_IterateAllStr`] — emit every key as UTF-8 (replaces
//!   `Trie_Iterate(t, "", 0, 0, 1)` + `runesToStr` for the dictionary
//!   dump path).
//!
//! Each iteration is a single C call that drives a callback per match,
//! avoiding the cross-FFI iterator-handle pattern used by `triemap_ffi`.

use crate::{RuneTrieMap, decode_score, rune, runes_to_utf8};
use libc::timespec;
use std::ffi::{c_char, c_int, c_void};
use std::slice;

/// Callback signature for rune-keyed iteration.
///
/// Receives the current key (as runes) and the stored payload. Return a
/// non-zero value to stop iteration early; `0` continues.
pub type RuneTrieMapRangeCallback = Option<
    unsafe extern "C" fn(
        runes: *const rune,
        len: usize,
        payload: *mut c_void,
        ctx: *mut c_void,
    ) -> c_int,
>;

/// Callback signature for the dictionary dump path.
///
/// The string is `rm_malloc`-allocated; the callback may consume it but
/// must not free it. Returning a non-zero value stops iteration; `0`
/// continues. After the callback returns, the iterator releases the
/// string via `rm_free`.
pub type RuneTrieMapStrCallback = Option<
    unsafe extern "C" fn(str: *const c_char, len: usize, score: f32, ctx: *mut c_void) -> c_int,
>;

/// Visit every entry whose key starts with `prefix`. Calls `cb` once
/// per match in lexicographic key order. Iteration stops if `cb`
/// returns non-zero.
///
/// # Safety
///
/// * `t` must be a valid, non-NULL pointer from `RuneTrieMap_New`.
/// * `prefix` must be valid for `prefix_len` reads, or `NULL` when
///   `prefix_len == 0`.
/// * `cb`, if non-NULL, must be safe to invoke with the trie's payloads
///   and the supplied `ctx`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RuneTrieMap_IteratePrefixedRune(
    t: *const RuneTrieMap,
    prefix: *const rune,
    prefix_len: usize,
    cb: RuneTrieMapRangeCallback,
    ctx: *mut c_void,
) {
    debug_assert!(!t.is_null(), "t cannot be NULL");
    let Some(cb) = cb else { return };
    // SAFETY: caller guarantees `t` is a valid pointer to a live trie.
    let RuneTrieMap(trie) = unsafe { &*t };

    let prefix_slice: &[rune] = if prefix_len > 0 {
        debug_assert!(
            !prefix.is_null(),
            "prefix cannot be NULL when prefix_len > 0"
        );
        // SAFETY: caller guarantees `prefix` is valid for `prefix_len` reads.
        unsafe { slice::from_raw_parts(prefix, prefix_len) }
    } else {
        &[]
    };

    for (runes, &payload) in trie.prefixed_iter(prefix_slice) {
        // SAFETY: caller's contract — `cb` is safe to invoke.
        let stop = unsafe { cb(runes.as_ptr(), runes.len(), payload, ctx) };
        if stop != 0 {
            break;
        }
    }
}

/// Visit every entry whose key matches the given rune-level wildcard
/// pattern. Pattern semantics match
/// [`trie_rs::RuneTrieMap::wildcard_iter`]: `?` matches one rune, `*`
/// any number of runes, `\` escapes the next rune.
///
/// `timeout` and `skip_timeout` mirror the legacy `Trie_IterateWildcard`
/// contract. When `skip_timeout` is `false` and `timeout` is non-NULL,
/// iteration is aborted once the deadline is reached. A `timeout`
/// argument of `{ 0, 0 }` is treated as no timeout regardless of
/// `skip_timeout`.
///
/// # Safety
///
/// * `t` must be a valid, non-NULL pointer from `RuneTrieMap_New`.
/// * `pattern` must be valid for `plen` reads, or `NULL` when `plen == 0`.
/// * `timeout` must be `NULL` or point to a valid `timespec`.
/// * `cb`, if non-NULL, must be safe to invoke with the trie's payloads
///   and the supplied `ctx`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RuneTrieMap_IterateWildcardRune(
    t: *const RuneTrieMap,
    pattern: *const rune,
    plen: usize,
    cb: RuneTrieMapRangeCallback,
    ctx: *mut c_void,
    timeout: *const timespec,
    skip_timeout: bool,
) {
    debug_assert!(!t.is_null(), "t cannot be NULL");
    let Some(cb) = cb else { return };
    // SAFETY: caller guarantees `t` is a valid pointer to a live trie.
    let RuneTrieMap(trie) = unsafe { &*t };

    let pattern_slice: &[rune] = if plen > 0 {
        debug_assert!(!pattern.is_null(), "pattern cannot be NULL when plen > 0");
        // SAFETY: caller guarantees `pattern` is valid for `plen` reads.
        unsafe { slice::from_raw_parts(pattern, plen) }
    } else {
        &[]
    };

    let deadline = if skip_timeout || timeout.is_null() {
        None
    } else {
        // SAFETY: caller guarantees `timeout` is either NULL (handled
        // above) or points to a valid `timespec`.
        let ts = unsafe { *timeout };
        if ts.tv_sec == 0 && ts.tv_nsec == 0 {
            None
        } else {
            Some(ts)
        }
    };

    let mut counter: u8 = 0;
    let mut should_stop = false;

    trie.wildcard_iter(pattern_slice, |runes, &payload| {
        if should_stop {
            return;
        }
        if let Some(d) = deadline {
            counter = counter.wrapping_add(1);
            // Match `triemap_ffi`'s sampling cadence: in optimized
            // builds we check the clock once every 100 iterations; in
            // debug builds we check every iteration.
            if counter == 100 || cfg!(debug_assertions) {
                let now = monotonic_now();
                if now.tv_sec > d.tv_sec || (now.tv_sec == d.tv_sec && now.tv_nsec > d.tv_nsec) {
                    should_stop = true;
                    return;
                }
                counter = 0;
            }
        }
        // SAFETY: caller's contract — `cb` is safe to invoke.
        let stop = unsafe { cb(runes.as_ptr(), runes.len(), payload, ctx) };
        if stop != 0 {
            should_stop = true;
        }
    });
}

/// Visit every entry in lexicographic order, decoding the rune key to
/// UTF-8 and the payload to its encoded `f32` score. The string passed
/// to the callback is `rm_malloc`-allocated and freed after the call
/// returns.
///
/// # Safety
///
/// * `t` must be a valid, non-NULL pointer from `RuneTrieMap_New`.
/// * `cb`, if non-NULL, must be safe to invoke with the supplied `ctx`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RuneTrieMap_IterateAllStr(
    t: *const RuneTrieMap,
    cb: RuneTrieMapStrCallback,
    ctx: *mut c_void,
) {
    debug_assert!(!t.is_null(), "t cannot be NULL");
    let Some(cb) = cb else { return };
    // SAFETY: caller guarantees `t` is a valid pointer to a live trie.
    let RuneTrieMap(trie) = unsafe { &*t };

    for (runes, &payload) in trie.iter() {
        let mut utflen: usize = 0;
        // SAFETY: `&mut utflen` is a valid mutable pointer.
        let s = unsafe { runes_to_utf8(&runes, &mut utflen as *mut usize) };
        if s.is_null() {
            continue;
        }
        // SAFETY: caller's contract — `cb` is safe to invoke.
        let stop = unsafe { cb(s, utflen, decode_score(payload), ctx) };
        // SAFETY: `s` is rm_malloc-allocated by `runesToStr`. We pair
        // the alloc with `RedisModule_Free` to release it.
        unsafe { rm_free(s as *mut c_void) };
        if stop != 0 {
            break;
        }
    }
}

/// Read the monotonic clock, mirroring `triemap_ffi::iter::timespec_monotonic_now`.
fn monotonic_now() -> timespec {
    let mut ts = std::mem::MaybeUninit::<timespec>::uninit();
    // SAFETY: `ts` is a valid mutable pointer to uninitialized
    // `timespec` storage; `clock_gettime` initializes it on success.
    let rc = unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC_RAW, ts.as_mut_ptr()) };
    if rc == 0 {
        // SAFETY: success path of `clock_gettime` fully initializes `ts`.
        unsafe { ts.assume_init() }
    } else {
        panic!("clock_gettime(CLOCK_MONOTONIC_RAW) failed");
    }
}

/// `RedisModule_Free` reaches us through `redis-module`'s static-mut
/// hook. We re-declare the call site as an inline helper so the unsafe
/// dereference is contained.
unsafe fn rm_free(p: *mut c_void) {
    // SAFETY: caller passes a pointer obtained from `rm_malloc` (via
    // `runesToStr`); the Redis allocator must be initialized for the
    // FFI crate to be exercised.
    let free =
        unsafe { redis_module::raw::RedisModule_Free.expect("Redis allocator not available") };
    // SAFETY: `free` is a valid function pointer if `RedisModule_Free`
    // was initialized; `p` is a valid `rm_malloc`-allocated pointer.
    unsafe { free(p) };
}
