/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Lifecycle, insert, find, and delete FFI functions for [`RuneTrieMap`].

use crate::{RuneTrieMap, decode_score, encode_score, rune, utf8_to_runes};
use std::ffi::{c_char, c_int, c_void};
use std::slice;

/// Sentinel returned by [`RuneTrieMap_FindRune`] / [`RuneTrieMap_FindStr`]
/// when the key is absent. Distinguishable from `NULL`, which is itself a
/// valid stored payload.
#[unsafe(no_mangle)]
#[used]
pub static mut RUNETRIEMAP_NOTFOUND: *mut c_void = c"NOT FOUND".as_ptr() as *mut _;

/// User-supplied callback invoked by [`RuneTrieMap_Free`] and
/// [`RuneTrieMap_DeleteRune`] to release a payload. May be `NULL`, in
/// which case payloads are leaked (or were never owned, e.g. for the
/// dictionary path where they encode `f32` scores in their bit pattern).
pub type RuneTrieMapFreeCallback = Option<unsafe extern "C" fn(payload: *mut c_void)>;

/// Allocate a new empty [`RuneTrieMap`]. Release with [`RuneTrieMap_Free`].
#[unsafe(no_mangle)]
pub extern "C" fn RuneTrieMap_New() -> *mut RuneTrieMap {
    Box::into_raw(Box::new(RuneTrieMap(trie_rs::RuneTrieMap::new())))
}

/// Free the trie and, if `freecb` is non-NULL, call it on each stored
/// payload before destroying the structure.
///
/// # Safety
///
/// `t` must be a pointer obtained from [`RuneTrieMap_New`] and not yet
/// freed, or `NULL`. After this call the pointer is invalid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RuneTrieMap_Free(t: *mut RuneTrieMap, freecb: RuneTrieMapFreeCallback) {
    if t.is_null() {
        return;
    }
    // SAFETY: caller guarantees `t` came from `Box::into_raw` and has
    // not been freed yet.
    let trie = unsafe { Box::from_raw(t) };
    if let Some(cb) = freecb {
        for (_, &payload) in trie.0.iter() {
            // SAFETY: caller's contract — `cb` must be safe to call on
            // every payload previously inserted into this trie.
            unsafe { cb(payload) };
        }
    }
    drop(trie);
}

/// Number of unique keys currently stored.
///
/// # Safety
///
/// `t` must be a valid, non-NULL pointer from [`RuneTrieMap_New`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RuneTrieMap_Size(t: *const RuneTrieMap) -> usize {
    debug_assert!(!t.is_null(), "t cannot be NULL");
    // SAFETY: caller guarantees `t` is a valid pointer to a live trie.
    let RuneTrieMap(trie) = unsafe { &*t };
    trie.len()
}

/// Approximate memory usage of the trie in bytes (cached, O(1)).
///
/// # Safety
///
/// `t` must be a valid, non-NULL pointer from [`RuneTrieMap_New`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RuneTrieMap_MemUsage(t: *const RuneTrieMap) -> usize {
    debug_assert!(!t.is_null(), "t cannot be NULL");
    // SAFETY: caller guarantees `t` is a valid pointer to a live trie.
    let RuneTrieMap(trie) = unsafe { &*t };
    trie.mem_usage()
}

// ===== rune-keyed entry points (suffix path) ============================

/// Insert a payload under the given rune key. Returns `1` if the key was
/// new, `0` if it already existed (in which case `freecb`, if provided,
/// is invoked on the previous payload before it is overwritten).
///
/// # Safety
///
/// * `t` must be a valid, non-NULL pointer from [`RuneTrieMap_New`].
/// * `runes` must be valid for `len` reads, or be `NULL` when `len == 0`.
/// * If `freecb` is non-NULL, it must be safe to invoke on any payload
///   previously inserted into this trie.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RuneTrieMap_InsertRune(
    t: *mut RuneTrieMap,
    runes: *const rune,
    len: usize,
    payload: *mut c_void,
    freecb: RuneTrieMapFreeCallback,
) -> c_int {
    debug_assert!(!t.is_null(), "t cannot be NULL");
    // SAFETY: caller guarantees `t` is a valid pointer to a live trie.
    let RuneTrieMap(trie) = unsafe { &mut *t };

    let key: &[rune] = if len > 0 {
        debug_assert!(!runes.is_null(), "runes cannot be NULL when len > 0");
        // SAFETY: caller guarantees `runes` is valid for `len` reads.
        unsafe { slice::from_raw_parts(runes, len) }
    } else {
        &[]
    };

    let mut was_vacant = true;
    trie.insert_with(key, |old| {
        if let Some(old_payload) = old {
            was_vacant = false;
            if let Some(cb) = freecb {
                // SAFETY: caller's contract guarantees `cb` is safe to
                // invoke on payloads stored in this trie.
                unsafe { cb(old_payload) };
            }
        }
        payload
    });
    if was_vacant { 1 } else { 0 }
}

/// Find the payload stored at the given rune key, or [`RUNETRIEMAP_NOTFOUND`]
/// if absent. Stored `NULL` payloads are returned as `NULL` and are
/// distinct from the not-found sentinel.
///
/// # Safety
///
/// * `t` must be a valid, non-NULL pointer from [`RuneTrieMap_New`].
/// * `runes` must be valid for `len` reads, or be `NULL` when `len == 0`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RuneTrieMap_FindRune(
    t: *const RuneTrieMap,
    runes: *const rune,
    len: usize,
) -> *mut c_void {
    debug_assert!(!t.is_null(), "t cannot be NULL");
    // SAFETY: caller guarantees `t` is a valid pointer to a live trie.
    let RuneTrieMap(trie) = unsafe { &*t };

    let key: &[rune] = if len > 0 {
        debug_assert!(!runes.is_null(), "runes cannot be NULL when len > 0");
        // SAFETY: caller guarantees `runes` is valid for `len` reads.
        unsafe { slice::from_raw_parts(runes, len) }
    } else {
        &[]
    };

    match trie.find(key) {
        Some(p) => *p,
        None => {
            // SAFETY: reading the value of a static mut is sound; the
            // sentinel is only initialized at definition site.
            unsafe { RUNETRIEMAP_NOTFOUND }
        }
    }
}

/// Delete the entry at the given rune key. Returns `1` if removed, `0`
/// if the key was not present. When the key is removed and `freecb` is
/// non-NULL, the callback is invoked on the previous payload.
///
/// # Safety
///
/// * `t` must be a valid, non-NULL pointer from [`RuneTrieMap_New`].
/// * `runes` must be valid for `len` reads, or be `NULL` when `len == 0`.
/// * If `freecb` is non-NULL, it must be safe to invoke on payloads
///   previously inserted into this trie.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RuneTrieMap_DeleteRune(
    t: *mut RuneTrieMap,
    runes: *const rune,
    len: usize,
    freecb: RuneTrieMapFreeCallback,
) -> c_int {
    debug_assert!(!t.is_null(), "t cannot be NULL");
    // SAFETY: caller guarantees `t` is a valid pointer to a live trie.
    let RuneTrieMap(trie) = unsafe { &mut *t };

    let key: &[rune] = if len > 0 {
        debug_assert!(!runes.is_null(), "runes cannot be NULL when len > 0");
        // SAFETY: caller guarantees `runes` is valid for `len` reads.
        unsafe { slice::from_raw_parts(runes, len) }
    } else {
        &[]
    };

    match trie.remove(key) {
        Some(payload) => {
            if let Some(cb) = freecb {
                // SAFETY: caller's contract.
                unsafe { cb(payload) };
            }
            1
        }
        None => 0,
    }
}

// ===== UTF-8-keyed entry points (dictionary path) ======================

/// Insert (or update) a UTF-8 keyed entry with an associated `f32`
/// score. If the key was new, the score is stored as-is and the
/// function returns `1`. If the key already exists:
/// * with `incr == 0`, the stored score is replaced with the new score;
/// * with `incr != 0`, the new score is added to the existing one.
///
/// In either update path the function returns `0`.
///
/// The score is encoded directly in the payload pointer (see
/// [`encode_score`]); no heap allocation occurs per entry.
///
/// # Safety
///
/// * `t` must be a valid, non-NULL pointer from [`RuneTrieMap_New`].
/// * `s` must be valid for `len` bytes, or be `NULL` when `len == 0`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RuneTrieMap_InsertStr(
    t: *mut RuneTrieMap,
    s: *const c_char,
    len: usize,
    score: f32,
    incr: c_int,
) -> c_int {
    debug_assert!(!t.is_null(), "t cannot be NULL");
    // SAFETY: caller guarantees `s` is valid for `len` reads.
    let runes = unsafe { utf8_to_runes(s, len) };
    if runes.is_empty() {
        return 0;
    }
    // SAFETY: caller guarantees `t` is a valid pointer to a live trie.
    let RuneTrieMap(trie) = unsafe { &mut *t };

    let mut was_vacant = true;
    trie.insert_with(&runes, |old| {
        if let Some(old_payload) = old {
            was_vacant = false;
            if incr != 0 {
                let combined = decode_score(old_payload) + score;
                encode_score(combined)
            } else {
                encode_score(score)
            }
        } else {
            encode_score(score)
        }
    });
    if was_vacant { 1 } else { 0 }
}

/// Look up the `f32` score for a UTF-8 keyed entry. Returns `1` if the
/// key was found (writing the score through `out_score`), `0` otherwise.
/// `out_score` may be `NULL` to query existence only.
///
/// # Safety
///
/// * `t` must be a valid, non-NULL pointer from [`RuneTrieMap_New`].
/// * `s` must be valid for `len` bytes, or be `NULL` when `len == 0`.
/// * `out_score` may be `NULL` or must point to writable `f32` storage.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RuneTrieMap_FindStr(
    t: *const RuneTrieMap,
    s: *const c_char,
    len: usize,
    out_score: *mut f32,
) -> c_int {
    debug_assert!(!t.is_null(), "t cannot be NULL");
    // SAFETY: caller guarantees `s` is valid for `len` bytes.
    let runes = unsafe { utf8_to_runes(s, len) };
    if runes.is_empty() {
        return 0;
    }
    // SAFETY: caller guarantees `t` is a valid pointer to a live trie.
    let RuneTrieMap(trie) = unsafe { &*t };

    match trie.find(&runes) {
        Some(&payload) => {
            if !out_score.is_null() {
                // SAFETY: caller's contract: `out_score` is writable.
                unsafe { out_score.write(decode_score(payload)) };
            }
            1
        }
        None => 0,
    }
}

/// Delete a UTF-8 keyed entry. Returns `1` if removed, `0` otherwise.
///
/// No payload free is performed because the dictionary path stores
/// scores inline in the payload slot rather than allocated payloads.
///
/// # Safety
///
/// * `t` must be a valid, non-NULL pointer from [`RuneTrieMap_New`].
/// * `s` must be valid for `len` bytes, or be `NULL` when `len == 0`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RuneTrieMap_DeleteStr(
    t: *mut RuneTrieMap,
    s: *const c_char,
    len: usize,
) -> c_int {
    debug_assert!(!t.is_null(), "t cannot be NULL");
    // SAFETY: caller guarantees `s` is valid for `len` bytes.
    let runes = unsafe { utf8_to_runes(s, len) };
    if runes.is_empty() {
        return 0;
    }
    // SAFETY: caller guarantees `t` is a valid pointer to a live trie.
    let RuneTrieMap(trie) = unsafe { &mut *t };
    if trie.remove(&runes).is_some() { 1 } else { 0 }
}
