/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! RDB serialization for the dictionary path.
//!
//! [`RuneTrieMap_RdbSaveDict`] / [`RuneTrieMap_RdbLoadDict`] mirror the
//! byte format produced by `TrieType_GenericSave(rdb, t, false, false)`
//! and consumed by `TrieType_GenericLoad(rdb, false, false)` — the
//! configuration used by `src/dictionary.c`'s spell-check dictionary
//! aux save/load. The wire format per entry is:
//!
//! ```text
//!   uint64  element_count
//!   for each element:
//!     string  utf8_key      (length includes a trailing NUL byte)
//!     double  score         (stored f32 widened to f64)
//! ```
//!
//! Suffix-path payloads are not RDB-persisted and so do not appear here.

use crate::{RuneTrieMap, decode_score, encode_score, runes_to_utf8, utf8_to_runes};
use redis_module::raw::RedisModuleIO;
use std::ffi::c_void;

/// Serialize the dictionary trie to RDB. Produces the exact byte
/// sequence written by `TrieType_GenericSave(rdb, trie, false, false)`,
/// allowing in-place replacement of the legacy save callback.
///
/// # Safety
///
/// * `rdb` must be a valid `RedisModuleIO` pointer for the active save
///   context.
/// * `t` must be a valid, non-NULL pointer from `RuneTrieMap_New`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RuneTrieMap_RdbSaveDict(rdb: *mut RedisModuleIO, t: *const RuneTrieMap) {
    debug_assert!(!t.is_null(), "t cannot be NULL");
    // SAFETY: caller guarantees `t` is a valid pointer to a live trie.
    let RuneTrieMap(trie) = unsafe { &*t };

    // The Redis module API exposes RDB I/O entry points as static-mut
    // function pointers; we read them once per save. Each access is its
    // own unsafe block so the safety justification is local.
    // SAFETY: callers exercise this only inside a Redis aux-save hook,
    // where the RDB I/O hooks are populated.
    let save_unsigned = unsafe {
        redis_module::raw::RedisModule_SaveUnsigned.expect("RedisModule_SaveUnsigned not available")
    };
    // SAFETY: same justification as `save_unsigned` above.
    let save_string = unsafe {
        redis_module::raw::RedisModule_SaveStringBuffer
            .expect("RedisModule_SaveStringBuffer not available")
    };
    // SAFETY: same justification as `save_unsigned` above.
    let save_double = unsafe {
        redis_module::raw::RedisModule_SaveDouble.expect("RedisModule_SaveDouble not available")
    };

    // SAFETY: `rdb` is a valid IO context per caller's contract.
    unsafe { save_unsigned(rdb, trie.len() as u64) };

    for (runes, &payload) in trie.iter() {
        let mut utflen: usize = 0;
        // SAFETY: `&mut utflen` is a valid mutable pointer.
        let s = unsafe { runes_to_utf8(&runes, &mut utflen as *mut usize) };
        if s.is_null() {
            continue;
        }
        // SAFETY: `s` is valid for `utflen + 1` bytes (NUL terminator
        // included by `runesToStr`); `rdb` is a valid IO context.
        unsafe { save_string(rdb, s, utflen + 1) };
        // SAFETY: `rdb` is a valid IO context per caller's contract.
        unsafe { save_double(rdb, decode_score(payload) as f64) };
        // SAFETY: `s` is rm_malloc-allocated; pair with `RedisModule_Free`.
        unsafe { rm_free(s as *mut c_void) };
    }
}

/// Deserialize the dictionary trie from RDB. Allocates a fresh
/// [`RuneTrieMap`] (release via [`crate::RuneTrieMap_Free`] with a NULL
/// `freecb`, since payloads are inline scores).
///
/// Returns `NULL` on a malformed stream (currently any underflow leaves
/// the partially-built trie in place; a stricter contract can be added
/// later if needed).
///
/// # Safety
///
/// `rdb` must be a valid `RedisModuleIO` pointer for the active load
/// context.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RuneTrieMap_RdbLoadDict(rdb: *mut RedisModuleIO) -> *mut RuneTrieMap {
    // SAFETY: callers exercise this only inside a Redis aux-load hook.
    let load_unsigned = unsafe {
        redis_module::raw::RedisModule_LoadUnsigned.expect("RedisModule_LoadUnsigned not available")
    };
    // SAFETY: same justification as `load_unsigned` above.
    let load_string = unsafe {
        redis_module::raw::RedisModule_LoadStringBuffer
            .expect("RedisModule_LoadStringBuffer not available")
    };
    // SAFETY: same justification as `load_unsigned` above.
    let load_double = unsafe {
        redis_module::raw::RedisModule_LoadDouble.expect("RedisModule_LoadDouble not available")
    };

    // SAFETY: `rdb` is a valid IO context per caller's contract.
    let elements = unsafe { load_unsigned(rdb) };

    let mut trie = trie_rs::RuneTrieMap::<*mut c_void>::new();

    for _ in 0..elements {
        let mut len: usize = 0;
        // SAFETY: `&mut len` is a valid mutable pointer; `rdb` is valid.
        let s = unsafe { load_string(rdb, &mut len as *mut usize) };
        if s.is_null() || len == 0 {
            // Bail out, leaving any prior entries; the caller's outer
            // aux-load handler decides cleanup policy.
            return Box::into_raw(Box::new(RuneTrieMap(trie)));
        }
        // SAFETY: `rdb` is a valid IO context.
        let score = unsafe { load_double(rdb) };

        // The stored length includes the trailing NUL written on save;
        // strip it before decoding to runes.
        // SAFETY: `s` is valid for `len` bytes per `LoadStringBuffer`'s
        // contract.
        let runes = unsafe { utf8_to_runes(s, len.saturating_sub(1)) };
        // SAFETY: `s` was rm_malloc-allocated by `LoadStringBuffer`;
        // pair with `RedisModule_Free`.
        unsafe { rm_free(s as *mut c_void) };

        if !runes.is_empty() {
            trie.insert_replace(&runes, encode_score(score as f32));
        }
    }

    Box::into_raw(Box::new(RuneTrieMap(trie)))
}

unsafe fn rm_free(p: *mut c_void) {
    // SAFETY: caller passes an `rm_malloc`-allocated pointer; the Redis
    // allocator must be initialized for the FFI crate to be exercised.
    let free =
        unsafe { redis_module::raw::RedisModule_Free.expect("Redis allocator not available") };
    // SAFETY: `free` is non-NULL (checked above); `p` is valid.
    unsafe { free(p) };
}
