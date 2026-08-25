/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! FFI transport for the trie RDB serialization.
//!
//! Bridges the pure-Rust framing in [`trie_rdb`] to Redis' RDB IO primitives.
//! [`trie_rdb`]'s serializers are generic over the shared `rdb_io::RdbIO`
//! trait, and the `rdb_io` crate implements that trait directly for
//! [`redis_module::RedisModuleIO`]. So each `extern "C"` entry point below just
//! wraps the caller's raw [`redis_module::raw::RedisModuleIO`] pointer in that
//! wrapper and hands it to the generic serializer — no per-crate bridge type is
//! needed.

#![allow(non_camel_case_types, non_snake_case)]

use redis_module::{RedisModuleIO as RmIo, raw::RedisModuleIO};
use trie_rdb::RdbOpts;
use trie_rdb::TrieEntry;
use trie_rdb::str_trie_map as str_rdb;
use trie_rs::str_trie_map::StrTrieMap;

/// Opaque FFI handle for a [`StrTrieMap<TrieEntry>`].
///
/// Construct via [`LexTrieRs_New`] or [`LexTrieRs_RdbLoad`].
/// Free via [`LexTrieRs_Free`].
pub struct LexTrieRs(StrTrieMap<TrieEntry>);

/// Create a new, empty [`LexTrieRs`].
///
/// The returned pointer owns its allocation and must be released through
/// [`LexTrieRs_Free`].
#[unsafe(no_mangle)]
pub extern "C" fn LexTrieRs_New() -> *mut LexTrieRs {
    Box::into_raw(Box::new(LexTrieRs(StrTrieMap::new())))
}

/// Free a [`LexTrieRs`] previously produced by [`LexTrieRs_New`] or
/// [`LexTrieRs_RdbLoad`].
///
/// # Safety
///
/// - `t` must be a non-NULL pointer previously returned by
///   [`LexTrieRs_New`] / [`LexTrieRs_RdbLoad`] and not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn LexTrieRs_Free(t: *mut LexTrieRs) {
    debug_assert!(!t.is_null(), "t cannot be NULL");
    // SAFETY: caller guarantees `t` came from `Box::into_raw` in this
    // module and has not been freed. Reconstructing the `Box` here drops
    // it and releases the allocation.
    drop(unsafe { Box::from_raw(t) });
}

/// Serialize a [`LexTrieRs`] to `io` in the trie RDB wire format.
///
/// Mirrors the C function `TrieType_GenericSave` for a Rust-side trie.
/// Save doesn't report errors at this layer; any underlying RDB IO error surfaces
/// later via `RedisModule_IsIOError` on the load side.
///
/// # Safety
///
/// - `io` must be a [valid] `*mut RedisModuleIO` supplied by the calling
///   Redis module command and remain valid for the duration of the call.
/// - `map` must be a [valid] pointer to a [`LexTrieRs`] (typically obtained
///   from [`LexTrieRs_New`] / [`LexTrieRs_RdbLoad`]). It is borrowed
///   immutably for the duration of the call; no aliasing mutable
///   references must exist.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn LexTrieRs_RdbSave(
    io: *mut RedisModuleIO,
    map: *const LexTrieRs,
    save_payloads: bool,
    save_num_docs: bool,
) {
    debug_assert!(!io.is_null(), "io cannot be NULL");
    debug_assert!(!map.is_null(), "map cannot be NULL");

    // SAFETY: caller guarantees `map` is a valid `*const LexTrieRs`
    // and that no aliasing mutable references exist for the call.
    let map = unsafe { &*map };
    let mut rm_io = RmIo::new(io);
    let opts = RdbOpts {
        payloads: save_payloads,
        num_docs: save_num_docs,
    };
    str_rdb::save(&map.0, &mut rm_io, opts);
}

/// Deserialize a [`LexTrieRs`] from `io` in the trie RDB wire format.
///
/// Mirrors the C function `TrieType_GenericLoad` for a Rust-side trie,
/// including its NULL return on any RDB IO or framing error.
///
/// On success, the caller owns the returned pointer and must release it
/// via [`LexTrieRs_Free`].
///
/// # Safety
///
/// - `io` must be a [valid] `*mut RedisModuleIO` supplied by the calling
///   Redis module type loader and remain valid for the duration of the
///   call.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn LexTrieRs_RdbLoad(
    io: *mut RedisModuleIO,
    load_payloads: bool,
    load_num_docs: bool,
) -> *mut LexTrieRs {
    debug_assert!(!io.is_null(), "io cannot be NULL");

    let mut rm_io = RmIo::new(io);
    let opts = RdbOpts {
        payloads: load_payloads,
        num_docs: load_num_docs,
    };
    match str_rdb::load(&mut rm_io, opts) {
        Ok(map) => Box::into_raw(Box::new(LexTrieRs(map))),
        Err(_) => std::ptr::null_mut(),
    }
}
