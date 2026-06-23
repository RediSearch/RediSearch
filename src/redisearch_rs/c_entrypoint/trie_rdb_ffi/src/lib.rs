/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! FFI transport for the lex-mode trie RDB serialization.
//!
//! Bridges the pure-Rust framing in [`trie_rdb`] to Redis' RDB IO
//! primitives (`RedisModule_SaveUnsigned`, `RedisModule_LoadStringBuffer`,
//! and friends), exposed through the higher-level wrappers in
//! [`redis_module::raw`].
//!
//! [`trie_rdb`] models save/load as the [`RdbWrite`]/[`RdbRead`] traits;
//! this crate supplies impls backed by a `*mut RedisModuleIO`. The
//! `extern "C"` entry points below let C callers drive the round-trip
//! without touching the Rust generics.

#![allow(non_camel_case_types, non_snake_case)]

use ffi::RedisModuleIO;
use redis_module::raw;
use trie_rdb::TrieEntry;
use trie_rdb::str as str_rdb;
use trie_rdb::{RdbError, RdbOpts, RdbRead, RdbWrite};
use trie_rs::str_trie_map::StrTrieMap;

/// Opaque FFI handle for a [`StrTrieMap<TrieEntry>`].
///
/// Distinct from the void-payload `TrieMap` exposed by the `triemap_ffi`
/// crate, so the two C symbol sets do not collide. Keys flow through
/// the UTF-8-validating [`str_rdb`] loader, so non-UTF-8 RDB payloads
/// surface as `NULL` from [`LexTrieRs_RdbLoad`]. Construct via
/// [`LexTrieRs_New`] or [`LexTrieRs_RdbLoad`]; free via [`LexTrieRs_Free`].
pub struct LexTrieRs(pub StrTrieMap<TrieEntry>);

/// [`RdbWrite`] backed by a raw `RedisModuleIO*`.
///
/// The pointer is captured at construction and never stored anywhere else.
/// Validity is the caller's responsibility (see the `Safety` blocks on the
/// `extern "C"` entry points below). NUL framing and the scratch buffer
/// that amortizes its allocation are owned by [`trie_rdb::byte::save`]; this
/// impl just forwards each slice straight to `RedisModule_SaveStringBuffer`.
struct RmIoWriter {
    io: *mut raw::RedisModuleIO,
}

impl RdbWrite for RmIoWriter {
    fn save_u64(&mut self, v: u64) {
        raw::save_unsigned(self.io, v);
    }

    fn save_f64(&mut self, v: f64) {
        raw::save_double(self.io, v);
    }

    fn save_bytes(&mut self, b: &[u8]) {
        raw::save_slice(self.io, b);
    }
}

/// [`RdbRead`] backed by a raw `RedisModuleIO*`.
///
/// The wrapped `redis_module::raw::load_*` calls already poll
/// `RedisModule_IsIOError` after each underlying `RedisModule_Load*` and
/// surface failures as `Err(short_read)`, so we do not need a separate
/// `IsIOError` check.
struct RmIoReader {
    io: *mut raw::RedisModuleIO,
}

impl RdbRead for RmIoReader {
    fn load_u64(&mut self) -> Result<u64, RdbError> {
        raw::load_unsigned(self.io).map_err(|_| RdbError::Io)
    }

    fn load_f64(&mut self) -> Result<f64, RdbError> {
        raw::load_double(self.io).map_err(|_| RdbError::Io)
    }

    fn load_bytes(&mut self) -> Result<Vec<u8>, RdbError> {
        raw::load_string_buffer(self.io)
            .map(|buf| buf.as_ref().to_vec())
            .map_err(|_| RdbError::Io)
    }
}

/// Allocate an empty [`LexTrieRs`] on the Rust heap.
///
/// The returned pointer owns its allocation and must be released through
/// [`LexTrieRs_Free`].
#[unsafe(no_mangle)]
pub extern "C" fn LexTrieRs_New() -> *mut LexTrieRs {
    Box::into_raw(Box::new(LexTrieRs(StrTrieMap::new())))
}

/// Free a [`LexTrieRs`] previously produced by [`LexTrieRs_New`] or
/// [`LexTrieRs_RdbLoad`]. A NULL pointer is a no-op.
///
/// # Safety
///
/// - `t` must be either NULL or a pointer previously returned by
///   [`LexTrieRs_New`] / [`LexTrieRs_RdbLoad`] and not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn LexTrieRs_Free(t: *mut LexTrieRs) {
    if t.is_null() {
        return;
    }
    // SAFETY: caller guarantees `t` came from `Box::into_raw` in this
    // module and has not been freed. Reconstructing the `Box` here drops
    // it and releases the allocation.
    drop(unsafe { Box::from_raw(t) });
}

/// Serialize a [`LexTrieRs`] to `io` in the lex-mode RDB wire format.
///
/// Mirrors the C function `TrieType_GenericSave` for a Rust-side trie.
/// Save is infallible at this layer; any underlying RDB IO error surfaces
/// later via `RedisModule_IsIOError` on the load side.
///
/// # Safety
///
/// - `io` must be a valid `*mut RedisModuleIO` supplied by the calling
///   Redis module command and remain valid for the duration of the call.
/// - `map` must be a valid pointer to a [`LexTrieRs`] (typically obtained
///   from [`LexTrieRs_New`] / [`LexTrieRs_RdbLoad`]). It is borrowed
///   immutably for the duration of the call; no aliasing mutable
///   references must exist.
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
    let mut w = RmIoWriter {
        io: io.cast::<raw::RedisModuleIO>(),
    };
    let opts = RdbOpts {
        payloads: save_payloads,
        num_docs: save_num_docs,
    };
    str_rdb::save(&map.0, &mut w, opts);
}

/// Deserialize a [`LexTrieRs`] from `io` in the lex-mode RDB wire format.
///
/// Mirrors the C function `TrieType_GenericLoad` for a Rust-side trie.
/// Returns NULL on any RDB IO or framing error, matching the C contract
/// for `TrieType_GenericLoad` at `src/trie/trie.c:431-438`.
///
/// On success, the caller owns the returned pointer and must release it
/// via [`LexTrieRs_Free`].
///
/// # Safety
///
/// - `io` must be a valid `*mut RedisModuleIO` supplied by the calling
///   Redis module type loader and remain valid for the duration of the
///   call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn LexTrieRs_RdbLoad(
    io: *mut RedisModuleIO,
    load_payloads: bool,
    load_num_docs: bool,
) -> *mut LexTrieRs {
    debug_assert!(!io.is_null(), "io cannot be NULL");

    let mut r = RmIoReader {
        io: io.cast::<raw::RedisModuleIO>(),
    };
    let opts = RdbOpts {
        payloads: load_payloads,
        num_docs: load_num_docs,
    };
    match str_rdb::load(&mut r, opts) {
        Ok(map) => Box::into_raw(Box::new(LexTrieRs(map))),
        Err(_) => std::ptr::null_mut(),
    }
}
