/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! RDB persistence entry points for a [`SpellCheckDictionary`].
//!
//! Thin wrappers over [`SpellCheckDictionary::rdb_save`] /
//! [`SpellCheckDictionary::rdb_load`], driven through the `RdbIO` impl the
//! `rdb_io` crate provides for [`redis_module::RedisModuleIO`].
//! The wire format and its fixed options (no payloads, no `num_docs`, score
//! normalized to 1) are documented on those methods.

use ffi::RedisModuleIO;
use redis_module::{RedisModuleIO as RmIo, raw};

use super::SpellCheckDictionary;

/// Serialize a [`SpellCheckDictionary`] to `io`.
///
/// Emits the same stream the C aux save callback writes per dict
/// (`TrieType_GenericSave` without payloads or `num_docs`). Save is
/// infallible at this layer; any underlying RDB IO error surfaces later via
/// `RedisModule_IsIOError` on the load side.
///
/// # Safety
///
/// 1. `io` must be a [valid], non-null `*mut RedisModuleIO` supplied by the
///    calling Redis module save callback, and remain valid for the duration
///    of the call.
/// 2. `dict` must be a [valid], non-null pointer obtained from
///    [`SpellCheckDictionary_New`] or [`SpellCheckDictionary_RdbLoad`]. No
///    mutating call on `dict` may run concurrently with this call.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
/// [`SpellCheckDictionary_New`]: super::SpellCheckDictionary_New
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SpellCheckDictionary_RdbSave(
    io: *mut RedisModuleIO,
    dict: *const SpellCheckDictionary,
) {
    debug_assert!(!io.is_null(), "io cannot be NULL");
    debug_assert!(!dict.is_null(), "dict cannot be NULL");

    // Safety: ensured by caller (2.)
    let dictionary = unsafe { &*dict };
    // Wrapping the pointer is safe; the caller's validity guarantee (1.)
    // covers the RDB writes performed through it for the duration of the call.
    let mut rm_io = RmIo::new(io.cast::<raw::RedisModuleIO>());
    dictionary.rdb_save(&mut rm_io);
}

/// Deserialize a [`SpellCheckDictionary`] from `io`, accepting the stream
/// the C aux save callback writes per dict. Returns NULL on any RDB IO or
/// framing error (including non-UTF-8 keys), matching the C contract of
/// `TrieType_GenericLoad`.
///
/// On success, the caller owns the returned pointer and must release it via
/// [`SpellCheckDictionary_Free`].
///
/// # Safety
///
/// 1. `io` must be a [valid], non-null `*mut RedisModuleIO` supplied by the
///    calling Redis module load callback, and remain valid for the duration
///    of the call.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
/// [`SpellCheckDictionary_Free`]: super::SpellCheckDictionary_Free
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SpellCheckDictionary_RdbLoad(
    io: *mut RedisModuleIO,
) -> *mut SpellCheckDictionary {
    debug_assert!(!io.is_null(), "io cannot be NULL");

    // Wrapping the pointer is safe; the caller's validity guarantee (1.)
    // covers the RDB reads performed through it for the duration of the call.
    let mut rm_io = RmIo::new(io.cast::<raw::RedisModuleIO>());
    match SpellCheckDictionary::rdb_load(&mut rm_io) {
        Ok(dictionary) => Box::into_raw(Box::new(dictionary)),
        Err(_) => std::ptr::null_mut(),
    }
}
