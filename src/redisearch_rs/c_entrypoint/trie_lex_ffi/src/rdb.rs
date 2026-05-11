/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! RDB save/load matching the C `TrieType_GenericSave/Load` wire format with
//! `savePayloads=false, saveNumDocs=false`.
//!
//! Format:
//!
//! ```text
//! u64        : element count
//! per entry  :
//!   StringBuffer (UTF-8, includes trailing `\0`) — `len + 1` bytes
//!   double                                       — score
//! ```
//!
//! Matching the C format byte-for-byte means an RDB written by either side is
//! loadable by the other. We use [`redis_module::raw`] for the
//! `RedisModule_Save*` / `RedisModule_Load*` calls.

use crate::TrieLex;
use crate::TrieLexEntry;
use crate::encoding::{be_runes_to_utf8, utf8_to_be_runes};
use lending_iterator::LendingIterator;
use redis_module::raw;
use redis_module::raw::RedisModuleIO;
use std::ptr;

/// Serialize `t` to the RDB stream in the C-compatible format.
///
/// # Safety
///
/// * `rdb` must be a non-null `RedisModuleIO*` provided by Redis to an active
///   save callback.
/// * `t` must point to a valid [`TrieLex`] allocated by
///   [`crate::TrieLex_New`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TrieLex_RdbSave(rdb: *mut RedisModuleIO, t: *const TrieLex) {
    debug_assert!(!rdb.is_null(), "rdb cannot be NULL");
    debug_assert!(!t.is_null(), "t cannot be NULL");

    // SAFETY: caller invariant: `t` is a valid, live `TrieLex`.
    let TrieLex(trie) = unsafe { &*t };

    raw::save_unsigned(rdb, trie.n_unique_keys() as u64);

    let mut iter = trie.lending_iter();
    while let Some((key_bytes, entry)) = iter.next() {
        let utf8 = be_runes_to_utf8(key_bytes);
        // The C side saves `slen + 1` bytes including a trailing NUL. We
        // replicate that: build a buffer ending in `\0`.
        let mut buf = Vec::with_capacity(utf8.len() + 1);
        buf.extend_from_slice(utf8.as_bytes());
        buf.push(0);
        raw::save_slice(rdb, &buf);
        raw::save_double(rdb, entry.score as f64);
    }
}

/// Deserialize a trie from the RDB stream. Returns NULL on I/O error.
///
/// # Safety
///
/// * `rdb` must be a non-null `RedisModuleIO*` provided by Redis to an active
///   load callback.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TrieLex_RdbLoad(rdb: *mut RedisModuleIO) -> *mut TrieLex {
    debug_assert!(!rdb.is_null(), "rdb cannot be NULL");

    let count = match raw::load_unsigned(rdb) {
        Ok(v) => v,
        Err(_) => return ptr::null_mut(),
    };

    let trie = Box::new(TrieLex(trie_rs::TrieMap::new()));
    let mut trie = trie;

    for _ in 0..count {
        let buf = match raw::load_string_buffer(rdb) {
            Ok(b) => b,
            Err(_) => return ptr::null_mut(),
        };
        let score = match raw::load_double(rdb) {
            Ok(s) => s,
            Err(_) => return ptr::null_mut(),
        };
        // The C save path writes `len + 1` bytes including a trailing NUL.
        // Strip it; tolerate a missing terminator just in case.
        let slice = buf.as_ref();
        let utf8 = if slice.last() == Some(&0) {
            &slice[..slice.len() - 1]
        } else {
            slice
        };
        let key = utf8_to_be_runes(utf8);
        let rune_count = key.len() / 2;
        if rune_count == 0 || rune_count >= crate::TRIE_LEX_MAX_RUNES {
            continue;
        }
        trie.0.insert(&key, TrieLexEntry { score: score as f32 });
    }

    Box::into_raw(trie)
}
