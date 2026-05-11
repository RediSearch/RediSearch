/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Lex-mode trie wrapper backed by [`trie_rs::TrieMap`].
//!
//! Spike for porting `src/trie/` (lex mode only). Replaces the C trie used by
//! `src/dictionary.c` while keeping the `rune` (16-bit) key model so callers
//! can continue to use `runesToStr` on iteration output.
//!
//! ## Key encoding
//!
//! Keys arrive as UTF-8 byte slices, are decoded one codepoint per rune
//! (truncated to `u16`), and stored as big-endian byte pairs. Big-endian was
//! chosen so byte-lex order on the encoded key matches rune-lex order on the
//! original input, mirroring the C trie's `Trie_Sort_Lex` semantics.
//!
//! ## Scope
//!
//! Only the operations called by `src/dictionary.c` are exposed: construction,
//! `InsertStringBuffer`, `Delete`, `Size`, `Free`, full lex-order iteration,
//! and RDB save/load matching the C `TrieType_GenericSave/Load` binary format
//! with `savePayloads=false, saveNumDocs=false`.

#![allow(non_camel_case_types, non_snake_case)]

use std::ffi::{c_char, c_int};
use std::slice;
use trie_rs::TrieMap;

mod encoding;
mod iter;
mod rdb;

pub use encoding::{trie_lex_rune, trie_lex_t_len};
pub use iter::*;
pub use rdb::*;

/// Per-entry data stored alongside each lex-trie key.
///
/// `payload` and `num_docs` are unused by `dictionary.c` and are not
/// exposed/persisted in this spike, but the struct is kept open so a future
/// branch can fill in the suffix-trie / suggestions use-cases.
#[derive(Clone, Debug)]
pub struct TrieLexEntry {
    pub score: f32,
}

/// Opaque wrapper around [`TrieMap<TrieLexEntry>`].
///
/// cheadergen emits this as a bare `typedef struct TrieLex TrieLex;` since
/// the generic field is not `#[repr(C)]`.
pub struct TrieLex(pub TrieMap<TrieLexEntry>);

/// Maximum number of runes per key, mirroring `TRIE_INITIAL_STRING_LEN` in
/// `src/trie/trie.h`. Inserts that would exceed this are silently dropped, as
/// in `Trie_InsertStringBuffer`.
pub const TRIE_LEX_MAX_RUNES: usize = 256;

/// Allocate an empty trie. Returned pointer must be freed with [`TrieLex_Free`].
#[unsafe(no_mangle)]
pub extern "C" fn TrieLex_New() -> *mut TrieLex {
    Box::into_raw(Box::new(TrieLex(TrieMap::new())))
}

/// Free a trie produced by [`TrieLex_New`] (or [`TrieLex_RdbLoad`]).
///
/// # Safety
///
/// * `t` must be either NULL or a pointer previously returned by
///   [`TrieLex_New`] or [`TrieLex_RdbLoad`].
/// * No live `TrieLexIterator` may borrow from `t` when this is called.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TrieLex_Free(t: *mut TrieLex) {
    if t.is_null() {
        return;
    }
    // SAFETY: caller invariant: `t` came from `Box::into_raw` in `TrieLex_New`
    // (or `TrieLex_RdbLoad`) and is not aliased.
    drop(unsafe { Box::from_raw(t) });
}

/// Number of unique keys currently stored.
///
/// # Safety
///
/// `t` must point to a valid trie allocated by [`TrieLex_New`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TrieLex_Size(t: *const TrieLex) -> usize {
    debug_assert!(!t.is_null(), "t cannot be NULL");
    // SAFETY: caller invariant ensures `t` is a valid, non-null pointer to a
    // `TrieLex` and that no exclusive reference exists concurrently.
    let TrieLex(trie) = unsafe { &*t };
    trie.n_unique_keys()
}

/// Insert a UTF-8 key. Returns 1 if the key was new, 0 if it already existed
/// or was rejected by the length guard.
///
/// `incr` mirrors the C `Trie_InsertStringBuffer(..., incr, ...)` flag:
/// when non-zero, the supplied `score` is *added* to the existing entry's
/// score; when zero, the score is replaced.
///
/// # Safety
///
/// * `t` must point to a valid trie allocated by [`TrieLex_New`].
/// * If `len > 0`, `s` must point to at least `len` initialized bytes.
/// * If `len == 0`, `s` may be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TrieLex_InsertStringBuffer(
    t: *mut TrieLex,
    s: *const c_char,
    len: usize,
    score: f64,
    incr: c_int,
) -> c_int {
    debug_assert!(!t.is_null(), "t cannot be NULL");
    // SAFETY: caller invariant: `t` is a valid `TrieLex` from `TrieLex_New`.
    let TrieLex(trie) = unsafe { &mut *t };

    let utf8: &[u8] = if len == 0 {
        b""
    } else {
        debug_assert!(!s.is_null(), "s cannot be NULL if len > 0");
        // SAFETY: caller invariant: `s` points to `len` valid bytes.
        unsafe { slice::from_raw_parts(s.cast::<u8>(), len) }
    };

    let key = encoding::utf8_to_be_runes(utf8);
    let rune_count = key.len() / 2;
    if rune_count == 0 || rune_count >= TRIE_LEX_MAX_RUNES {
        return 0;
    }

    let mut was_new: c_int = 0;
    let score_f32 = score as f32;
    trie.insert_with(&key, |old| match old {
        Some(mut existing) => {
            if incr != 0 {
                existing.score += score_f32;
            } else {
                existing.score = score_f32;
            }
            existing
        }
        None => {
            was_new = 1;
            TrieLexEntry { score: score_f32 }
        }
    });
    was_new
}

/// Delete a UTF-8 key. Returns 1 if the key existed and was removed, 0
/// otherwise.
///
/// # Safety
///
/// * `t` must point to a valid trie allocated by [`TrieLex_New`].
/// * If `len > 0`, `s` must point to at least `len` initialized bytes.
/// * If `len == 0`, `s` may be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TrieLex_Delete(t: *mut TrieLex, s: *const c_char, len: usize) -> c_int {
    debug_assert!(!t.is_null(), "t cannot be NULL");
    // SAFETY: caller invariant: `t` is a valid `TrieLex`.
    let TrieLex(trie) = unsafe { &mut *t };

    let utf8: &[u8] = if len == 0 {
        b""
    } else {
        debug_assert!(!s.is_null(), "s cannot be NULL if len > 0");
        // SAFETY: caller invariant: `s` points to `len` valid bytes.
        unsafe { slice::from_raw_parts(s.cast::<u8>(), len) }
    };

    let key = encoding::utf8_to_be_runes(utf8);
    let rune_count = key.len() / 2;
    if rune_count == 0 || rune_count >= TRIE_LEX_MAX_RUNES {
        return 0;
    }

    if trie.remove(&key).is_some() { 1 } else { 0 }
}

