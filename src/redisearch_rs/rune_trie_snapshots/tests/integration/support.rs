/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! FFI helpers shared by the snapshot test modules.
//!
//! The helpers exist so each test module can spend its lines on the *scenario*
//! it pins, not on boilerplate around `Trie_InsertStringBuffer`,
//! `Trie_IterateAll`, and rune→UTF-8 decoding. Anything that varies per
//! scenario (custom dump formats, scenario-specific assertions) stays local to
//! the test module.

use std::fmt::Write as _;
use std::ptr;

use ffi::{
    RSPayload, Trie, Trie_Delete, Trie_InsertStringBuffer, Trie_IterateAll, Trie_Size,
    TrieIterator_Free, TrieIterator_Next, rune, t_len,
};
use libc::c_char;

pub const ADD_REPLACE: i32 = 0;
pub const ADD_INCR: i32 = 1;

/// Decode a rune buffer (BMP-only `u16` codepoints) into a Rust `String`.
///
/// SAFETY: `ptr` must point to `len` initialized runes.
pub unsafe fn runes_to_string(ptr: *const rune, len: usize) -> String {
    let slice = unsafe { std::slice::from_raw_parts(ptr, len) };
    String::from_utf16(slice).expect("trie runes are valid BMP UTF-16")
}

/// UTF-16 encode `s` for passing to FFI calls that take `(*const rune, len)`.
pub fn encode_runes(s: &str) -> Vec<rune> {
    s.encode_utf16().collect()
}

/// Call `Trie_InsertStringBuffer` with the given parameters and return the C rc.
///
/// SAFETY: `term.as_bytes()` is a valid borrow for the call; the C trie copies
/// the buffer internally before returning.
pub unsafe fn trie_insert_full(
    trie: *mut Trie,
    term: &str,
    score: f64,
    incr: i32,
    num_docs: usize,
) -> i32 {
    unsafe {
        Trie_InsertStringBuffer(
            trie,
            term.as_ptr() as *const c_char,
            term.len(),
            score,
            incr,
            ptr::null_mut(),
            num_docs,
        )
    }
}

/// `ADD_REPLACE` insert with score=1.0, numDocs=1. Returns the C rc.
///
/// SAFETY: see [`trie_insert_full`].
pub unsafe fn trie_insert(trie: *mut Trie, term: &str) -> i32 {
    unsafe { trie_insert_full(trie, term, 1.0, ADD_REPLACE, 1) }
}

/// `ADD_REPLACE` insert with caller-supplied score/numDocs; discards the rc.
///
/// SAFETY: see [`trie_insert_full`].
pub unsafe fn trie_insert_scored(trie: *mut Trie, term: &str, score: f64, num_docs: usize) {
    let _ = unsafe { trie_insert_full(trie, term, score, ADD_REPLACE, num_docs) };
}

/// SAFETY: `term.as_bytes()` is a valid borrow for the call.
pub unsafe fn trie_delete(trie: *mut Trie, term: &str) -> i32 {
    unsafe { Trie_Delete(trie, term.as_ptr() as *const c_char, term.len()) }
}

/// `Trie_IterateAll` dump in `  {term:width$}  score=...  numDocs=...` format.
///
/// Test modules with a custom dump format (codepoint annotations, rune-length
/// previews, …) define their own helper instead of calling this.
pub fn dump_all(trie: *mut Trie, width: usize) -> String {
    // SAFETY: `trie` was created via `NewTrie` and has not been freed.
    let size = unsafe { Trie_Size(trie) };
    // SAFETY: `trie` is live; the iterator is freed below.
    let it = unsafe { Trie_IterateAll(trie) };

    let mut runes_ptr: *mut rune = ptr::null_mut();
    let mut rune_len: t_len = 0;
    let mut payload = RSPayload {
        data: ptr::null_mut(),
        len: 0,
    };
    let mut score: f32 = 0.0;
    let mut num_docs: usize = 0;

    let mut out = String::new();
    writeln!(&mut out, "size: {size}").unwrap();
    writeln!(&mut out, "entries:").unwrap();

    // SAFETY: out-pointers are valid for writes; the iterator owns and reuses
    // the returned `runes_ptr` buffer across iterations.
    while unsafe {
        TrieIterator_Next(
            it,
            &mut runes_ptr,
            &mut rune_len,
            &mut payload,
            &mut score,
            &mut num_docs,
            ptr::null_mut(),
        )
    } != 0
    {
        // SAFETY: iterator hands us a valid rune buffer of length `rune_len`.
        let term = unsafe { runes_to_string(runes_ptr, rune_len as usize) };
        writeln!(
            &mut out,
            "  {term:width$}  score={score}  numDocs={num_docs}"
        )
        .unwrap();
    }

    // SAFETY: `it` was just produced by `Trie_IterateAll` and not freed yet.
    unsafe { TrieIterator_Free(it) };
    out
}
