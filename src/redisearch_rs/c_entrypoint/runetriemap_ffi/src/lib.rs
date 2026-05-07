/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! C-callable FFI surface for [`trie_rs::RuneTrieMap`].
//!
//! Exposes the rune-keyed trie wrapper to the C codebase under two
//! payload conventions:
//!
//! * **Suffix path** — caller-owned `*mut c_void` payloads, freed via a
//!   user-supplied callback. Insert/find/delete operate on `&[rune]`.
//! * **Dictionary path** — UTF-8 string keys, with an `f32` score
//!   encoded directly in the payload pointer slot. No payload allocation
//!   takes place; iteration and RDB save/load decode the score back from
//!   the pointer bits.
//!
//! Both conventions share the same opaque [`RuneTrieMap`] handle. The
//! caller picks the right entry-point set per call site.

#![allow(non_camel_case_types, non_snake_case)]

use std::ffi::{c_char, c_void};

mod iter;
mod ops;
mod rdb;

pub use iter::*;
pub use ops::*;
pub use rdb::*;

pub use trie_rs::opaque::RuneTrieMap;

/// Mirror of the C `rune` typedef from `src/trie/rune_util.h`.
///
/// cbindgen omits this type (see `cbindgen.toml`); the manually-emitted
/// `typedef uint16_t rune;` keeps the FFI header source-compatible with
/// the existing `rune` alias used by suffix and dictionary callers.
pub type rune = u16;

// Bindings to `src/trie/rune_util.{h,c}`. We import these rather than
// reimplementing UTF-8 ↔ rune conversion in Rust so that byte-level
// parity with the legacy C trie is guaranteed (notably for RDB).
//
// `strToRunesN`: decode a UTF-8 byte sequence into a buffer of runes
//   and return the rune count. The output buffer must have capacity
//   for at least one rune per input byte.
// `runesToStr`: encode a rune slice as a heap-allocated UTF-8 C
//   string. Sets `*utflen` to the byte length (excluding NUL); the
//   returned pointer is `rm_malloc`-allocated.
unsafe extern "C" {
    fn strToRunesN(s: *const c_char, slen: usize, outbuf: *mut rune) -> usize;
    fn runesToStr(input: *const rune, len: usize, utflen: *mut usize) -> *mut c_char;
}

/// Encode an `f32` score as a `*mut c_void` pointer, for storage in a
/// payload-sized slot without a heap allocation.
///
/// The score's IEEE-754 bit pattern is widened to a `usize` and cast to
/// a pointer. On 64-bit platforms this is lossless. The decoder
/// [`decode_score`] reverses the operation.
pub(crate) const fn encode_score(score: f32) -> *mut c_void {
    score.to_bits() as usize as *mut c_void
}

/// Decode a score previously stored via [`encode_score`].
pub(crate) fn decode_score(p: *mut c_void) -> f32 {
    f32::from_bits(p as usize as u32)
}

/// Convert a UTF-8 byte slice to a vector of runes via the C
/// [`strToRunesN`] reader, ensuring byte-level parity with the legacy
/// trie's input handling.
///
/// # Safety
///
/// `s` must point to `slen` readable bytes (or be NULL when `slen == 0`).
pub(crate) unsafe fn utf8_to_runes(s: *const c_char, slen: usize) -> Vec<rune> {
    if slen == 0 {
        return Vec::new();
    }
    let mut buf: Vec<rune> = Vec::with_capacity(slen);
    // SAFETY: caller guarantees `s` is a valid pointer to `slen` bytes,
    // and `buf` has capacity `slen` for output.
    let n = unsafe { strToRunesN(s, slen, buf.as_mut_ptr()) };
    debug_assert!(n <= slen, "rune count {n} exceeds byte count {slen}");
    // SAFETY: `strToRunesN` initialized `n` runes (n <= slen <= capacity).
    unsafe { buf.set_len(n) };
    buf
}

/// Convert a rune slice to a heap-allocated UTF-8 C string via the C
/// [`runesToStr`] writer. The returned pointer is `rm_malloc`-allocated;
/// the caller must release it via `rm_free`.
///
/// `*out_len` receives the number of UTF-8 bytes (excluding the NUL).
///
/// # Safety
///
/// `out_len` must be a valid mutable pointer to a `usize`.
pub(crate) unsafe fn runes_to_utf8(runes: &[rune], out_len: *mut usize) -> *mut c_char {
    // SAFETY: `runes.as_ptr()` is valid for `runes.len()` reads;
    // `out_len` validity is required by the function safety contract.
    unsafe { runesToStr(runes.as_ptr(), runes.len(), out_len) }
}
