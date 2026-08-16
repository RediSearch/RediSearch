/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Conversions between UTF-8 strings and runes.
//!
//! A rune ([`u16`]) is a UTF-16 code unit; tries store and look up terms as
//! rune arrays.

use std::ptr::NonNull;

/// Maximum number of runes (lowercased codepoints) allowed in a single conversion.
pub const MAX_RUNE_STR_LEN: usize = ffi::MAX_RUNE_STR_LEN as usize;

/// Error returned when a rune sequence exceeds [`MAX_RUNE_STR_LEN`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("rune length {len} exceeds maximum {MAX_RUNE_STR_LEN}")]
pub struct RuneStrTooLong {
    pub len: usize,
}

/// Convert a rune slice into a byte slice using UFT-8 encoding mechanism
/// without validating actual unicode code points.
///
/// Returns [`Err(`[`RuneStrTooLong`]`)`] if `runes` exceeds
/// [`MAX_RUNE_STR_LEN`].
pub fn runes_to_bytes(runes: &[ffi::rune]) -> Result<Vec<u8>, RuneStrTooLong> {
    let mut len: usize = 0;
    // SAFETY: `runes` is a valid slice of `runes.len()` runes; `runesToStr`
    // returns a freshly allocated, NUL-terminated buffer of `len` bytes, or NULL
    // when the slice exceeds `MAX_RUNE_STR_LEN`.
    let ptr = unsafe { ffi::runesToStr(runes.as_ptr(), runes.len(), &mut len) };
    let Some(ptr) = NonNull::new(ptr) else {
        return Err(RuneStrTooLong { len: runes.len() });
    };
    // SAFETY: `ptr` points to `len` valid bytes written by the call above.
    let bytes = unsafe { std::slice::from_raw_parts(ptr.as_ptr().cast::<u8>(), len) }.to_vec();
    // SAFETY: `RedisModule_Free` is set during module init and not mutated
    // afterwards.
    let rm_free = unsafe { redis_module::RedisModule_Free.expect("Redis allocator not available") };
    // SAFETY: `ptr` was allocated by the module allocator inside `runesToStr`.
    unsafe { rm_free(ptr.as_ptr().cast::<std::ffi::c_void>()) };
    Ok(bytes)
}

/// Convert a UTF-8 string to a lowercase array of runes ([`u16`]) for trie
/// lookups.
///
/// Each Unicode codepoint is lowercased and then truncated to [`u16`].
///
/// Returns [`Err(`[`RuneStrTooLong`]`)`] if the resulting rune count exceeds
/// [`MAX_RUNE_STR_LEN`].
pub fn str_to_lower_runes(s: &str) -> Result<Vec<u16>, RuneStrTooLong> {
    let runes: Vec<u16> = s
        .chars()
        .flat_map(char::to_lowercase)
        .map(|c| c as u16)
        .collect();
    if runes.len() > MAX_RUNE_STR_LEN {
        return Err(RuneStrTooLong { len: runes.len() });
    }
    Ok(runes)
}
