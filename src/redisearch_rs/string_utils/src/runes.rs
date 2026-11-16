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

/// Maximum number of runes (lowercased codepoints) allowed in a single conversion.
pub const MAX_RUNE_STR_LEN: usize = ffi::MAX_RUNE_STR_LEN as usize;

/// Error returned when a rune sequence exceeds [`MAX_RUNE_STR_LEN`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("rune length {len} exceeds maximum {MAX_RUNE_STR_LEN}")]
pub struct RuneStrTooLong {
    pub len: usize,
}

/// Encode `runes` as UTF-8 into `out`, replacing its contents, so a caller
/// converting many keys reuses one buffer. Each rune is encoded by its own value,
/// surrogates included, and the first zero rune ends the input.
///
/// Returns [`Err(`[`RuneStrTooLong`]`)`] if `runes` exceeds
/// [`MAX_RUNE_STR_LEN`], leaving `out` empty.
pub fn runes_to_bytes_into(runes: &[ffi::rune], out: &mut Vec<u8>) -> Result<(), RuneStrTooLong> {
    out.clear();
    if runes.len() > MAX_RUNE_STR_LEN {
        return Err(RuneStrTooLong { len: runes.len() });
    }
    out.reserve(runes.len() * ffi::RUNE_MAX_UTF8_LEN as usize);
    // SAFETY: `runes` is valid for `runes.len()` reads, and `out` has room for the
    // `RUNE_MAX_UTF8_LEN` bytes per rune `runesToStrInto` may write.
    let len = unsafe { ffi::runesToStrInto(runes.as_ptr(), runes.len(), out.as_mut_ptr().cast()) };
    // SAFETY: `runesToStrInto` initialized the first `len` bytes, within the capacity.
    unsafe { out.set_len(len) };
    Ok(())
}

/// [`runes_to_bytes_into`] into a new vector.
pub fn runes_to_bytes(runes: &[ffi::rune]) -> Result<Vec<u8>, RuneStrTooLong> {
    let mut out = Vec::new();
    runes_to_bytes_into(runes, &mut out)?;
    Ok(out)
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
