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

/// Convert a rune slice into a byte slice, mimicking `ffi::runesToStr`.
///
/// Returns [`Err(`[`RuneStrTooLong`]`)`] if `runes` exceeds
/// [`MAX_RUNE_STR_LEN`].
pub fn runes_to_bytes(runes: &[ffi::rune]) -> Result<Vec<u8>, RuneStrTooLong> {
    if runes.len() > MAX_RUNE_STR_LEN {
        return Err(RuneStrTooLong { len: runes.len() });
    }
    // Initial capacity matches an all-ASCII non NUL-terminated rune slice.
    let mut bytes = Vec::with_capacity(runes.len());
    for rune in runes.iter().copied() {
        match rune {
            0 => break,
            0x0001..=0x007F => bytes.push(rune as u8),
            0x0080..=0x07FF => bytes.extend([0xC0 | (rune >> 6) as u8, 0x80 | (rune & 0x3F) as u8]),
            _ => bytes.extend([
                0xE0 | (rune >> 12) as u8,
                0x80 | ((rune >> 6) & 0x3F) as u8,
                0x80 | (rune & 0x3F) as u8,
            ]),
        }
    }
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
