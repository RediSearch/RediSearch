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

/// Error returned when the lowercased rune sequence exceeds
/// [`MAX_RUNE_STR_LEN`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("lowercased rune length {len} exceeds maximum {MAX_RUNE_STR_LEN}")]
pub struct RuneStrTooLong {
    pub len: usize,
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

/// Convert a rune slice into an owned UTF-8 byte string.
///
/// Returns [`None`] when the slice is longer than [`MAX_RUNE_STR_LEN`] runes.
pub fn runes_to_utf8(runes: &[u16]) -> Option<Vec<u8>> {
    if runes.len() > MAX_RUNE_STR_LEN {
        return None;
    }
    // A rune encodes to at most three bytes, and the slice is already known to
    // be within `MAX_RUNE_STR_LEN`, so reserving the worst case costs at most a
    // few kilobytes and removes the growth entirely.
    let mut bytes = Vec::with_capacity(runes.len() * 3);
    for &rune in runes {
        // Encoding stops at the first `0` rune: keys are stored NUL-terminated,
        // so a `0` marks the end of the string and any runes past it are ignored.
        if rune == 0 {
            break;
        }
        // Each rune is a UTF-16 code unit widened to its codepoint and UTF-8
        // encoded directly.
        let cp = rune as u32;
        match cp {
            0x0000..=0x007F => bytes.push(cp as u8),
            // `0x0080..=0x07FF`: encoded as two bytes, splitting the 11 payload
            // bits into a `110xxxxx` lead byte and a `10xxxxxx` continuation byte.
            0x0080..=0x07FF => {
                bytes.push(0xC0 | (cp >> 6) as u8);
                bytes.push(0x80 | (cp & 0x3F) as u8);
            }
            // `0x0800..=0xFFFF`, including the surrogate range (`0xD800..=0xDFFF`,
            // produced when an astral-plane codepoint was truncated to `u16` at
            // index time): encoded to its 3-byte form, not rejected, so the
            // reconstructed key stays matchable against the inverted index. Lone
            // surrogates are not valid Unicode scalar values, so `char` / `String`
            // cannot represent them and the bytes are assembled by hand rather
            // than via `char::encode_utf8`.
            _ => {
                bytes.push(0xE0 | (cp >> 12) as u8);
                bytes.push(0x80 | ((cp >> 6) & 0x3F) as u8);
                bytes.push(0x80 | (cp & 0x3F) as u8);
            }
        }
    }
    Some(bytes)
}

/// Like [`runes_to_utf8`], but hands back an owned [`String`].
///
/// Returns [`None`] when the reconstructed bytes are not valid UTF-8 — which
/// happens exactly when a rune lies in the surrogate range `0xD800..=0xDFFF`,
/// where [`runes_to_utf8`] emits (technically ill-formed) WTF-8 bytes that
/// a [`String`] cannot hold — or when the slice is longer than
/// [`MAX_RUNE_STR_LEN`] runes.
pub fn runes_to_string(runes: &[u16]) -> Option<String> {
    String::from_utf8(runes_to_utf8(runes)?).ok()
}
