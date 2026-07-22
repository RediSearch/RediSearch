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
    // Measure the folded length first, in a single non-allocating pass, and
    // reject an over-limit string before allocating a buffer proportional to it.
    // A caller-supplied wildcard token can be arbitrarily long, so allocating
    // the full rune vector just to discover it exceeds the limit would waste
    // memory proportional to the input. This mirrors the C `strToLowerRunes`,
    // which measures with `nu_strtransformnlen` before allocating.
    let len = s.chars().flat_map(char::to_lowercase).count();
    if len > MAX_RUNE_STR_LEN {
        return Err(RuneStrTooLong { len });
    }
    // The measured length is exact, so the buffer is sized once and never grown.
    let mut runes = Vec::with_capacity(len);
    runes.extend(s.chars().flat_map(char::to_lowercase).map(|c| c as u16));
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

/// Decode a byte string into the runes ([`u16`]) a term was indexed under,
/// reproducing the C `strToRunesN` conversion the trie is built with.
///
/// Two kinds of input reach this decoder and both must map to the same rune key
/// the term was stored under:
/// - A **rune-encoded (WTF-8) key** — the inverse of [`runes_to_utf8`] — whose
///   codepoints are all `0x0000..=0xFFFF`, so it is at most three bytes per rune,
///   **including lone surrogates** (`0xD800..=0xDFFF`, which valid UTF-8 forbids
///   but which appear when a non-BMP codepoint was truncated to [`u16`] at index
///   time).
/// - A **raw query token** typed by the user, which may contain a genuine
///   four-byte (non-BMP) sequence — e.g. an emoji. The index stored such a term
///   by truncating each codepoint to [`u16`] (`strToRunesN` does `(rune)cp`), so
///   this decoder truncates the four-byte codepoint the same way, letting the
///   query resolve to the same key instead of silently missing it.
///
/// Unlike a UTF-8-validating decode, this recovers the same runes the term was
/// stored under, so a surrogate-bearing key still matches.
///
/// The sequence width is classified from the lead byte by the same ranges C's
/// `nu_utf8_read` uses — `0x00..=0x7F` → 1 byte, `0x80..=0xDF` → 2, `0xE0..=0xEF`
/// → 3, `0xF0..=0xFF` → 4 — **not** by the stricter `110xxxxx`/`11110xxx` bit
/// prefixes. So a byte the strict tests reject (a bare continuation byte or an
/// `0xF8..=0xFF` lead) is still consumed as a multi-byte sequence, matching how
/// the C indexer decoded it; classifying it as malformed here would make such a
/// key unlookable.
///
/// Continuation bytes are likewise **not** validated to be of the `10xxxxxx`
/// form: each is masked (`& 0x3F`) and folded into the codepoint unconditionally,
/// exactly as `nu_utf8_read` does. So `[0xC3, b'(']` decodes to the same rune the
/// C indexer would have produced (`0x00E8`) rather than stopping. Validating
/// either the lead or the continuation bytes would diverge from how terms are
/// actually stored.
///
/// Decoding stops only at the first NUL codepoint (keys are NUL-terminated) and
/// at a *truncated* sequence — a lead byte whose continuation bytes run past the
/// end of `bytes` (the one place this is stricter than the C decoder, which would
/// read past its buffer); no read ever crosses that end.
pub fn utf8_to_runes(bytes: &[u8]) -> Vec<u16> {
    let mut runes = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        // Classify the width from the lead byte using C `nu_utf8_read`'s ranges.
        let width = if b < 0x80 {
            1
        } else if b < 0xE0 {
            2
        } else if b < 0xF0 {
            3
        } else {
            4
        };
        // A sequence whose continuation bytes run past the end of `bytes` is
        // truncated: stop rather than read out of bounds (C would read past its
        // buffer here).
        if i + width > bytes.len() {
            break;
        }
        // Decode the codepoint, masking continuation bytes with `& 0x3F` exactly
        // as `nu_utf8_read` does, then truncate to `u16` as `strToRunesN`'s
        // `(rune)cp` does — so the recovered runes match the key the trie was
        // built under (a non-BMP codepoint folds to its low 16 bits).
        let codepoint: u32 = match width {
            1 => u32::from(b),
            2 => (u32::from(b) & 0x1F) << 6 | (u32::from(bytes[i + 1]) & 0x3F),
            3 => {
                (u32::from(b) & 0x0F) << 12
                    | (u32::from(bytes[i + 1]) & 0x3F) << 6
                    | (u32::from(bytes[i + 2]) & 0x3F)
            }
            _ => {
                (u32::from(b) & 0x07) << 18
                    | (u32::from(bytes[i + 1]) & 0x3F) << 12
                    | (u32::from(bytes[i + 2]) & 0x3F) << 6
                    | (u32::from(bytes[i + 3]) & 0x3F)
            }
        };
        // Keys are NUL-terminated; a `0` codepoint marks the end of the string.
        // Test the full codepoint before truncating, as `strToRunesN` does — a
        // four-byte codepoint is never `0`, so it is never mistaken for a
        // terminator.
        if codepoint == 0 {
            break;
        }
        runes.push(codepoint as u16);
        i += width;
    }
    runes
}
