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
/// The whole of `s` is converted: a NUL codepoint is folded and truncated like
/// any other rather than ending the conversion, unlike [`utf8_to_lower_runes`],
/// which stops there because the key it builds must match how a term was
/// indexed.
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

/// The codepoints of a rune-encoded byte string, decoded exactly as the trie's
/// keys were encoded and *before* any truncation to [`u16`].
///
/// Yielding [`u32`] rather than a rune lets a caller that case-folds do so on the
/// full codepoint, which is where the fold is defined; truncating first would
/// fold the wrong character. Callers that do not fold truncate immediately.
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
/// exactly as `nu_utf8_read` does. So `[0xC3, b'(']` decodes to the same
/// codepoint the C indexer would have produced (`0x00E8`) rather than stopping.
/// Validating either the lead or the continuation bytes would diverge from how
/// terms are actually stored.
///
/// A *truncated* trailing sequence — a lead byte whose continuation bytes run
/// past the end of the input — still yields a codepoint, with the missing bytes
/// taken as zero. That reproduces what the C decoder observes: the token buffers
/// it reads are NUL-terminated, so it consumes terminator bytes as the missing
/// continuations and produces exactly the same codepoint (`[0xC3]` → `0x00C0`).
/// Dropping the sequence instead would turn a one-rune key into an empty one,
/// which as a prefix matches every term rather than the term it names. No read
/// ever crosses the end of the input: the zeros are supplied, not read.
///
/// A NUL codepoint is yielded like any other; stopping there is the caller's
/// choice, since only some of the conversions treat it as a terminator.
struct Codepoints<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Codepoints<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }
}

impl Iterator for Codepoints<'_> {
    type Item = u32;

    fn next(&mut self) -> Option<u32> {
        let b = *self.bytes.get(self.offset)?;
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
        // Continuation byte `k` of the current sequence, masked with `& 0x3F`
        // exactly as `nu_utf8_read` does. One that runs past the end of `bytes`
        // is taken as zero — the terminator byte the C decoder reads there.
        let cont = |k: usize| -> u32 {
            u32::from(self.bytes.get(self.offset + k).copied().unwrap_or(0)) & 0x3F
        };
        let codepoint = match width {
            1 => u32::from(b),
            2 => (u32::from(b) & 0x1F) << 6 | cont(1),
            3 => (u32::from(b) & 0x0F) << 12 | cont(1) << 6 | cont(2),
            _ => (u32::from(b) & 0x07) << 18 | cont(1) << 12 | cont(2) << 6 | cont(3),
        };
        // Clamp so a truncated sequence leaves the iterator exactly at the end
        // rather than past it.
        self.offset = (self.offset + width).min(self.bytes.len());
        Some(codepoint)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.bytes.len() - self.offset;
        // Upper bound: a one-byte sequence per remaining byte.
        //
        // Lower bound: each codepoint consumes at most four bytes, and every
        // remaining byte is consumed — a truncated trailing sequence still
        // yields a codepoint rather than stranding its bytes.
        (remaining.div_ceil(4), Some(remaining))
    }
}

/// Lowercase a single decoded codepoint, yielding the codepoints it folds to.
///
/// A value that is not a Unicode scalar value — a lone surrogate, or anything an
/// out-of-range lead byte produced — has no case mapping and is passed through
/// unchanged, mirroring the C fold, which leaves a codepoint it has no entry for
/// alone. Exactly one of the two branches below yields.
fn fold_codepoint(codepoint: u32) -> impl Iterator<Item = u32> {
    let scalar = char::from_u32(codepoint);
    let folded = scalar.map(char::to_lowercase);
    let verbatim = scalar.is_none().then_some(codepoint);
    folded.into_iter().flatten().map(u32::from).chain(verbatim)
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
/// stored under, so a surrogate-bearing key still matches. See [`Codepoints`]
/// for how a sequence's width and codepoint are derived, including from bytes a
/// strict decoder would reject.
///
/// Decoding stops at the first NUL codepoint (keys are NUL-terminated). A
/// truncated trailing sequence still yields a codepoint, its missing bytes taken
/// as zero; no read ever crosses the end of `bytes`.
pub fn utf8_to_runes(bytes: &[u8]) -> Vec<u16> {
    let mut runes = Vec::with_capacity(bytes.len());
    runes.extend(
        Codepoints::new(bytes)
            // Keys are NUL-terminated; a `0` codepoint marks the end of the
            // string. Test the full codepoint before truncating, as
            // `strToRunesN` does — a four-byte codepoint is never `0`, so it is
            // never mistaken for a terminator.
            .take_while(|&codepoint| codepoint != 0)
            // Truncate to `u16` as `strToRunesN`'s `(rune)cp` does, so the
            // recovered runes match the key the trie was built under (a non-BMP
            // codepoint folds to its low 16 bits).
            .map(|codepoint| codepoint as u16),
    );
    runes
}

/// Lowercase a byte string and convert it to the runes ([`u16`]) a term is
/// looked up under, reproducing the C `strToLowerRunes` conversion.
///
/// This is the case-folding counterpart of [`utf8_to_runes`] and shares its
/// lenient decoding: `bytes` is a binary-safe query token or trie key, not
/// necessarily valid UTF-8, and every byte sequence must fold to the same runes
/// the term was indexed under. In particular a malformed sequence such as
/// `[0xC3, b'(']` folds to `0x00E8` and a WTF-8 lone surrogate to its own rune,
/// where a validating decode would substitute [`char::REPLACEMENT_CHARACTER`]
/// and look up a key that was never stored.
///
/// Each codepoint is lowercased *before* being truncated to [`u16`], which is
/// the order the C conversion uses and the only correct one: a non-BMP codepoint
/// folds by its full value, so truncating first would fold an unrelated
/// character (`U+10400` folds to `U+10428` → rune `0x0428`, whereas truncating
/// first would fold `U+0400` → rune `0x0450`).
///
/// Decoding stops at the first codepoint that decodes to `0`, like
/// [`utf8_to_runes`]. This is not merely truncation: a term is stored under the
/// runes up to its first such codepoint — both the tokenizer that lowercases
/// document text and the trie encoder terminate there — so a lookup key must too,
/// or it would carry a `0` rune no stored term has and match nothing. The `0`
/// is tested on the decoded codepoint, before folding and truncation, so a byte
/// sequence that decodes to `0` without being a literal NUL byte (e.g. the
/// overlong encoding `[0xC0, 0x80]`) ends the key just the same. A truncated
/// trailing sequence still yields a codepoint, its missing bytes taken as zero,
/// and no read crosses the end of `bytes`.
///
/// Returns [`Err(`[`RuneStrTooLong`]`)`] if the folded rune count exceeds
/// [`MAX_RUNE_STR_LEN`], measured before the buffer is allocated so that an
/// over-limit token does not allocate proportionally to its length.
pub fn utf8_to_lower_runes(bytes: &[u8]) -> Result<Vec<u16>, RuneStrTooLong> {
    let len = Codepoints::new(bytes)
        .take_while(|&codepoint| codepoint != 0)
        .flat_map(fold_codepoint)
        .count();
    if len > MAX_RUNE_STR_LEN {
        return Err(RuneStrTooLong { len });
    }
    let mut runes = Vec::with_capacity(len);
    runes.extend(
        Codepoints::new(bytes)
            .take_while(|&codepoint| codepoint != 0)
            .flat_map(fold_codepoint)
            .map(|codepoint| codepoint as u16),
    );
    Ok(runes)
}

#[cfg(test)]
mod test {
    use super::*;

    /// The bounds [`Codepoints`] reports must bracket the number of codepoints
    /// it actually yields, including when the input ends in a truncated sequence
    /// whose missing bytes are supplied as zero.
    #[test]
    fn size_hint_brackets_the_yielded_count() {
        // A complete four-byte sequence followed by a lead byte whose
        // continuation bytes run past the end: five bytes, two codepoints.
        let cases: &[&[u8]] = &[
            b"",
            b"a",
            &[0xF0, 0x41, 0x41, 0x41, 0xF0],
            &[0xC3],
            &[b'a', 0xE2, 0x82],
            "Héllo Wörld".as_bytes(),
            &[0xF0, 0x9F, 0x98, 0x80],
        ];

        for bytes in cases {
            let (lower, upper) = Codepoints::new(bytes).size_hint();
            let count = Codepoints::new(bytes).count();
            assert!(
                lower <= count && count <= upper.expect("the upper bound is always known"),
                "size_hint {:?} does not bracket {count} for {bytes:?}",
                (lower, upper),
            );
        }
    }
}
