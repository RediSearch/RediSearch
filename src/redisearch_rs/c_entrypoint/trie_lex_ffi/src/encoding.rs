/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Rune key encoding.
//!
//! The C trie keys 16-bit runes. We store byte slices in [`trie_rs::TrieMap`]
//! and encode each rune as a big-endian `u16`. Two consequences:
//!
//! * Byte-lex order matches rune-lex order — required for `Trie_Sort_Lex`
//!   semantics. (Little-endian would invert ordering on every rune, since the
//!   high byte would land second.)
//! * Every key has even byte length, and chunked decode is unambiguous — no
//!   UTF-8 boundary surprises when iterating partial matches.
//!
//! Conversion from UTF-8 happens one codepoint at a time; codepoints above
//! `U+FFFF` are truncated to their low 16 bits, matching the lossy behavior
//! of the C side when `TRIE_32BIT_RUNES` is not defined.

/// C-side alias for a single 16-bit rune. Matches `rune` in `src/trie/rune_util.h`.
pub type trie_lex_rune = u16;
/// C-side alias for the rune-length type. Matches `t_len` in `src/trie/trie.h`.
pub type trie_lex_t_len = u16;

/// Encode a UTF-8 byte slice into the trie's big-endian rune key.
///
/// Invalid UTF-8 sequences are replaced by `U+FFFD` (`String::from_utf8_lossy`
/// behavior). Each codepoint is truncated to a `u16`.
pub fn utf8_to_be_runes(input: &[u8]) -> Vec<u8> {
    // Fast path: pure ASCII. Each byte expands to `[0x00, byte]`.
    if input.is_ascii() {
        let mut out = Vec::with_capacity(input.len() * 2);
        for &b in input {
            out.push(0u8);
            out.push(b);
        }
        return out;
    }
    let s = std::string::String::from_utf8_lossy(input);
    let mut out = Vec::with_capacity(s.len() * 2);
    for c in s.chars() {
        let r = c as u32 as u16;
        out.extend_from_slice(&r.to_be_bytes());
    }
    out
}

/// Decode a big-endian rune key (as stored in the trie) back into a `Vec<u16>`
/// of runes. Returns an empty vector for an empty input. Odd trailing bytes
/// would indicate corruption and are dropped.
pub fn be_runes_to_runes(encoded: &[u8]) -> Vec<u16> {
    let mut out = Vec::with_capacity(encoded.len() / 2);
    for chunk in encoded.chunks_exact(2) {
        out.push(u16::from_be_bytes([chunk[0], chunk[1]]));
    }
    out
}

/// Decode an encoded rune key directly into a UTF-8 `String`. Each rune is
/// interpreted as a Unicode codepoint; unpaired surrogates (`U+D800..U+DFFF`)
/// fall back to `U+FFFD` so the result is always valid UTF-8.
///
/// Used by [`crate::TrieLex_RdbSave`] to recover the original term for the
/// C-side wire format.
pub fn be_runes_to_utf8(encoded: &[u8]) -> String {
    let mut out = String::with_capacity(encoded.len());
    for chunk in encoded.chunks_exact(2) {
        let r = u16::from_be_bytes([chunk[0], chunk[1]]);
        let c = char::from_u32(r as u32).unwrap_or('\u{FFFD}');
        out.push(c);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_ascii() {
        let cases: &[&[u8]] = &[b"", b"a", b"hello", b"AaZz09"];
        for &input in cases {
            let encoded = utf8_to_be_runes(input);
            assert_eq!(encoded.len(), input.len() * 2, "input={:?}", input);
            let runes = be_runes_to_runes(&encoded);
            assert_eq!(runes.len(), input.len(), "input={:?}", input);
            let back = be_runes_to_utf8(&encoded);
            assert_eq!(back.as_bytes(), input, "input={:?}", input);
        }
    }

    #[test]
    fn round_trip_non_ascii_bmp() {
        // BMP non-ASCII: still round-trips exactly.
        let cases: &[&str] = &["café", "naïve", "Ω", "你好", "Привет"];
        for &s in cases {
            let encoded = utf8_to_be_runes(s.as_bytes());
            // 2 bytes per codepoint.
            assert_eq!(
                encoded.len(),
                s.chars().count() * 2,
                "input={:?}",
                s
            );
            let back = be_runes_to_utf8(&encoded);
            assert_eq!(back, s, "input={:?}", s);
        }
    }

    #[test]
    fn non_bmp_is_truncated_lossy() {
        // U+1F600 (😀) has u32 0x1F600 → truncated to u16 0xF600 (a private-use
        // codepoint). Lossy but deterministic; matches the C side's behavior
        // when `TRIE_32BIT_RUNES` is not defined.
        let s = "😀";
        let encoded = utf8_to_be_runes(s.as_bytes());
        assert_eq!(encoded, vec![0xF6, 0x00]);
    }

    #[test]
    fn byte_lex_matches_rune_lex_for_bmp() {
        // Pick a few rune pairs that disagree if we used little-endian.
        // 0x0100 (Ā) is rune-greater than 0x00FF (ÿ), and its BE encoding
        // [0x01, 0x00] is byte-greater than [0x00, 0xFF].
        let a = utf8_to_be_runes("ÿ".as_bytes()); // U+00FF
        let b = utf8_to_be_runes("Ā".as_bytes()); // U+0100
        assert!(a < b);
        // Multi-rune: "ÿ" < "ÿa" (shorter prefix wins in lex order)
        let c = utf8_to_be_runes("ÿa".as_bytes());
        assert!(a < c);
    }

    #[test]
    fn invalid_utf8_becomes_replacement_char() {
        // 0xFF alone is not a valid UTF-8 start byte.
        let encoded = utf8_to_be_runes(&[0xFFu8]);
        // U+FFFD → [0xFF, 0xFD]
        assert_eq!(encoded, vec![0xFF, 0xFD]);
    }
}
