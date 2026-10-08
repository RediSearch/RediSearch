/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Models the byte walk of the vendored `libnu` UTF-8 decoder, and the guards
//! its unbounded read-ahead forces on callers.
//!
//! Several C helpers — `strToLowerRunes` and the trie's fuzzy pattern folding
//! among them — decode their input with `nu_utf8_read` (`deps/libnu/utf8.h`),
//! which consumes a fixed 1–4 bytes from whatever lead byte it lands on without
//! checking that many bytes remain. Any caller handing such a helper a byte
//! string that is not known to be well-formed UTF-8 — a query token, say — has
//! to keep that read inside its own allocation.
//!
//! This module models that decoder's walk once, for every caller that needs it:
//! [`tail_may_overread`] says whether an input can trip the read-ahead,
//! [`NU_MAX_READAHEAD`] says how many trailing zero bytes a padded copy needs
//! when it can, and [`nu_rune_count`] says how many steps that walk takes.
//! [`nu_utf8_decode`] and [`nu_utf8_encode`] turn each step into a codepoint
//! and back under the same lax rules, so bytes that are not UTF-8 can still be
//! transformed character by character.

/// The most bytes the C decoder (`nu_utf8_read`) reads past a multibyte lead
/// byte. A UTF-8 sequence is at most four bytes, so the decoder touches at most
/// three bytes beyond the lead — and it does so without any bounds check.
/// Padding a decoder input with this many trailing zero bytes keeps a truncated
/// trailing lead byte from reading out of bounds.
pub const NU_MAX_READAHEAD: usize = 3;

/// How many bytes the C decoder (`nu_utf8_read`) consumes for the lead byte `b`,
/// mirroring its branches exactly — including that it treats a stray
/// continuation byte (`0x80..=0xBF`) as a two-byte lead rather than rejecting it.
pub const fn nu_seq_len(b: u8) -> usize {
    match b {
        0x00..=0x7F => 1,
        0x80..=0xDF => 2,
        0xE0..=0xEF => 3,
        0xF0..=0xFF => 4,
    }
}

/// Whether handing `bytes` to the C decoder would read past its end.
///
/// The decoder walks the input one sequence at a time, reading [`nu_seq_len`]
/// bytes from each position it lands on without checking that many remain. This
/// replays that same walk — the lengths alone decide where it lands, so no
/// decoding is needed — and reports whether any step would run off the end. It
/// is exact rather than conservative: a well-formed input, whatever its last
/// character, never trips it, and only a genuinely truncated trailing sequence
/// does.
///
/// `false` means no read can escape `bytes`, so it may be decoded in place; the
/// padded copy is only needed when this returns `true`.
pub fn tail_may_overread(bytes: &[u8]) -> bool {
    let mut pos = 0;
    while pos < bytes.len() {
        let seq_len = nu_seq_len(bytes[pos]);
        if pos + seq_len > bytes.len() {
            return true;
        }
        pos += seq_len;
    }
    false
}

/// How many steps the C decoder (`nu_utf8_read`) takes across `bytes` — the
/// rune count C's `strToRunes` ends up with, as long as no sequence in `bytes`
/// decodes to codepoint 0.
///
/// Replays the same walk as [`tail_may_overread`], counting steps instead of
/// checking for over-read, and so inherits [`nu_seq_len`]'s lack of validation:
/// a truncated trailing sequence counts as one step, with the cursor stepping
/// past the end of `bytes`.
///
/// C has a second stopping condition this walk cannot see: `strToRunes` decodes
/// a codepoint per step and stops at the first zero one, which an embedded NUL
/// byte, an overlong sequence such as `C0 80`, and a zero-padded truncated tail
/// all produce. For those inputs this over-counts. A caller that must not
/// over-count has to reject them.
pub fn nu_rune_count(bytes: &[u8]) -> usize {
    let mut pos = 0;
    let mut runes = 0;
    while pos < bytes.len() {
        pos += nu_seq_len(bytes[pos]);
        runes += 1;
    }
    runes
}

/// Decode one UTF-8 sequence, however malformed.
///
/// `seq` must hold exactly [`nu_seq_len`] of its first byte. Only the payload
/// bits of each byte are read, and the trailing bytes are never checked for
/// being continuation bytes, so every sequence decodes to some value rather
/// than being rejected: `C8 3A` reads as U+023A, the overlong `C0 80` as 0, and
/// a four-byte lead from `F8` upwards can yield a value above U+10FFFF.
///
/// # Panics
///
/// If `seq` is empty or longer than four bytes.
pub const fn nu_utf8_decode(seq: &[u8]) -> u32 {
    const fn payload(b: u8) -> u32 {
        (b & 0x3F) as u32
    }
    match *seq {
        [b0] => b0 as u32,
        [b0, b1] => ((b0 & 0x1F) as u32) << 6 | payload(b1),
        [b0, b1, b2] => ((b0 & 0x0F) as u32) << 12 | payload(b1) << 6 | payload(b2),
        [b0, b1, b2, b3] => {
            ((b0 & 0x07) as u32) << 18 | payload(b1) << 12 | payload(b2) << 6 | payload(b3)
        }
        _ => panic!("a sequence is one to four bytes long"),
    }
}

/// Append the UTF-8 encoding of `codepoint` to `out`.
///
/// The width depends on the value alone, so this also encodes what
/// [`nu_utf8_decode`] produces from malformed input — surrogates, and values
/// up to U+1FFFFF — which [`char::encode_utf8`] cannot represent. Bits above
/// U+1FFFFF are dropped.
pub fn nu_utf8_encode(codepoint: u32, out: &mut Vec<u8>) {
    let continuation = |shift: u32| 0x80 | ((codepoint >> shift) & 0x3F) as u8;
    if codepoint < 0x80 {
        out.push(codepoint as u8);
    } else if codepoint < 0x800 {
        out.extend([0xC0 | (codepoint >> 6) as u8, continuation(0)]);
    } else if codepoint < 0x1_0000 {
        out.extend([
            0xE0 | ((codepoint >> 12) & 0x0F) as u8,
            continuation(6),
            continuation(0),
        ]);
    } else {
        out.extend([
            0xF0 | ((codepoint >> 18) & 0x07) as u8,
            continuation(12),
            continuation(6),
            continuation(0),
        ]);
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn well_formed_input_needs_no_padding() {
        // Nothing here can over-read, so all of it decodes in place: ASCII, and
        // sequences of every width sitting flush against the end.
        assert!(!tail_may_overread(b""));
        assert!(!tail_may_overread(b"hello"));
        assert!(!tail_may_overread(b"ab\0cd"));
        assert!(!tail_may_overread("é".as_bytes()));
        assert!(!tail_may_overread("日本".as_bytes()));
        assert!(!tail_may_overread("ab😀".as_bytes()));
    }

    #[test]
    fn truncated_trailing_sequence_needs_padding() {
        // A lead byte announcing more bytes than remain: the decoder would read
        // past the end, so these must take the padded-copy path.
        assert!(tail_may_overread(b"ab\xF0"));
        assert!(tail_may_overread(b"ab\xF0\x9F"));
        assert!(tail_may_overread(b"ab\xF0\x9F\x98"));
        assert!(tail_may_overread(b"ab\xE0"));
        assert!(tail_may_overread(b"ab\xC3"));
        // A stray continuation byte at the end is read as a two-byte lead.
        assert!(tail_may_overread(b"ab\x80"));
    }

    #[test]
    fn earlier_malformed_bytes_do_not_force_padding() {
        // The damage is mid-string: the walk resynchronises past it and still
        // lands inside the input, so no over-read is possible.
        assert!(!tail_may_overread(b"\xC3(ab"));
    }

    #[test]
    fn rune_count_matches_codepoints_for_well_formed_input() {
        assert_eq!(nu_rune_count(b""), 0);
        assert_eq!(nu_rune_count(b"hello"), 5);
        assert_eq!(nu_rune_count("é".as_bytes()), 1);
        assert_eq!(nu_rune_count("日本".as_bytes()), 2);
        // An astral character is one four-byte sequence, hence one rune here —
        // even though C stores it as a truncated `uint16_t`.
        assert_eq!(nu_rune_count("ab😀".as_bytes()), 3);
    }

    #[test]
    fn rune_count_replays_the_decoder_on_malformed_input() {
        // A stray continuation byte is read as a two-byte lead, so it swallows
        // the byte after it rather than counting as one rune of its own.
        assert_eq!(nu_rune_count(b"\x80a"), 1);
        // A truncated trailing sequence still counts as one step, with the
        // cursor stepping past the end.
        assert_eq!(nu_rune_count(b"a\xF0"), 2);
        assert_eq!(nu_rune_count(b"a\xE0\x80"), 2);
    }

    #[test]
    fn rune_count_overcounts_a_sequence_decoding_to_zero() {
        // `C0 80` is an overlong encoding of codepoint 0, which libnu decodes
        // rather than rejects, so C stops there and reports one rune. This walk
        // has no codepoint to stop on and keeps stepping.
        assert_eq!(nu_rune_count(b"a\xC0\x80b"), 3);
        // Same divergence from an embedded NUL, where C reports two runes.
        assert_eq!(nu_rune_count(b"ab\0cd"), 5);
    }

    #[test]
    fn decode_matches_well_formed_utf8() {
        for c in ['a', 'é', 'Ⱥ', '日', '😀', '\u{10FFFF}'] {
            let mut buf = [0; 4];
            let seq = c.encode_utf8(&mut buf).as_bytes();
            assert_eq!(nu_utf8_decode(seq), u32::from(c), "{c:?}");
        }
    }

    #[test]
    fn decode_masks_payload_bits_without_validating() {
        // `:` is no continuation byte, but only its low six bits are read.
        assert_eq!(nu_utf8_decode(b"\xC8\x3A"), 0x23A);
        // A stray continuation byte is read as a two-byte lead.
        assert_eq!(nu_utf8_decode(b"\x80\x41"), 0x01);
        // An overlong encoding decodes to the value it spells out.
        assert_eq!(nu_utf8_decode(b"\xC0\x80"), 0);
        // A surrogate and a value beyond U+10FFFF both come out as they are.
        assert_eq!(nu_utf8_decode(b"\xED\xA0\x80"), 0xD800);
        assert_eq!(nu_utf8_decode(b"\xF7\xBF\xBF\xBF"), 0x1F_FFFF);
    }

    #[test]
    fn encode_matches_well_formed_utf8() {
        for c in ['a', 'é', 'Ⱥ', '日', '😀', '\u{10FFFF}'] {
            let mut out = Vec::new();
            nu_utf8_encode(u32::from(c), &mut out);
            assert_eq!(out, c.to_string().as_bytes(), "{c:?}");
        }
    }

    #[test]
    fn encode_writes_values_no_char_can_hold() {
        let mut out = Vec::new();
        nu_utf8_encode(0xD800, &mut out);
        assert_eq!(out, b"\xED\xA0\x80");

        out.clear();
        nu_utf8_encode(0x1F_FFFF, &mut out);
        assert_eq!(out, b"\xF7\xBF\xBF\xBF");
    }
}
