/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Models the byte walk of the vendored `libnu` UTF-8 decoder.
//!
//! The decoder (`nu_utf8_read`, `deps/libnu/utf8.h`) consumes a fixed 1–4
//! bytes from whatever lead byte it lands on and decodes without validating.
//! [`nu_seq_len`] mirrors how many bytes each step takes, and
//! [`nu_utf8_decode`] and [`nu_utf8_encode`] turn each step into a codepoint
//! and back under the same lax rules, so bytes that are not UTF-8 can still be
//! transformed character by character.

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
