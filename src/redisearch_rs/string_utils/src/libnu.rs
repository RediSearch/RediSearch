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
//! when it can, [`nu_rune_count`] says how many steps that walk takes, and
//! [`nu_decodes_a_zero_codepoint`] says whether one of those steps produces
//! the zero codepoint that C stops on.

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

/// Whether the C decoder produces the zero codepoint anywhere along its walk
/// of `bytes` — the stopping condition of C's `strToRunes` that
/// [`nu_rune_count`] cannot see.
///
/// A literal NUL byte produces it, but so do byte patterns no NUL scan finds:
/// an overlong encoding of codepoint 0 such as `C0 80`, and a degenerate
/// sequence assembling to zero such as the continuation pair `80 80` (the
/// decoder treats a stray continuation byte as a two-byte lead and masks both
/// bytes' value bits to nothing).
///
/// Replays the decoder's bit assembly exactly: no validation, only the value
/// bits of each byte, over the [`nu_seq_len`] stride. A truncated trailing
/// sequence is not stepped into — there the C decoder reads bytes outside the
/// input, which no in-bounds replay can model — so this is only a complete
/// answer when [`tail_may_overread`] is `false`.
pub fn nu_decodes_a_zero_codepoint(bytes: &[u8]) -> bool {
    let mut pos = 0;
    while pos < bytes.len() {
        let Some(seq) = bytes.get(pos..pos + nu_seq_len(bytes[pos])) else {
            return false;
        };
        let codepoint = match *seq {
            [b0] => u32::from(b0),
            [b0, b1] => (u32::from(b0) & 0x1F) << 6 | (u32::from(b1) & 0x3F),
            [b0, b1, b2] => {
                (u32::from(b0) & 0x0F) << 12 | (u32::from(b1) & 0x3F) << 6 | (u32::from(b2) & 0x3F)
            }
            [b0, b1, b2, b3] => {
                (u32::from(b0) & 0x07) << 18
                    | (u32::from(b1) & 0x3F) << 12
                    | (u32::from(b2) & 0x3F) << 6
                    | (u32::from(b3) & 0x3F)
            }
            _ => unreachable!("nu_seq_len returns 1..=4"),
        };
        if codepoint == 0 {
            return true;
        }
        pos += seq.len();
    }
    false
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
    fn zero_codepoint_walk_finds_every_zero_encoding() {
        // The three shapes that hide a zero from a NUL-byte scan, plus the
        // literal byte itself.
        assert!(nu_decodes_a_zero_codepoint(b"ab\0cd"));
        assert!(nu_decodes_a_zero_codepoint(b"a\xC0\x80b"));
        assert!(nu_decodes_a_zero_codepoint(b"\x80\x80"));
        assert!(nu_decodes_a_zero_codepoint(b"a\xE0\x80\x80"));
        assert!(nu_decodes_a_zero_codepoint(b"a\xF0\x80\x80\x80"));
    }

    #[test]
    fn zero_codepoint_walk_passes_well_formed_input() {
        assert!(!nu_decodes_a_zero_codepoint(b""));
        assert!(!nu_decodes_a_zero_codepoint(b"hello"));
        assert!(!nu_decodes_a_zero_codepoint("日本".as_bytes()));
        assert!(!nu_decodes_a_zero_codepoint("ab😀".as_bytes()));
        // Malformed but nonzero: resynchronises and keeps walking.
        assert!(!nu_decodes_a_zero_codepoint(b"\xC3(ab"));
    }

    #[test]
    fn zero_codepoint_walk_stops_before_a_truncated_tail() {
        // The truncated step itself is out of scope — `tail_may_overread`
        // owns it — but a zero found before it must still be reported.
        assert!(!nu_decodes_a_zero_codepoint(b"ab\xF0"));
        assert!(nu_decodes_a_zero_codepoint(b"a\0b\xF0"));
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
}
