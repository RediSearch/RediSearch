/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Guards for the unbounded read-ahead of the vendored `libnu` UTF-8 decoder.
//!
//! Several C helpers — `strToLowerRunes` and the trie's fuzzy pattern folding
//! among them — decode their input with `nu_utf8_read` (`deps/libnu/utf8.h`),
//! which consumes a fixed 1–4 bytes from whatever lead byte it lands on without
//! checking that many bytes remain. Any caller handing such a helper a byte
//! string that is not known to be well-formed UTF-8 — a query token, say — has
//! to keep that read inside its own allocation.
//!
//! This module models that read-ahead once, for every caller that needs it:
//! [`tail_may_overread`] says whether an input can trip it, and
//! [`NU_MAX_READAHEAD`] says how many trailing zero bytes a padded copy needs
//! when it can.

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
}
