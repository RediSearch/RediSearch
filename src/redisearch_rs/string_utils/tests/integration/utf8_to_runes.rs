/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use string_utils::runes::{runes_to_utf8, utf8_to_runes};

#[test]
fn ascii_round_trips() {
    assert_eq!(
        utf8_to_runes(b"abc"),
        vec![b'a' as u16, b'b' as u16, b'c' as u16]
    );
}

#[test]
fn empty_yields_empty() {
    assert_eq!(utf8_to_runes(b""), Vec::<u16>::new());
}

#[test]
fn two_and_three_byte_bmp() {
    // `é` (U+00E9, 2-byte) and `€` (U+20AC, 3-byte).
    assert_eq!(utf8_to_runes("é€".as_bytes()), vec![0x00E9, 0x20AC]);
}

#[test]
fn stops_at_nul() {
    // A NUL byte terminates the key: bytes after it are ignored.
    assert_eq!(utf8_to_runes(b"ab\0cd"), vec![b'a' as u16, b'b' as u16]);
}

#[test]
fn truncated_trailing_sequence_zero_fills() {
    // A lead byte whose continuation bytes run past the end still yields a rune,
    // its missing bytes taken as zero — the terminator byte the C decoder reads
    // there. Dropping the sequence would turn a one-rune key into an empty one.
    // `[0xE2, 0x82]` is a 3-byte lead with one byte missing:
    // (0xE2 & 0x0F) << 12 | (0x82 & 0x3F) << 6 | 0 = 0x2080.
    assert_eq!(
        utf8_to_runes(&[b'a', 0xE2, 0x82]),
        vec![b'a' as u16, 0x2080]
    );
    // `[0xC3]` is a 2-byte lead with its continuation missing:
    // (0xC3 & 0x1F) << 6 | 0 = 0x00C0.
    assert_eq!(utf8_to_runes(&[0xC3]), vec![0x00C0]);
}

#[test]
fn lead_byte_width_matches_c_ranges() {
    // Width is classified by C `nu_utf8_read`'s lead-byte ranges, not the strict
    // `110xxxxx`/`11110xxx` prefixes. A bare continuation byte (0x80..=0xBF) as a
    // lead is a 2-byte start in C: `[0x81, 0x81]` -> (0x81 & 0x1F) << 6 | (0x81 &
    // 0x3F) = 0x41, not a stop.
    assert_eq!(utf8_to_runes(&[0x81, 0x81]), vec![0x41]);
    // A 0xF8..=0xFF lead is a 4-byte start in C (truncated to `u16`); it must not
    // be treated as malformed. For 0xF8 0x81 0x82 0x83 the codepoint is
    // (0x81 & 0x3F) << 12 | (0x82 & 0x3F) << 6 | (0x83 & 0x3F) = 0x1083.
    assert_eq!(utf8_to_runes(&[0xF8, 0x81, 0x82, 0x83]), vec![0x1083]);
}

#[test]
fn malformed_continuation_is_masked_like_c() {
    // Continuation bytes are not validated: a lead byte followed by a
    // non-continuation byte is masked and folded like the C `nu_utf8_read`, not
    // rejected. `[0xC3, b'(']` -> (0xC3 & 0x1F) << 6 | (0x28 & 0x3F) = 0x00E8,
    // the same rune the C indexer would store. Validating here would make a
    // malformed-but-indexed key stop matching, diverging from how it was stored.
    assert_eq!(utf8_to_runes(&[0xC3, b'(']), vec![0x00E8]);
}

#[test]
fn four_byte_utf8_truncates_like_strtorunesn() {
    // A raw query token may hold a genuine 4-byte (non-BMP) sequence — e.g. the
    // emoji "😀" = F0 9F 98 80, U+1F600. The index stored it by truncating the
    // codepoint to `u16` (`(rune)0x1F600 == 0xF600`), so decoding must truncate
    // the same way for the query to resolve to the stored key.
    assert_eq!(utf8_to_runes("😀".as_bytes()), vec![0xF600]);
    assert_eq!(
        utf8_to_runes(&[b'a', 0xF0, 0x9F, 0x98, 0x80, b'b']),
        vec![b'a' as u16, 0xF600, b'b' as u16]
    );
    // A truncated 4-byte sequence zero-fills its two missing bytes rather than
    // reading past the slice: (0xF0 & 0x07) << 18 | (0x9F & 0x3F) << 12 = 0x1F000.
    assert_eq!(
        utf8_to_runes(&[b'a', 0xF0, 0x9F]),
        vec![b'a' as u16, 0xF000]
    );
}

#[test]
fn surrogate_round_trips() {
    // A lone surrogate rune (as produced when a non-BMP codepoint is truncated to
    // `u16` at index time) is emitted as 3-byte WTF-8 by `runes_to_utf8` and must
    // decode back to the same rune — the case a UTF-8-validating decode would
    // drop.
    let runes = vec![0xD800u16, 0x0041, 0xDFFF];
    let bytes = runes_to_utf8(&runes).expect("encodes to WTF-8");
    assert!(
        std::str::from_utf8(&bytes).is_err(),
        "surrogate bytes are not valid UTF-8"
    );
    assert_eq!(utf8_to_runes(&bytes), runes);
}

#[cfg_attr(miri, ignore = "Too slow to be run under miri.")]
#[test]
fn round_trips_all_bmp_runes() {
    // Every non-zero BMP rune (skipping `0`, the terminator) survives an
    // encode/decode round-trip, surrogates included. `runes_to_utf8` caps at
    // `MAX_RUNE_STR_LEN`, so walk the space in chunks under that limit.
    for chunk in (1..=0xFFFFu16).collect::<Vec<u16>>().chunks(512) {
        let bytes = runes_to_utf8(chunk).expect("chunk is within MAX_RUNE_STR_LEN");
        assert_eq!(utf8_to_runes(&bytes), chunk.to_vec());
    }
}

#[cfg(not(miri))]
mod proptest_checks {
    use proptest::prelude::*;
    use string_utils::runes::{MAX_RUNE_STR_LEN, runes_to_utf8, utf8_to_runes};

    proptest! {
        /// For any sequence of non-zero runes (surrogates included), encoding
        /// with `runes_to_utf8` and decoding back reproduces the exact runes, so
        /// `utf8_to_runes` is the inverse of `runes_to_utf8`. `0` is excluded
        /// because it is the string terminator, not a stored rune.
        #[test]
        fn inverts_runes_to_utf8(
            runes in proptest::collection::vec(1u16..=0xFFFF, 0..=MAX_RUNE_STR_LEN),
        ) {
            let bytes = runes_to_utf8(&runes).expect("within MAX_RUNE_STR_LEN");
            prop_assert_eq!(utf8_to_runes(&bytes), runes);
        }

        /// Decoding arbitrary — including malformed or truncated — bytes never
        /// panics or reads out of bounds, and never yields more runes than input
        /// bytes (each rune consumes at least one byte).
        #[test]
        fn arbitrary_bytes_never_panic(bytes in proptest::collection::vec(any::<u8>(), 0..512)) {
            let runes = utf8_to_runes(&bytes);
            prop_assert!(runes.len() <= bytes.len());
        }
    }
}
