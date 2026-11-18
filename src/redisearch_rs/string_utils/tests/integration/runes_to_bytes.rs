/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use rstest::rstest;
use string_utils::runes::{
    MAX_RUNE_STR_LEN, RuneStrTooLong, lower_runes, runes_to_bytes, runes_to_bytes_into,
};

#[test]
fn roundtrips_ascii() {
    let runes = lower_runes(b"apricot").unwrap();
    assert_eq!(runes_to_bytes(&runes).unwrap(), b"apricot");
}

#[test]
fn roundtrips_multibyte() {
    let runes = lower_runes("é".as_bytes()).unwrap();
    assert_eq!(runes_to_bytes(&runes).unwrap(), "é".as_bytes());
}

#[test]
fn empty_slice_returns_empty_vec() {
    assert_eq!(runes_to_bytes(&[]).unwrap(), b"");
}

/// The byte-length boundaries of the encoding.
#[rstest]
#[case::one_byte_max(0x007F, &[0x7F])]
#[case::two_byte_min(0x0080, &[0xC2, 0x80])]
#[case::two_byte_max(0x07FF, &[0xDF, 0xBF])]
#[case::three_byte_min(0x0800, &[0xE0, 0xA0, 0x80])]
#[case::three_byte_max(0xFFFF, &[0xEF, 0xBF, 0xBF])]
fn encodes_one_two_and_three_byte_runes(#[case] rune: u16, #[case] expected: &[u8]) {
    assert_eq!(runes_to_bytes(&[rune]).unwrap(), expected);
}

/// A surrogate is encoded by its own scalar value, so a pair becomes two
/// three-byte sequences rather than the four-byte form of the character it
/// would denote in UTF-16.
#[rstest]
#[case::lone(&[0xD83D], &[0xED, 0xA0, 0xBD])]
#[case::pair(&[0xD83D, 0xDE00], &[0xED, 0xA0, 0xBD, 0xED, 0xB8, 0x80])]
fn surrogates_encode_as_wtf8_and_are_never_paired(#[case] runes: &[u16], #[case] expected: &[u8]) {
    assert_eq!(runes_to_bytes(runes).unwrap(), expected);
}

/// early zero rune termination.
#[rstest]
#[case::leading(&[0, b'a' as u16], b"")]
#[case::embedded(&[b'a' as u16, 0, b'b' as u16], b"a")]
#[case::trailing(&[b'a' as u16, 0], b"a")]
fn zero_rune_terminates_the_sequence(#[case] runes: &[u16], #[case] expected: &[u8]) {
    assert_eq!(runes_to_bytes(runes).unwrap(), expected);
}

#[test]
fn exactly_at_limit() {
    let runes = vec![b'a' as u16; MAX_RUNE_STR_LEN];
    assert_eq!(runes_to_bytes(&runes).unwrap().len(), MAX_RUNE_STR_LEN);
}

#[test]
fn exceeds_limit() {
    let runes = vec![b'a' as u16; MAX_RUNE_STR_LEN + 1];
    assert_eq!(runes_to_bytes(&runes), Err(RuneStrTooLong));
}

/// Every rune that is a Unicode scalar value encodes as that character's UTF-8.
#[test]
#[cfg_attr(miri, ignore = "exhaustive over every rune, too slow under miri")]
fn every_scalar_rune_encodes_as_its_character() {
    for rune in 1..=u16::MAX {
        let Some(c) = char::from_u32(rune.into()) else {
            continue;
        };
        assert_eq!(
            runes_to_bytes(&[rune]).unwrap(),
            c.to_string().as_bytes(),
            "{rune:#x}"
        );
    }
}

/// A reused buffer holds exactly what a fresh one would, with no stale bytes.
#[test]
#[cfg_attr(miri, ignore = "exhaustive over every rune, too slow under miri")]
fn the_buffered_encoder_replaces_the_buffer_contents() {
    let mut out = b"stale bytes".to_vec();
    runes_to_bytes_into(&[0x41, 0x7FF], &mut out).unwrap();
    assert_eq!(out, b"A\xDF\xBF");
}

#[test]
fn the_buffered_encoder_empties_the_buffer_on_rejection() {
    let mut out = b"stale".to_vec();
    runes_to_bytes_into(&vec![b'a' as u16; MAX_RUNE_STR_LEN], &mut out).unwrap();
    assert_eq!(out.len(), MAX_RUNE_STR_LEN);

    let err = runes_to_bytes_into(&vec![b'a' as u16; MAX_RUNE_STR_LEN + 1], &mut out);
    assert_eq!(err, Err(RuneStrTooLong));
    assert!(out.is_empty());
}
