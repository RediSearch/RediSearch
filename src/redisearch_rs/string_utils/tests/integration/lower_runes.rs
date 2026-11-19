/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! [`lower_runes`], [`fold_runes`], [`fold_rune`] and [`decode_runes`].

use rstest::rstest;
use string_utils::runes::{
    MAX_RUNE_STR_LEN, RuneStrTooLong, decode_runes, fold_rune, fold_runes, lower_runes,
};

fn runes(s: &str) -> Vec<u16> {
    s.encode_utf16().collect()
}

#[rstest]
#[case::ascii("ABC", "abc")]
#[case::multibyte("É", "é")]
#[case::empty("", "")]
// U+0130 lowercases to U+0069 followed by U+0307.
#[case::multi_codepoint("\u{130}", "i\u{307}")]
// Each character lowercases alone, so a final sigma stays σ.
#[case::no_context("ΟΔΟΣ", "οδοσ")]
// U+10400 lowercases to U+10428, which is truncated to its low 16 bits.
#[case::non_bmp_truncated("\u{10400}", "\u{0428}")]
fn lowers_each_codepoint(#[case] input: &str, #[case] expected: &str) {
    assert_eq!(lower_runes(input.as_bytes()).unwrap(), runes(expected));
}

#[rstest]
#[case::ascii("yY", "yy")]
#[case::without_lowercase("Ø∏πåÅ", "ø∏πåå")]
// Folding differs from lowercasing for these.
#[case::final_sigma("ς", "σ")]
#[case::cherokee_small_to_capital("\u{AB70}", "\u{13A0}")]
// A folding to several codepoints keeps only the first.
#[case::sharp_s("ß", "s")]
#[case::ligature("ﬁ", "f")]
// U+10400 folds to U+10428 before it is truncated, not after.
#[case::non_bmp_truncated("\u{10400}", "\u{0428}")]
fn folds_each_codepoint_to_its_first(#[case] input: &str, #[case] expected: &str) {
    assert_eq!(fold_runes(input.as_bytes()).unwrap(), runes(expected));
}

#[test]
fn fold_rune_folds_the_rune_it_is_given() {
    assert_eq!(fold_rune('Å' as u16), 'å' as u16);
    assert_eq!(fold_rune('∏' as u16), '∏' as u16);
    assert_eq!(fold_rune(0xD800), 0xD800);
}

/// Decoding ends at the first codepoint 0, a NUL byte or an overlong one.
#[rstest]
#[case::leading_nul(b"\0AB", "")]
#[case::embedded_nul(b"a\0BCD", "a")]
#[case::overlong_nul(b"Ab\xC0\x80CD", "ab")]
fn conversions_stop_at_nul(#[case] input: &[u8], #[case] expected_lower: &str) {
    let expected = runes(expected_lower);
    assert_eq!(lower_runes(input).unwrap(), expected);
    assert_eq!(fold_runes(input).unwrap(), expected);
    let decoded: Vec<u16> = decode_runes(input).collect();
    assert_eq!(decoded.len(), expected.len());
}

/// Malformed bytes decode to whatever their payload bits spell.
#[test]
fn malformed_bytes_decode_without_validation() {
    // `C8 3A` reads as U+023A, whose lowercase is U+2C65.
    assert_eq!(lower_runes(b"\xC8\x3A").unwrap(), [0x2C65]);
    // A surrogate is no character, so it is kept as it is.
    assert_eq!(lower_runes(b"\xED\xA0\x80").unwrap(), [0xD800]);
}

/// A lead byte cut off by the end of the input decodes as if zeros followed.
#[rstest]
#[case::two_byte(b"ab\xC3", 0xC0)]
#[case::three_byte(b"ab\xE4", 0x4000)]
#[case::four_byte(b"ab\xF0", 0)]
fn a_truncated_sequence_decodes_zero_padded(#[case] input: &[u8], #[case] last: u16) {
    let decoded: Vec<u16> = decode_runes(input).collect();
    let mut expected = vec![u16::from(b'a'), u16::from(b'b')];
    if last != 0 {
        expected.push(last);
    }
    assert_eq!(decoded, expected);
}

#[test]
fn exactly_at_limit() {
    let input = vec![b'A'; MAX_RUNE_STR_LEN];
    assert_eq!(lower_runes(&input).unwrap().len(), MAX_RUNE_STR_LEN);
    assert_eq!(fold_runes(&input).unwrap().len(), MAX_RUNE_STR_LEN);
}

#[test]
fn exceeds_limit() {
    let input = vec![b'A'; MAX_RUNE_STR_LEN + 1];
    assert_eq!(lower_runes(&input), Err(RuneStrTooLong));
    assert_eq!(fold_runes(&input), Err(RuneStrTooLong));
}

/// The limit applies to the lowercased length, which can exceed the input's.
#[test]
fn the_limit_counts_lowercased_runes() {
    let input = "\u{130}".repeat(MAX_RUNE_STR_LEN / 2 + 1);
    assert_eq!(lower_runes(input.as_bytes()), Err(RuneStrTooLong));
    assert!(fold_runes(input.as_bytes()).is_ok());
}
