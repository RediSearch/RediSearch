/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Disabled under Miri: [`runes_to_bytes`] delegates to the C `runesToStr`,
#![cfg(not(miri))]

use rstest::rstest;
use string_utils::runes::{
    MAX_RUNE_STR_LEN, runes_to_bytes, runes_to_bytes_into, str_to_lower_runes,
};

#[test]
fn roundtrips_ascii() {
    let runes = str_to_lower_runes("apricot").unwrap();
    assert_eq!(runes_to_bytes(&runes).unwrap(), b"apricot");
}

#[test]
fn roundtrips_multibyte() {
    let runes = str_to_lower_runes("é").unwrap();
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
    let err = runes_to_bytes(&runes).unwrap_err();
    assert_eq!(err.len, MAX_RUNE_STR_LEN + 1);
}

/// `runesToStr` and the buffered `runesToStrInto` it is built on agree on every
/// rune, each alone and between two others, and a reused buffer keeps no stale bytes.
#[test]
fn the_buffered_encoder_matches_the_allocating_one_on_every_rune() {
    let mut out = vec![b'x'; 8];
    for rune in 0..=u16::MAX {
        for runes in [&[rune][..], &[0x41, rune, 0x7FF]] {
            runes_to_bytes_into(runes, &mut out).unwrap();
            assert_eq!(out, c_runes_to_str(runes), "runes {runes:x?}");
        }
    }
}

/// The C `runesToStr`'s bytes, read before freeing them.
fn c_runes_to_str(runes: &[u16]) -> Vec<u8> {
    let mut len = 0;
    // SAFETY: `runes` is valid for `runes.len()` reads and within `MAX_RUNE_STR_LEN`.
    let ptr = unsafe { ffi::runesToStr(runes.as_ptr(), runes.len(), &mut len) };
    assert!(!ptr.is_null());
    // SAFETY: `runesToStr` returned `len` initialized bytes at `ptr`.
    let bytes = unsafe { std::slice::from_raw_parts(ptr.cast::<u8>(), len) }.to_vec();
    // SAFETY: `ptr` came from the module allocator, which the test binary mocks.
    unsafe { redis_module::RedisModule_Free.unwrap()(ptr.cast()) };
    bytes
}

#[test]
fn the_buffered_encoder_rejects_what_the_c_one_rejects() {
    let mut out = b"stale".to_vec();
    runes_to_bytes_into(&vec![b'a' as u16; MAX_RUNE_STR_LEN], &mut out).unwrap();
    assert_eq!(out.len(), MAX_RUNE_STR_LEN);

    let err = runes_to_bytes_into(&vec![b'a' as u16; MAX_RUNE_STR_LEN + 1], &mut out).unwrap_err();
    assert_eq!(err.len, MAX_RUNE_STR_LEN + 1);
    assert!(out.is_empty());
}
