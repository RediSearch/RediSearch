/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use string_utils::runes::{MAX_RUNE_STR_LEN, runes_to_utf8, str_to_lower_runes};

/// Widen each character of `s` to a single rune, without lowercasing — the
/// inverse of what [`runes_to_utf8`] reconstructs for BMP input.
fn runes(s: &str) -> Vec<u16> {
    s.chars().map(|c| c as u16).collect()
}

#[test]
fn ascii_round_trips() {
    assert_eq!(runes_to_utf8(&runes("abc")).unwrap(), b"abc");
}

#[test]
fn empty_yields_empty_string() {
    assert_eq!(runes_to_utf8(&[]).unwrap(), Vec::<u8>::new());
}

#[test]
fn bmp_non_ascii_round_trips() {
    // `é` (U+00E9) and `€` (U+20AC) are BMP codepoints that fit a single rune,
    // so widening then UTF-8 encoding reproduces the original bytes.
    let s = "é€";
    assert_eq!(runes_to_utf8(&runes(s)).unwrap(), s.as_bytes());
}

#[test]
fn inverts_str_to_lower_runes() {
    // Lowercasing to runes and back reconstructs the lowercased string's bytes.
    let s = "Héllo Wörld";
    let lowered: String = s.chars().flat_map(char::to_lowercase).collect();
    let recovered = runes_to_utf8(&str_to_lower_runes(s).unwrap()).unwrap();
    assert_eq!(recovered, lowered.as_bytes());
}

#[test]
fn surrogate_rune_is_encoded_not_rejected() {
    // A lone surrogate (U+D800) is not a valid Unicode scalar value, but it is a
    // valid rune — produced when an astral-plane codepoint is truncated to `u16`
    // at index time. It must be encoded to its 3-byte UTF-8 form (so the key
    // still matches the index), not rejected.
    assert_eq!(runes_to_utf8(&[0xD800]).unwrap(), vec![0xED, 0xA0, 0x80]);
}

#[test]
fn too_long_returns_none() {
    // Beyond the maximum rune length the conversion cannot be performed and
    // returns `None` rather than a truncated string.
    let too_long = vec![b'a' as u16; MAX_RUNE_STR_LEN + 1];
    assert!(runes_to_utf8(&too_long).is_none());
}

#[test]
fn stops_at_first_null_rune() {
    // Keys are stored NUL-terminated, so a `0` rune ends the string and any
    // runes after it are ignored — matching C `runesToStr`.
    assert_eq!(runes_to_utf8(&[b'a' as u16, 0, b'b' as u16]).unwrap(), b"a");
    assert_eq!(runes_to_utf8(&[0]).unwrap(), Vec::<u8>::new());
}

/// Compare the hand-rolled encoder against the real C `runesToStr`, which is the
/// routine it reimplements. Covers every rune value including the surrogate
/// range, where both must emit identical 3-byte WTF-8 output.
#[cfg(not(miri))]
mod ffi_comparison {
    use proptest::prelude::*;
    use std::ffi::c_void;
    use string_utils::runes::runes_to_utf8;

    /// Call C `runesToStr` via FFI and return the bytes it produced, or `None`
    /// when it returns NULL (the slice exceeds `MAX_RUNE_STR_LEN`).
    fn c_runes_to_str(runes: &[u16]) -> Option<Vec<u8>> {
        let mut len: usize = 0;
        // SAFETY: `runes` is a valid slice of `runes.len()` runes; `runesToStr`
        // returns a freshly `rm_calloc`'d, NUL-terminated buffer of `len` bytes,
        // or NULL.
        let ptr = unsafe { ffi::runesToStr(runes.as_ptr(), runes.len(), &mut len) };
        if ptr.is_null() {
            return None;
        }
        // SAFETY: `ptr` points to `len` valid bytes.
        let bytes = unsafe { std::slice::from_raw_parts(ptr.cast::<u8>(), len) }.to_vec();
        // SAFETY: `RedisModule_Free` is a `static mut` set by the test harness and
        // not mutated concurrently.
        let rm_free = unsafe { ffi::RedisModule_Free.expect("Redis allocator not available") };
        // SAFETY: `ptr` was allocated by `rm_calloc` (backed by RedisModule_Alloc).
        unsafe { rm_free(ptr.cast::<c_void>()) };
        Some(bytes)
    }

    fn assert_matches_c(runes: &[u16]) {
        let c_bytes = c_runes_to_str(runes).expect("FFI runesToStr returned NULL unexpectedly");
        let rust = runes_to_utf8(runes).expect("runes_to_utf8 returned None");
        assert_eq!(rust, c_bytes, "mismatch for runes {runes:?}");
    }

    #[test]
    fn ffi_surrogate() {
        assert_matches_c(&[0xD800]);
    }

    proptest! {
        // Any rune value, surrogates included, up to `MAX_RUNE_STR_LEN` long.
        #[test]
        fn matches_c(runes in proptest::collection::vec(any::<u16>(), 0..=128)) {
            assert_matches_c(&runes);
        }
    }
}
