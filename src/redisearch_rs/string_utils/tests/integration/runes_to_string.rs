/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use string_utils::runes::{MAX_RUNE_STR_LEN, runes_to_string};

/// Widen each character of `s` to a single rune, without lowercasing.
fn runes(s: &str) -> Vec<u16> {
    s.chars().map(|c| c as u16).collect()
}

#[test]
fn ascii() {
    assert_eq!(runes_to_string(&runes("abc")).unwrap(), "abc");
}

#[test]
fn empty() {
    assert_eq!(runes_to_string(&[]).unwrap(), "");
}

#[test]
fn bmp_non_ascii() {
    // `é` (U+00E9) and `€` (U+20AC) are BMP scalar values, so the widened runes
    // round-trip back to the original string.
    let s = "é€";
    assert_eq!(runes_to_string(&runes(s)).unwrap(), s);
}

#[test]
fn surrogate_is_rejected() {
    // A lone surrogate (U+D800) is a valid rune but not a valid Unicode scalar
    // value, so the reconstructed bytes are not valid UTF-8. Unlike
    // `runes_to_utf8` (which yields the 3-byte WTF-8 form), the `String`
    // variant returns `None` rather than an ill-formed string.
    assert_eq!(runes_to_string(&[0xD800]), None);
}

#[test]
fn too_long_returns_none() {
    let too_long = vec![b'a' as u16; MAX_RUNE_STR_LEN + 1];
    assert!(runes_to_string(&too_long).is_none());
}

#[test]
fn stops_at_first_null_rune() {
    // Inherited from `runes_to_utf8`: a `0` rune ends the string.
    assert_eq!(
        runes_to_string(&[b'a' as u16, 0, b'b' as u16]).unwrap(),
        "a"
    );
}

/// Compare `runes_to_string` against the real C `runesToStr`, which is the
/// routine it reimplements. Only exercised for scalar-value runes (no
/// surrogates), the domain where a `String` result is defined.
#[cfg(not(miri))]
mod ffi_comparison {
    use proptest::prelude::*;
    use std::ffi::c_void;
    use string_utils::runes::runes_to_string;

    /// Call C `runesToStr` via FFI and return the UTF-8 bytes it produced, or
    /// `None` when it returns NULL (the slice exceeds `MAX_RUNE_STR_LEN`).
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
        let rust = runes_to_string(runes).expect("runes_to_string returned None unexpectedly");
        assert_eq!(
            rust.as_bytes(),
            c_bytes.as_slice(),
            "mismatch for runes {runes:?}: rust={:?}, c={c_bytes:?}",
            rust.as_bytes(),
        );
    }

    #[test]
    fn ffi_ascii() {
        assert_matches_c(&[b'A' as u16, b'b' as u16, b'C' as u16]);
    }

    #[test]
    fn ffi_empty() {
        assert_matches_c(&[]);
    }

    #[test]
    fn ffi_bmp_non_ascii() {
        // "Héllo Wörld" widened to runes.
        assert_matches_c(&"Héllo Wörld".chars().map(|c| c as u16).collect::<Vec<_>>());
    }

    /// Generate rune slices of BMP scalar values (excluding the surrogate range
    /// `0xD800..=0xDFFF`), up to `MAX_RUNE_STR_LEN` long.
    fn scalar_runes() -> impl Strategy<Value = Vec<u16>> {
        proptest::collection::vec(prop_oneof![0x0000u16..=0xD7FF, 0xE000u16..=0xFFFF], 0..=128)
    }

    proptest! {
        #[test]
        fn matches_c(runes in scalar_runes()) {
            assert_matches_c(&runes);
        }
    }
}
