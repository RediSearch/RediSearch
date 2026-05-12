/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use query_eval::string_utils::{MAX_RUNESTR_LEN, str_to_lower_runes};

#[test]
fn ascii() {
    let runes = str_to_lower_runes("ABC").unwrap();
    assert_eq!(runes, vec![b'a' as u16, b'b' as u16, b'c' as u16]);
}

#[test]
fn unicode_multibyte() {
    // 'É' (U+00C9) → 'é' (U+00E9)
    let runes = str_to_lower_runes("É").unwrap();
    assert_eq!(runes, vec![0x00E9]);
}

#[test]
fn empty() {
    let runes = str_to_lower_runes("").unwrap();
    assert!(runes.is_empty());
}

#[test]
fn multi_codepoint_mapping() {
    // U+0130 LATIN CAPITAL LETTER I WITH DOT ABOVE → U+0069 + U+0307
    let runes = str_to_lower_runes("\u{0130}").unwrap();
    assert_eq!(runes, vec![0x0069, 0x0307]);
}

#[test]
fn too_long() {
    let long = "a".repeat(MAX_RUNESTR_LEN + 1);
    assert!(str_to_lower_runes(&long).is_none());
}

// strToLowerRunes calls nu_readstr which reads until a null byte (ignoring
// the utf8_len parameter). Rust &str is not null-terminated, so we must
// pass a CString copy. Compare Rust output against C for BMP-only strings
// where libnu and Rust's Unicode tables agree on lowercasing.
#[cfg(not(miri))]
mod ffi_comparison {
    use super::super::LIBNU_DIVERGENT;
    use proptest::prelude::*;
    use query_eval::string_utils::{MAX_RUNESTR_LEN, str_to_lower_runes};
    use std::alloc::{Layout, dealloc};
    use std::ffi::CString;

    unsafe extern "C" {
        fn strToLowerRunes(
            str: *const std::os::raw::c_char,
            utf8_len: usize,
            unicode_len: *mut usize,
        ) -> *mut u16;
    }

    /// Call `strToLowerRunes` via FFI and return the resulting rune slice.
    fn c_str_to_lower_runes(s: &str) -> Option<Vec<u16>> {
        let cstr = CString::new(s).expect("input must not contain null bytes");
        let mut unicode_len: usize = 0;
        // SAFETY: `cstr` is a valid null-terminated buffer. `strToLowerRunes`
        // uses `utf8_len` for the lowercasing loop.
        let ptr = unsafe { strToLowerRunes(cstr.as_ptr(), s.len(), &mut unicode_len) };
        if ptr.is_null() {
            return None;
        }
        // SAFETY: `ptr` is a valid array of `unicode_len` runes allocated via
        // `Vec::into_boxed_slice` (global allocator).
        let runes = unsafe { std::slice::from_raw_parts(ptr, unicode_len) }.to_vec();
        // SAFETY: `strToLowerRunes` allocated `unicode_len + 1` u16 elements
        // via the global allocator (`Vec::into_boxed_slice` then `mem::forget`).
        unsafe {
            dealloc(ptr.cast(), Layout::array::<u16>(unicode_len + 1).unwrap());
        }
        Some(runes)
    }

    fn assert_runes_match_c(s: &str) {
        let c_result = c_str_to_lower_runes(s);
        let rust_result = str_to_lower_runes(s);
        assert_eq!(
            rust_result, c_result,
            "mismatch for input {:?}: rust={:?}, c={:?}",
            s, rust_result, c_result,
        );
    }

    #[test]
    fn ffi_ascii() {
        assert_runes_match_c("ABC");
    }

    #[test]
    fn ffi_empty() {
        assert_runes_match_c("");
    }

    #[test]
    fn ffi_unicode() {
        assert_runes_match_c("Héllo Wörld");
    }

    #[test]
    fn ffi_too_long() {
        let long = "a".repeat(MAX_RUNESTR_LEN + 1);
        assert_runes_match_c(&long);
    }

    /// Generate BMP strings excluding codepoints where libnu (Unicode 9.0)
    /// and Rust (Unicode 17.0) disagree on lowercasing.
    fn bmp_runes_safe() -> impl Strategy<Value = String> {
        proptest::collection::vec(
            (0x0001u32..=0xD7FFu32).prop_filter_map("valid non-divergent BMP char", |cp| {
                if LIBNU_DIVERGENT.contains(&cp) {
                    return None;
                }
                char::from_u32(cp)
            }),
            0..100,
        )
        .prop_map(|chars| chars.into_iter().collect::<String>())
    }

    proptest! {
        #[test]
        fn ffi_matches_rust(s in bmp_runes_safe()) {
            assert_runes_match_c(&s);
        }
    }
}

#[cfg(not(miri))]
mod proptest_checks {
    use proptest::prelude::*;
    use query_eval::string_utils::str_to_lower_runes;

    /// Generate strings of BMP-only codepoints (U+0000..U+FFFF), since
    /// rune is u16 and non-BMP codepoints are truncated.
    fn bmp_string() -> impl Strategy<Value = String> {
        proptest::collection::vec(
            prop_oneof![
                5 => 0x41u32..=0x7Au32,
                3 => 0xC0u32..=0x4FFu32,
                1 => 0x4E00u32..=0x9FFFu32,
                1 => 0x0020u32..=0xD7FFu32,
            ]
            .prop_filter_map("valid BMP char", |cp| char::from_u32(cp)),
            0..200,
        )
        .prop_map(|chars| chars.into_iter().collect::<String>())
    }

    proptest! {
        #[test]
        fn agrees_with_std_lowercase(s in bmp_string()) {
            if let Some(runes) = str_to_lower_runes(&s) {
                let from_runes: String = runes
                    .iter()
                    .filter_map(|&r| char::from_u32(r as u32))
                    .collect();
                let expected: String = s
                    .chars()
                    .flat_map(char::to_lowercase)
                    .filter(|&c| (c as u32) <= 0xFFFF)
                    .collect();
                assert_eq!(from_runes, expected, "mismatch for input {:?}", s);
            }
        }

        #[test]
        fn accepts_within_limit(s in "[a-zA-Z]{0,100}") {
            assert!(str_to_lower_runes(&s).is_some());
        }
    }
}
