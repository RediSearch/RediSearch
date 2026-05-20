/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use query_eval::string_utils::tag_strtolower;

#[test]
fn unescape_punct() {
    assert_eq!(tag_strtolower("hello\\!world", false), "hello!world");
}

#[test]
fn unescape_space() {
    assert_eq!(tag_strtolower("hello\\ world", false), "hello world");
}

#[test]
fn no_unescape_alpha() {
    assert_eq!(tag_strtolower("hello\\nworld", false), "hello\\nworld");
}

#[test]
fn case_sensitive() {
    assert_eq!(tag_strtolower("Hello\\!World", true), "Hello!World");
}

#[test]
fn case_insensitive() {
    assert_eq!(tag_strtolower("Hello\\!World", false), "hello!world");
}

#[test]
fn empty() {
    assert_eq!(tag_strtolower("", false), "");
}

#[test]
fn trailing_backslash() {
    assert_eq!(tag_strtolower("abc\\", false), "abc\\");
}

#[test]
fn unescape_vertical_tab() {
    // Vertical tab (0x0B) is whitespace per C isspace() — must be unescaped.
    assert_eq!(tag_strtolower("hello\\\x0Bworld", false), "hello\x0Bworld");
}

#[test]
fn consecutive_escapes() {
    // `\\!` = bytes [0x5C, 0x5C, 0x21]: first `\` is removed (next is
    // punct `\`), second `\` and `!` are kept — matches C behavior.
    assert_eq!(tag_strtolower("\\\\!", false), "\\!");
}

#[test]
fn escaped_backslash_before_alpha() {
    // `\\n` = bytes [0x5C, 0x5C, 0x6E]: first `\` removed (next is punct
    // `\`), second `\` kept, `n` is not punct/space so kept verbatim.
    assert_eq!(tag_strtolower("\\\\n", false), "\\n");
}

#[test]
fn final_sigma() {
    // Greek capital sigma at the end of a word must lowercase to ς (U+03C2,
    // final sigma), not σ (U+03C3). This requires whole-string context from
    // str::to_lowercase — per-character char::to_lowercase always gives σ.
    assert_eq!(tag_strtolower("ΣΣΣΣΣ", false), "σσσσς");
    assert_eq!(tag_strtolower("ΝΕΑΝΊΑΣ", false), "νεανίας");
}

#[test]
fn case_insensitive_matches_str_to_lowercase() {
    // Without escapes, case-insensitive tag_strtolower must produce the
    // same result as str::to_lowercase (including context-dependent rules).
    for s in ["ΣΣΣΣΣ", "ΝΕΑΝΊΑΣ", "Straße", "HELLO", "σίγμα"] {
        assert_eq!(
            tag_strtolower(s, false),
            s.to_lowercase(),
            "tag_strtolower({s:?}, false) != str::to_lowercase",
        );
    }
}

// Compare Rust tag_strtolower against C tag_strtolower via FFI.
// The C function may rm_free/rm_malloc *pstr, so input must be
// allocated with the Redis allocator.
#[cfg(not(miri))]
mod ffi_comparison {
    use super::super::LIBNU_DIVERGENT;
    use proptest::prelude::*;
    use query_eval::string_utils::tag_strtolower;
    use std::ffi::c_void;

    /// Call C `tag_strtolower` via FFI and return the resulting string.
    fn c_tag_strtolower(s: &str, case_sensitive: bool) -> String {
        // SAFETY: extern statics are initialized by the test harness (mock allocator).
        let rm_alloc = unsafe { ffi::RedisModule_Alloc.expect("Redis allocator not available") };
        let rm_free = unsafe { ffi::RedisModule_Free.expect("Redis allocator not available") };

        // Allocate with rm_malloc: C code may rm_free this pointer.
        let buf_len = s.len() + 1; // +1 for null terminator
        // SAFETY: `buf_len` is non-zero; allocator is initialized by the test harness.
        let buf = unsafe { rm_alloc(buf_len) }.cast::<u8>();
        assert!(!buf.is_null());
        // SAFETY: `buf` is a valid allocation of `buf_len` bytes.
        unsafe {
            std::ptr::copy_nonoverlapping(s.as_ptr(), buf, s.len());
            *buf.add(s.len()) = 0; // null terminator
        }

        let mut ptr = buf.cast::<std::os::raw::c_char>();
        let mut len = s.len();
        // SAFETY: `ptr` is a valid rm_malloc'd, null-terminated buffer.
        // `tag_strtolower` may rm_free and replace `ptr`.
        unsafe {
            ffi::tag_strtolower(&mut ptr, &mut len, if case_sensitive { 1 } else { 0 });
        }

        // SAFETY: `ptr` (possibly reallocated) is valid for `len` bytes.
        let result = unsafe { std::slice::from_raw_parts(ptr.cast::<u8>(), len) }.to_vec();
        // SAFETY: `ptr` was allocated by `rm_malloc` (via `tag_strtolower` or the original alloc).
        unsafe { rm_free(ptr.cast::<c_void>()) };

        String::from_utf8(result).expect("C tag_strtolower result must be valid UTF-8")
    }

    fn assert_tag_matches_c(s: &str, case_sensitive: bool) {
        let c_result = c_tag_strtolower(s, case_sensitive);
        let rust_result = tag_strtolower(s, case_sensitive);
        assert_eq!(
            rust_result, c_result,
            "mismatch for input {:?} (case_sensitive={}): rust={:?}, c={:?}",
            s, case_sensitive, rust_result, c_result,
        );
    }

    #[test]
    fn ffi_unescape_case_sensitive_shrinks_in_place() {
        // Escape removal shortens the string without reallocating. The
        // dealloc must use the original allocation size, not the shorter
        // output length.
        assert_tag_matches_c("hello\\!world", true);
        assert_tag_matches_c("a\\!b\\!c\\!d", true);
    }

    #[test]
    fn ffi_unescape_punct() {
        assert_tag_matches_c("hello\\!world", false);
    }

    #[test]
    fn ffi_unescape_space() {
        assert_tag_matches_c("hello\\ world", false);
    }

    #[test]
    fn ffi_no_unescape_alpha() {
        assert_tag_matches_c("hello\\nworld", false);
    }

    #[test]
    fn ffi_case_sensitive() {
        assert_tag_matches_c("Hello\\!World", true);
    }

    #[test]
    fn ffi_case_insensitive() {
        assert_tag_matches_c("Hello\\!World", false);
    }

    #[test]
    fn ffi_empty() {
        assert_tag_matches_c("", false);
    }

    #[test]
    fn ffi_trailing_backslash() {
        assert_tag_matches_c("abc\\", false);
    }

    #[test]
    fn ffi_consecutive_escapes() {
        assert_tag_matches_c("\\\\!", false);
    }

    /// Generate BMP strings biased toward backslashes and punctuation,
    /// excluding libnu-divergent codepoints.
    fn tag_input_bmp() -> impl Strategy<Value = String> {
        let ch = prop_oneof![
            3 => Just('\\'),
            2 => (0x20u32..=0x2Fu32).prop_filter_map("ascii punct", |cp| char::from_u32(cp)),
            3 => (0x41u32..=0x7Au32).prop_filter_map("ascii alpha", |cp| char::from_u32(cp)),
            2 => (0x00C0u32..=0xD7FFu32).prop_filter_map("non-divergent BMP", |cp| {
                if LIBNU_DIVERGENT.contains(&cp) {
                    return None;
                }
                char::from_u32(cp)
            }),
        ];
        proptest::collection::vec(ch, 1..100)
            .prop_map(|chars| chars.into_iter().collect::<String>())
    }

    proptest! {
        #[test]
        fn ffi_matches_rust_sensitive(s in tag_input_bmp()) {
            assert_tag_matches_c(&s, true);
        }

        #[test]
        fn ffi_matches_rust_insensitive(s in tag_input_bmp()) {
            assert_tag_matches_c(&s, false);
        }
    }
}

#[cfg(not(miri))]
mod proptest_checks {
    use proptest::prelude::*;
    use query_eval::string_utils::tag_strtolower;

    /// Generate ASCII strings biased toward backslashes and punctuation.
    fn tag_input() -> impl Strategy<Value = String> {
        let byte = prop_oneof![
            3 => Just(b'\\'),
            2 => 0x20..=0x2Fu8, // punctuation/space range
            5 => 0x41..=0x7Au8, // alphanumeric range
        ];
        proptest::collection::vec(byte, 0..100).prop_map(|v| String::from_utf8(v).unwrap())
    }

    proptest! {
        #[test]
        fn case_insensitive_is_lowercase(s in tag_input()) {
            let result = tag_strtolower(&s, false);
            assert_eq!(
                result,
                result.to_lowercase(),
                "case-insensitive result not lowercase for input {:?}",
                s
            );
        }

        #[test]
        fn case_sensitive_then_lower_eq_case_insensitive(s in tag_input()) {
            let sensitive = tag_strtolower(&s, true);
            let then_lower = sensitive.to_lowercase();
            let insensitive = tag_strtolower(&s, false);
            assert_eq!(
                then_lower, insensitive,
                "tag_strtolower(s, true).to_lowercase() != tag_strtolower(s, false) for {:?}",
                s
            );
        }
    }
}
