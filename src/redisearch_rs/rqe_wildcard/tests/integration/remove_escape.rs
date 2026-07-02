/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::borrow::Cow;

use rqe_wildcard::remove_escape;

#[test]
fn no_escapes() {
    let result = remove_escape("foo");
    assert!(matches!(result, Cow::Borrowed(_)));
    assert_eq!(result, "foo");
}

#[test]
fn empty() {
    let result = remove_escape("");
    assert!(matches!(result, Cow::Borrowed(_)));
    assert_eq!(result, "");
}

// ── beginning of string ──

#[test]
fn escape_at_beginning() {
    assert_eq!(remove_escape("\\foo"), "foo");
}

#[test]
fn double_escape_at_beginning() {
    assert_eq!(remove_escape("\\\\foo"), "\\foo");
}

#[test]
fn escaped_quote_at_beginning() {
    assert_eq!(remove_escape("\\'foo"), "'foo");
}

#[test]
fn double_escaped_quote_at_beginning() {
    // \\' → escaped backslash + literal quote
    assert_eq!(remove_escape("\\\\'foo"), "\\'foo");
}

// ── mid string ──

#[test]
fn escape_mid() {
    assert_eq!(remove_escape("f\\oo"), "foo");
}

#[test]
fn double_escape_mid() {
    assert_eq!(remove_escape("f\\\\oo"), "f\\oo");
}

#[test]
fn escaped_quote_mid() {
    assert_eq!(remove_escape("f\\'oo"), "f'oo");
}

#[test]
fn double_escaped_quote_mid() {
    assert_eq!(remove_escape("f\\\\'oo"), "f\\'oo");
}

// ── end of string ──

#[test]
fn escape_at_end() {
    assert_eq!(remove_escape("foo\\"), "foo");
}

#[test]
fn double_escape_at_end() {
    assert_eq!(remove_escape("foo\\\\"), "foo\\");
}

#[test]
fn escaped_quote_at_end() {
    assert_eq!(remove_escape("foo\\'"), "foo'");
}

#[test]
fn double_escaped_quote_at_end() {
    assert_eq!(remove_escape("foo\\\\'"), "foo\\'");
}

// ── extra edge cases ──

#[test]
fn consecutive_escapes() {
    assert_eq!(remove_escape("\\a\\b\\c"), "abc");
}

#[test]
fn only_backslashes() {
    // \\ → second \, then trailing \ is dropped
    assert_eq!(remove_escape("\\\\\\"), "\\");
}

// ── remove_escape vs ffi::Wildcard_RemoveEscape ────────

#[cfg(not(miri))]
mod ffi_comparison {
    use proptest::prelude::*;
    use rqe_wildcard::remove_escape;

    fn c_wildcard_remove_escape(input: &[u8]) -> Vec<u8> {
        let mut buf = input.to_vec();
        buf.push(0);
        // SAFETY: `buf` is a valid, null-terminated, mutable buffer. The C
        // function only writes within `[0..len]` and returns the new length.
        let new_len = unsafe { ffi::Wildcard_RemoveEscape(buf.as_mut_ptr().cast(), input.len()) };
        buf.truncate(new_len);
        buf
    }

    fn assert_matches_c(input: &str) {
        let c_result = c_wildcard_remove_escape(input.as_bytes());
        let rust_result = remove_escape(input);
        assert_eq!(
            rust_result.as_bytes(),
            c_result.as_slice(),
            "mismatch for input {:?}: rust={:?}, c={:?}",
            input,
            rust_result,
            String::from_utf8_lossy(&c_result),
        );
    }

    #[test]
    fn ffi_no_escapes() {
        assert_matches_c("foo");
    }

    // No ffi_empty test: the C do-while loop in Wildcard_RemoveEscape
    // always executes once, so it returns length 1 for len=0 input (a
    // harmless off-by-one). The Rust version correctly returns empty.

    #[test]
    fn ffi_escape_at_beginning() {
        assert_matches_c("\\foo");
    }

    #[test]
    fn ffi_double_escape_at_beginning() {
        assert_matches_c("\\\\foo");
    }

    #[test]
    fn ffi_escaped_quote_at_beginning() {
        assert_matches_c("\\'foo");
    }

    #[test]
    fn ffi_double_escaped_quote_at_beginning() {
        assert_matches_c("\\\\'foo");
    }

    #[test]
    fn ffi_escape_mid() {
        assert_matches_c("f\\oo");
    }

    #[test]
    fn ffi_double_escape_mid() {
        assert_matches_c("f\\\\oo");
    }

    #[test]
    fn ffi_escaped_quote_mid() {
        assert_matches_c("f\\'oo");
    }

    #[test]
    fn ffi_double_escaped_quote_mid() {
        assert_matches_c("f\\\\'oo");
    }

    #[test]
    fn ffi_escape_at_end() {
        assert_matches_c("foo\\");
    }

    #[test]
    fn ffi_double_escape_at_end() {
        assert_matches_c("foo\\\\");
    }

    #[test]
    fn ffi_escaped_quote_at_end() {
        assert_matches_c("foo\\'");
    }

    #[test]
    fn ffi_double_escaped_quote_at_end() {
        assert_matches_c("foo\\\\'");
    }

    #[test]
    fn ffi_consecutive_escapes() {
        assert_matches_c("\\a\\b\\c");
    }

    #[test]
    fn ffi_only_backslashes() {
        assert_matches_c("\\\\\\");
    }

    fn wildcard_str() -> impl Strategy<Value = String> {
        let byte = prop_oneof![
            3 => Just(b'\\'),
            7 => 0x20..=0x7Eu8,
        ];
        proptest::collection::vec(byte, 1..128).prop_map(|v| String::from_utf8(v).unwrap())
    }

    proptest! {
        #[test]
        fn ffi_matches_rust(input in wildcard_str()) {
            assert_matches_c(&input);
        }
    }
}
