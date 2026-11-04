/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use rqe_wildcard::remove_escape_in_place;

/// Unescape `pattern` in place and return the prefix the new length covers.
///
/// The bytes past that length are the terminator and then scratch, so a caller
/// only ever looks at the prefix — and so does this. [`terminates_a_shortened_pattern`]
/// covers the terminator separately.
fn unescape(pattern: &[u8]) -> Vec<u8> {
    let mut buf = pattern.to_vec();
    let len = remove_escape_in_place(&mut buf);
    buf.truncate(len);
    buf
}

#[test]
fn terminates_a_shortened_pattern() {
    // Shortening frees the byte the terminator goes in, so the result reads as a
    // C string with nothing more from the caller.
    for pattern in [&br"a\*b"[..], br"abc\", b"a\0b", br"\\"] {
        let mut buf = pattern.to_vec();
        let len = remove_escape_in_place(&mut buf);
        assert!(len < pattern.len(), "{pattern:?} should have shortened");
        assert_eq!(buf[len], 0, "{pattern:?} must be terminated at its new end");
    }
}

#[test]
fn leaves_an_unshortened_pattern_untouched() {
    // Nothing to re-terminate, and nowhere to do it: the byte after the pattern is
    // past the end of the slice. The buffer must come back exactly as it went in.
    for pattern in [&b"abc"[..], b"\0a", b"a?b*c"] {
        let mut buf = pattern.to_vec();
        let len = remove_escape_in_place(&mut buf);
        assert_eq!(len, pattern.len(), "{pattern:?} should not have shortened");
        assert_eq!(buf, pattern, "{pattern:?} must be left alone");
    }
}

#[test]
fn empty_pattern_is_left_alone() {
    assert_eq!(unescape(b""), b"");
}

#[test]
fn no_escapes() {
    assert_eq!(unescape(b"foo"), b"foo");
}

// ── beginning of pattern ──

#[test]
fn escape_at_beginning() {
    assert_eq!(unescape(br"\foo"), b"foo");
}

#[test]
fn double_escape_at_beginning() {
    assert_eq!(unescape(br"\\foo"), br"\foo");
}

#[test]
fn escaped_quote_at_beginning() {
    assert_eq!(unescape(br"\'foo"), b"'foo");
}

#[test]
fn double_escaped_quote_at_beginning() {
    // \\' → escaped backslash + literal quote
    assert_eq!(unescape(br"\\'foo"), br"\'foo");
}

// ── mid pattern ──

#[test]
fn escape_mid() {
    assert_eq!(unescape(br"f\oo"), b"foo");
}

#[test]
fn double_escape_mid() {
    assert_eq!(unescape(br"f\\oo"), br"f\oo");
}

#[test]
fn escaped_quote_mid() {
    assert_eq!(unescape(br"f\'oo"), b"f'oo");
}

#[test]
fn double_escaped_quote_mid() {
    assert_eq!(unescape(br"f\\'oo"), br"f\'oo");
}

// ── end of pattern ──

#[test]
fn escape_at_end() {
    assert_eq!(unescape(br"foo\"), b"foo");
}

#[test]
fn double_escape_at_end() {
    assert_eq!(unescape(br"foo\\"), br"foo\");
}

#[test]
fn escaped_quote_at_end() {
    assert_eq!(unescape(br"foo\'"), b"foo'");
}

#[test]
fn double_escaped_quote_at_end() {
    assert_eq!(unescape(br"foo\\'"), br"foo\'");
}

// ── extra edge cases ──

#[test]
fn consecutive_escapes() {
    assert_eq!(unescape(br"\a\b\c"), b"abc");
}

#[test]
fn only_backslashes() {
    // \\ → second \, then trailing \ is dropped
    assert_eq!(unescape(br"\\\"), br"\");
}

#[test]
fn collapses_wildcard_escapes() {
    assert_eq!(unescape(br"a\*b\?c"), b"a*b?c");
}

#[test]
fn collapses_one_level_only() {
    // A second application would eat the surviving backslash, which is why the
    // routine must be applied at most once per pattern.
    assert_eq!(unescape(br"a\\b"), br"a\b");
}

#[test]
fn handles_a_non_utf8_pattern() {
    // A pattern reaching the AST through a query parameter is binary data, so the
    // routine works on bytes: `0xff` is not valid UTF-8 in any position.
    assert_eq!(unescape(b"a\\\xffb"), b"a\xffb");
}

// ── NUL handling ──

#[test]
fn ends_the_pattern_at_a_nul() {
    // Everything downstream reads the unescaped pattern up to its terminator, so
    // an interior NUL ends it — even with no escape anywhere to trigger a rewrite.
    assert_eq!(unescape(b"a\0b"), b"a");
    assert_eq!(unescape(b"abc"), b"abc");
}

#[test]
fn ends_the_pattern_at_an_escaped_nul() {
    // Escaping does not rescue a NUL: it is still where the pattern stops.
    assert_eq!(unescape(b"a\\\0b"), b"a");
}

#[test]
fn keeps_a_leading_nul() {
    // The exception: the scan only notices a NUL from index 1 onwards, so one at
    // index 0 does not end the pattern. It survives a later rewrite too, since the
    // copy starts at the escape that triggered it.
    assert_eq!(unescape(b"\0"), b"\0");
    assert_eq!(unescape(b"\0a"), b"\0a");
    assert_eq!(unescape(b"\0a\\b"), b"\0ab");
    // A second NUL is at index 1, so it ends the pattern as usual.
    assert_eq!(unescape(b"\0\0a"), b"\0");
}

// ── remove_escape_in_place vs ffi::Wildcard_RemoveEscape ────────

#[cfg(not(miri))]
mod ffi_comparison {
    use proptest::prelude::*;

    use super::unescape;

    fn c_wildcard_remove_escape(input: &[u8]) -> Vec<u8> {
        let mut buf = input.to_vec();
        buf.push(0);
        // SAFETY: `buf` is a valid, null-terminated, mutable buffer. The C
        // function only writes within `[0..len]` and returns the new length.
        let new_len = unsafe { ffi::Wildcard_RemoveEscape(buf.as_mut_ptr().cast(), input.len()) };
        buf.truncate(new_len);
        buf
    }

    fn assert_matches_c(input: &[u8]) {
        let c_result = c_wildcard_remove_escape(input);
        let rust_result = unescape(input);
        assert_eq!(
            rust_result, c_result,
            "mismatch for input {input:?}: rust={rust_result:?}, c={c_result:?}",
        );
    }

    #[test]
    fn ffi_no_escapes() {
        assert_matches_c(b"foo");
    }

    #[test]
    fn ffi_empty() {
        assert_matches_c(b"");
    }

    #[test]
    fn ffi_escape_at_beginning() {
        assert_matches_c(br"\foo");
    }

    #[test]
    fn ffi_double_escape_at_beginning() {
        assert_matches_c(br"\\foo");
    }

    #[test]
    fn ffi_escaped_quote_at_beginning() {
        assert_matches_c(br"\'foo");
    }

    #[test]
    fn ffi_double_escaped_quote_at_beginning() {
        assert_matches_c(br"\\'foo");
    }

    #[test]
    fn ffi_escape_mid() {
        assert_matches_c(br"f\oo");
    }

    #[test]
    fn ffi_double_escape_mid() {
        assert_matches_c(br"f\\oo");
    }

    #[test]
    fn ffi_escaped_quote_mid() {
        assert_matches_c(br"f\'oo");
    }

    #[test]
    fn ffi_double_escaped_quote_mid() {
        assert_matches_c(br"f\\'oo");
    }

    #[test]
    fn ffi_escape_at_end() {
        assert_matches_c(br"foo\");
    }

    #[test]
    fn ffi_double_escape_at_end() {
        assert_matches_c(br"foo\\");
    }

    #[test]
    fn ffi_escaped_quote_at_end() {
        assert_matches_c(br"foo\'");
    }

    #[test]
    fn ffi_double_escaped_quote_at_end() {
        assert_matches_c(br"foo\\'");
    }

    #[test]
    fn ffi_consecutive_escapes() {
        assert_matches_c(br"\a\b\c");
    }

    #[test]
    fn ffi_only_backslashes() {
        assert_matches_c(br"\\\");
    }

    #[test]
    fn ffi_nul_ends_the_pattern() {
        assert_matches_c(b"a\0b");
    }

    #[test]
    fn ffi_leading_nul_is_kept() {
        assert_matches_c(b"\0a\\b");
    }

    /// Weighted toward the bytes that drive the branches: the escape, the NUL that
    /// ends a pattern, and a byte that is not valid UTF-8 anywhere. Every other
    /// byte is opaque to the routine, so one printable range stands in for them
    /// all. Both special bytes are reachable through a query parameter.
    fn wildcard_bytes() -> impl Strategy<Value = Vec<u8>> {
        let byte = prop_oneof![
            3 => Just(b'\\'),
            1 => Just(0u8),
            1 => Just(0xffu8),
            5 => 0x20..=0x7Eu8,
        ];
        proptest::collection::vec(byte, 1..128)
    }

    proptest! {
        #[test]
        fn ffi_matches_rust(input in wildcard_bytes()) {
            assert_matches_c(&input);
        }
    }
}
