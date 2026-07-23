/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ffi::c_char;

use query_term::RSTokenFlags;
use rs_token::RSTokenRef;

/// Build a raw [`ffi::RSToken`] borrowing `s`'s bytes, with `str_` null when `s`
/// is `None`.
fn build_raw(s: Option<&[u8]>, flags: RSTokenFlags) -> ffi::RSToken {
    // SAFETY: `RSToken` is plain data whose all-zero bit pattern is a valid,
    // empty token.
    let mut raw: ffi::RSToken = unsafe { std::mem::zeroed() };
    match s {
        Some(bytes) => {
            raw.str_ = bytes.as_ptr() as *mut c_char;
            raw.len = bytes.len();
        }
        None => {
            raw.str_ = std::ptr::null_mut();
            raw.len = 0;
        }
    }
    raw.set_flags(flags);
    raw
}

/// Run `f` on a not-necessarily-NUL-terminated [`RSTokenRef`] wrapping `s`.
fn with_token<R>(s: Option<&[u8]>, flags: RSTokenFlags, f: impl FnOnce(RSTokenRef) -> R) -> R {
    let raw = build_raw(s, flags);
    // SAFETY: `raw.str_`/`raw.len` describe `s`'s bytes (or a null string),
    // which outlive the borrow passed to `f`, satisfying `from_ffi`'s contract.
    f(unsafe { RSTokenRef::from_ffi(&raw) })
}

/// Run `f` on a NUL-terminated [`RSTokenRef<true>`] wrapping `content` (a null
/// token when `content` is `None`). `content` is the string *without* its
/// terminator: the helper owns a NUL-terminated copy so that, per the
/// `(str_, len)` convention, `len` is the content length and `str_[len]` is the
/// terminator.
fn with_nul_token<R>(
    content: Option<&[u8]>,
    flags: RSTokenFlags,
    f: impl FnOnce(RSTokenRef<true>) -> R,
) -> R {
    let owned = content.map(|c| {
        let mut v = c.to_vec();
        v.push(0);
        v
    });
    // SAFETY: `RSToken` is plain data whose all-zero bit pattern is a valid,
    // empty token.
    let mut raw: ffi::RSToken = unsafe { std::mem::zeroed() };
    if let (Some(buf), Some(c)) = (&owned, content) {
        raw.str_ = buf.as_ptr() as *mut c_char;
        raw.len = c.len();
    }
    raw.set_flags(flags);
    // SAFETY: `owned` keeps a NUL-terminated buffer alive for the borrow passed
    // to `f`; `raw.len` is its content length, so `str_[len]` is the terminator,
    // satisfying `from_nul_terminated_ffi`'s contract.
    f(unsafe { RSTokenRef::from_nul_terminated_ffi(&raw) })
}

#[test]
fn exposes_bytes_len_and_flags() {
    with_token(Some(b"Hello"), 0x2A, |tok| {
        assert_eq!(tok.len(), 5);
        assert!(!tok.is_empty());
        assert_eq!(tok.as_bytes(), Some(&b"Hello"[..]));
        assert_eq!(tok.flags(), 0x2A);
    });
}

#[test]
fn null_string_yields_none() {
    with_nul_token(None, 0, |tok| {
        assert!(tok.is_empty());
        assert_eq!(tok.as_bytes(), None);
        assert!(tok.as_lower_runes().is_none());
        // The token carries no string, so `as_c_str` returns `None`.
        assert!(tok.as_c_str().is_none());
    });
}

#[test]
fn exposes_nul_terminated_c_str() {
    with_nul_token(Some(b"Hello"), 0, |tok| {
        assert_eq!(tok.len(), 5);
        let c_str = tok.as_c_str().expect("token carries a string");
        assert_eq!(c_str, c"Hello");
    });
}

#[test]
fn lower_runes_lowercases() {
    with_token(Some(b"HeLLo"), 0, |tok| {
        let runes = tok.as_lower_runes().unwrap().unwrap();
        let expected: Vec<u16> = "hello".encode_utf16().collect();
        assert_eq!(runes, expected);
    });
}

#[test]
fn lower_runes_decodes_invalid_utf8_leniently() {
    // A token is a byte string, and a term is indexed under the runes its bytes
    // decode to without validation: `[0xC3, b'(']` must resolve 0x00E8, the rune
    // the index stored it as. Validating would build a key from replacement
    // characters that was never stored.
    with_token(Some(b"\xC3("), 0, |tok| {
        let runes = tok.as_lower_runes().unwrap().unwrap();
        assert_eq!(runes, vec![0x00E8]);
    });
}

#[test]
fn lower_runes_preserves_surrogate_bytes() {
    // The three-byte form of a lone surrogate — what a non-BMP codepoint
    // truncated to a rune re-encodes to — survives the conversion instead of
    // being replaced.
    with_token(Some(b"\xED\xA0\x80"), 0, |tok| {
        let runes = tok.as_lower_runes().unwrap().unwrap();
        assert_eq!(runes, vec![0xD800]);
    });
}

#[test]
fn lower_runes_too_long_errors() {
    // One rune past the maximum yields `RuneStrTooLong` rather than truncating.
    let content = vec![b'a'; string_utils::runes::MAX_RUNE_STR_LEN + 1];
    with_token(Some(&content), 0, |tok| {
        let err = tok
            .as_lower_runes()
            .expect("token carries a string")
            .expect_err("string exceeds the maximum rune length");
        assert_eq!(err.len, string_utils::runes::MAX_RUNE_STR_LEN + 1);
    });
}

#[test]
fn as_ptr_round_trips_to_the_borrowed_token() {
    let raw = build_raw(Some(b"Hello"), 0);
    // SAFETY: `raw` outlives the borrow held by `tok`, satisfying `from_ffi`.
    let tok = unsafe { RSTokenRef::from_ffi(&raw) };
    assert_eq!(tok.as_ptr(), std::ptr::from_ref(&raw));
}

/// A non-NUL-terminated string handed to [`RSTokenRef::from_nul_terminated_ffi`]
/// trips the debug-only termination check. The check compiles out of release
/// builds, so this test only runs when debug assertions are enabled.
#[cfg(debug_assertions)]
#[test]
#[should_panic(expected = "token string must be NUL-terminated")]
fn non_nul_terminated_string_trips_debug_assert() {
    // `str_[len]` is `b'X'`, not a terminator: in bounds and readable (so the
    // check's own dereference is sound) but non-zero, so the assertion fails.
    let buf = b"abcX";
    // SAFETY: `RSToken` is plain data whose all-zero bit pattern is a valid,
    // empty token.
    let mut raw: ffi::RSToken = unsafe { std::mem::zeroed() };
    raw.str_ = buf.as_ptr() as *mut c_char;
    raw.len = 3;
    // SAFETY: `raw.str_`/`raw.len` describe a valid 3-byte range whose next byte
    // is in bounds and readable, so the debug check's dereference is sound — it
    // observes the missing terminator and panics.
    let _ = unsafe { RSTokenRef::from_nul_terminated_ffi(&raw) };
}
