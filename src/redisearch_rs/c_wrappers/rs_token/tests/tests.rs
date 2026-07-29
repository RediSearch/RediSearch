/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#![cfg_attr(miri, allow(dead_code, unused_imports))]

use std::ffi::c_char;

use query_term::RSTokenFlags;
use rs_token::RSTokenRef;

// Install the mock Redis allocator so the C `strToLowerRunes` can allocate, and
// force the combined C bundle to be linked into the test binary.
redis_mock::mock_or_stub_missing_redis_c_symbols!();
extern crate redisearch_rs;

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
    f(unsafe { RSTokenRef::from_ffi(&raw const raw) })
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
    f(unsafe { RSTokenRef::from_nul_terminated_ffi(&raw const raw) })
}

#[test]
fn exposes_bytes_len_and_flags() {
    with_token(Some(b"Hello"), 0x2A, |tok| {
        assert_eq!(tok.len(), 5);
        assert!(!tok.is_empty());
        // SAFETY: the token is a local that nothing mutates or frees while the
        // slice lives.
        assert_eq!(unsafe { tok.as_bytes() }, Some(&b"Hello"[..]));
        assert_eq!(tok.flags(), 0x2A);
    });
}

#[test]
fn null_string_yields_none() {
    with_nul_token(None, 0, |tok| {
        assert!(tok.is_empty());
        // SAFETY (both): the token is a local that nothing mutates or frees
        // while the (absent) views live.
        assert_eq!(unsafe { tok.as_bytes() }, None);
        assert!(tok.as_lower_runes().is_none());
        // The token carries no string, so `as_c_str` returns `None`.
        assert!(unsafe { tok.as_c_str() }.is_none());
    });
}

#[test]
fn exposes_nul_terminated_c_str() {
    with_nul_token(Some(b"Hello"), 0, |tok| {
        assert_eq!(tok.len(), 5);
        // SAFETY: `with_nul_token` keeps the buffer alive and unmutated for the
        // whole closure, so the `CStr` stays valid.
        let c_str = unsafe { tok.as_c_str() }.expect("token carries a string");
        assert_eq!(c_str, c"Hello");
    });
}

// These tests call the C `strToLowerRunes`, so they cannot run under miri.
#[cfg(not(miri))]
mod lower_runes {
    use super::*;

    #[test]
    fn lower_runes_lowercases() {
        with_nul_token(Some(b"HeLLo"), 0, |tok| {
            let runes = tok.as_lower_runes().unwrap();
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
        with_nul_token(Some(b"\xC3("), 0, |tok| {
            let runes = tok.as_lower_runes().unwrap();
            assert_eq!(runes, vec![0x00E8]);
        });
    }

    #[test]
    fn lower_runes_preserves_surrogate_bytes() {
        // The three-byte form of a lone surrogate — what a non-BMP codepoint
        // truncated to a rune re-encodes to — survives the conversion instead of
        // being replaced.
        with_nul_token(Some(b"\xED\xA0\x80"), 0, |tok| {
            let runes = tok.as_lower_runes().unwrap();
            assert_eq!(runes, vec![0xD800]);
        });
    }

    #[test]
    fn lower_runes_works_without_nul_termination() {
        // `as_lower_runes` is length-bounded, so it is available on the default
        // (not-necessarily-NUL-terminated) variant too. `with_token` borrows the
        // slice directly, with no terminator after its `len` bytes.
        with_token(Some(b"HeLLo"), 0, |tok| {
            let runes = tok.as_lower_runes().unwrap();
            let expected: Vec<u16> = "hello".encode_utf16().collect();
            assert_eq!(runes, expected);
        });
    }

    // Best exercised under AddressSanitizer, where an unbounded read would fault.
    #[test]
    fn lower_runes_truncated_lead_stays_in_bounds() {
        // `0xF0` starts a four-byte sequence, but only one byte follows it. The C
        // decoder reads a fixed four bytes regardless; `as_lower_runes` pads the
        // decoder input so that over-read lands in its own trailing zero bytes rather
        // than past the token. The leading content still decodes normally.
        with_token(Some(b"ab\xF0"), 0, |tok| {
            let runes = tok.as_lower_runes().expect("conversion succeeds");
            assert_eq!(&runes[..2], &[b'a' as u16, b'b' as u16]);
        });
    }

    #[test]
    fn lower_runes_too_long_yields_none() {
        // One rune past the maximum yields `None` rather than truncating: the C
        // `strToLowerRunes` declines the conversion and returns a null pointer.
        let content = vec![b'a'; ffi::MAX_RUNE_STR_LEN as usize + 1];
        with_nul_token(Some(&content), 0, |tok| {
            assert!(tok.as_lower_runes().is_none());
        });
    }

    #[test]
    fn lower_runes_huge_token_yields_none_without_huge_copy() {
        // A token far larger than any storable term is rejected after copying only a
        // bounded prefix (`MAX_DECODE_BYTES`), not the whole input. The result is the
        // same `None` the full decode would produce, since it can match no term.
        let content = vec![b'a'; 1 << 20];
        with_token(Some(&content), 0, |tok| {
            assert!(tok.as_lower_runes().is_none());
        });
    }
}

#[test]
fn as_ptr_round_trips_to_the_borrowed_token() {
    let raw = build_raw(Some(b"Hello"), 0);
    // SAFETY: `raw` outlives the borrow held by `tok`, satisfying `from_ffi`.
    let tok = unsafe { RSTokenRef::from_ffi(&raw const raw) };
    assert_eq!(tok.as_ptr(), std::ptr::from_ref(&raw));
}

/// A handle retained across an in-place mutation of the token (as query
/// evaluation performs from C) and then read must not be undefined behaviour.
/// This holds only because the handle carries *raw* provenance; a handle derived
/// from a `&ffi::RSToken` would be invalidated by the foreign write. Run under
/// miri (both Stacked and Tree Borrows) this is the regression guard.
#[test]
fn handle_survives_foreign_mutation() {
    let mut raw = build_raw(Some(b"hello"), 0);
    // A raw pointer to the token, as C holds to the node it owns.
    let node: *mut ffi::RSToken = &raw mut raw;
    // SAFETY: `node` is non-null and raw (not reference-derived), and `raw`
    // outlives `tok`, satisfying `from_ffi`.
    let tok = unsafe { RSTokenRef::from_ffi(node.cast_const()) };
    // "C evaluation" mutates the token in place through its own pointer.
    // SAFETY: `node` is valid and there is no live reference aliasing it.
    unsafe { (*node).len = 3 };
    // A later safe accessor must observe the mutation without UB.
    assert_eq!(tok.len(), 3);
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
    let _ = unsafe { RSTokenRef::from_nul_terminated_ffi(&raw const raw) };
}
