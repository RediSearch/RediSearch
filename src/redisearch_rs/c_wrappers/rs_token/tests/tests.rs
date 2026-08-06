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
use rs_token::{RSTokenMut, RSTokenRef, TokenTooLong};

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

/// The production lifecycle: C mutates a node's token in place *between*
/// evaluations, and each evaluation mints a fresh handle. A handle's `'a` is a
/// no-mutation window, so the write must fall outside every window — but the
/// pointer C writes through, and the one a later handle reads through, alias the
/// same token, and neither may be invalidated by the other. Run under miri (both
/// Stacked and Tree Borrows) this is the regression guard for that aliasing.
#[test]
fn handle_reflects_mutation_between_borrows() {
    let mut raw = build_raw(Some(b"hello"), 0);
    // A raw pointer to the token, as C holds to the node it owns.
    let node: *mut ffi::RSToken = &raw mut raw;

    // First evaluation: mint a handle, read through it, and let it die.
    // SAFETY: `node` is non-null, valid, and raw (not reference-derived); `raw`
    // outlives the handle and nothing mutates the token while it is live.
    let tok = unsafe { RSTokenRef::from_ffi(node.cast_const()) };
    assert_eq!(tok.len(), 5);

    // "C evaluation" then mutates the token in place through its own pointer,
    // with no handle live.
    // SAFETY: `node` is valid and there is no live reference aliasing it.
    unsafe { (*node).len = 3 };

    // The next evaluation mints a fresh handle and observes the mutation.
    // SAFETY: as above for the new no-mutation window.
    let tok = unsafe { RSTokenRef::from_ffi(node.cast_const()) };
    assert_eq!(tok.len(), 3);
    assert_eq!(tok.as_bytes(), Some(&b"hel"[..]));
}

/// Back a token's string with a mutable, NUL-terminated buffer — the way the
/// query parser does — apply `f` to an [`RSTokenMut`] over it, and report what the
/// rewrite left behind.
///
/// `content` is the string *without* its terminator, so `len` is the content
/// length and `str_[len]` is the terminator. Returns the token's resulting bytes
/// alongside the whole backing buffer, so a caller can also inspect the byte past
/// the token's new end.
///
/// The handle is confined to an inner scope: it is the only way to reach the token
/// while it is live, and the buffer is read again only after it is dropped.
fn rewrite(
    content: &[u8],
    f: impl FnOnce(&mut RSTokenMut) -> Result<(), TokenTooLong>,
) -> (Vec<u8>, Vec<c_char>) {
    let mut buf: Vec<c_char> = content
        .iter()
        .map(|&b| b as c_char)
        .chain(std::iter::once(0))
        .collect();
    // SAFETY: `RSToken` is plain data whose all-zero bit pattern is a valid,
    // empty token.
    let mut raw: ffi::RSToken = unsafe { std::mem::zeroed() };
    raw.str_ = buf.as_mut_ptr();
    raw.len = content.len();
    {
        // SAFETY: `raw` is a valid token that outlives the handle, and its `str_`
        // addresses `buf`'s `len` writable content bytes followed by the terminator
        // at `str_[len]`. Nothing else reaches the token or the buffer for the
        // handle's lifetime, which ends with this scope.
        let mut tok = unsafe { RSTokenMut::from_nul_terminated_ffi(&raw mut raw) };
        // None of these fixtures is anywhere near the length limit, so a refusal
        // here would mean the bound check fired on a token it should not have.
        f(&mut tok).expect("token is short enough to rewrite");
    }
    let bytes = buf[..raw.len].iter().map(|&c| c as u8).collect();
    (bytes, buf)
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

/// The same check on the mutating handle, which repeats it rather than sharing
/// [`RSTokenRef`]'s: its constructor takes `*mut`, so it cannot go through the
/// shared one to get it.
#[cfg(debug_assertions)]
#[test]
#[should_panic(expected = "token string must be NUL-terminated")]
fn non_nul_terminated_string_trips_debug_assert_on_mut_handle() {
    // As above: `str_[len]` is `b'X'` — in bounds and readable, so the check's own
    // dereference is sound, but non-zero, so the assertion fails.
    let mut buf: Vec<c_char> = b"abcX".iter().map(|&b| b as c_char).collect();
    // SAFETY: `RSToken` is plain data whose all-zero bit pattern is a valid,
    // empty token.
    let mut raw: ffi::RSToken = unsafe { std::mem::zeroed() };
    raw.str_ = buf.as_mut_ptr();
    raw.len = 3;
    // SAFETY: `raw.str_`/`raw.len` describe a valid 3-byte writable range whose
    // next byte is in bounds and readable, so the debug check's dereference is
    // sound — it observes the missing terminator and panics. Nothing else reaches
    // `raw` or `buf` for the handle's lifetime.
    let _ = unsafe { RSTokenMut::from_nul_terminated_ffi(&raw mut raw) };
}

#[test]
fn mut_handle_accepts_a_token_carrying_no_string() {
    // A null `str_` is an empty token, per the constructor's contract: the
    // termination check has nothing to inspect, the shared reborrow reports no
    // string, and a rewrite is a no-op rather than a null dereference. The
    // `rewrite` helper always allocates a buffer, so this shape is built by hand.
    //
    // No FFI is reached — `remove_wildcard_escapes` returns on the empty case
    // before the converter — so this needs no `miri` exemption.
    let mut raw = build_raw(None, 0);
    {
        // SAFETY: `raw` is a valid token that outlives the handle and carries no
        // string, so `len` is zero as the contract requires. Nothing else reaches
        // it for the handle's lifetime, which ends with this scope.
        let mut tok = unsafe { RSTokenMut::from_nul_terminated_ffi(&raw mut raw) };
        assert!(tok.as_ref().is_empty());
        assert_eq!(tok.as_ref().as_bytes(), None);
        assert_eq!(tok.as_ref().as_c_str(), None);
        assert_eq!(tok.remove_wildcard_escapes(), Ok(()));
        assert!(tok.as_ref().is_empty());
    }
    assert!(raw.str_.is_null(), "the token must be left untouched");
    assert_eq!(raw.len, 0);
}

#[test]
fn mut_handle_reads_through_as_ref() {
    // The shared reborrow is how a rewrite's result is read back, so it must see
    // the same string and length the handle holds.
    let (_bytes, _buf) = rewrite(b"he?l*o", |tok| {
        assert_eq!(tok.as_ref().len(), 6);
        assert_eq!(tok.as_ref().as_bytes(), Some(&b"he?l*o"[..]));
        assert_eq!(tok.as_ref().as_c_str(), Some(c"he?l*o"));
        Ok(())
    });
}

#[test]
fn remove_wildcard_escapes_leaves_an_empty_token_alone() {
    // An empty query parameter reaches the AST as a one-byte allocation holding
    // only the terminator. The empty case returns before the converter is called
    // at all, which is what keeps the write off the end of that single byte —
    // hence no FFI here, and no reason to skip this under `miri`.
    let (bytes, buf) = rewrite(b"", |tok| tok.remove_wildcard_escapes());
    assert_eq!(bytes, b"");
    assert_eq!(buf, vec![0 as c_char], "the buffer must be left untouched");
}

// These tests call the C `Wildcard_RemoveEscape`, so they cannot run under miri.
#[cfg(not(miri))]
mod remove_wildcard_escapes {
    use super::*;

    #[test]
    fn collapses_escapes_in_place() {
        // `w'a\*b\?c'` — the escaped `*` and `?` are literals, so both backslashes
        // go and the token shortens by two.
        let (bytes, buf) = rewrite(br"a\*b\?c", |tok| tok.remove_wildcard_escapes());
        assert_eq!(bytes, b"a*b?c");
        assert_eq!(
            buf[bytes.len()],
            0,
            "the token must be re-terminated in place"
        );
    }

    #[test]
    fn leaves_an_unescaped_token_alone() {
        let (bytes, _buf) = rewrite(b"he?l*o", |tok| tok.remove_wildcard_escapes());
        assert_eq!(bytes, b"he?l*o");
    }

    #[test]
    fn collapses_one_level_only() {
        // `w'a\\b'` — the escaped backslash collapses to a single literal one, which
        // is left as-is. Removal is single-level, so a second application would eat
        // that backslash too; the token must only ever be unescaped once.
        let (bytes, _buf) = rewrite(br"a\\b", |tok| tok.remove_wildcard_escapes());
        assert_eq!(bytes, br"a\b");
    }

    #[test]
    fn collapses_a_leading_escape() {
        // The escape sits at the very start, so the copy begins at the token's first
        // byte rather than partway through it.
        let (bytes, _buf) = rewrite(br"\*abc", |tok| tok.remove_wildcard_escapes());
        assert_eq!(bytes, b"*abc");
    }

    #[test]
    fn collapses_a_token_that_is_all_escapes() {
        // Every byte pair is an escape, so the token halves — the largest shrink a
        // non-empty pattern can produce.
        let (bytes, buf) = rewrite(br"\*\?", |tok| tok.remove_wildcard_escapes());
        assert_eq!(bytes, b"*?");
        assert_eq!(
            buf[bytes.len()],
            0,
            "the token must be re-terminated in place"
        );
    }

    #[test]
    fn drops_a_trailing_backslash() {
        // The escape has nothing after it, so the converter copies the terminator
        // into its place and stops. This is the only shape that reads the byte past
        // the token's content, which is why the buffer must stay NUL-terminated.
        let (bytes, buf) = rewrite(b"abc\\", |tok| tok.remove_wildcard_escapes());
        assert_eq!(bytes, b"abc");
        assert_eq!(
            buf[bytes.len()],
            0,
            "the token must be re-terminated in place"
        );
    }

    #[test]
    fn handles_a_non_utf8_token() {
        // A pattern reaching the AST through a query parameter is binary data, so the
        // rewrite has to work on bytes: `0xff` is not valid UTF-8 in any position.
        let (bytes, _buf) = rewrite(b"a\\\xffb", |tok| tok.remove_wildcard_escapes());
        assert_eq!(bytes, b"a\xffb");
    }

    #[test]
    fn truncates_at_an_interior_nul() {
        // An interior NUL ends the pattern even though there is no escape to collapse,
        // so the token is cut there rather than kept whole.
        let (bytes, _buf) = rewrite(b"a\0b", |tok| tok.remove_wildcard_escapes());
        assert_eq!(bytes, b"a");
    }

    #[test]
    fn shortened_token_still_reads_as_a_c_str() {
        // The rewrite re-terminates at the new end, so a handle taken afterwards
        // still yields a NUL-terminated string rather than running on into the
        // bytes the collapse left stranded.
        let (_bytes, _buf) = rewrite(br"a\*b\?c", |tok| {
            tok.remove_wildcard_escapes()?;
            assert_eq!(tok.as_ref().as_c_str(), Some(c"a*b?c"));
            Ok(())
        });
    }
}
