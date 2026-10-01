/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! [`string_utils::tag::unescape`], alone and followed by
//! [`string_utils::unicode::tolower_bytes`].

use std::borrow::Cow;

use string_utils::tag::unescape;

#[test]
fn removes_escapes_before_punctuation_and_whitespace() {
    assert_eq!(unescape(br"a\!b\ c\,d"), &b"a!b c,d"[..]);
}

#[test]
fn removes_an_escape_before_a_vertical_tab() {
    // Vertical tab counts as whitespace here, although
    // `u8::is_ascii_whitespace` excludes it.
    assert_eq!(unescape(b"a\\\x0Bb"), &b"a\x0Bb"[..]);
}

#[test]
fn keeps_a_backslash_before_other_bytes() {
    // Only punctuation and whitespace can be escaped, and a trailing backslash
    // has nothing to escape.
    assert_eq!(unescape(br"a\nb\"), &br"a\nb\"[..]);
}

#[test]
fn collapses_one_level_only() {
    // The first `\` escapes the second, which is then kept as a literal and so
    // escapes nothing.
    assert_eq!(unescape(br"\\!"), &br"\!"[..]);
    assert_eq!(unescape(br"\\n"), &br"\n"[..]);
}

#[test]
fn keeps_the_bytes_after_an_interior_nul() {
    // Escape removal stops at the NUL, and what follows is kept verbatim.
    assert_eq!(unescape(b"\\,a\0\\,B"), &b",a\0\\,B"[..]);
}

#[test]
fn borrows_the_input_when_there_is_nothing_to_remove() {
    for input in [&b""[..], b"abc", br"a\n", b"a\0\\,"] {
        let Cow::Borrowed(borrowed) = unescape(input) else {
            panic!("{input:?} was copied");
        };
        assert_eq!(borrowed.as_ptr(), input.as_ptr(), "{input:?}");
        assert_eq!(borrowed.len(), input.len(), "{input:?}");
    }
    assert!(matches!(unescape(br"a\,b"), Cow::Owned(_)));
}

// These tests call the C implementation, so they cannot run under miri.
#[cfg(not(miri))]
mod ffi_comparison {
    use proptest::prelude::*;
    use string_utils::{tag::unescape, unicode::tolower_bytes};

    use crate::ffi_comparison;

    /// What C `tag_strtolower` computes: escape removal, then lowercasing
    /// unless `case_sensitive` is set.
    fn normalize(bytes: &[u8], case_sensitive: bool) -> Vec<u8> {
        let unescaped = unescape(bytes);
        if case_sensitive {
            unescaped.into_owned()
        } else {
            tolower_bytes(&unescaped).into_owned()
        }
    }

    /// Whether C garbles `bytes`: a removable escape before an interior NUL.
    fn c_garbles(bytes: &[u8]) -> bool {
        let Some(nul) = bytes.iter().position(|&b| b == 0) else {
            return false;
        };
        bytes[..nul].windows(2).any(|w| {
            w[0] == b'\\' && (w[1].is_ascii_punctuation() || b" \t\n\r\x0B\x0C".contains(&w[1]))
        })
    }

    #[test]
    fn escape_removal_can_change_the_decoding() {
        // Removing the `\` pairs `C8` with `:`, which reads as U+023A and
        // lowercases to U+2C65. The escaped string would lowercase differently.
        let input = b"\xC8\\:";
        assert_eq!(normalize(input, false), "\u{2c65}".as_bytes());
        assert_eq!(
            ffi_comparison::tag_strtolower(input, false),
            normalize(input, false)
        );
    }

    #[test]
    fn diverges_from_c_only_where_c_garbles() {
        // C shortens the length by the escapes it removed before the NUL
        // without moving the bytes after it.
        let input = b"\\,a\0\\,B";
        assert!(c_garbles(input));
        assert_ne!(
            ffi_comparison::tag_strtolower(input, true),
            normalize(input, true)
        );
    }

    proptest! {
        #[test]
        fn matches_c(bytes in ffi_comparison::tag_bytes(), case_sensitive: bool) {
            // Skipped rather than rejected: the generator produces escapes and
            // NULs often enough that rejecting would exhaust proptest's budget.
            if !c_garbles(&bytes) {
                prop_assert_eq!(
                    normalize(&bytes, case_sensitive),
                    ffi_comparison::tag_strtolower(&bytes, case_sensitive)
                );
            }
        }
    }
}
