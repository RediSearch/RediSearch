/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! [`string_utils::tag::unescape`].

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
