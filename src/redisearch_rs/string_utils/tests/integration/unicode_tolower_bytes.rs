/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! [`string_utils::unicode::tolower_bytes`], checked against C
//! `unicode_tolower` on arbitrary bytes.

use std::borrow::Cow;

use proptest::prelude::*;
use string_utils::unicode::{tolower, tolower_bytes};

#[test]
fn lowercases_per_character() {
    // Σ always maps to σ, with no context-dependent final-sigma rule.
    assert_eq!(tolower_bytes("ΣΣΣΣΣ".as_bytes()), "σσσσσ".as_bytes());
    assert_eq!(tolower_bytes("ΝΕΑΝΊΑΣ".as_bytes()), "νεανίασ".as_bytes());
}

#[test]
fn ends_at_an_interior_nul() {
    assert_eq!(tolower_bytes(b"A\0B"), &b"a"[..]);
    assert_eq!(tolower_bytes("É\0B".as_bytes()), "é".as_bytes());
    // An overlong encoding of NUL ends the string the same way.
    assert_eq!(tolower_bytes(b"\xC3\x89\xC0\x80B"), "é".as_bytes());
}

#[test]
fn a_nul_after_a_lead_byte_is_read_as_its_continuation() {
    // `C3 00` decodes as U+00C0, which lowercases to U+00E0, so the NUL does
    // not end the string.
    assert_eq!(tolower_bytes(b"\xC3\0B"), "àb".as_bytes());
}

#[test]
fn a_leading_nul_leaves_the_string_unchanged() {
    assert_eq!(tolower_bytes(b"\0AB"), &b"\0AB"[..]);
    assert_eq!(tolower_bytes(b"\xC0\x80AB"), &b"\xC0\x80AB"[..]);
}

#[test]
fn a_malformed_sequence_is_rewritten_in_canonical_form() {
    // `C8 3F` decodes as U+023F, which is already lowercase, and is written
    // back as its well-formed encoding `C8 BF`.
    assert_eq!(tolower_bytes(b"\xC8\x3F"), &b"\xC8\xBF"[..]);
    // A surrogate has no case and comes back as it went in.
    assert_eq!(tolower_bytes(b"A\xED\xA0\x80"), &b"a\xED\xA0\x80"[..]);
}

#[test]
fn a_truncated_trailing_sequence_is_kept_verbatim() {
    assert_eq!(tolower_bytes(b"\xC3\x89\xE2\x82"), &b"\xC3\xA9\xE2\x82"[..]);
}

#[test]
fn borrows_a_prefix_when_nothing_changes() {
    for input in [&b""[..], b"abc", b"ab\0CD", "straße".as_bytes()] {
        let Cow::Borrowed(borrowed) = tolower_bytes(input) else {
            panic!("{input:?} was copied");
        };
        assert_eq!(borrowed.as_ptr(), input.as_ptr(), "{input:?}");
    }
    assert!(matches!(tolower_bytes(b"Ab"), Cow::Owned(_)));
    assert!(matches!(tolower_bytes("Σ".as_bytes()), Cow::Owned(_)));
}

// proptest reads the working directory, which miri's isolation forbids.
#[cfg(not(miri))]
proptest! {
    #[test]
    fn matches_tolower_on_text_without_nul(s in "[^\0]*") {
        let (bytes, text) = (tolower_bytes(s.as_bytes()), tolower(&s));
        prop_assert_eq!(bytes.as_ref(), text.as_bytes());
    }
}

// These tests call the C implementation, so they cannot run under miri.
#[cfg(not(miri))]
mod ffi_comparison {
    use proptest::prelude::*;
    use string_utils::unicode::tolower_bytes;

    use crate::ffi_comparison;

    proptest! {
        #[test]
        fn matches_c(bytes in ffi_comparison::tag_bytes()) {
            let rust = tolower_bytes(&bytes);
            prop_assert_eq!(rust.as_ref(), ffi_comparison::unicode_tolower(&bytes));
        }
    }
}
