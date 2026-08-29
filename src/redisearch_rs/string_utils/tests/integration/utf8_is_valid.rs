/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Tests for [`string_utils::utf8::is_valid`].

use string_utils::utf8::is_valid;

#[test]
fn accepts_well_formed() {
    assert!(is_valid(b""));
    assert!(is_valid(b"plain ascii"));
    assert!(is_valid("héllo wörld".as_bytes()));
    assert!(is_valid("日本語".as_bytes()));
    // U+1F600, an astral plane code point encoded as four bytes.
    assert!(is_valid("\u{1F600}".as_bytes()));
    // Interior NUL bytes are valid UTF-8; only the C boundary treats them as terminators.
    assert!(is_valid(b"a\0b"));
}

#[test]
fn rejects_ill_formed() {
    // A continuation byte with no lead byte.
    assert!(!is_valid(b"\x80"));
    // A lead byte announcing two bytes, with the continuation byte missing.
    assert!(!is_valid(b"\xc3"));
    // Overlong encoding of '/' (U+002F), the classic path-traversal smuggling form.
    assert!(!is_valid(b"\xc0\xaf"));
    // U+D800, a surrogate code point, in the encoding UTF-8 forbids for it.
    assert!(!is_valid(b"\xed\xa0\x80"));
    // A five-byte sequence, above the U+10FFFF ceiling.
    assert!(!is_valid(b"\xf8\x88\x80\x80\x80"));
    // Latin-1 bytes: valid text in another encoding, ill-formed here.
    assert!(!is_valid(b"caf\xe9"));
    // Ill-formed bytes are rejected wherever they sit in an otherwise valid value.
    assert!(!is_valid(b"valid prefix \xff trailing"));
}
