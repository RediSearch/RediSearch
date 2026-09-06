/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Tests for [`Tag`], the NUL-free-by-construction tag newtype.

use tag_index::Tag;

#[test]
fn rejects_an_interior_nul() {
    assert!(Tag::new(b"foo\0bar").is_none());
}

#[test]
fn accepts_nul_free_bytes() {
    assert_eq!(Tag::new(b"foo").map(Tag::as_bytes), Some(&b"foo"[..]));
}

#[test]
fn as_ptr_points_at_the_underlying_bytes() {
    let tag = Tag::new(b"foo").unwrap();
    assert_eq!(tag.as_ptr(), tag.as_bytes().as_ptr());
}

#[test]
fn tag_is_not_a_fresh_allocation() {
    let v = "foo".to_string();
    let tag = Tag::new(v.as_bytes()).unwrap();
    assert_eq!(tag.as_ptr(), v.as_ptr());
}

#[test]
fn new_unchecked_accepts_nul_free_bytes() {
    // SAFETY: the literal has no interior NUL byte.
    let tag = unsafe { Tag::new_unchecked(b"foo") };
    assert_eq!(tag.as_bytes(), b"foo");
}
