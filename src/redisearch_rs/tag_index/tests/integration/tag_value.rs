/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Tests for [`TagValue`], the NUL-free-by-construction tag newtype.

use tag_index::TagValue;

#[test]
fn rejects_an_interior_nul() {
    assert!(TagValue::new(b"foo\0bar").is_none());
}

#[test]
fn accepts_nul_free_bytes() {
    assert_eq!(
        TagValue::new(b"foo").map(TagValue::as_bytes),
        Some(&b"foo"[..])
    );
}

#[test]
fn from_str_rejects_an_interior_nul() {
    assert!(TagValue::from_str("foo\0bar").is_none());
}
