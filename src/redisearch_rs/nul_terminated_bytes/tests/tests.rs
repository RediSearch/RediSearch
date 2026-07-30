/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use nul_terminated_bytes::NulTerminatedBytes;

#[test]
fn from_slice_appends_single_nul() {
    let buf = NulTerminatedBytes::from(b"field_name");
    assert_eq!(buf.len(), b"field_name".len());
    assert!(!buf.is_empty());
    assert_eq!(buf.as_bytes(), b"field_name");
    assert_eq!(buf.as_bytes_with_nul(), b"field_name\0");
}

#[test]
fn from_slice_preserves_interior_nul_bytes() {
    let buf = NulTerminatedBytes::from(b"a\0b");
    assert_eq!(buf.len(), 3);
    assert_eq!(buf.as_bytes(), b"a\0b");
    assert_eq!(buf.as_bytes_with_nul(), b"a\0b\0");
}

#[test]
fn empty_payload_is_just_the_nul() {
    let buf = NulTerminatedBytes::from(b"");
    assert_eq!(buf.len(), 0);
    assert!(buf.is_empty());
    assert_eq!(buf.as_bytes(), b"");
    assert_eq!(buf.as_bytes_with_nul(), b"\0");
}

#[test]
fn from_vec_matches_from_slice() {
    let buf = NulTerminatedBytes::from(b"a\0b".to_vec());
    assert_eq!(buf.len(), 3);
    assert_eq!(buf.as_bytes(), b"a\0b");
    assert_eq!(buf.as_bytes_with_nul(), b"a\0b\0");
}

#[test]
fn into_raw_parts_from_raw_parts_round_trip() {
    let buf = NulTerminatedBytes::from(b"a\0b");
    let (ptr, len) = buf.into_raw_parts();
    // SAFETY: `ptr`/`len` come straight from `into_raw_parts` above and have not
    // been freed or reconstructed.
    let buf = unsafe { NulTerminatedBytes::from_raw_parts(ptr, len) };
    assert_eq!(buf.len(), 3);
    assert_eq!(buf.as_bytes_with_nul(), b"a\0b\0");
}
