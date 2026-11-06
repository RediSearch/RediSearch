/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use fnv_ffi::{fnv_64a_buf, rs_fnv_32a_buf};

/// C hashes the empty token with a NULL buffer; hashing nothing leaves the
/// offset basis unchanged.
#[test]
fn an_empty_null_buffer_hashes_to_the_offset_basis() {
    // SAFETY: (1) `len` is 0, so `buf` may be NULL.
    assert_eq!(unsafe { rs_fnv_32a_buf(std::ptr::null(), 0, 7) }, 7);
    // SAFETY: (1) `len` is 0, so `buf` may be NULL.
    assert_eq!(unsafe { fnv_64a_buf(std::ptr::null(), 0, 7) }, 7);
}

/// Published FNV-1a test vectors for `"a"` with the standard offset bases.
#[test]
fn a_non_empty_buffer_matches_the_reference_hashes() {
    let buf = b"a";
    // SAFETY: (1) `buf` is a live region of `buf.len()` bytes.
    let hash32 = unsafe { rs_fnv_32a_buf(buf.as_ptr().cast(), buf.len(), 0x811c9dc5) };
    assert_eq!(hash32, 0xe40c292c);
    // SAFETY: (1) `buf` is a live region of `buf.len()` bytes.
    let hash64 = unsafe { fnv_64a_buf(buf.as_ptr().cast(), buf.len(), 0xcbf29ce484222325) };
    assert_eq!(hash64, 0xaf63dc4c8601ec8c);
}
