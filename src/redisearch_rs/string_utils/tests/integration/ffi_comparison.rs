/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! The C string normalization functions the Rust ones are compared against,
//! and inputs to compare them on.

use std::ffi::{c_char, c_void};

use proptest::prelude::*;

/// Copy `bytes` into a NUL-terminated buffer from the Redis module allocator,
/// run `f` on its pointer and length, and return the bytes the pointer and
/// length then describe, freeing whichever buffer that is.
fn with_module_buffer(bytes: &[u8], f: impl FnOnce(&mut *mut c_char, &mut usize)) -> Vec<u8> {
    // SAFETY: the mock allocator sets this once, before any test runs, and
    // nothing writes it afterwards.
    let alloc = unsafe { redis_module::RedisModule_Alloc }.expect("RedisModule_Alloc unset");
    // SAFETY: as above.
    let free = unsafe { redis_module::RedisModule_Free }.expect("RedisModule_Free unset");

    // SAFETY: the mock allocator is installed for the whole test binary.
    let buf = unsafe { alloc(bytes.len() + 1) }.cast::<u8>();
    assert!(!buf.is_null());
    // SAFETY: `buf` is a fresh allocation of `bytes.len() + 1` bytes that
    // nothing else reaches.
    let dst = unsafe { std::slice::from_raw_parts_mut(buf, bytes.len() + 1) };
    dst[..bytes.len()].copy_from_slice(bytes);
    dst[bytes.len()] = 0;

    let mut ptr = buf.cast::<c_char>();
    let mut len = bytes.len();
    f(&mut ptr, &mut len);

    // SAFETY: `f` leaves `ptr` addressing at least `len` initialized bytes.
    let result = unsafe { std::slice::from_raw_parts(ptr.cast::<u8>(), len) }.to_vec();
    // SAFETY: `ptr` is the module allocation now holding the result, and
    // `result` is a copy.
    unsafe { free(ptr.cast::<c_void>()) };
    result
}

/// C `tag_strtolower`.
pub fn tag_strtolower(bytes: &[u8], case_sensitive: bool) -> Vec<u8> {
    with_module_buffer(bytes, |ptr, len| {
        // SAFETY: `*ptr` is a NUL-terminated module allocation of `*len`
        // content bytes, which the call may free and replace.
        unsafe { ffi::tag_strtolower(ptr, len, i32::from(case_sensitive)) }
    })
}

/// C `unicode_tolower`.
pub fn unicode_tolower(bytes: &[u8]) -> Vec<u8> {
    with_module_buffer(bytes, |ptr, len| {
        // SAFETY: `*ptr` addresses `*len` bytes followed by a terminator. The
        // call rewrites them in place or returns a replacement module
        // allocation, leaving the original to the caller.
        let replacement = unsafe { ffi::unicode_tolower_fn(*ptr, len) };
        if !replacement.is_null() {
            // SAFETY: the mock allocator is installed, as above.
            let free = unsafe { redis_module::RedisModule_Free }.expect("RedisModule_Free unset");
            // SAFETY: `*ptr` is the original module allocation, which nothing
            // reads once replaced.
            unsafe { free(ptr.cast::<c_void>()) };
            *ptr = replacement;
        }
    })
}

/// Byte strings mixing escapes, NULs, well-formed characters of every width,
/// and arbitrary — often malformed — bytes.
pub fn tag_bytes() -> impl Strategy<Value = Vec<u8>> {
    let piece = prop_oneof![
        3 => Just(vec![b'\\']),
        2 => (0x20..=0x2Fu8).prop_map(|b| vec![b]),
        3 => (0x41..=0x7Au8).prop_map(|b| vec![b]),
        1 => Just(vec![0]),
        3 => any::<u8>().prop_map(|b| vec![b]),
        3 => any::<char>().prop_map(|c| c.to_string().into_bytes()),
    ];
    proptest::collection::vec(piece, 0..40).prop_map(|pieces| pieces.concat())
}
