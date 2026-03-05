/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#![allow(clippy::missing_safety_doc, clippy::undocumented_unsafe_blocks)]

use redis_mock::mock_or_stub_missing_redis_c_symbols;
use std::ffi::c_char;
use value::RsString;

mock_or_stub_missing_redis_c_symbols!();

#[allow(non_upper_case_globals)]
#[unsafe(no_mangle)]
pub static mut RSDummyContext: *mut redis_mock::ffi::RedisModuleCtx =
    redis_mock::globals::redis_module_ctx();

/// Allocate a nul-terminated C string using the mock Redis allocator.
fn rm_alloc_string(s: &str) -> (*mut c_char, u32) {
    let len = s.len();
    let ptr = redis_mock::allocator::alloc_shim(len + 1) as *mut c_char;
    unsafe { std::ptr::copy_nonoverlapping(s.as_ptr(), ptr as *mut u8, len) };
    let nul_ptr = unsafe { ptr.add(len) };
    unsafe { *nul_ptr = 0 };
    (ptr, len as u32)
}

#[test]
fn from_vec_as_ptr_len() {
    let s = RsString::from_vec(b"hello".to_vec());
    let (ptr, len) = s.as_ptr_len();
    let bytes = unsafe { std::slice::from_raw_parts(ptr as *const u8, len as usize) };
    assert_eq!(bytes, b"hello");
    assert_eq!(len, 5);
}

#[test]
fn from_vec_as_bytes() {
    let s = RsString::from_vec(b"hello".to_vec());
    assert_eq!(s.as_bytes(), b"hello");
}

#[test]
#[cfg_attr(miri, ignore = "miri does not support FFI functions")]
fn rm_alloc_string_as_ptr_len() {
    let (ptr, len) = rm_alloc_string("redis");
    let s = unsafe { RsString::rm_alloc_string(ptr, len) };
    let (out_ptr, out_len) = s.as_ptr_len();
    let bytes = unsafe { std::slice::from_raw_parts(out_ptr as *const u8, out_len as usize) };
    assert_eq!(bytes, b"redis");
    assert_eq!(out_len, 5);
}

#[test]
#[cfg_attr(miri, ignore = "miri does not support FFI functions")]
fn rm_alloc_string_as_bytes() {
    let (ptr, len) = rm_alloc_string("redis");
    let s = unsafe { RsString::rm_alloc_string(ptr, len) };
    assert_eq!(s.as_bytes(), b"redis");
}

#[test]
fn borrowed_string_as_ptr_len() {
    let s = unsafe { RsString::borrowed_string(c"borrowed".as_ptr(), 8) };
    let (ptr, len) = s.as_ptr_len();
    let bytes = unsafe { std::slice::from_raw_parts(ptr as *const u8, len as usize) };
    assert_eq!(bytes, b"borrowed");
    assert_eq!(len, 8);
}

#[test]
fn borrowed_string_as_bytes() {
    let s = unsafe { RsString::borrowed_string(c"borrowed".as_ptr(), 8) };
    assert_eq!(s.as_bytes(), b"borrowed");
}
