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
use std::ffi::{CString, c_char};
use value::RsString;

mock_or_stub_missing_redis_c_symbols!();

#[allow(non_upper_case_globals)]
#[unsafe(no_mangle)]
pub static mut RSDummyContext: *mut redis_mock::ffi::RedisModuleCtx =
    redis_mock::globals::redis_module_ctx();

/// Allocate a nul-terminated C string using the mock Redis allocator.
fn rm_alloc_cstring(s: &str) -> (*mut c_char, u32) {
    let len = s.len();
    let ptr = redis_mock::allocator::alloc_shim(len + 1) as *mut c_char;
    unsafe { std::ptr::copy_nonoverlapping(s.as_ptr(), ptr as *mut u8, len) };
    let nul_ptr = unsafe { ptr.add(len) };
    unsafe { *nul_ptr = 0 };
    (ptr, len as u32)
}

/// Allocate a C string **without** a nul terminator using the mock Redis allocator.
fn rm_alloc_raw(s: &str) -> (*mut c_char, u32) {
    let len = s.len();
    let ptr = redis_mock::allocator::alloc_shim(len) as *mut c_char;
    unsafe {
        std::ptr::copy_nonoverlapping(s.as_ptr(), ptr as *mut u8, len);
    }
    (ptr, len as u32)
}

#[test]
fn cstring_as_ptr_len_checked() {
    let s = RsString::cstring(CString::new("hello").unwrap());
    let (ptr, len) = s.as_ptr_len_checked();
    let bytes = unsafe { std::slice::from_raw_parts(ptr as *const u8, len as usize) };
    assert_eq!(bytes, b"hello");
    assert_eq!(len, 5);
}

#[test]
fn cstring_as_ptr_len() {
    let s = RsString::cstring(CString::new("hello").unwrap());
    let (ptr, len) = s.as_ptr_len();
    let bytes = unsafe { std::slice::from_raw_parts(ptr as *const u8, len as usize) };
    assert_eq!(bytes, b"hello");
    assert_eq!(len, 5);
}

#[test]
fn cstring_as_bytes_checked() {
    let s = RsString::cstring(CString::new("hello").unwrap());
    assert_eq!(s.as_bytes_checked(), b"hello");
}

#[test]
fn cstring_as_bytes() {
    let s = RsString::cstring(CString::new("hello").unwrap());
    assert_eq!(s.as_bytes(), b"hello");
}

#[test]
fn rm_alloc_string_as_ptr_len_checked() {
    let (ptr, len) = rm_alloc_cstring("redis");
    let s = unsafe { RsString::rm_alloc_string(ptr, len) };
    let (out_ptr, out_len) = s.as_ptr_len_checked();
    let bytes = unsafe { std::slice::from_raw_parts(out_ptr as *const u8, out_len as usize) };
    assert_eq!(bytes, b"redis");
    assert_eq!(out_len, 5);
}

#[test]
fn rm_alloc_string_as_bytes_checked() {
    let (ptr, len) = rm_alloc_cstring("redis");
    let s = unsafe { RsString::rm_alloc_string(ptr, len) };
    assert_eq!(s.as_bytes_checked(), b"redis");
}

#[test]
fn rm_alloc_string_as_ptr_len() {
    let (ptr, len) = rm_alloc_cstring("redis");
    let s = unsafe { RsString::rm_alloc_string(ptr, len) };
    let (out_ptr, out_len) = s.as_ptr_len();
    let bytes = unsafe { std::slice::from_raw_parts(out_ptr as *const u8, out_len as usize) };
    assert_eq!(bytes, b"redis");
    assert_eq!(out_len, 5);
}

#[test]
fn rm_alloc_string_as_bytes() {
    let (ptr, len) = rm_alloc_cstring("redis");
    let s = unsafe { RsString::rm_alloc_string(ptr, len) };
    assert_eq!(s.as_bytes(), b"redis");
}

#[test]
fn rm_alloc_without_nul_as_ptr_len() {
    let (ptr, len) = rm_alloc_raw("raw");
    let s = unsafe { RsString::rm_alloc_string_without_nul_terminator(ptr, len) };
    let (out_ptr, out_len) = s.as_ptr_len();
    let bytes = unsafe { std::slice::from_raw_parts(out_ptr as *const u8, out_len as usize) };
    assert_eq!(bytes, b"raw");
    assert_eq!(out_len, 3);
}

#[test]
fn rm_alloc_without_nul_as_bytes() {
    let (ptr, len) = rm_alloc_raw("raw");
    let s = unsafe { RsString::rm_alloc_string_without_nul_terminator(ptr, len) };
    assert_eq!(s.as_bytes(), b"raw");
}

#[test]
#[cfg(debug_assertions)]
#[should_panic(expected = "as_ptr_len_checked() called on possibly non-nul-terminated string")]
fn rm_alloc_without_nul_as_ptr_len_checked_panics() {
    let (ptr, len) = rm_alloc_raw("raw");
    let s = unsafe { RsString::rm_alloc_string_without_nul_terminator(ptr, len) };
    s.as_ptr_len_checked();
}

#[test]
#[cfg(debug_assertions)]
#[should_panic(expected = "as_bytes_checked() called on possibly non-nul-terminated string")]
fn rm_alloc_without_nul_as_bytes_checked_panics() {
    let (ptr, len) = rm_alloc_raw("raw");
    let s = unsafe { RsString::rm_alloc_string_without_nul_terminator(ptr, len) };
    s.as_bytes_checked();
}

#[test]
fn borrowed_string_as_ptr_len_checked() {
    let s = unsafe { RsString::borrowed_string(c"borrowed".as_ptr(), 8) };
    let (ptr, len) = s.as_ptr_len_checked();
    let bytes = unsafe { std::slice::from_raw_parts(ptr as *const u8, len as usize) };
    assert_eq!(bytes, b"borrowed");
    assert_eq!(len, 8);
}

#[test]
fn borrowed_string_as_bytes_checked() {
    let s = unsafe { RsString::borrowed_string(c"borrowed".as_ptr(), 8) };
    assert_eq!(s.as_bytes_checked(), b"borrowed");
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
