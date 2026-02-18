/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#![allow(clippy::missing_safety_doc, clippy::undocumented_unsafe_blocks)]

use libc::size_t;
use redis_mock::mock_or_stub_missing_redis_c_symbols;
use std::ffi::{CString, c_char};
use value::{RsString, RsValue, SharedRsValue};
use value_ffi::constructors::{
    RSValue_NewBorrowedString, RSValue_NewCopiedString, RSValue_NewNull, RSValue_NewNumber,
    RSValue_NewString, RSValue_NewTrio,
};
use value_ffi::getters::{RSValue_String_Get, RSValue_StringPtrLen};
use value_ffi::setters::{RSValue_SetConstString, RSValue_SetString};

mock_or_stub_missing_redis_c_symbols!();

#[allow(non_upper_case_globals)]
#[unsafe(no_mangle)]
pub static mut RSDummyContext: *mut redis_mock::ffi::RedisModuleCtx =
    redis_mock::globals::redis_module_ctx();

/// Allocate a null-terminated C string using the mock Redis allocator.
///
/// Returns a pointer and the string length (without the null terminator).
/// The returned pointer is suitable for [`RSValue_NewString`] and [`RSValue_SetString`].
fn rm_alloc_cstring(s: &str) -> (*mut c_char, u32) {
    let len = s.len();
    let ptr = redis_mock::allocator::alloc_shim(len + 1) as *mut c_char;
    unsafe {
        std::ptr::copy_nonoverlapping(s.as_ptr(), ptr as *mut u8, len);
        *ptr.add(len) = 0;
    }
    (ptr, len as u32)
}

/// Reclaim ownership of a raw [`RsValue`] pointer and drop it.
///
/// # Safety
///
/// `ptr` must be a valid owned pointer obtained from an `RSValue_*` constructor.
unsafe fn drop_value(ptr: *mut RsValue) {
    drop(unsafe { SharedRsValue::from_raw(ptr) });
}

#[test]
fn new_string_creates_rm_alloc_string() {
    let (ptr, len) = rm_alloc_cstring("hello");
    let value = unsafe { RSValue_NewString(ptr, len) };

    let mut out_len: u32 = 0;
    let str_ptr = unsafe { RSValue_String_Get(value, &mut out_len) };
    let bytes = unsafe { std::slice::from_raw_parts(str_ptr as *const u8, out_len as usize) };
    assert_eq!(bytes, b"hello");
    assert_eq!(out_len, 5);

    unsafe { drop_value(value) };
}

#[test]
fn new_borrowed_string_creates_borrowed_string() {
    let value = unsafe { RSValue_NewBorrowedString(c"world".as_ptr(), 5) };

    let mut out_len: u32 = 0;
    let str_ptr = unsafe { RSValue_String_Get(value, &mut out_len) };
    let bytes = unsafe { std::slice::from_raw_parts(str_ptr as *const u8, out_len as usize) };
    assert_eq!(bytes, b"world");
    assert_eq!(out_len, 5);

    unsafe { drop_value(value) };
}

#[test]
fn new_copied_string_creates_independent_copy() {
    let source = CString::new("copied").unwrap();
    let value = unsafe { RSValue_NewCopiedString(source.as_ptr(), source.to_bytes().len() as u32) };

    // Drop the source to demonstrate the copy is independent.
    drop(source);

    let mut out_len: u32 = 0;
    let str_ptr = unsafe { RSValue_String_Get(value, &mut out_len) };
    let bytes = unsafe { std::slice::from_raw_parts(str_ptr as *const u8, out_len as usize) };
    assert_eq!(bytes, b"copied");
    assert_eq!(out_len, 6);

    unsafe { drop_value(value) };
}

#[test]
fn string_get_accepts_null_lenp() {
    let value = unsafe { RSValue_NewBorrowedString(c"test".as_ptr(), 4) };

    let str_ptr = unsafe { RSValue_String_Get(value, std::ptr::null_mut()) };
    assert!(!str_ptr.is_null());

    unsafe { drop_value(value) };
}

/// `RSValue_String_Get` panics (aborts) when called on a non-`String` value.
/// Because the function is `extern "C"`, the panic cannot unwind and instead
/// triggers a process abort, which makes `#[should_panic]` unusable here.
/// The graceful alternative `RSValue_StringPtrLen` returns null instead and
/// is tested in [`string_ptr_len_returns_null_for_non_string`].

#[test]
fn string_ptr_len_with_string_value() {
    let value = unsafe { RSValue_NewBorrowedString(c"direct".as_ptr(), 6) };

    let mut out_len: size_t = 0;
    let str_ptr = unsafe { RSValue_StringPtrLen(value, &mut out_len) };
    let bytes = unsafe { std::slice::from_raw_parts(str_ptr as *const u8, out_len) };
    assert_eq!(bytes, b"direct");
    assert_eq!(out_len, 6);

    unsafe { drop_value(value) };
}

#[test]
fn string_ptr_len_dereferences_ref_to_string() {
    // No FFI constructor for Ref, so build one directly from Rust types.
    let inner = SharedRsValue::new(RsValue::String(RsString::cstring(
        CString::new("referenced").unwrap(),
    )));
    let ref_value = SharedRsValue::new(RsValue::Ref(inner));
    let ptr = ref_value.into_raw() as *mut RsValue;

    let mut out_len: size_t = 0;
    let str_ptr = unsafe { RSValue_StringPtrLen(ptr, &mut out_len) };
    let bytes = unsafe { std::slice::from_raw_parts(str_ptr as *const u8, out_len) };
    assert_eq!(bytes, b"referenced");
    assert_eq!(out_len, 10);

    unsafe { drop_value(ptr) };
}

#[test]
fn string_ptr_len_follows_trio_left_to_string() {
    let left = unsafe { RSValue_NewBorrowedString(c"left-value".as_ptr(), 10) };
    let middle = RSValue_NewNull();
    let right = RSValue_NewNull();
    let trio = unsafe { RSValue_NewTrio(left, middle, right) };

    let mut out_len: size_t = 0;
    let str_ptr = unsafe { RSValue_StringPtrLen(trio, &mut out_len) };
    let bytes = unsafe { std::slice::from_raw_parts(str_ptr as *const u8, out_len) };
    assert_eq!(bytes, b"left-value");
    assert_eq!(out_len, 10);

    unsafe { drop_value(trio) };
}

#[test]
fn string_ptr_len_returns_null_for_non_string() {
    let value = RSValue_NewNumber(42.0);

    let mut out_len: size_t = 99;
    let str_ptr = unsafe { RSValue_StringPtrLen(value, &mut out_len) };
    assert!(str_ptr.is_null());
    // Length should not be written when null is returned.
    assert_eq!(out_len, 99);

    unsafe { drop_value(value) };
}

#[test]
fn set_string_replaces_value_with_rm_alloc_string() {
    let value = RSValue_NewNumber(42.0);

    let (str_ptr, _) = rm_alloc_cstring("updated");
    unsafe { RSValue_SetString(value, str_ptr, 7) };

    let mut out_len: u32 = 0;
    let result_ptr = unsafe { RSValue_String_Get(value, &mut out_len) };
    let bytes = unsafe { std::slice::from_raw_parts(result_ptr as *const u8, out_len as usize) };
    assert_eq!(bytes, b"updated");
    assert_eq!(out_len, 7);

    unsafe { drop_value(value) };
}

#[test]
fn set_const_string_replaces_value_with_borrowed_string() {
    let value = RSValue_NewNumber(42.0);

    unsafe { RSValue_SetConstString(value, c"constant".as_ptr(), 8) };

    let mut out_len: u32 = 0;
    let result_ptr = unsafe { RSValue_String_Get(value, &mut out_len) };
    let bytes = unsafe { std::slice::from_raw_parts(result_ptr as *const u8, out_len as usize) };
    assert_eq!(bytes, b"constant");
    assert_eq!(out_len, 8);

    unsafe { drop_value(value) };
}
