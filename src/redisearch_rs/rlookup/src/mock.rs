/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! This module contains mocked implementations of Redis C functions used in
//! [RLookup] for testing purposes. It mocks the ref counting of the cache and
//! the `HiddenString` type used in the C code.

use std::{
    cmp,
    ffi::{c_char, c_int},
    mem::offset_of,
    ptr::NonNull,
    sync::atomic::{AtomicUsize, Ordering},
};

#[derive(Default, Copy, Clone)]
#[repr(C)]
struct UserString {
    user: *const c_char,
    length: usize,
}

/// Mock implementation of `HiddenString_GetUnsafe` from obfuscation/hidden.h for testing purposes
#[unsafe(no_mangle)]
extern "C" fn HiddenString_GetUnsafe(
    value: *const ffi::HiddenString,
    length: *mut usize,
) -> *const c_char {
    let text = unsafe { value.cast::<UserString>().as_ref().unwrap() };
    if text.length != 0 {
        unsafe {
            *length = text.length;
        }
    }

    text.user
}

/// Mock implementation of `NewHiddenString` from obfuscation/hidden.h for testing purposes
#[unsafe(no_mangle)]
extern "C" fn NewHiddenString(
    user: *const c_char,
    length: usize,
    take_ownership: bool,
) -> *mut ffi::HiddenString {
    assert!(
        !take_ownership,
        "tests are not allowed to move ownership to C"
    );
    let value = Box::new(UserString { user, length });
    Box::into_raw(value).cast()
}

/// Mock implementation of `HiddenString_Free` from obfuscation/hidden.h for testing purposes
#[unsafe(no_mangle)]
extern "C" fn HiddenString_Free(value: *const ffi::HiddenString, took_ownership: bool) {
    assert!(
        !took_ownership,
        "tests are not allowed to move ownership to C"
    );

    drop(unsafe { Box::from_raw(value.cast_mut().cast::<UserString>()) });
}

/// Returns the following:
/// - 0, if the strings are the same
/// - a negative value if left is less than right
/// - a positive value if left is greater than right
#[unsafe(no_mangle)]
extern "C" fn HiddenString_CompareC(
    left: Option<NonNull<ffi::HiddenString>>,
    right: *const c_char,
    right_length: usize,
) -> c_int {
    let Some(left_ptr) = left else {
        // Treat None as empty; compare to right's length.
        return if right_length == 0 { 0 } else { -1 };
    };

    let left = unsafe { left_ptr.cast::<UserString>().as_ref() };

    let left_ptr = if left.user.is_null() {
        std::ptr::null()
    } else {
        left.user
    };
    let left_length = left.length;

    let right_ptr = if right.is_null() {
        std::ptr::null()
    } else {
        right
    };

    // Handle empty cases early to avoid UB in strncmp.
    if left_length == 0 && right_length == 0 {
        return 0;
    } else if left_length == 0 {
        return -1;
    } else if right_length == 0 {
        return 1;
    }

    // Safe to call strncmp now (pointers non-null).
    let min_len = cmp::min(left_length, right_length);
    let result = unsafe { libc::strncmp(left_ptr, right_ptr, min_len) };

    if result != 0 {
        result
    } else {
        // Length difference (saturate to i32 to avoid panic).
        let left_len_i32 = c_int::try_from(left_length).unwrap_or(c_int::MAX);
        let right_len_i32 = c_int::try_from(right_length).unwrap_or(c_int::MAX);
        left_len_i32 - right_len_i32
    }
}

/// Mock implementation of `IndexSpecCache_Decref` from spec.h for testing purposes
#[unsafe(no_mangle)]
extern "C" fn IndexSpecCache_Decref(s: Option<NonNull<ffi::IndexSpecCache>>) {
    let s = s.unwrap();
    let refcount = unsafe {
        s.byte_add(offset_of!(ffi::IndexSpecCache, refcount))
            .cast::<usize>()
    };

    let refcount = unsafe { AtomicUsize::from_ptr(refcount.as_ptr()) };

    if refcount.fetch_sub(1, Ordering::Relaxed) == 1 {
        drop(unsafe { Box::from_raw(s.as_ptr()) });
    }
}
