//! This module contains mocked implementations of some Redis C functions.

use std::{
    cmp,
    ffi::{CStr, c_char, c_int},
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
    let Some(left) = left else {
        return -1 * c_int::try_from(right_length).unwrap();
    };

    let left = unsafe { left.cast::<UserString>().as_ref() };

    unsafe {
        let left = (!left.user.is_null()).then(|| CStr::from_ptr(left.user));
        let right = (!right.is_null()).then(|| CStr::from_ptr(right));
        println!("HiddenString_CompareC = left {left:?} right {right:?}",);
    }

    let result = unsafe { libc::strncmp(left.user, right, cmp::min(left.length, right_length)) };

    println!("HiddenString_CompareC = result = {result}");

    if result != 0 || left.length == right_length {
        result
    } else {
        c_int::try_from(left.length).unwrap() - c_int::try_from(right_length).unwrap()
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
