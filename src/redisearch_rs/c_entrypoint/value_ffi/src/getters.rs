/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::util::expect_value;
use ffi::RedisModuleString;
use libc::{c_char, size_t};
use std::ffi::c_double;
use value::RsValue;

/// Gets the numeric value from an [`RsValue`].
///
/// # Safety
///
/// 1. `value` must point to a valid [`RsValue`] obtained from an `RSValue_*` function.
///
/// # Panic
///
/// Panics if the value is not a number type.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Number_Get(value: *const RsValue) -> c_double {
    // Safety: ensured by caller (1.)
    let value = unsafe { expect_value(value) };

    if let RsValue::Number(number) = value {
        *number
    } else {
        panic!("Expected a number value")
    }
}

/// Borrows an immutable reference to the left value of a trio.
///
/// # Safety
///
/// 1. `value` must point to a valid [`RsValue`] obtained from an `RSValue_*` function.
///
/// # Panic
///
/// Panics if the value is not a trio type.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Trio_GetLeft(value: *const RsValue) -> *const RsValue {
    // Safety: ensured by caller (1.)
    let value = unsafe { expect_value(value) };

    if let RsValue::Trio(trio) = value {
        trio.left().as_ptr()
    } else {
        panic!("Expected a trio value")
    }
}

/// Borrows an immutable reference to the middle value of a trio.
///
/// # Safety
///
/// 1. `value` must point to a valid [`RsValue`] obtained from an `RSValue_*` function.
///
/// # Panic
///
/// Panics if the value is not a trio type.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Trio_GetMiddle(value: *const RsValue) -> *const RsValue {
    // Safety: ensured by caller (1.)
    let value = unsafe { expect_value(value) };

    if let RsValue::Trio(trio) = value {
        trio.middle().as_ptr()
    } else {
        panic!("Expected a trio value")
    }
}

/// Borrows an immutable reference to the right value of a trio.
///
/// # Safety
///
/// 1. `value` must point to a valid [`RsValue`] obtained from an `RSValue_*` function.
///
/// # Panic
///
/// Panics if the value is not a trio type.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Trio_GetRight(value: *const RsValue) -> *const RsValue {
    // Safety: ensured by caller (1.)
    let value = unsafe { expect_value(value) };

    if let RsValue::Trio(trio) = value {
        trio.right().as_ptr()
    } else {
        panic!("Expected a trio value")
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_String_Get(value: *const RsValue, lenp: *mut u32) -> *mut c_char {
    let value = unsafe { expect_value(value) };

    let RsValue::String(string) = value else {
        panic!("unsupported RSValue_String_Get type")
    };

    let (ptr, len) = string.as_ptr_len();

    if let Some(lenp) = unsafe { lenp.as_mut() } {
        *lenp = len;
    }

    ptr as *mut _
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_RedisString_Get(
    value: *const RsValue,
) -> *const RedisModuleString {
    let value = unsafe { expect_value(value) };

    let RsValue::RedisString(redis_string) = value else {
        panic!("Expected redis string")
    };

    redis_string.as_ptr()
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_StringPtrLen(
    value: *const RsValue,
    len_ptr: *mut size_t,
) -> *const c_char {
    let mut value = unsafe { expect_value(value) };

    let (ptr, len) = loop {
        match value {
            RsValue::String(str) => break str.as_ptr_len(),
            RsValue::RedisString(str) => break str.as_ptr_len(),
            RsValue::Ref(ref_val) => value = ref_val.value(),
            RsValue::Trio(trio) => value = trio.left().value(),
            _ => return std::ptr::null(),
        }
    };

    if let Some(len_ptr) = unsafe { len_ptr.as_mut() } {
        *len_ptr = len as usize;
    }
    ptr
}
