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

/// Returns a pointer to the string data of an [`RsValue`] and optionally writes the string
/// length to `lenp`, if `lenp` is a non-null pointer.
///
/// The returned pointer borrows from the [`RsValue`] and must not outlive it.
///
/// # Safety
///
/// 1. `value` must point to a valid [`RsValue`] obtained from an `RSValue_*` function.
/// 2. `lenp` must be either null or a [valid], non-null pointer to a `u32`.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
///
/// # Panic
///
/// Panics if the value is not a `String` type.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_String_Get(
    value: *const RsValue,
    lenp: *mut u32,
) -> *const c_char {
    // Safety: ensured by caller (1.)
    let value = unsafe { expect_value(value) };

    let RsValue::String(str) = value else {
        panic!("Expected 'String' type");
    };

    let (ptr, len) = str.as_ptr_len();

    // Safety: ensured by caller (2.)
    if let Some(lenp) = unsafe { lenp.as_mut() } {
        *lenp = len;
    }

    ptr
}

/// Returns a read only reference to the underlying [`RedisModuleString`] of an [`RsValue`].
///
/// The returned reference borrows from the [`RsValue`] and must not outlive it.
///
/// # Safety
///
/// 1. `value` must point to a valid [`RsValue`] obtained from an `RSValue_*` function.
///
/// # Panic
///
/// Panics if the value is not a `RedisString` type.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_RedisString_Get(
    value: *const RsValue,
) -> *const RedisModuleString {
    // Safety: ensured by caller (1.)
    let value = unsafe { expect_value(value) };

    let RsValue::RedisString(str) = value else {
        panic!("Expected 'RedisString' type")
    };

    str.as_ptr()
}

/// Returns a pointer to the string data of an [`RsValue`] and optionally writes the string
/// length to `len_ptr`.
///
/// Unlike [`RSValue_String_Get`], this function handles all string variants (including
/// `RedisString`) and automatically dereferences `Ref` values and follows through the left
/// element of `Trio` values. Returns null for non-string variants.
///
/// The returned pointer borrows from the [`RsValue`] and must not outlive it.
///
/// # Safety
///
/// 1. `value` must point to a valid [`RsValue`] obtained from an `RSValue_*` function.
/// 2. `len_ptr` must be either null or a [valid], non-null pointer to a `size_t`.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_StringPtrLen(
    value: *const RsValue,
    len_ptr: *mut size_t,
) -> *const c_char {
    // Safety: ensured by caller (1.)
    let mut value = unsafe { expect_value(value) };

    let (ptr, len) = loop {
        match value {
            RsValue::String(str) => {
                let (ptr, len) = str.as_ptr_len();
                break (ptr, len as usize);
            }
            RsValue::RedisString(str) => break str.as_ptr_len(),
            RsValue::Ref(ref_val) => value = ref_val.value(),
            RsValue::Trio(trio) => value = trio.left().value(),
            _ => return std::ptr::null(),
        }
    };

    // Safety: ensured by caller (2.)
    if let Some(len_ptr) = unsafe { len_ptr.as_mut() } {
        *len_ptr = len;
    }
    ptr
}
