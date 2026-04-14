/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use ffi::RedisModuleString;
use libc::{c_char, size_t};
use std::ffi::c_double;
use value::{SharedValueRef, Value};

/// Gets the numeric value from an [`RSValue`].
///
/// # Panic
///
/// Panics if the value is not a [`Value::Number`].
///
/// # Safety
///
/// 1. `value` must point to a valid [`RSValue`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Number_Get(value: SharedValueRef) -> c_double {
    if let Value::Number(number) = &**value {
        *number
    } else {
        panic!("Expected a number value")
    }
}

/// Borrows an immutable reference to the left value of a trio.
///
/// # Panic
///
/// Panics if the value is not a [`Value::Trio`].
///
/// # Safety
///
/// 1. `value` must point to a valid [`RSValue`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Trio_GetLeft(value: SharedValueRef) -> SharedValueRef {
    if let Value::Trio(trio) = &**value {
        trio.left().as_ref()
    } else {
        panic!("Expected a trio value")
    }
}

/// Borrows an immutable reference to the middle value of a trio.
///
/// # Panic
///
/// Panics if the value is not a [`Value::Trio`].
///
/// # Safety
///
/// 1. `value` must point to a valid [`RSValue`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Trio_GetMiddle(value: SharedValueRef) -> SharedValueRef {
    if let Value::Trio(trio) = &**value {
        trio.middle().as_ref()
    } else {
        panic!("Expected a trio value")
    }
}

/// Borrows an immutable reference to the right value of a trio.
///
/// # Panic
///
/// Panics if the value is not a [`Value::Trio`].
///
/// # Safety
///
/// 1. `value` must point to a valid [`RSValue`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Trio_GetRight(value: SharedValueRef) -> SharedValueRef {
    if let Value::Trio(trio) = &**value {
        trio.right().as_ref()
    } else {
        panic!("Expected a trio value")
    }
}

/// Returns a pointer to the string data of an [`RSValue`] and optionally writes the string
/// length to `lenp`, if `lenp` is a non-null pointer.
///
/// The returned pointer borrows from the [`RSValue`] and must not outlive it.
///
/// # Panic
///
/// Panics if the value is not a [`Value::String`].
///
/// # Safety
///
/// 1. `value` must point to a valid [`RSValue`].
/// 2. `lenp` must be either null or a [valid], non-null pointer to a `u32`.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_String_Get(
    value: SharedValueRef,
    lenp: *mut u32,
) -> *const c_char {
    let Value::String(str) = &**value else {
        panic!("Expected 'String' type");
    };

    let (ptr, len) = str.as_ptr_len();

    // Safety: ensured by caller (2.)
    if let Some(lenp) = unsafe { lenp.as_mut() } {
        *lenp = len;
    }

    ptr
}

/// Returns a read only reference to the underlying [`RedisModuleString`] of an [`RSValue`].
///
/// The returned reference borrows from the [`RSValue`] and must not outlive it.
///
/// # Panic
///
/// Panics if the value is not a [`Value::RedisString`].
///
/// # Safety
///
/// 1. `value` must point to a valid [`RSValue`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_RedisString_Get(
    value: SharedValueRef,
) -> *const RedisModuleString {
    let Value::RedisString(str) = &**value else {
        panic!("Expected 'RedisString' type")
    };

    str.as_ptr()
}

/// Returns a pointer to the string data of an [`RSValue`] and optionally writes the string
/// length to `len_ptr`.
///
/// Unlike [`RSValue_String_Get`], this function handles all string variants (including
/// `RedisString`) and automatically dereferences `Ref` values and follows through the left
/// element of `Trio` values. Returns null for non-string variants.
///
/// The returned pointer borrows from the [`RSValue`] and must not outlive it.
///
/// # Safety
///
/// 1. `value` must point to a valid [`RSValue`].
/// 2. `len_ptr` must be either null or a [valid], non-null pointer to a `size_t`.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_StringPtrLen(
    value: SharedValueRef,
    len_ptr: *mut size_t,
) -> *const c_char {
    let value = value.fully_dereferenced_ref_and_trio();

    let (ptr, len) = match value {
        Value::String(str) => {
            let (ptr, len) = str.as_ptr_len();
            (ptr, len as usize)
        }
        Value::RedisString(str) => str.as_ptr_len(),
        _ => return std::ptr::null(),
    };

    // Safety: ensured by caller (2.)
    if let Some(len_ptr) = unsafe { len_ptr.as_mut() } {
        *len_ptr = len;
    }
    ptr
}
