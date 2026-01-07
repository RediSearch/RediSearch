/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{ffi::c_char, mem::ManuallyDrop, ptr::NonNull};

use c_ffi_utils::expect_unchecked;
use ffi::RedisModuleString;
use value::{RsValue, Value, shared::SharedRsValue};

/// Creates a heap-allocated `RsValue` wrapping a string.
/// Doesn't duplicate the string. Use strdup if the value needs to be detached.
///
/// # Safety
/// - (1) `str` must not be NULL;
/// - (2) `len` must match the length of `str`;
/// - (3) `str` must point to a valid, C string with a length of at most `u32::MAX` bytes;
/// - (4) `str` must not be aliased.
/// - (5) `str` must point to a location allocated using `rm_alloc`
/// - (6) `RedisModule_Alloc` must not be mutated for the lifetime of the
///   `OpaqueRsValue`.
///
/// @param str The string to wrap (ownership is transferred)
/// @param len The length of the string
/// @return A pointer to a heap-allocated RsValue
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SharedRsValue_NewString(
    str: Option<NonNull<c_char>>,
    len: u32,
) -> *const RsValue {
    // Safety: caller must ensure (1).
    let str = unsafe { expect_unchecked!(str) };
    // Safety: caller must ensure (2), (3), (4), (5) and (6),
    // upholding the safety requirements of `SharedRsValue::take_rm_alloc_string`
    let shared_value = unsafe { SharedRsValue::take_rm_alloc_string(str, len) };
    shared_value.into_raw()
}

/// Creates a heap-allocated RSValue wrapping a null-terminated C string.
///
/// # Safety
/// - (1) `str` must point to a valid C string with a length of at most `u32::MAX` bytes;
/// - (2) `str` must be NULL-terminated.
///
/// Furthermore, see [`SharedRsValue_NewString`].
///
/// @param str The null-terminated string to wrap (ownership is transferred)
/// @return A pointer to a heap-allocated RSValue
pub unsafe extern "C" fn SharedRsValue_NewCString(str: Option<NonNull<c_char>>) -> *const RsValue {
    // Safety:
    // Caller must ensure (1)
    let str = unsafe { expect_unchecked!(str) };

    let len = {
        // Safety:
        // Caller must ensure (2)
        unsafe { libc::strlen(str.as_ptr()) }
    };

    // Safety: caller must ensure (1)
    let len =
        unsafe { expect_unchecked!(len.try_into(), "Length of str cannot be more than u32::MAX") };

    // Safety: see above safety comments
    unsafe { SharedRsValue_NewString(Some(str), len) }
}

/// Creates a heap-allocated `SharedRsValue` wrapping a const string.
///
/// # Safety
/// - (1) `str` must live as least as long as the returned [`SharedRsValue`].
/// - (2) `str` must point to a byte sequence that is valid for reads of `len` bytes.
///
/// @param str The null-terminated string to wrap (ownership is transferred)
/// @return A pointer to a heap-allocated RsValue wrapping a constant C string
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SharedRsValue_NewConstString(
    str: *const c_char,
    len: u32,
) -> *const RsValue {
    // Safety: the safety requirements of this function uphold those
    // of `SharedRsValue::const_string`.
    let shared_value = unsafe { SharedRsValue::const_string(str, len) };
    shared_value.into_raw()
}

/// Creates a heap-allocated `RsValue` which increments and owns a reference to the Redis string.
/// The RsValue will decrement the refcount when freed.
///
/// # Safety
/// - (1) `str` must be non-null
/// - (2) `str` must point to a valid [`RedisModuleString`]
///   with a reference count of at least 1.
///
/// @param str The RedisModuleString to wrap (refcount is incremented)
/// @return A pointer to a heap-allocated RsValue
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SharedRsValue_NewRedisString(
    str: Option<NonNull<RedisModuleString>>,
) -> *const RsValue {
    // Safety: caller must ensure (1).
    let str = unsafe { expect_unchecked!(str) };
    // Safety: the safety requirements of this function uphold those
    // of `SharedRsValue::redis_string`.
    let shared_value = unsafe { SharedRsValue::redis_string(str) };
    shared_value.into_raw()
}

/// Creates a heap-allocated `RsValue` with a copied string.
/// The string is duplicated using `rm_malloc`.
///
/// # Safety
/// - (1) `str` must be a valid pointer to a char sequence of `len` chars.
///
/// @param s The string to copy
/// @param dst The length of the string to copy
/// @return A pointer to a heap-allocated `RsValue` owning the copied string
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SharedRsValue_NewCopiedString(
    str: *const c_char,
    len: u32,
) -> *const RsValue {
    debug_assert!(!str.is_null(), "`str` must not be NULL");
    // Safety: the safety requirements of this function uphold those
    // of `SharedRsValue::copy_rm_alloc_string`.
    let shared_value = unsafe { SharedRsValue::copy_rm_alloc_string(str, len) };
    shared_value.into_raw()
}

/// Creates a heap-allocated `RsValue` by parsing a string as a number.
/// Returns an undefined value if the string cannot be parsed as a valid number.
///
/// # Safety
/// - (1) `str` must be a valid const pointer to a char sequence of `len` bytes.
///
/// @param p The string to parse
/// @param l The length of the string
/// @return A pointer to a heap-allocated `RsValue`
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SharedRsValue_NewParsedNumber(
    str: *const c_char,
    len: usize,
) -> *const RsValue {
    if len == 0 {
        return SharedRsValue::undefined().into_raw();
    }

    // Safety: caller must ensure (1).
    let str = unsafe { std::slice::from_raw_parts(str as *const u8, len) };
    let Ok(str) = std::str::from_utf8(str) else {
        return SharedRsValue::undefined().into_raw();
    };
    let Ok(n) = str.parse() else {
        return SharedRsValue::undefined().into_raw();
    };
    let shared_value = SharedRsValue::number(n);
    shared_value.into_raw()
}

/// Creates a heap-allocated `RsValue` containing a number.
///
/// @param n The numeric value to wrap
/// @return A pointer to a heap-allocated `RsValue` of type `RsValueType_Number`
#[unsafe(no_mangle)]
pub extern "C" fn SharedRsValue_NewNumber(n: f64) -> *const RsValue {
    let shared_value = SharedRsValue::number(n);
    shared_value.into_raw()
}

/// Creates a heap-allocated `RsValue` containing a number from an int64.
/// This operation casts the passed `i64` to an `f64`, possibly losing information.
///
/// @param ii The int64 value to convert and wrap
/// @return A pointer to a heap-allocated `RsValue` of type `RsValueType_Number`
#[unsafe(no_mangle)]
pub extern "C" fn SharedRsValue_NewNumberFromInt64(dd: i64) -> *const RsValue {
    let shared_value = SharedRsValue::number(dd as f64);
    shared_value.into_raw()
}

/// Creates a heap-allocated RsValue Trio from three RsValues.
/// Takes ownership of all three values.
///
/// # Safety
///
/// - (1) `left`, `middle`, and `right` must be valid pointers to [`RsValue`]
///   obtained from [`SharedRsValue::into_raw`].
///
/// @param left The left value (ownership is transferred)
/// @param middle The middle value (ownership is transferred)
/// @param right The right value (ownership is transferred)
/// @return A pointer to a heap-allocated RsValue of type RsValueType_Trio
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SharedRsValue_NewTrio(
    left: *const RsValue,
    middle: *const RsValue,
    right: *const RsValue,
) -> *const RsValue {
    // Safety: caller must ensure (1).
    let left = unsafe { SharedRsValue::from_raw(left) };
    // Safety: caller must ensure (1).
    let middle = unsafe { SharedRsValue::from_raw(middle) };
    // Safety: caller must ensure (1).
    let right = unsafe { SharedRsValue::from_raw(right) };

    let shared_value = SharedRsValue::trio(left, middle, right);
    shared_value.into_raw()
}

/// Gets the `f64` wrapped by the `SharedRsValue`
///
/// # Safety
/// - (1) `v` must be a valid pointer to [`RsValue`] obtained from [`SharedRsValue::into_raw`].
/// - (2) `v` must be a number value.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SharedRsValue_Number_Get(v: *const RsValue) -> f64 {
    // Safety: caller must ensure (1).
    let v = unsafe { SharedRsValue::from_raw(v) };
    let v = ManuallyDrop::new(v);
    // Safety: caller must ensure (2).
    if let RsValue::Number(num) = v.value() {
        *num
    } else {
        panic!("v must be of type 'Number'");
    }
}
