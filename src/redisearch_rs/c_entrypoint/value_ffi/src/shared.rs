/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{ffi::c_char, ptr::NonNull};

use c_ffi_utils::expect_unchecked;
use ffi::RedisModuleString;
use libc::strlen;
use value::{
    Value,
    collection::{RsValueArray, RsValueMap},
    shared::SharedRsValue,
};

use crate::value_type::{AsRsValueType, RsValueType};

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
) -> SharedRsValue {
    // Safety: caller must ensure (1).
    let str = unsafe { expect_unchecked!(str) };
    // Safety: caller must ensure (2), (3), (4), (5) and (6),
    // upholding the safety requirements of `SharedRsValue::take_rm_alloc_string`
    unsafe { SharedRsValue::take_rm_alloc_string(str, len) }
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
pub unsafe extern "C" fn SharedRsValue_NewCString(str: Option<NonNull<c_char>>) -> SharedRsValue {
    // Safety:
    // Caller must ensure (1)
    let str = unsafe { expect_unchecked!(str) };

    let len = {
        // Safety:
        // Caller must ensure (2)
        unsafe { strlen(str.as_ptr()) }
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
) -> SharedRsValue {
    // Safety: the safety requirements of this function uphold those
    // of `SharedRsValue::const_string`.
    unsafe { SharedRsValue::const_string(str, len) }
}

/// Creates a heap-allocated `RsValue` wrapping a RedisModuleString.
/// Does not increment the refcount of the Redis string.
/// The passed Redis string's refcount does not get decremented
/// upon freeing the returned RsValue.
///
/// # Safety
/// - (1) The passed pointer must be non-null and valid for reads.
/// - (2) The reference count of the [`RedisModuleString`] `str` points to
///   must be at least 1 for the lifetime of the created [`SharedRsValue`]
///
/// @param str The RedisModuleString to wrap
/// @return A pointer to a heap-allocated RsValue
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SharedRsValue_NewBorrowedRedisString(
    str: Option<NonNull<RedisModuleString>>,
) -> SharedRsValue {
    // Safety: caller must ensure (1).
    let str = unsafe { expect_unchecked!(str) };
    // Safety: the safety requirements of this function uphold those
    // of `SharedRsValue::borrowed_redis_string`.
    unsafe { SharedRsValue::borrowed_redis_string(str) }
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
pub unsafe extern "C" fn SharedRsValue_NewOwnedRedisString(
    str: Option<NonNull<RedisModuleString>>,
) -> SharedRsValue {
    // Safety: caller must ensure (1).
    let str = unsafe { expect_unchecked!(str) };
    // Safety: the safety requirements of this function uphold those
    // of `SharedRsValue::retain_owned_redis_string`.
    unsafe { SharedRsValue::retain_owned_redis_string(str) }
}

/// Creates a heap-allocated `RsValue` which steals a reference to the Redis string.
/// The caller's reference is transferred to the RsValue.
///
/// # Safety
/// - (1) `str` must be non-null
/// - (2) `str` must point to a valid [`RedisModuleString`]
///   with a reference count of at least 1.
///
/// @param s The RedisModuleString to wrap (ownership is transferred)
/// @return A pointer to a heap-allocated RsValue
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SharedRsValue_NewStolenRedisString(
    str: Option<NonNull<RedisModuleString>>,
) -> SharedRsValue {
    // Safety: caller must ensure (1).
    let str = unsafe { expect_unchecked!(str) };
    // Safety: the safety requirements of this function uphold those
    // of `SharedRsValue::take_owned_redis_string`.
    unsafe { SharedRsValue::take_owned_redis_string(str) }
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
) -> SharedRsValue {
    debug_assert!(!str.is_null(), "`str` must not be NULL");
    // Safety: the safety requirements of this function uphold those
    // of `SharedRsValue::copy_rm_alloc_string`.
    unsafe { SharedRsValue::copy_rm_alloc_string(str, len) }
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
) -> SharedRsValue {
    if len == 0 {
        return SharedRsValue::undefined();
    }

    // Safety: caller must ensure (1).
    let str = unsafe { std::slice::from_raw_parts(str as *const u8, len) };
    let Ok(str) = std::str::from_utf8(str) else {
        return SharedRsValue::undefined();
    };
    let Ok(n) = str.parse() else {
        return SharedRsValue::undefined();
    };
    SharedRsValue::number(n)
}

/// Creates a heap-allocated `RsValue` containing a number.
///
/// @param n The numeric value to wrap
/// @return A pointer to a heap-allocated `RsValue` of type `RsValueType_Number`
#[unsafe(no_mangle)]
pub extern "C" fn SharedRsValue_NewNumber(n: f64) -> SharedRsValue {
    SharedRsValue::number(n)
}

/// Creates a heap-allocated `RsValue` containing a number from an int64.
/// This operation casts the passed `i64` to an `f64`, possibly losing information.
///
/// @param ii The int64 value to convert and wrap
/// @return A pointer to a heap-allocated `RsValue` of type `RsValueType_Number`
#[unsafe(no_mangle)]
pub extern "C" fn SharedRsValue_NewNumberFromInt64(dd: i64) -> SharedRsValue {
    SharedRsValue::number(dd as f64)
}

/// Creates a heap-allocated `RsValue` array from existing values.
/// Takes ownership of the values (values will be freed when array is freed).
///
/// @param vals The values array to use for the array (ownership is transferred)
/// @param len Number of values
/// @return A pointer to a heap-allocated `RsValue` of type `RsValueType_Array`
#[unsafe(no_mangle)]
pub extern "C" fn SharedRsValue_NewArray(vals: RsValueArray) -> SharedRsValue {
    SharedRsValue::array(vals)
}

/// Creates a heap-allocated RsValue of type RsValue_Map from an RsValueMap.
/// Takes ownership of the map structure and all its entries.
///
/// @param map The RsValueMap to wrap (ownership is transferred)
/// @return A pointer to a heap-allocated RsValue of type RsValueType_Map
#[unsafe(no_mangle)]
pub extern "C" fn SharedRsValue_NewMap(map: RsValueMap) -> SharedRsValue {
    SharedRsValue::map(map)
}

/// Creates a heap-allocated `RsValue` array from NULL terminated C strings.
///
/// # Safety
/// - (1) If `sz > 0`, `str` must be non-null;
/// - (2) If `sz > 0`, `str` must be valid for reads of `sz * size_of::<NonNull<c_char>>` bytes;
/// - (3) If `sz > 0`, `str` must be a valid pointer
///   to a sequence if valid NULL-terminated C strings of length `sz`.
///
/// @param strs Array of string pointers
/// @param sz Number of strings in the array
/// @return A pointer to a heap-allocated RsValue array
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SharedRsValue_NewStringArray(
    strs: Option<NonNull<Option<NonNull<c_char>>>>,
    sz: u32,
) -> SharedRsValue {
    let strs = if sz == 0 {
        &[]
    } else {
        // Safety: caller must ensure (1)
        let strs = unsafe { expect_unchecked!(strs) };
        // Safety: caller must ensure (2)
        unsafe { std::slice::from_raw_parts(strs.as_ptr(), sz as usize) }
    };
    // Safety: all items of the produced `RsValueArray` are initialized using
    // `RsValue::write_entry` below.
    let mut array = unsafe { RsValueArray::reserve_uninit(sz) };

    strs.iter()
        .copied()
        // Safety: caller must ensure (3), and therefore `str` is valid to pass to
        // `SharedRsValue_NewCString`.
        .map(|str| unsafe { SharedRsValue_NewCString(str) })
        .enumerate()
        // Safety: `i` does not exceed the capacity of `array`.
        .for_each(|(i, v)| unsafe { array.inner_mut().write_entry(v, i as u32) });

    SharedRsValue::array(array)
}

/// Creates a heap-allocated RsValue array from NULL terminated C string constants.
///
/// # Safety
/// - (1) If `sz > 0`, `str` must be non-null;
/// - (2) If `sz > 0`, `str` must be valid for reads of `sz * size_of::<NonNull<c_char>>` bytes;
/// - (3) If `sz > 0`, `str` must point to a sequence of valid NULL-terminated C strings of length `sz`;
/// - (4) For each of the strings `str` in `strs`, `strlen(str)` must not exceed `u32::MAX`.
///
/// @param strs Array of string pointers
/// @param sz Number of strings in the array
/// @return A pointer to a heap-allocated RsValue array
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SharedRsValue_NewConstStringArray(
    strs: *mut *const c_char,
    sz: u32,
) -> SharedRsValue {
    let strs = if sz == 0 {
        &[]
    } else {
        debug_assert!(!strs.is_null(), "`strs` must not be NULL");
        // Safety: caller must ensure (1) and (2).
        unsafe { std::slice::from_raw_parts(strs, sz as usize) }
    };

    // Safety: all items of the produced `RsValueArray` are initialized using
    // `RsValue::write_entry` below.
    let mut array = unsafe { RsValueArray::reserve_uninit(sz) };

    strs.iter()
        .copied()
        .map(|str| {
            // Safety: caller must ensure (3).
            let len = unsafe { strlen(str) };
            // Safety: caller must ensure (4).
            let len = unsafe {
                expect_unchecked!(len.try_into(), "`strlen(str)` must note exceed u32::MAX")
            };
            (str, len)
        })
        // Safety: caller must ensure (3), and therefore `str` is valid to pass to
        // `SharedRsValue_NewCString`.
        .map(|(str, len)| unsafe { SharedRsValue_NewConstString(str, len) })
        .enumerate()
        // Safety: `i` does not exceed the capacity of `array`.
        .for_each(|(i, v)| unsafe { array.inner_mut().write_entry(v, i as u32) });

    SharedRsValue::array(array)
}

/// Creates a heap-allocated RsValue Trio from three RsValues.
/// Takes ownership of all three values.
///
/// @param left The left value (ownership is transferred)
/// @param middle The middle value (ownership is transferred)
/// @param right The right value (ownership is transferred)
/// @return A pointer to a heap-allocated RsValue of type RsValueType_Trio
#[unsafe(no_mangle)]
pub extern "C" fn SharedRsValue_NewTrio(
    left: SharedRsValue,
    middle: SharedRsValue,
    right: SharedRsValue,
) -> SharedRsValue {
    SharedRsValue::trio(left, middle, right)
}

/// Get the type of a `SharedRsValue` as an [`RsValueType`].
///
/// @param v The value to inspect
/// @return The `RsValueType` of the value
#[unsafe(no_mangle)]
pub extern "C" fn SharedRsValue_Type(v: SharedRsValue) -> RsValueType {
    v.as_value_type()
}

/// Check if the `SharedRsValue` is a reference.
///
/// @param v The value to check
/// @return true if the value is of type [`RsValueType::Ref`], false otherwise
#[unsafe(no_mangle)]
pub extern "C" fn SharedRsValue_IsReference(v: SharedRsValue) -> bool {
    v.as_value_type().is_ref()
}

/// Check if the `SharedRsValue` is a number.
///
/// @param v The value to check
/// @return true if the value is of type [`RsValueType::Number`], false otherwise
#[unsafe(no_mangle)]
pub extern "C" fn SharedRsValue_IsNumber(v: SharedRsValue) -> bool {
    v.as_value_type().is_number()
}

/// Check if the `SharedRsValue` is a string.
///
/// @param v The value to check
/// @return true if the value is of type [`RsValueType::String`], false otherwise
#[unsafe(no_mangle)]
pub extern "C" fn SharedRsValue_IsString(v: SharedRsValue) -> bool {
    v.as_value_type().is_string()
}

/// Check if the `SharedRsValue` is an array.
///
/// @param v The value to check
/// @return true if the value is of type [`RsValueType::Array`], false otherwise
#[unsafe(no_mangle)]
pub extern "C" fn SharedRsValue_IsArray(v: SharedRsValue) -> bool {
    v.as_value_type().is_array()
}

/// Check if the `SharedRsValue` is a Redis string type.
///
/// @param v The value to check
/// @return true if the value is of type [`RsValueType::BorrowedRedisString`], false otherwise
#[unsafe(no_mangle)]
pub extern "C" fn SharedRsValue_IsRedisString(v: SharedRsValue) -> bool {
    v.as_value_type().is_borrowed_redis_string()
}

/// Check if the `SharedRsValue` is an owned Redis string.
///
/// @param v The value to check
/// @return true if the value is of type [`RsValueType::OwnedRedisString`], false otherwise
#[unsafe(no_mangle)]
pub extern "C" fn SharedRsValue_IsOwnRString(v: SharedRsValue) -> bool {
    v.as_value_type().is_owned_redis_string()
}

/// Check whether the `RsValue` is a trio.
///
/// @param v The value to check
/// @return true if the value is of type [`RsValueType::Trio`], false otherwise
#[unsafe(no_mangle)]
pub extern "C" fn SharedRsValue_IsTrio(v: SharedRsValue) -> bool {
    v.as_value_type().is_trio()
}

/// Returns true if the value contains any type of string
///
/// @param v The value to check
/// @return true if the value is any type of string, false otherwise
#[unsafe(no_mangle)]
pub extern "C" fn SharedRsValue_IsAnyString(v: SharedRsValue) -> bool {
    v.as_value_type().is_any_string()
}

/// Check if the value is NULL;
///
/// @param v The value to check
/// @return true if the value is NULL, false otherwise
#[unsafe(no_mangle)]
pub extern "C" fn SharedRsValue_IsNull(v: SharedRsValue) -> bool {
    v.as_value_type().is_null()
}

/// Gets the `f64` wrapped by the [`SharedRsValue`]
///
/// # Safety
/// - (1) `v` must be a number value.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SharedRsValue_Number_Get(v: SharedRsValue) -> f64 {
    // Safety: caller must ensure (1).
    unsafe { expect_unchecked!(v.get_number(), "v must be of type 'Number'") }
}

/// Convert a [`SharedRsValue`] to a number type in-place.
/// This clears the existing value and replaces it with the given value.
///
/// @param v The value to modify
/// @param n The numeric value to set
pub extern "C" fn SharedRsValue_IntoNumber(mut v: SharedRsValue, n: f64) {
    v.to_number(n);
}
