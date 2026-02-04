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
use value::{
    RsValue,
    collection::{RsValueArray, RsValueMap},
    shared::SharedRsValue,
    strings::{ConstString, RedisString, RmAllocString},
};

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
    // upholding the safety requirements of `RmAllocString::take_unchecked`
    let shared_value = SharedRsValue::new(RsValue::RmAllocString(unsafe {
        RmAllocString::take_unchecked(str, len)
    }));
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
    // of `ConstString::new`.
    let shared_value =
        SharedRsValue::new(RsValue::ConstString(unsafe { ConstString::new(str, len) }));
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
    // of `RedisString::take`.
    let shared_value = SharedRsValue::new(RsValue::RedisString(unsafe { RedisString::take(str) }));
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
    // of `RmAllocString::copy_from_string`.
    let shared_value = SharedRsValue::new(RsValue::RmAllocString(unsafe {
        RmAllocString::copy_from_string(str, len)
    }));
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
        return SharedRsValue::new(RsValue::Undefined).into_raw();
    }

    // Safety: caller must ensure (1).
    let str = unsafe { std::slice::from_raw_parts(str as *const u8, len) };
    let Ok(str) = std::str::from_utf8(str) else {
        return SharedRsValue::new(RsValue::Undefined).into_raw();
    };
    let Ok(n) = str.parse() else {
        return SharedRsValue::new(RsValue::Undefined).into_raw();
    };
    let shared_value = SharedRsValue::new(RsValue::Number(n));
    shared_value.into_raw()
}

/// Creates a heap-allocated `RsValue` containing a number from an int64.
/// This operation casts the passed `i64` to an `f64`, possibly losing information.
///
/// @param ii The int64 value to convert and wrap
/// @return A pointer to a heap-allocated `RsValue` of type `RsValueType_Number`
#[unsafe(no_mangle)]
pub extern "C" fn SharedRsValue_NewNumberFromInt64(dd: i64) -> *const RsValue {
    let shared_value = SharedRsValue::new(RsValue::Number(dd as f64));
    shared_value.into_raw()
}

/// Creates a heap-allocated `RsValue` array from existing values.
/// Takes ownership of the values (values will be freed when array is freed).
///
/// @param vals The values array to use for the array (ownership is transferred)
/// @param len Number of values
/// @return A pointer to a heap-allocated `RsValue` of type `RsValueType_Array`
#[unsafe(no_mangle)]
pub extern "C" fn SharedRsValue_NewArray(vals: RsValueArray) -> *const RsValue {
    let shared_value = SharedRsValue::new(RsValue::Array(vals));
    shared_value.into_raw()
}

/// Creates a heap-allocated RsValue of type RsValue_Map from an RsValueMap.
/// Takes ownership of the map structure and all its entries.
///
/// @param map The RsValueMap to wrap (ownership is transferred)
/// @return A pointer to a heap-allocated RsValue of type RsValueType_Map
#[unsafe(no_mangle)]
pub extern "C" fn SharedRsValue_NewMap(map: RsValueMap) -> *const RsValue {
    let shared_value = SharedRsValue::new(RsValue::Map(map));
    shared_value.into_raw()
}
