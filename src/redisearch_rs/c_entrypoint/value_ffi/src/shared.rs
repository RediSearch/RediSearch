/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{
    ffi::{c_char, c_double},
    ptr::NonNull,
};

use libc::strlen;
use redis_module::RedisModuleString;
use value::{
    RsValueInternal, Value,
    collection::{RsValueArray, RsValueMap},
    shared::SharedRsValue,
};

/// Creates a heap-allocated `RsValue` wrapping a string.
/// Doesn't duplicate the string. Use strdup if the value needs to be detached.
/// @param str The string to wrap (ownership is transferred)
/// @param len The length of the string
/// @return A pointer to a heap-allocated RsValue
#[unsafe(no_mangle)]
pub extern "C" fn SharedRsValue_NewString(str: Option<NonNull<c_char>>, len: u32) -> SharedRsValue {
    todo!()
}

/**
 * Creates a heap-allocated RSValue wrapping a null-terminated C string.
 *
 * # Safety
 *
 * - `str` must point to a valid, NULL-terminated C string with a length of at most `u32::MAX` bytes.
 *
 * @param str The null-terminated string to wrap (ownership is transferred)
 * @return A pointer to a heap-allocated RSValue
 */
pub unsafe extern "C" fn RSValue_NewCString(str: Option<NonNull<c_char>>) -> SharedRsValue {
    debug_assert!(str.is_some(), "str cannot be NULL");
    let len = {
        // Safety:
        // Caller must ensure `str` is a valid pointer to a C string.
        let str = unsafe { str.unwrap_unchecked() };
        // Safety:
        // Caller must ensure `str` is a NULL-terminated C string
        unsafe { strlen(str.as_ptr()) }
    };

    #[cfg(debug_assertions)]
    let len = len
        .try_into()
        .expect("Length of str cannot be more than u32::MAX");
    #[cfg(not(debug_assertions))]
    // Safety: Caller has to ensure that str is a valid C string, so its length cannot exceed u32::MAX
    let len = unsafe { len.try_into().unwrap_unchecked() };

    SharedRsValue_NewString(str, len)
}

/// Creates a heap-allocated `RsValue` wrapping a const string.
///
/// # Safety
/// - `str` must be a valid const pointer to a char sequence of `len` chars.
///
/// @param str The null-terminated string to wrap (ownership is transferred)
/// @return A pointer to a heap-allocated RsValue wrapping a constant C string
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SharedRsValue_NewConstString(
    str: *const c_char,
    len: usize,
) -> SharedRsValue {
    todo!()
}

/// Creates a heap-allocated `RsValue` wrapping a RedisModuleString.
/// Does not increment the refcount of the Redis string.
/// The passed Redis string's refcount does not get decremented
/// upon freeing the returned RsValue.
/// @param str The RedisModuleString to wrap
/// @return A pointer to a heap-allocated RsValue
#[unsafe(no_mangle)]
pub extern "C" fn SharedRsValue_NewBorrowedRedisString(
    str: *const RedisModuleString,
) -> SharedRsValue {
    todo!()
}

/// Creates a heap-allocated `RsValue` which increments and owns a reference to the Redis string.
/// The RsValue will decrement the refcount when freed.
/// @param str The RedisModuleString to wrap (refcount is incremented)
/// @return A pointer to a heap-allocated RsValue
#[unsafe(no_mangle)]
pub extern "C" fn SharedRsValue_NewOwnedRedisString(str: *mut RedisModuleString) -> SharedRsValue {
    todo!()
}

/// Creates a heap-allocated `RsValue` which steals a reference to the Redis string.
/// The caller's reference is transferred to the RsValue.
/// @param s The RedisModuleString to wrap (ownership is transferred)
/// @return A pointer to a heap-allocated RsValue
#[unsafe(no_mangle)]
pub extern "C" fn SharedRsValue_NewStolenRedisString(str: *mut RedisModuleString) -> SharedRsValue {
    todo!()
}

/// Creates a heap-allocated `RsValue` with a copied string.
/// The string is duplicated using `rm_malloc`.
///
/// # Safety
/// - `str` must be a valid pointer to a char sequence of `len` chars.
///
/// @param s The string to copy
/// @param dst The length of the string to copy
/// @return A pointer to a heap-allocated `RsValue` owning the copied string
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SharedRsValue_NewCopiedString(
    str: *const c_char,
    len: usize,
) -> SharedRsValue {
    debug_assert!(!str.is_null(), "pointer `str` was NULL");
    todo!()
}

/// Creates a heap-allocated `RsValue` by parsing a string as a number.
/// Returns an undefined value if the string cannot be parsed as a valid number.
///
/// # Safety
/// - `str` must be a valid const pointer to a char sequence of `len` chars.
///
/// @param p The string to parse
/// @param l The length of the string
/// @return A pointer to a heap-allocated `RsValue` or NULL on parse failure
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SharedRsValue_NewParsedNumber(
    str: *const c_char,
    len: usize,
) -> SharedRsValue {
    todo!()
}

/// Creates a heap-allocated `RsValue` containing a number.
/// @param n The numeric value to wrap
/// @return A pointer to a heap-allocated `RsValue` of type `RsValueType_Number`
#[unsafe(no_mangle)]
pub extern "C" fn SharedRsValue_NewNumber(n: f64) -> SharedRsValue {
    todo!()
}

/// Creates a heap-allocated `RsValue` containing a number from an int64.
/// @param ii The int64 value to convert and wrap
/// @return A pointer to a heap-allocated `RsValue` of type `RsValueType_Number`
#[unsafe(no_mangle)]
pub extern "C" fn SharedRsValue_NewNumberFromInt64(dd: i64) -> SharedRsValue {
    todo!()
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

/// Creates a heap-allocated RsValue array from NULL terminated C strings.
/// @param strs Array of string pointers
/// @param sz Number of strings in the array
/// @return A pointer to a heap-allocated RsValue array
#[unsafe(no_mangle)]
pub extern "C" fn SharedRsValue_NewStringArray(strs: *mut *mut c_char, sz: u32) -> SharedRsValue {
    todo!()
}

/// Creates a heap-allocated RsValue array from NULL terminated C string constants.
/// @param strs Array of string pointers
/// @param sz Number of strings in the array
/// @return A pointer to a heap-allocated RsValue array
#[unsafe(no_mangle)]
pub extern "C" fn SharedRsValue_NewConstStringArray(
    strs: *mut *const c_char,
    sz: u32,
) -> SharedRsValue {
    todo!()
}

/// Creates a heap-allocated RsValue Trio from three RsValues.
/// Takes ownership of all three values.
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
    todo!()
}
