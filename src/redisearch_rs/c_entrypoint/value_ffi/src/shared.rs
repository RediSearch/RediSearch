/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{ffi::c_char, ptr::NonNull};

use c_ffi_utils::{expect_unchecked, opaque::IntoOpaque};
use ffi::RedisModuleString;
use libc::strlen;
use value::{
    Value,
    collection::{RsValueArray, RsValueMap},
    shared::SharedRsValue,
};

use crate::{
    apply_with_dyn_ptr,
    dynamic::{
        DynRsValue, DynRsValuePtr,
        opaque::{OpaqueDynRsValue, OpaqueDynRsValuePtr},
    },
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
pub unsafe extern "C" fn RsValue_NewString(
    str: Option<NonNull<c_char>>,
    len: u32,
) -> OpaqueDynRsValue {
    // Safety: caller must ensure (1).
    let str = unsafe { expect_unchecked!(str) };
    // Safety: caller must ensure (2), (3), (4), (5) and (6),
    // upholding the safety requirements of `SharedRsValue::take_rm_alloc_string`
    let v = unsafe { SharedRsValue::take_rm_alloc_string(str, len) };
    DynRsValue::from(v).into_opaque()
}

/// Creates a heap-allocated RSValue wrapping a null-terminated C string.
///
/// # Safety
/// - (1) `str` must point to a valid C string with a length of at most `u32::MAX` bytes;
/// - (2) `str` must be NULL-terminated.
///
/// Furthermore, see [`RsValue_NewString`].
///
/// @param str The null-terminated string to wrap (ownership is transferred)
/// @return A pointer to a heap-allocated RSValue
pub unsafe extern "C" fn RsValue_NewCString(str: Option<NonNull<c_char>>) -> OpaqueDynRsValue {
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
    unsafe { RsValue_NewString(Some(str), len) }
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
pub unsafe extern "C" fn RsValue_NewConstString(str: *const c_char, len: u32) -> OpaqueDynRsValue {
    // Safety: the safety requirements of this function uphold those
    // of `SharedRsValue::const_string`.
    let v = unsafe { SharedRsValue::const_string(str, len) };
    DynRsValue::from(v).into_opaque()
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
pub unsafe extern "C" fn RsValue_NewBorrowedRedisString(
    str: Option<NonNull<RedisModuleString>>,
) -> OpaqueDynRsValue {
    // Safety: caller must ensure (1).
    let str = unsafe { expect_unchecked!(str) };
    // Safety: the safety requirements of this function uphold those
    // of `SharedRsValue::borrowed_redis_string`.
    let v = unsafe { SharedRsValue::borrowed_redis_string(str) };
    DynRsValue::from(v).into_opaque()
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
pub unsafe extern "C" fn RsValue_NewOwnedRedisString(
    str: Option<NonNull<RedisModuleString>>,
) -> OpaqueDynRsValue {
    // Safety: caller must ensure (1).
    let str = unsafe { expect_unchecked!(str) };
    // Safety: the safety requirements of this function uphold those
    // of `SharedRsValue::retain_owned_redis_string`.
    let v = unsafe { SharedRsValue::retain_owned_redis_string(str) };
    DynRsValue::from(v).into_opaque()
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
pub unsafe extern "C" fn RsValue_NewStolenRedisString(
    str: Option<NonNull<RedisModuleString>>,
) -> OpaqueDynRsValue {
    // Safety: caller must ensure (1).
    let str = unsafe { expect_unchecked!(str) };
    // Safety: the safety requirements of this function uphold those
    // of `SharedRsValue::take_owned_redis_string`.
    let v = unsafe { SharedRsValue::take_owned_redis_string(str) };
    DynRsValue::from(v).into_opaque()
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
pub unsafe extern "C" fn RsValue_NewCopiedString(str: *const c_char, len: u32) -> OpaqueDynRsValue {
    debug_assert!(!str.is_null(), "`str` must not be NULL");
    // Safety: the safety requirements of this function uphold those
    // of `SharedRsValue::copy_rm_alloc_string`.
    let v = unsafe { SharedRsValue::copy_rm_alloc_string(str, len) };
    DynRsValue::from(v).into_opaque()
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
pub unsafe extern "C" fn RsValue_NewParsedNumber(
    str: *const c_char,
    len: usize,
) -> OpaqueDynRsValue {
    if len == 0 {
        return DynRsValue::from(SharedRsValue::undefined()).into_opaque();
    }

    // Safety: caller must ensure (1).
    let str = unsafe { std::slice::from_raw_parts(str as *const u8, len) };
    let Ok(str) = std::str::from_utf8(str) else {
        return DynRsValue::from(SharedRsValue::undefined()).into_opaque();
    };
    let Ok(n) = str.parse() else {
        return DynRsValue::from(SharedRsValue::undefined()).into_opaque();
    };
    let v = SharedRsValue::number(n);
    DynRsValue::from(v).into_opaque()
}

/// Creates a heap-allocated `RsValue` containing a number.
///
/// @param n The numeric value to wrap
/// @return A pointer to a heap-allocated `RsValue` of type `RsValueType_Number`
#[unsafe(no_mangle)]
pub extern "C" fn RsValue_NewNumber(n: f64) -> OpaqueDynRsValue {
    let v = SharedRsValue::number(n);
    DynRsValue::from(v).into_opaque()
}

/// Creates a heap-allocated `RsValue` containing a number from an int64.
/// This operation casts the passed `i64` to an `f64`, possibly losing information.
///
/// @param ii The int64 value to convert and wrap
/// @return A pointer to a heap-allocated `RsValue` of type `RsValueType_Number`
#[unsafe(no_mangle)]
pub extern "C" fn RsValue_NewNumberFromInt64(dd: i64) -> OpaqueDynRsValue {
    let v = SharedRsValue::number(dd as f64);
    DynRsValue::from(v).into_opaque()
}

/// Creates a heap-allocated `RsValue` array from existing values.
/// Takes ownership of the values (values will be freed when array is freed).
///
/// @param vals The values array to use for the array (ownership is transferred)
/// @param len Number of values
/// @return A pointer to a heap-allocated `RsValue` of type `RsValueType_Array`
#[unsafe(no_mangle)]
pub extern "C" fn RsValue_NewArray(vals: RsValueArray) -> OpaqueDynRsValue {
    let v = SharedRsValue::array(vals);
    DynRsValue::from(v).into_opaque()
}

/// Creates a heap-allocated RsValue of type RsValue_Map from an RsValueMap.
/// Takes ownership of the map structure and all its entries.
///
/// @param map The RsValueMap to wrap (ownership is transferred)
/// @return A pointer to a heap-allocated RsValue of type RsValueType_Map
#[unsafe(no_mangle)]
pub extern "C" fn RsValue_NewMap(map: RsValueMap) -> OpaqueDynRsValue {
    let v = SharedRsValue::map(map);
    DynRsValue::from(v).into_opaque()
}

/// Creates a heap-allocated `RsValue` array from NULL terminated C strings.
///
/// # Safety
/// - (1) If `sz > 0`, `str` must be non-null;
/// - (2) If `sz > 0`, `str` must be valid for reads of `sz * size_of::<NonNull<c_char>>` bytes;
/// - (3) If `sz > 0`, `str` must be a valid, unique pointer
///   to a sequence of valid NULL-terminated C strings of length `sz` that each have been
///   allocated using `rm_alloc`.
/// - (4) [`RedisModule_Alloc`](ffi::RedisModule_Alloc) must not be mutated for the lifetime of the
///   `OwnedRmAllocString`.
///
/// @param strs Array of string pointers
/// @param sz Number of strings in the array
/// @return A pointer to a heap-allocated RsValue array
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RsValue_NewStringArray(
    strs: Option<NonNull<Option<NonNull<c_char>>>>,
    sz: u32,
) -> OpaqueDynRsValue {
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
        .map(|str| {
            // Safety:
            // Caller must ensure (3)
            let str = unsafe { expect_unchecked!(str) };

            let len = {
                // Safety:
                // Caller must ensure (3)
                unsafe { strlen(str.as_ptr()) }
            };

            // Safety: caller must ensure (3)
            let len = unsafe {
                expect_unchecked!(len.try_into(), "Length of str cannot be more than u32::MAX")
            };
            (str, len)
        })
        // Safety: in the above closure we have ensured:
        // - `len` matches the length of `str`
        // - `str` is at most `u32::MAX` bytes long
        // Furthermore, caller must ensure (3) and (4), therefore all other
        // safety requirements of `SharedRsValue::take_rm_alloc_string` are met.
        .map(|(str, len)| unsafe { SharedRsValue::take_rm_alloc_string(str, len) })
        .enumerate()
        // Safety: `i` does not exceed the capacity of `array`.
        .for_each(|(i, v)| unsafe { array.inner_mut().write_entry(v, i as u32) });

    let v = SharedRsValue::array(array);
    DynRsValue::from(v).into_opaque()
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
pub unsafe extern "C" fn RsValue_NewConstStringArray(
    strs: *mut *const c_char,
    sz: u32,
) -> OpaqueDynRsValue {
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
        // `SharedRsValue::const_string`.
        .map(|(str, len)| unsafe { SharedRsValue::const_string(str, len) })
        .enumerate()
        // Safety: `i` does not exceed the capacity of `array`.
        .for_each(|(i, v)| unsafe { array.inner_mut().write_entry(v, i as u32) });

    let v = SharedRsValue::array(array);
    DynRsValue::from(v).into_opaque()
}

/// Creates a heap-allocated RsValue Trio from three RsValues.
/// Takes ownership of all three values.
///
/// # Safety
/// - (1) `v` must originate from a call to [`RsValue_DynPtr`](crate::RsValue_DynPtr)
///
/// @param left The left value (ownership is transferred)
/// @param middle The middle value (ownership is transferred)
/// @param right The right value (ownership is transferred)
/// @return A pointer to a heap-allocated RsValue of type RsValueType_Trio
#[unsafe(no_mangle)]
pub extern "C" fn RsValue_NewTrio(
    left: OpaqueDynRsValuePtr,
    middle: OpaqueDynRsValuePtr,
    right: OpaqueDynRsValuePtr,
) -> OpaqueDynRsValue {
    // Safety: caller must ensure (1)
    let left = unsafe { DynRsValuePtr::from_opaque(left) };
    // Safety: caller must ensure (1)
    let middle = unsafe { DynRsValuePtr::from_opaque(middle) };
    // Safety: caller must ensure (1)
    let right = unsafe { DynRsValuePtr::from_opaque(right) };
    // Safety: caller must ensure (1)
    let left = unsafe { apply_with_dyn_ptr!(left, |v| v.to_shared()) };
    // Safety: caller must ensure (1)
    let middle = unsafe { apply_with_dyn_ptr!(middle, |v| v.to_shared()) };
    // Safety: caller must ensure (1)
    let right = unsafe { apply_with_dyn_ptr!(right, |v| v.to_shared()) };

    let v = SharedRsValue::trio(left, middle, right);
    DynRsValue::from(v).into_opaque()
}

/// Converts the RsValueRef to a SharedRsValue, so it can be passed to
/// e.g. [`RsValueMap_SetEntry`](crate::collection::RsValueMap_SetEntry) or
/// [`RsValueArray_SetEntry`](crate::collection::RsValueArray_SetEntry) or
/// Takes ownership of the value.
///
/// # Safety
/// - (1) `v` must originate from a call to [`RsValue_DynPtr`](crate::RsValue_DynPtr)
///
/// @param v The value to convert (ownership is transferred)
/// @return A pointer to a heap-allocated SharedRsValue
#[unsafe(no_mangle)]
pub extern "C" fn RsValue_ToShared(v: OpaqueDynRsValuePtr) -> SharedRsValue {
    // Safety: caller must ensure (1)
    let v = unsafe { DynRsValuePtr::from_opaque(v) };
    // Safety: caller must ensure (1)
    unsafe { apply_with_dyn_ptr!(v, |v| v.to_shared()) }
}
