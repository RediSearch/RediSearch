/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#![allow(non_camel_case_types, non_snake_case)]

use std::{
    ffi::{c_char, c_double},
    hint::unreachable_unchecked,
    ptr::NonNull,
};

use c_ffi_utils::{expect_unchecked, opaque::IntoOpaque};
use ffi::RedisModuleString;
use value::{
    RsValue, Value,
    dynamic::{
        DynRsValue, DynRsValueRef,
        opaque::{OpaqueDynRsValue, OpaqueDynRsValueRef},
    },
};

use crate::value_type::{AsRsValueType, RsValueType};

pub mod collection;
pub mod shared;
pub mod value_type;

/// Creates a stack-allocated, undefined `RsValue`.
///
/// @returns a stack-allocated `RsValue` of type `RsValueType_Undef`
#[unsafe(no_mangle)]
pub extern "C" fn RsValue_Undefined() -> OpaqueDynRsValue {
    DynRsValue::from(RsValue::undefined()).into_opaque()
}

/// Creates a stack-allocated `RsValue` containing a number.
/// The returned value is not allocated on the heap and should not be freed.
///
/// @param n The numeric value to wrap
/// @return A stack-allocated `RsValue` of type `RsValueType_Number`
#[unsafe(no_mangle)]
pub extern "C" fn RsValue_Number(n: c_double) -> OpaqueDynRsValue {
    DynRsValue::from(RsValue::number(n)).into_opaque()
}

/// Creates a stack-allocated `RsValue` containing a malloc'd string.
/// The returned value itself is not heap-allocated, but does take ownership of the string.
///
/// # Safety
/// - (1) `str` must be non-null;
/// - (2) `str` must point to a valid C string that was allocated using `rm_malloc`;
/// - (3) The passed length must match the length to the string;
/// - (4) `str` must not be aliased;
/// - (5) `RedisModule_Alloc` must not be mutated for the lifetime of the
///   `OpaqueRsValue`.
///
/// @param str The malloc'd string to wrap (ownership is transferred)
/// @param len The length of the string
/// @return A stack-allocated `RsValue` of type `RsValueType_String` with `RSString_Malloc` subtype
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RsValue_String(
    str: Option<NonNull<c_char>>,
    len: u32,
) -> OpaqueDynRsValue {
    // Safety: caller must ensure (1)
    let str = unsafe { expect_unchecked!(str) };
    // Safety: caller must ensure (2), (3), (4), and (5)
    let v = unsafe { RsValue::take_rm_alloc_string(str, len) };
    DynRsValue::from(v).into_opaque()
}

/// Returns a pointer to a statically allocated NULL `RsValue`.
/// This is a singleton - the same pointer is always returned.
/// DO NOT free or modify this value.
///
/// @return A pointer to a static `RsValue` of type `RsValueType_Null`
#[unsafe(no_mangle)]
pub extern "C" fn RsValue_NullStatic() -> OpaqueDynRsValueRef {
    static RSVALUE_NULL: DynRsValue = DynRsValue::null_const();
    RSVALUE_NULL.as_ref().into_opaque()
}

/// Get the type of an `RsValue` as an [`RsValueType`].
///
/// # Safety
/// - (1) `v` must originate from a call to [`RsValue_DynRef`].
///
/// @param v The value to inspectw
/// @return The `RsValueType` of the value
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RsValue_Type(v: OpaqueDynRsValueRef) -> RsValueType {
    // Safety: Caller must ensure (1)
    let v = unsafe { DynRsValueRef::from_opaque(v) };
    v.as_value_type()
}

/// Check if the `RsValue` is a reference.
///
/// # Safety
/// See [`RsValue_Type`].
///
/// @param v The value to check
/// @return true if the value is of type [`RsValueType::Ref`], false otherwise
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RsValue_IsReference(v: OpaqueDynRsValueRef) -> bool {
    // Safety: caller must ensure safety requirements of `RsValue_Type`
    // are met.
    unsafe { RsValue_Type(v).is_ref() }
}

/// Check if the `RsValue` is a number.
///
/// # Safety
/// See [`RsValue_Type`].
///
/// @param v The value to check
/// @return true if the value is of type [`RsValueType::Number`], false otherwise
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RsValue_IsNumber(v: OpaqueDynRsValueRef) -> bool {
    // Safety: caller must ensure safety requirements of `RsValue_Type`
    // are met.
    unsafe { RsValue_Type(v).is_number() }
}

/// Check if the `RsValue` is a string.
///
/// # Safety
/// See [`RsValue_Type`].
///
/// @param v The value to check
/// @return true if the value is of type [`RsValueType::String`], false otherwise
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RsValue_IsString(v: OpaqueDynRsValueRef) -> bool {
    // Safety: caller must ensure safety requirements of `RsValue_Type`
    // are met.
    unsafe { RsValue_Type(v).is_string() }
}

/// Check if the `RsValue` is an array.
///
/// # Safety
/// See [`RsValue_Type`].
///
/// @param v The value to check
/// @return true if the value is of type [`RsValueType::Array`], false otherwise
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RsValue_IsArray(v: OpaqueDynRsValueRef) -> bool {
    // Safety: caller must ensure safety requirements of `RsValue_Type`
    // are met.
    unsafe { RsValue_Type(v).is_array() }
}

/// Check if the `RsValue` is a Redis string type.
///
/// # Safety
/// See [`RsValue_Type`].
///
/// @param v The value to check
/// @return true if the value is of type [`RsValueType::BorrowedRedisString`], false otherwise
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RsValue_IsRedisString(v: OpaqueDynRsValueRef) -> bool {
    // Safety: caller must ensure safety requirements of `RsValue_Type`
    // are met.
    unsafe { RsValue_Type(v).is_borrowed_redis_string() }
}

/// Check if the `RsValue` is an owned Redis string.
///
/// # Safety
/// See [`RsValue_Type`].
///
/// @param v The value to check
/// @return true if the value is of type [`RsValueType::OwnedRedisString`], false otherwise
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RsValue_IsOwnRString(v: OpaqueDynRsValueRef) -> bool {
    // Safety: caller must ensure safety requirements of `RsValue_Type`
    // are met.
    unsafe { RsValue_Type(v).is_owned_redis_string() }
}

/// Check whether the `RsValue` is a trio.
///
/// # Safety
/// See [`RsValue_Type`].
///
/// @param v The value to check
/// @return true if the value is of type [`RsValueType::Trio`], false otherwise
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RsValue_IsTrio(v: OpaqueDynRsValueRef) -> bool {
    // Safety: caller must ensure safety requirements of `RsValue_Type`
    // are met.
    unsafe { RsValue_Type(v).is_trio() }
}

/// Returns true if the value contains any type of string
///
/// # Safety
/// See [`RsValue_Type`].
///
/// @param v The value to check
/// @return true if the value is any type of string, false otherwise
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RsValue_IsAnyString(v: OpaqueDynRsValueRef) -> bool {
    // Safety: caller must ensure safety requirements of `RsValue_Type`
    // are met.
    unsafe { RsValue_Type(v).is_any_string() }
}

/// Check if the value is NULL;
///
/// # Safety
/// See [`RsValue_Type`].
///
/// @param v The value to check
/// @return true if the value is NULL, false otherwise
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RsValue_IsNull(v: OpaqueDynRsValueRef) -> bool {
    // Safety: caller must ensure safety requirements of `RsValue_Type`
    // are met.
    unsafe { RsValue_Type(v).is_null() }
}

/// Gets the `f64` wrapped by the [`OpaqueDynRsValue`]
///
/// # Safety
/// - (1) `v` originate from a call to [`RsValue_DynRef`].
/// - (2) `v` must be a number value.
///
/// @param v A reference to the `RsValue` from which to obtain the numeric value
/// @return The numeric value held by the `RsValue`
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RsValue_Number_Get(v: OpaqueDynRsValueRef) -> f64 {
    // Safety: caller must ensure (1)
    let v = unsafe { DynRsValueRef::from_opaque(v) };
    // Safety: caller must ensure (2).
    unsafe { expect_unchecked!(v.get_number(), "v must be of type 'Number'") }
}

/// Convert an [`OpaqueDynRsValue`] to a number type in-place.
/// This clears the existing value and replaces it with the given value.
///
/// # Safety
/// - (1) `v` must be non-null;
/// - (2) `v` must point to an `RsValue` originating from one of the constructors.
///
/// @param v The value to modify
/// @param n The numeric value to set
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RsValue_IntoNumber(v: Option<NonNull<OpaqueDynRsValue>>, n: f64) {
    // Safety: caller must ensure (1)
    let v = unsafe { expect_unchecked!(v) };
    // Safety: caller must ensure (2)
    let v = unsafe { DynRsValue::from_opaque_mut_ptr(v.as_ptr()) };
    // Safety: caller must ensure (1). The previous statement casts the pointer
    // to an `Option<&mut RsValue>`, which will be None if and only if `v` were null.
    let v = unsafe { v.unwrap_unchecked() };

    v.to_number(n);
}

/// Get the string value and length from an RSValue of type [`RsValueType::RmAllocString`] or
/// [`RsValueType::ConstString`].
///
/// # Safety
/// - (1) `v` must originate from a call to [`RsValue_DynRef`];
/// - (2) If `lenp` is non-null, it must be a well-aligned pointer to a `u32` that is valid for writes;
/// - (3) The value must be either of type [`RsValueType::RmAllocString`] or
///   [`RsValueType::ConstString`].
///
/// @param v A reference to the `RsValue` from which to obtain the string
/// @param lenp A nullable pointer to which the length will be written
/// @return The string held by `v`
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RsValue_String_Get(
    v: OpaqueDynRsValueRef,
    lenp: Option<NonNull<u32>>,
) -> *const c_char {
    // Safety: caller must ensure (1)
    let v = unsafe { DynRsValueRef::from_opaque(v) };

    debug_assert!(
        v.as_value_type().is_const_string() || v.as_value_type().is_rm_alloc_string(),
        "`v` must be either an rm_alloc string or a const string"
    );

    if let Some(s) = v.as_rm_alloc_string() {
        let s_bytes = s.as_bytes();

        if let Some(lenp) = lenp {
            // Safety: caller must ensure (2)
            unsafe { lenp.write(s_bytes.len() as u32) };
        }

        return s_bytes.as_ptr() as *const c_char;
    }

    if let Some(s) = v.as_const_string() {
        let s_bytes = s.as_bytes();

        if let Some(lenp) = lenp {
            // Safety: caller must ensure (2)
            unsafe { lenp.write(s_bytes.len() as u32) };
        }

        return s_bytes.as_ptr() as *const c_char;
    }

    // Safety: caller must ensure (3)
    unsafe { unreachable_unchecked() }
}

/// Get the string value and length from an `RsValue` of type [`RsValueType::RmAllocString`] or
/// [`RsValueType::ConstString`].
///
/// # Safety
/// - (1) `v` must originate from a call to [`RsValue_DynRef`];
/// - (2) The value must be either of type [`RsValueType::RmAllocString`] or
///   [`RsValueType::ConstString`].
///
/// @param v A reference to the `RsValue` from which to obtain the string
/// @return The string held by `v`
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RsValue_String_GetPtr(v: OpaqueDynRsValueRef) -> *const c_char {
    // Safety: caller must ensure (1) and (2)
    unsafe { RsValue_String_Get(v, None) }
}

/// Get the [`RedisModuleString`] from an `RsValue` of type  [`RsValueType::OwnedRedisString`] or
/// [`RsValueType::BorrowedRedisString`].
///
/// # Safety
/// - (1) `v` must originate from a call to [`RsValue_DynRef`];
/// - (2) The value must be either of type [`RsValueType::OwnedRedisString`] or
///   [`RsValueType::BorrowedRedisString`].
///
/// @param v A reference to the `RsValue` from which to obtain the Redis string
/// @return The Redis string held by `v`
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RsValue_RedisString_Get(
    v: OpaqueDynRsValueRef,
) -> *const RedisModuleString {
    // Safety: caller must ensure (1)
    let v = unsafe { DynRsValueRef::from_opaque(v) };

    if let Some(s) = v.as_owned_redis_string() {
        return s.as_ptr();
    }

    if let Some(s) = v.as_borrowed_redis_string() {
        return s.as_ptr();
    }
    // Safety: caller must ensure (2)
    unsafe { unreachable_unchecked() }
}

/// Gets the string pointer and length from the value,
/// dereferencing in case `value` is a (chain of) RsValue
/// references. Works for all RsValue string types.
///
/// The returned string may or may not be null-terminated.
///
/// # Safety
/// - (1) `v` must originate from a call to [`RsValue_DynRef`].
/// - (2) The value `v` points to must be of any of the string types;
/// - (3) The length of the string the value holds must not exceed [`u32::MAX`];
/// - (4) `lenp` must be non-null, well-aligned and valid for writes;
/// - (5) The returned pointer is invalidated upon mutation of the value.
///
/// @param v The value from which to obtain the data
/// @param lenp The location to which to write the string length
/// @return A pointer to the start of the string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RsValue_StringPtrLen(
    v: OpaqueDynRsValueRef,
    lenp: Option<NonNull<u32>>,
) -> *const c_char {
    // Safety: caller must ensure (5)
    let lenp = unsafe { expect_unchecked!(lenp) };
    // Safety: caller must ensure (1)
    let v = unsafe { DynRsValueRef::from_opaque(v) };

    let v = v.deep_deref();
    // Safety: Caller must ensure (2)
    let bytes = unsafe { expect_unchecked!(v.string_as_bytes(), "`v` is not a string") };
    // Safety: Caller must ensure (3)
    let len: u32 =
        unsafe { expect_unchecked!(bytes.len().try_into(), "string length exceeds `u32::MAX`") };
    // Safety: Caller must ensure (4)
    unsafe { lenp.write(len) };
    bytes.as_ptr() as *const c_char
}

/// Get an item from an array value
///
/// # Safety
/// - (1) `v` must originate from a call to [`RsValue_DynRef`].
/// - (2) The `RsValue` `v` points to must be of type [`RsValueType::Array`]
/// - (3) `index` must be less than the capacity of the array held by the value.
///
/// @param v A reference to an `RsValue` array from which to get the item
/// @param i The index
/// @return A reference to the `RsValue` at index `i`
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RsValue_ArrayItem(
    v: OpaqueDynRsValueRef,
    index: u32,
) -> OpaqueDynRsValueRef {
    // Safety: caller must ensure (1)
    let v = unsafe { DynRsValueRef::from_opaque(v) };

    // Safety: caller must ensure (2) and (3)
    let item = unsafe {
        expect_unchecked!(
            v.array_get(index),
            "Array index out of bounds or value is not an array"
        )
    };
    item.to_dyn_ref().into_opaque()
}

/// Get the capacity of an array value.
///
/// # Safety
/// - (1) `v` must originate from a call to [`RsValue_DynRef`].
/// - (2) The `RsValue` `v` points to must be of type [`RsValueType::Array`]
///
/// @param v A reference to an `RsValue` array for which to obtain the length
/// @return The array length
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RsValue_ArrayLen(v: OpaqueDynRsValueRef) -> u32 {
    // Safety: caller must ensure (1)
    let v = unsafe { DynRsValueRef::from_opaque(v) };

    // Safety: caller must ensure (2)
    unsafe { expect_unchecked!(v.array_cap(), "Value is not an array") }
}

/// Get the capacity of a map value, i.e. the number of entries it holds.
///
/// # Safety
/// - (1) `v` must originate from a call to [`RsValue_DynRef`].
/// - (2) The `RsValue` `v` points to must be of type [`RsValueType::Map`]
///
/// @param v A reference to an `RsValue` map for which to obtain the length
/// @return The map length
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RsValue_Map_Len(v: OpaqueDynRsValueRef) -> u32 {
    // Safety: caller must ensure (1)
    let v = unsafe { DynRsValueRef::from_opaque(v) };

    // Safety: caller must ensure (2)
    unsafe { expect_unchecked!(v.map_cap(), "Value is not a map") }
}

/// Get an entry from a map value.
///
/// # Safety
/// - (1) `v` must originate from a call to [`RsValue_DynRef`].
/// - (2) The `RsValue` `v` points to must be of type [`RsValueType::Map`]
/// - (3) `index` must be less than the capacity of the map held by the value.
/// - (4) `key` must be non-null, well-aligned, and valid for writes
/// - (5) `value` must be non-null, well-aligned, and valid for writes
///
/// @param v A reference to an `RsValue` array from which to get the item
/// @param i The index
/// @return A reference to the `RsValue` at index `i`
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RsValue_Map_GetEntry(
    v: OpaqueDynRsValueRef,
    index: u32,
    key: Option<NonNull<OpaqueDynRsValue>>,
    value: Option<NonNull<OpaqueDynRsValue>>,
) {
    // Safety: caller must ensure (4)
    let key = unsafe { expect_unchecked!(key) };
    // Safety: caller must ensure (5)
    let value = unsafe { expect_unchecked!(value) };

    // Safety: caller must ensure (1)
    let v = unsafe { DynRsValueRef::from_opaque(v) };

    // Safety: caller must ensure (2) and (3)
    let entry = unsafe {
        expect_unchecked!(
            v.map_get(index),
            "Map index out of bounds or value is not a map"
        )
    };

    let entry_key = DynRsValue::from(entry.key.clone()).into_opaque();
    // Safety: caller must ensure (4)
    unsafe { key.write(entry_key) };

    let entry_value = DynRsValue::from(entry.key.clone()).into_opaque();
    // Safety: caller must ensure (5)
    unsafe { value.write(entry_value) };
}

/// Get the left value of a trio value.
///
/// # Safety
/// - (1) `v` must originate from a call to [`RsValue_DynRef`].
/// - (2) The `RsValue` `v` points to must be of type [`RsValueType::Trio`]
///
/// @param v A reference to the trio value to extract the left value from
/// @return The left value of the trio
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RsValue_Trio_GetLeft(v: OpaqueDynRsValueRef) -> OpaqueDynRsValue {
    // Safety: caller must ensure (1)
    let v = unsafe { DynRsValueRef::from_opaque(v) };
    // Safety: caller must ensure (2)
    let trio = unsafe { expect_unchecked!(v.as_trio(), "Value is not a trio") };

    let left = trio.left().clone();

    DynRsValue::from(left).into_opaque()
}

/// Get the middle value of a trio value.
///
/// # Safety
/// - (1) `v` must originate from a call to [`RsValue_DynRef`].
/// - (2) The `RsValue` `v` points to must be of type [`RsValueType::Trio`]
///
/// @param v A reference to the trio value to extract the middle value from
/// @return The middle value of the trio
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RsValue_Trio_GetMiddle(v: OpaqueDynRsValueRef) -> OpaqueDynRsValue {
    // Safety: caller must ensure (1)
    let v = unsafe { DynRsValueRef::from_opaque(v) };
    // Safety: caller must ensure (2)
    let trio = unsafe { expect_unchecked!(v.as_trio(), "Value is not a trio") };

    let middle = trio.middle().clone();

    DynRsValue::from(middle).into_opaque()
}

/// Get the right value of a trio value.
///
/// # Safety
/// - (1) `v` must originate from a call to [`RsValue_DynRef`].
/// - (2) The `RsValue` `v` points to must be of type [`RsValueType::Trio`]
///
/// @param v A reference to the trio value to extract the right value from
/// @return The right value of the trio
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RsValue_Trio_GetRight(v: OpaqueDynRsValueRef) -> OpaqueDynRsValue {
    // Safety: caller must ensure (1)
    let v = unsafe { DynRsValueRef::from_opaque(v) };
    // Safety: caller must ensure (2)
    let trio = unsafe { expect_unchecked!(v.as_trio(), "Value is not a trio") };

    let right = trio.right().clone();

    DynRsValue::from(right).into_opaque()
}

/// Repeatedly dereference self until ending up at a non-reference value.
///
/// # Safety
/// - (1) `v` must originate from a call to [`RsValue_DynRef`].
///
/// @param v The value to dereference
/// @return The value at the end of the reference chain
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RsValue_Dereference(v: OpaqueDynRsValueRef) -> OpaqueDynRsValueRef {
    // Safety: caller must ensure (1)
    let v = unsafe { DynRsValueRef::from_opaque(v) };

    let v_deref = v.deep_deref();

    DynRsValueRef::into_opaque(v_deref)
}

/// Create a reference to the value. This reference is different from
/// a pointer to an `RsValue`, which in case of a shared value would
/// require dereferencing twice in order to reach the value itself.
///
/// # Safety
/// - (1) The `RsValue` `v` points to must originate from one of the `RsValue` constructors,
///   i.e. [`RsValue_Undefined`], [`RsValue_Number`], [`RsValue_String`],
///   or [`RsValue_NullStatic`].
/// - (2) `v` must be non-null.
///
/// @param v A pointer to the `RsValue` to convert to an `RsValueRef`
/// @return A reference to the `RsValue` v points to.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RsValue_DynRef(v: *mut OpaqueDynRsValue) -> OpaqueDynRsValueRef {
    // Safety: caller must ensure (1)
    let v = unsafe { DynRsValue::from_opaque_ptr(v) };
    // Safety: caller must ensure (2)
    let v = unsafe { expect_unchecked!(v, "`v` must not be NULL") };
    DynRsValueRef::from(v).into_opaque()
}

/// Free an RsValue.
///
/// # Safety
/// - (1) `v` must point to an `RsValue` that originates from one of the `RsValue` constructors,
///   i.e. [`RsValue_Undefined`], [`RsValue_Number`], [`RsValue_String`], or [`RsValue_NullStatic`].
/// - (2) `v` is no longer valid after this call as the `RsValue` it points to is destructed.
///
/// @param v Pointer to the `RsValue` that is to be freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RsValue_Free(v: *mut OpaqueDynRsValue) {
    // Safety: caller must ensure (1)
    let v = unsafe { v.read() };
    // Safety: caller must ensure (1)
    let v = unsafe { DynRsValue::from_opaque(v) };
    drop(v);
}

#[cfg(test)]
redis_mock::bind_redis_alloc_symbols_to_mock_impl!();

#[cfg(test)]
mod test {
    use c_ffi_utils::opaque::IntoOpaque;
    use std::{ffi::CStr, ptr::NonNull};

    use value::{RsValue, Value, dynamic::DynRsValue};

    use crate::{RsValue_Free, RsValue_IntoNumber, shared::RsValue_NewConstString};

    #[test]
    fn create_replace_free() {
        const STR: &CStr = c"hello";

        // Unique
        // Safety: `STR` is const and its length is 5.
        let v = unsafe { RsValue::const_string(STR.as_ptr(), 5) };
        let v = DynRsValue::from(v);
        let mut v = v.into_opaque();

        // Safety: `v` is nonnull and is correctly initialized.
        unsafe { RsValue_IntoNumber(Some(NonNull::from_mut(&mut v)), 1.23) };

        // Safety: `v` originates from a call to `DynRsValue::into_opaque`
        let mut v = unsafe { DynRsValue::from_opaque(v) };
        // Safety: `v` is nonnull and is correctly initialized.
        unsafe { RsValue_Free(v.as_opaque_mut_ptr()) };

        // Shared
        // Safety: `STR` is const and its length is 5
        let mut v = unsafe { RsValue_NewConstString(STR.as_ptr(), 5) };

        // Safety: `v` is nonnull and is correctly initialized.
        unsafe { RsValue_IntoNumber(Some(NonNull::from_mut(&mut v)), 1.23) };
        // Safety: `v` originates from a call to one of the constructors
        let mut v = unsafe { DynRsValue::from_opaque(v) };
        // Safety: `v` is nonnull and is correctly initialized.
        unsafe { RsValue_Free(v.as_opaque_mut_ptr()) }

        // Avoid double free
        std::mem::forget(v);
    }
}
