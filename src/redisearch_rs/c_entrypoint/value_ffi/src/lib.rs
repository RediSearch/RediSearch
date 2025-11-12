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
    ptr::NonNull,
};

use c_ffi_utils::{expect_unchecked, opaque::IntoOpaque};
use value::{RsValue, Value, opaque::OpaqueRsValue};

use crate::value_type::{AsRsValueType, RsValueType};

pub mod collection;
pub mod shared;
pub mod value_type;

/// Creates a stack-allocated, undefined `RsValue`.
/// @returns a stack-allocated `RsValue` of type `RsValueType_Undef`
#[unsafe(no_mangle)]
pub extern "C" fn RsValue_Undefined() -> OpaqueRsValue {
    RsValue::undefined().into_opaque()
}

/// Creates a stack-allocated `RsValue` containing a number.
/// The returned value is not allocated on the heap and should not be freed.
/// @param n The numeric value to wrap
/// @return A stack-allocated `RsValue` of type `RsValueType_Number`
#[unsafe(no_mangle)]
pub extern "C" fn RsValue_Number(n: c_double) -> OpaqueRsValue {
    RsValue::number(n).into_opaque()
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
pub unsafe extern "C" fn RsValue_String(str: Option<NonNull<c_char>>, len: u32) -> OpaqueRsValue {
    // Safety: caller must ensure (1)
    let str = unsafe { expect_unchecked!(str) };
    // Safety: caller must ensure (2), (3), (4), and (5)
    let v = unsafe { RsValue::take_rm_alloc_string(str, len) };
    v.into_opaque()
}

/// Returns a pointer to a statically allocated NULL `RsValue`.
/// This is a singleton - the same pointer is always returned.
/// DO NOT free or modify this value.
///
/// @return A pointer to a static `RsValue` of type `RsValueType_Null`
#[unsafe(no_mangle)]
pub extern "C" fn RsValue_NullStatic() -> *const OpaqueRsValue {
    static RSVALUE_NULL: RsValue = RsValue::null_const();
    RSVALUE_NULL.as_opaque_ptr()
}

/// Get the type of an `RsValue` as an [`RsValueType`].
///
/// @param v The value to inspect
/// @return The `RsValueType` of the value
///
/// # Safety
/// The passed pointer must originate from one of the `RsValue` constructors,
/// i.e. [`RsValue_Undefined`], [`RsValue_Number`], [`RsValue_String`],
/// or [`RsValue_NullStatic`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RsValue_Type(v: *const OpaqueRsValue) -> RsValueType {
    debug_assert!(!v.is_null(), "`v` must not be NULL");
    // Safety:
    // The caller must guarantee that `v` originates from one of the RsValue constructors,
    // all of which produce an `OpaqueRsValue` by calling `RsValue::into_opaque()`.
    let v = unsafe { RsValue::from_opaque_ptr(v) };
    v.unwrap().as_value_type()
}

/// Check if the `RsValue` is a reference.
///
/// @param v The value to check
/// @return true if the value is of type [`RsValueType::Ref`], false otherwise
///
/// # Safety
/// See [`RsValue_Type`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RsValue_IsReference(v: *const OpaqueRsValue) -> bool {
    // Safety: caller must ensure safety requirements of `RsValue_Type`
    // are met.
    unsafe { RsValue_Type(v).is_ref() }
}

/// Check if the `RsValue` is a number.
///
/// @param v The value to check
/// @return true if the value is of type [`RsValueType::Number`], false otherwise
///
/// # Safety
/// See [`RsValue_Type`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RsValue_IsNumber(v: *const OpaqueRsValue) -> bool {
    // Safety: caller must ensure safety requirements of `RsValue_Type`
    // are met.
    unsafe { RsValue_Type(v).is_number() }
}

/// Check if the `RsValue` is a string.
///
/// @param v The value to check
/// @return true if the value is of type [`RsValueType::String`], false otherwise
///
/// # Safety
/// See [`RsValue_Type`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RsValue_IsString(v: *const OpaqueRsValue) -> bool {
    // Safety: caller must ensure safety requirements of `RsValue_Type`
    // are met.
    unsafe { RsValue_Type(v).is_string() }
}

/// Check if the `RsValue` is an array.
///
/// @param v The value to check
/// @return true if the value is of type [`RsValueType::Array`], false otherwise
///
/// # Safety
/// See [`RsValue_Type`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RsValue_IsArray(v: *const OpaqueRsValue) -> bool {
    // Safety: caller must ensure safety requirements of `RsValue_Type`
    // are met.
    unsafe { RsValue_Type(v).is_array() }
}

/// Check if the `RsValue` is a Redis string type.
///
/// @param v The value to check
/// @return true if the value is of type [`RsValueType::BorrowedRedisString`], false otherwise
///
/// # Safety
/// See [`RsValue_Type`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RsValue_IsRedisString(v: *const OpaqueRsValue) -> bool {
    // Safety: caller must ensure safety requirements of `RsValue_Type`
    // are met.
    unsafe { RsValue_Type(v).is_borrowed_redis_string() }
}

/// Check if the `RsValue` is an owned Redis string.
///
/// @param v The value to check
/// @return true if the value is of type [`RsValueType::OwnedRedisString`], false otherwise
///
/// # Safety
/// See [`RsValue_Type`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RsValue_IsOwnRString(v: *const OpaqueRsValue) -> bool {
    // Safety: caller must ensure safety requirements of `RsValue_Type`
    // are met.
    unsafe { RsValue_Type(v).is_owned_redis_string() }
}

/// Check whether the `RsValue` is a trio.
///
/// @param v The value to check
/// @return true if the value is of type [`RsValueType::Trio`], false otherwise
///
/// # Safety
/// See [`RsValue_Type`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RsValue_IsTrio(v: *const OpaqueRsValue) -> bool {
    // Safety: caller must ensure safety requirements of `RsValue_Type`
    // are met.
    unsafe { RsValue_Type(v).is_trio() }
}

/// Returns true if the value contains any type of string
///
/// @param v The value to check
/// @return true if the value is any type of string, false otherwise
///
/// # Safety
/// See [`RsValue_Type`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RsValue_IsAnyString(v: *const OpaqueRsValue) -> bool {
    // Safety: caller must ensure safety requirements of `RsValue_Type`
    // are met.
    unsafe { RsValue_Type(v).is_any_string() }
}

/// Check if the value is NULL;
///
/// @param v The value to check
/// @return true if the value is NULL, false otherwise
///
/// # Safety
/// See [`RsValue_Type`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RsValue_IsNull(v: *const OpaqueRsValue) -> bool {
    // Safety: caller must ensure safety requirements of `RsValue_Type`
    // are met.
    unsafe { RsValue_Type(v).is_null() }
}

/// Gets the `f64` wrapped by the [`RsValue`]
///
/// # Safety
/// - (1) `v` must point to an `RsValue` originating from one of the constructors.
/// - (2) `v` must be non-null;
/// - (3) `v` must be valid for reads;
/// - (4) `v` must be a number value.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RsValue_Number_Get(v: *const OpaqueRsValue) -> f64 {
    // Safety: caller must ensure (1)
    let v = unsafe { RsValue::from_opaque_ptr(v) };
    // Safety: caller must ensure (2) and (3)
    let v = unsafe { expect_unchecked!(v, "`v` must not be NULL") };
    // Safety: caller must ensure (4).
    unsafe { expect_unchecked!(v.get_number(), "v must be of type 'Number'") }
}

/// Convert an [`RsValue`] to a number type in-place.
/// This clears the existing value and replaces it with the given value.
///
/// @param v The value to modify
/// @param n The numeric value to set
///
/// # Safety
/// - (1) `v` must be non-null;
/// - (2) `v` must point to an `RsValue` originating from one of the constructors.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RsValue_IntoNumber(v: Option<NonNull<OpaqueRsValue>>, n: f64) {
    // Safety: caller must ensure (1)
    let v = unsafe { expect_unchecked!(v) };
    // Safety: caller must ensure (2)
    let v = unsafe { RsValue::from_opaque_mut_ptr(v.as_ptr()) };
    // Safety: caller must ensure (1). The previous statement casts the pointer
    // to an `Option<&mut RsValue>`, which will be None if and only if `v` were null.
    let v = unsafe { v.unwrap_unchecked() };

    v.to_number(n);
}
