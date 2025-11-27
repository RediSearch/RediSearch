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
    mem,
    ptr::NonNull,
};

use c_ffi_utils::{expect_unchecked, opaque::IntoOpaque};
use value::{RsValue, Value, shared::SharedRsValue};

use crate::{
    dynamic::{
        DynRsValue, DynRsValuePtr,
        opaque::{OpaqueDynRsValue, OpaqueDynRsValuePtr},
    },
    value_type::{AsRsValueType, RsValueType},
};
use ffi::RedisModuleString;

pub mod collection;
pub mod dynamic;
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

#[unsafe(no_mangle)]
pub extern "C" fn RSValue_NewUndefined() -> OpaqueDynRsValuePtr {
    // First try
    let val = DynRsValue::from(RsValue::undefined());
    // CRITICAL: val will be freed after this, so DynRsValuePtr will point to a freed value
    DynRsValuePtr::from_dyn_value(&val).into_opaque();

    // Another try
    let val = SharedRsValue::undefined(); // which is a null pointer
    let dyn_val = DynRsValue::Shared(val);
    let dyn_val_ptr = DynRsValuePtr::from_dyn_value(&val); // will be of type Shared with a null pointer
    // CRITICAL: Now it contains a null pointer and calls to e.g.

    // We can't use SharedRsValue::undefined() because DynRsValuePtr::Shared points to a
    // RsValueInternal and RsValueInternal doesn't has an undefined option.

    // How should this be handled?
}

#[unsafe(no_mangle)]
pub extern "C" fn RSValue_NewString(_str: *const c_char, _len: u32) -> OpaqueDynRsValuePtr {
    // create a RsValue::String
    // then wrap that in a DynRsValue::Shared
    unimplemented!()
}

// Something similar as the above for RSValue_NewConstString, RSValue_NewBorrowedRedisString, RSValue_NewOwnedRedisString
// RSValue_NewStolenRedisString, RSValue_NewCopiedString, RSValue_NewParsedNumber, RSValue_NewNumber,
// RSValue_NewNumberFromInt64, RSValue_NewArray, RSValue_NewMap, RSValue_NewVStringArray, RSValue_NewStringArray,
// RSValue_NewConstStringArray, RSValue_NewTrio...
// all returning an OpaqueDynRsValuePtr, right?

/// Returns a pointer to a statically allocated NULL `RsValue`.
/// This is a singleton - the same pointer is always returned.
/// DO NOT free or modify this value.
///
/// @return A pointer to a static `RsValue` of type `RsValueType_Null`
#[unsafe(no_mangle)]
pub extern "C" fn RsValue_NullStatic() -> OpaqueDynRsValuePtr {
    static RSVALUE_NULL: DynRsValue = DynRsValue::null_const();
    DynRsValuePtr::from_dyn_value(&RSVALUE_NULL).into_opaque()
}

/// Get the type of an `RsValue` as an [`RsValueType`].
///
/// # Safety
/// - (1) `v` must originate from a call to [`RsValue_DynPtr`].
///
/// @param v The value to inspect
/// @return The `RsValueType` of the value
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RsValue_Type(v: OpaqueDynRsValuePtr) -> RsValueType {
    // Safety: caller must ensure (1)
    let v = unsafe { DynRsValuePtr::from_opaque(v) };
    // Safety: caller must ensure (1)
    unsafe { apply_with_dyn_ptr!(v, |v| v.as_value_type()) }
}

/// Check if the `RsValue` is a reference.
///
/// # Safety
/// See [`RsValue_Type`].
///
/// @param v The value to check
/// @return true if the value is of type [`RsValueType::Ref`], false otherwise
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RsValue_IsReference(v: OpaqueDynRsValuePtr) -> bool {
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
pub unsafe extern "C" fn RsValue_IsNumber(v: OpaqueDynRsValuePtr) -> bool {
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
pub unsafe extern "C" fn RsValue_IsString(v: OpaqueDynRsValuePtr) -> bool {
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
pub unsafe extern "C" fn RsValue_IsArray(v: OpaqueDynRsValuePtr) -> bool {
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
pub unsafe extern "C" fn RsValue_IsRedisString(v: OpaqueDynRsValuePtr) -> bool {
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
pub unsafe extern "C" fn RsValue_IsOwnRString(v: OpaqueDynRsValuePtr) -> bool {
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
pub unsafe extern "C" fn RsValue_IsTrio(v: OpaqueDynRsValuePtr) -> bool {
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
pub unsafe extern "C" fn RsValue_IsAnyString(v: OpaqueDynRsValuePtr) -> bool {
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
pub unsafe extern "C" fn RsValue_IsNull(v: OpaqueDynRsValuePtr) -> bool {
    // Safety: caller must ensure safety requirements of `RsValue_Type`
    // are met.
    unsafe { RsValue_Type(v).is_null() }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_SetNumber(_v: OpaqueDynRsValuePtr, _n: c_double) {
    unimplemented!()
}

/// Gets the `f64` wrapped by the [`OpaqueDynRsValue`]
///
/// # Safety
/// - (1) `v` originate from a call to [`RsValue_DynPtr`].
/// - (2) `v` must be a number value.
///
/// @param v A reference to the `RsValue` from which to obtain the numeric value
/// @return The numeric value held by the `RsValue`
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RsValue_Number_Get(v: OpaqueDynRsValuePtr) -> f64 {
    // Safety: caller must ensure (1)
    let v = unsafe { DynRsValuePtr::from_opaque(v) };
    // Safety: caller must ensure (1)
    let n = unsafe { apply_with_dyn_ptr!(v, |v| v.get_number()) };
    // Safety: caller must ensure (2).
    unsafe { expect_unchecked!(n, "v must be of type 'Number'") }
}

/// Convert an `RsValue` to a number type in-place.
/// This clears the existing value and replaces it with the given value.
///
/// # Safety
/// - (1) `v` must be non-null;
/// - (2) `v` must point to an `RsValue` originating from one of the constructors.
///
/// @param v The value to modify
/// @param n The numeric value to set
// DAX: Re 1: `RsValue_IntoNumber` is normally called with a *RSValue` which in the new setup is going to be an `OpaqueDynRsValuePtr`.
// DAX: An `OpaqueDynRsValuePtr` is NOT a `Option<NonNull<OpaqueDynRsValue>>`
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
/// - (1) `v` must originate from a call to [`RsValue_DynPtr`];
/// - (2) If `lenp` is non-null, it must be a well-aligned pointer to a `u32` that is valid for writes;
/// - (3) The value must be either of type [`RsValueType::RmAllocString`] or
///   [`RsValueType::ConstString`].
///
/// @param v A reference to the `RsValue` from which to obtain the string
/// @param lenp A nullable pointer to which the length will be written
/// @return The string held by `v`
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RsValue_String_Get(
    v: OpaqueDynRsValuePtr,
    lenp: Option<NonNull<u32>>,
) -> *const c_char {
    // Safety: caller must ensure (1)
    let v = unsafe { DynRsValuePtr::from_opaque(v) };

    #[allow(clippy::multiple_unsafe_ops_per_block)]
    #[allow(unused_unsafe)]
    // Safety: caller must ensure (1)
    unsafe {
        apply_with_dyn_ptr!(v, |v| {
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
        })
    }
}

/// Get the string value and length from an `RsValue` of type [`RsValueType::RmAllocString`] or
/// [`RsValueType::ConstString`].
///
/// # Safety
/// - (1) `v` must originate from a call to [`RsValue_DynPtr`];
/// - (2) The value must be either of type [`RsValueType::RmAllocString`] or
///   [`RsValueType::ConstString`].
///
/// @param v A reference to the `RsValue` from which to obtain the string
/// @return The string held by `v`
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RsValue_String_GetPtr(v: OpaqueDynRsValuePtr) -> *const c_char {
    // Safety: caller must ensure (1) and (2)
    unsafe { RsValue_String_Get(v, None) }
}

/// Get the [`RedisModuleString`] from an `RsValue` of type  [`RsValueType::OwnedRedisString`] or
/// [`RsValueType::BorrowedRedisString`].
///
/// # Safety
/// - (1) `v` must originate from a call to [`RsValue_DynPtr`];
/// - (2) The value must be either of type [`RsValueType::OwnedRedisString`] or
///   [`RsValueType::BorrowedRedisString`].
///
/// @param v A reference to the `RsValue` from which to obtain the Redis string
/// @return The Redis string held by `v`
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RsValue_RedisString_Get(
    v: OpaqueDynRsValuePtr,
) -> *const RedisModuleString {
    // Safety: caller must ensure (1)
    let v = unsafe { DynRsValuePtr::from_opaque(v) };

    #[allow(clippy::multiple_unsafe_ops_per_block)]
    #[allow(unused_unsafe)]
    // Safety: caller must ensure (1)
    unsafe {
        apply_with_dyn_ptr!(v, |v| {
            if let Some(s) = v.as_owned_redis_string() {
                return s.as_ptr();
            }

            if let Some(s) = v.as_borrowed_redis_string() {
                return s.as_ptr();
            }
            // Safety: caller must ensure (2)
            unsafe { unreachable_unchecked() }
        })
    }
}

/// Gets the string pointer and length from the value,
/// dereferencing in case `value` is a (chain of) RsValue
/// references. Works for all RsValue string types.
///
/// The returned string may or may not be null-terminated.
///
/// # Safety
/// - (1) `v` must originate from a call to [`RsValue_DynPtr`].
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
    v: OpaqueDynRsValuePtr,
    lenp: Option<NonNull<u32>>,
) -> *const c_char {
    // Safety: caller must ensure (5)
    let lenp = unsafe { expect_unchecked!(lenp) };
    // Safety: caller must ensure (1)
    let v = unsafe { DynRsValuePtr::from_opaque(v) };

    // It's hard to make Rust understand that in fact
    // we're doing a single unsafe operation that applies
    // a macro which does more unsafe operations,
    // rather than a whole bunch of unsafe operations
    // in a single block.
    #[allow(clippy::multiple_unsafe_ops_per_block)]
    #[allow(unused_unsafe)]
    // Safety: caller must ensure (1)
    unsafe {
        apply_with_dyn_ptr!(v, |v| {
            let v = v.deep_deref();
            // Safety: Caller must ensure (2)
            let bytes = unsafe { expect_unchecked!(v.string_as_bytes(), "`v` is not a string") };
            // Safety: Caller must ensure (3)
            let len: u32 = unsafe {
                expect_unchecked!(bytes.len().try_into(), "string length exceeds `u32::MAX`")
            };
            // Safety: Caller must ensure (4)
            unsafe { lenp.write(len) };
            bytes.as_ptr() as *const c_char
        })
    }
}

/// Get an item from an array value
///
/// # Safety
/// - (1) `v` must originate from a call to [`RsValue_DynPtr`].
/// - (2) The `RsValue` `v` points to must be of type [`RsValueType::Array`]
/// - (3) `index` must be less than the capacity of the array held by the value.
///
/// @param v A reference to an `RsValue` array from which to get the item
/// @param i The index
/// @return A reference to the `RsValue` at index `i`
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RsValue_ArrayItem(
    v: OpaqueDynRsValuePtr,
    index: u32,
) -> OpaqueDynRsValuePtr {
    // Safety: caller must ensure (1)
    let v = unsafe { DynRsValuePtr::from_opaque(v) };

    #[allow(clippy::multiple_unsafe_ops_per_block)]
    #[allow(unused_unsafe)]
    // Safety: caller must ensure (1)
    unsafe {
        apply_with_dyn_ptr!(v, |v| {
            // Safety: caller must ensure (2) and (3)
            let item = unsafe {
                expect_unchecked!(
                    v.array_get(index),
                    "Array index out of bounds or value is not an array"
                )
            };
            DynRsValuePtr::from_dyn_value_ref(item.to_dyn_ref()).into_opaque()
        })
    }
}

/// Get the capacity of an array value.
///
/// # Safety
/// - (1) `v` must originate from a call to [`RsValue_DynPtr`].
/// - (2) The `RsValue` `v` points to must be of type [`RsValueType::Array`]
///
/// @param v A reference to an `RsValue` array for which to obtain the length
/// @return The array length
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RsValue_ArrayLen(v: OpaqueDynRsValuePtr) -> u32 {
    // Safety: caller must ensure (1)
    let v = unsafe { DynRsValuePtr::from_opaque(v) };

    #[allow(clippy::multiple_unsafe_ops_per_block)]
    #[allow(unused_unsafe)]
    // Safety: caller must ensure (1)
    unsafe {
        apply_with_dyn_ptr!(
            v,
            // Safety: caller must ensure (2)
            |v| unsafe { expect_unchecked!(v.array_cap(), "Value is not an array") }
        )
    }
}

/// Get the capacity of a map value, i.e. the number of entries it holds.
///
/// # Safety
/// - (1) `v` must originate from a call to [`RsValue_DynPtr`].
/// - (2) The `RsValue` `v` points to must be of type [`RsValueType::Map`]
///
/// @param v A reference to an `RsValue` map for which to obtain the length
/// @return The map length
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RsValue_Map_Len(v: OpaqueDynRsValuePtr) -> u32 {
    // Safety: caller must ensure (1)
    let v = unsafe { DynRsValuePtr::from_opaque(v) };

    #[allow(clippy::multiple_unsafe_ops_per_block)]
    #[allow(unused_unsafe)]
    // Safety: caller must ensure (1)
    unsafe {
        apply_with_dyn_ptr!(
            v,
            // Safety: caller must ensure (2)
            |v| unsafe { expect_unchecked!(v.map_cap(), "Value is not a map") }
        )
    }
}

/// Get an entry from a map value.
///
/// # Safety
/// - (1) `v` must originate from a call to [`RsValue_DynPtr`].
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
    v: OpaqueDynRsValuePtr,
    index: u32,
    key: Option<NonNull<OpaqueDynRsValue>>,
    value: Option<NonNull<OpaqueDynRsValue>>,
) {
    // Safety: caller must ensure (4)
    let key = unsafe { expect_unchecked!(key) };
    // Safety: caller must ensure (5)
    let value = unsafe { expect_unchecked!(value) };

    // Safety: caller must ensure (1)
    let v = unsafe { DynRsValuePtr::from_opaque(v) };

    #[allow(clippy::multiple_unsafe_ops_per_block)]
    #[allow(unused_unsafe)]
    // Safety: caller must ensure (1)
    unsafe {
        apply_with_dyn_ptr!(v, |v| {
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

            let entry_value = DynRsValue::from(entry.value.clone()).into_opaque();
            // Safety: caller must ensure (5)
            unsafe { value.write(entry_value) };
        })
    }
}

/// Get the left value of a trio value.
///
/// # Safety
/// - (1) `v` must originate from a call to [`RsValue_DynPtr`].
/// - (2) The `RsValue` `v` points to must be of type [`RsValueType::Trio`]
///
/// @param v A reference to the trio value to extract the left value from
/// @return The left value of the trio
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RsValue_Trio_GetLeft(v: OpaqueDynRsValuePtr) -> OpaqueDynRsValue {
    // Safety: caller must ensure (1)
    let v = unsafe { DynRsValuePtr::from_opaque(v) };

    #[allow(clippy::multiple_unsafe_ops_per_block)]
    #[allow(unused_unsafe)]
    // Safety: caller must ensure (1)
    unsafe {
        apply_with_dyn_ptr!(v, |v| {
            // Safety: caller must ensure (2)
            let trio = unsafe { expect_unchecked!(v.as_trio(), "Value is not a trio") };

            let left = trio.left().clone();

            DynRsValue::from(left).into_opaque()
        })
    }
}

/// Get the middle value of a trio value.
///
/// # Safety
/// - (1) `v` must originate from a call to [`RsValue_DynPtr`].
/// - (2) The `RsValue` `v` points to must be of type [`RsValueType::Trio`]
///
/// @param v A reference to the trio value to extract the middle value from
/// @return The middle value of the trio
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RsValue_Trio_GetMiddle(v: OpaqueDynRsValuePtr) -> OpaqueDynRsValue {
    // Safety: caller must ensure (1)
    let v = unsafe { DynRsValuePtr::from_opaque(v) };

    #[allow(clippy::multiple_unsafe_ops_per_block)]
    #[allow(unused_unsafe)]
    // Safety: caller must ensure (1)
    unsafe {
        apply_with_dyn_ptr!(v, |v| {
            // Safety: caller must ensure (2)
            let trio = unsafe { expect_unchecked!(v.as_trio(), "Value is not a trio") };

            let middle = trio.middle().clone();

            DynRsValue::from(middle).into_opaque()
        })
    }
}

/// Get the right value of a trio value.
///
/// # Safety
/// - (1) `v` must originate from a call to [`RsValue_DynPtr`].
/// - (2) The `RsValue` `v` points to must be of type [`RsValueType::Trio`]
///
/// @param v A reference to the trio value to extract the right value from
/// @return The right value of the trio
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RsValue_Trio_GetRight(v: OpaqueDynRsValuePtr) -> OpaqueDynRsValue {
    // Safety: caller must ensure (1)
    let v = unsafe { DynRsValuePtr::from_opaque(v) };

    #[allow(clippy::multiple_unsafe_ops_per_block)]
    #[allow(unused_unsafe)]
    // Safety: caller must ensure (1)
    unsafe {
        apply_with_dyn_ptr!(v, |v| {
            // Safety: caller must ensure (2)
            let trio = unsafe { expect_unchecked!(v.as_trio(), "Value is not a trio") };

            let right = trio.right().clone();

            DynRsValue::from(right).into_opaque()
        })
    }
}

/// Increment the reference count of a shared `RsValue`, ensuring
/// it doesn't get freed until after `RsValue_DecrRef` is called.
/// Does nothing when passing an exclusive `RsValue`.
///
/// # Safety
/// - (1) `v` must originate from a call to [`RsValue_DynPtr`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RsValue_IncrRef(v: OpaqueDynRsValuePtr) {
    // Safety: caller must ensure (1)
    let v = unsafe { DynRsValuePtr::from_opaque(v) };

    // Safety: caller must ensure (1)
    match v {
        DynRsValuePtr::Shared(v) => {
            // Safety: caller must ensure (1), thereby
            // guaranteeing that `v` originates from a call
            // to `SharedRsValue::as_raw` which destructures
            // the `SharedRsValue` and returns the `*const RsValueInternal`
            // it wraps. Furthermore, the resulting `SharedRsValue`
            // is forgotten rather than dropped below.
            let v = unsafe { SharedRsValue::from_raw(v) };
            std::mem::forget(v.clone()); // Increment refcount
            std::mem::forget(v); // Don't decrement refcount
        }
        DynRsValuePtr::Exclusive(_) => (), // do nothing
    }
}

/// Decrement the reference count of an `RsValue`.
/// Simply drops the value if it is exclusive.
///
/// # Safety
/// - (1) `v` must originate from a call to [`RsValue_DynPtr`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RsValue_DecrRef(v: OpaqueDynRsValuePtr) {
    // Safety: caller must ensure (1)
    let v = unsafe { DynRsValuePtr::from_opaque(v) };

    match v {
        // Safety: caller must ensure (1)
        DynRsValuePtr::Exclusive(v) => drop(unsafe { v.read() }),
        // Safety: caller must ensure (1)
        DynRsValuePtr::Shared(v) => drop(unsafe { SharedRsValue::from_raw(v) }),
    }
}

/// Repeatedly dereference self until ending up at a non-reference value.
///
/// # Safety
/// - (1) `v` must originate from a call to [`RsValue_DynPtr`].
///
/// @param v The value to dereference
/// @return The value at the end of the reference chain
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RsValue_Dereference(v: OpaqueDynRsValuePtr) -> OpaqueDynRsValuePtr {
    // Safety: caller must ensure (1)
    let v = unsafe { DynRsValuePtr::from_opaque(v) };
    // Safety: caller must ensure (1)
    let v_ptr = unsafe {
        apply_with_dyn_ptr!(v, |v| {
            let v_ref = v.deep_deref();
            DynRsValuePtr::from_dyn_value_ref(v_ref)
        })
    };

    v_ptr.into_opaque()
}

/// Convert `dst` to a reference to `src`. If `src` is exclusive,
/// it gets converted to a shared value first.
///
/// # Safety
/// - (1) `dst` must be non-null.
/// - (2) The `RsValue` `dst` points to must originate from one of the `RsValue` constructors,
///   i.e. [`RsValue_Undefined`], [`RsValue_Number`], [`RsValue_String`],
///   or [`RsValue_NullStatic`].
/// - (3) `src` must originate from a call to [`RsValue_DynPtr`].
// DAX: Re 2: `RsValue_NullStatic` is not the same. It returns a `RsValuePtr` instead of the other three returning a `RsValue`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RsValue_MakeReference(
    dst: Option<NonNull<OpaqueDynRsValue>>,
    src: OpaqueDynRsValuePtr,
) {
    // Safety: caller must ensure (1)
    let dst = unsafe { expect_unchecked!(dst) };
    // Safety: caller must ensure (2)
    let dst = unsafe { DynRsValue::from_opaque_non_null(dst) };

    // Safety: caller must ensure (3)
    let src = unsafe { DynRsValuePtr::from_opaque(src) };
    // Safety: caller must ensure (3)
    let src = unsafe { apply_with_dyn_ptr!(src, |src| { src.to_shared() }) };

    dst.to_reference(src);
}

/// Convert `dst` to a reference to `src`, *without incrementing the reference count of `src`*.
/// If `src` is exclusive, it gets converted to a shared value first.
///
/// # Safety
/// - (1) `dst` must be non-null.
/// - (2) The `RsValue` `dst` points to must originate from one of the `RsValue` constructors,
///   i.e. [`RsValue_Undefined`], [`RsValue_Number`], [`RsValue_String`],
///   or [`RsValue_NullStatic`].
/// - (3) `src` must originate from a call to [`RsValue_DynPtr`].
/// - (4) `src` is invalid after a call to this function, and must not be used after
///   being passed to this function.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RsValue_MakeOwnReference(
    dst: Option<NonNull<OpaqueDynRsValue>>,
    src: OpaqueDynRsValuePtr,
) {
    // Safety: caller must ensure (1)
    let dst = unsafe { expect_unchecked!(dst) };
    // Safety: caller must ensure (2)
    let dst = unsafe { DynRsValue::from_opaque_non_null(dst) };

    // Safety: caller must ensure (3)
    let src = unsafe { DynRsValuePtr::from_opaque(src) };

    let src = match src {
        DynRsValuePtr::Exclusive(v) => {
            // Safety: caller must ensure (3)
            let v = unsafe { &*v };
            v.to_shared()
        }
        DynRsValuePtr::Shared(v) => {
            // `v` originates from a call to [`RsValue_DynPtr`],
            // which calls `SharedRsValue::as_raw` to create the pointer.
            // This function moves rather than copies the `SharedRsValue`,
            // and by safety requirement (4), `v` is regarded invalid.
            // Therefore, in this case it's correct to not `mem::forget`
            // the value produced below.
            // Safety: caller must ensure (3) and (4)
            unsafe { SharedRsValue::from_raw(v) }
        }
    };

    dst.to_reference(src);
}

/// Clone `src` and assign it to `dst`, thereby
/// dropping the `RsValue`.
///
/// # Safety
/// - (1) `dst` must be non-null.
/// - (2) The `RsValue` `dst` points to must originate from one of the `RsValue` constructors,
///   i.e. [`RsValue_Undefined`], [`RsValue_Number`], [`RsValue_String`],
///   or [`RsValue_NullStatic`].
/// - (3) `v` must originate from a call to [`RsValue_DynPtr`].
///
/// @param dst A pointer to which to write a clone of `src`
/// @param src A pointer to a value that is to be cloned and assigned to `dst`.
pub unsafe extern "C" fn RsValue_Replace(
    dst: Option<NonNull<OpaqueDynRsValue>>,
    src: OpaqueDynRsValuePtr,
) {
    // Safety: caller must ensure (1)
    let dst = unsafe { expect_unchecked!(dst) };
    // Safety: caller must ensure (2)
    let dst = unsafe { DynRsValue::from_opaque_non_null(dst) };

    // Safety: caller must ensure (3)
    let src = unsafe { DynRsValuePtr::from_opaque(src) };

    match src {
        DynRsValuePtr::Exclusive(v) => {
            // Safety: caller must ensure (3)
            let v = unsafe { v.as_ref() };
            // Safety: caller must ensure (3)
            let v = unsafe { expect_unchecked!(v, "`v` must not be null") };
            *dst = v.clone().into();
        }
        DynRsValuePtr::Shared(v) => {
            // Safety: caller must ensure (3)
            let v = unsafe { SharedRsValue::from_raw(v) };
            *dst = v.clone().into();
            mem::forget(v);
        }
    }
}

/// Obtain a dynamic pointer to the value. This pointer is different from
/// a pointer to an `RsValue`, which in case of a shared value would
/// require dereferencing twice in order to reach the value itself.
///
/// # Safety
/// - (1) The `RsValue` `v` points to must originate from one of the `RsValue` constructors,
///   i.e. [`RsValue_Undefined`], [`RsValue_Number`], [`RsValue_String`],
///   or [`RsValue_NullStatic`].
/// - (2) `v` must be non-null.
///
/// @param v A pointer to the `RsValue` to convert to an `RsValuePtr`
/// @return A reference to the `RsValue` v points to.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RsValue_DynPtr(v: *mut OpaqueDynRsValue) -> OpaqueDynRsValuePtr {
    // Safety: caller must ensure (1)
    let v = unsafe { DynRsValue::from_opaque_ptr(v) };
    // Safety: caller must ensure (2)
    let v = unsafe { expect_unchecked!(v, "`v` must not be NULL") };
    DynRsValuePtr::from_dyn_value(v).into_opaque()
}

/// Clear an `RsValue` in-place.
/// This clears the existing value and replaces it with the given value.
///
/// # Safety
/// - (1) `v` must be non-null;
/// - (2) `v` must point to an `RsValue` originating from one of the constructors.
///
/// @param v The value to clear
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RsValue_Clear(v: Option<NonNull<OpaqueDynRsValue>>) {
    // Safety: caller must ensure (1)
    let v = unsafe { expect_unchecked!(v) };
    // Safety: caller must ensure (2)
    let v = unsafe { DynRsValue::from_opaque_mut_ptr(v.as_ptr()) };
    // Safety: caller must ensure (1). The previous statement casts the pointer
    // to an `Option<&mut RsValue>`, which will be None if and only if `v` were null.
    let v = unsafe { v.unwrap_unchecked() };
    v.clear();
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
pub unsafe extern "C" fn RsValue_Free(v: Option<NonNull<OpaqueDynRsValue>>) {
    // Safety: caller must ensure (1)
    let v = unsafe { expect_unchecked!(v) };
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
    use std::{
        ffi::{CStr, c_char},
        ptr::NonNull,
    };

    use value::{RsValue, Value};

    use crate::{
        RsValue_DecrRef, RsValue_DynPtr, RsValue_Free, RsValue_IncrRef, RsValue_IntoNumber,
        RsValue_MakeOwnReference, RsValue_MakeReference, RsValue_Number, RsValue_Replace,
        dynamic::{DynRsValue, opaque::OpaqueDynRsValue},
        shared::{RsValue_NewConstString, RsValue_NewNumber},
    };
    const STR: &CStr = c"hello";
    const STRPTR: *const c_char = STR.as_ptr();
    const STRLEN: u32 = STR.count_bytes() as u32;

    #[test]
    fn create_convert_free_exclusive() {
        // Safety: `STRPTR` points to const `STR` and its length is `STRLEN`
        let v = unsafe { RsValue::const_string(STRPTR, STRLEN) };
        let v = DynRsValue::from(v);
        let mut v = v.into_opaque();

        // Safety: `v` is nonnull and is correctly initialized.
        unsafe { RsValue_IntoNumber(Some(NonNull::from_mut(&mut v)), 1.23) };

        // Safety: `v` originates from a call to `DynRsValue::into_opaque`
        let mut v = unsafe { DynRsValue::from_opaque(v) };
        // Safety: `v` is nonnull and is correctly initialized.
        unsafe { RsValue_Free(Some(v.as_opaque_non_null())) };
    }

    #[test]
    fn create_convert_free_shared() {
        // Safety: `STRPTR` points to const `STR` and its length is `STRLEN`
        let mut v = unsafe { RsValue_NewConstString(STRPTR, STRLEN) };

        // Safety: `v` is nonnull and is correctly initialized.
        unsafe { RsValue_IntoNumber(Some(NonNull::from_mut(&mut v)), 1.23) };
        // Safety: `v` originates from a call to one of the constructors
        let mut v = unsafe { DynRsValue::from_opaque(v) };
        // Safety: `v` is nonnull and is correctly initialized.
        unsafe { RsValue_Free(Some(v.as_opaque_non_null())) }

        // Avoid double free
        std::mem::forget(v);
    }

    #[test]
    fn create_replace_free_exclusive_exclusive() {
        // Safety: `STRPTR` points to const `STR` and its length is `STRLEN`
        let v_src = unsafe { RsValue::const_string(STRPTR, STRLEN) };
        let v_src = DynRsValue::from(v_src);
        let mut v_src = v_src.into_opaque();
        // Safety: `v_src` is nonnull and is correctly initialized.
        let v_src_ptr = unsafe { RsValue_DynPtr(&mut v_src as *mut _) };

        let mut v_dst = RsValue_Number(1.23);
        let v_dst_ptr = Some(NonNull::from_mut(&mut v_dst));

        // Safety: `v_dst_ptr` and `v_src_ptr` are created according to the
        // requirements of `RsValue_Replace`
        unsafe {
            RsValue_Replace(v_dst_ptr, v_src_ptr);
        }

        // Safety: `v_dst_ptr` was not freed before and will not be used
        // after below call
        unsafe { RsValue_Free(v_dst_ptr) };
        // Safety: `v_src_ptr` was not freed before and will not be used
        // after below call
        unsafe { RsValue_Free(Some(NonNull::from_mut(&mut v_src))) };
    }

    #[test]
    fn create_replace_free_shared_shared() {
        // Safety: `STRPTR` points to const `STR` and its length is `STRLEN`
        let mut v_src = unsafe { RsValue_NewConstString(STRPTR, STRLEN) };
        // Safety: `v_src` is nonnull and is correctly initialized.
        let v_src_ptr = unsafe { RsValue_DynPtr(&mut v_src as *mut _) };

        let mut v_dst = RsValue_NewNumber(1.23);
        let v_dst_ptr = Some(NonNull::from_mut(&mut v_dst));

        // Safety: `v_dst_ptr` and `v_src_ptr` are created according to the
        // requirements of `RsValue_Replace`
        unsafe {
            RsValue_Replace(v_dst_ptr, v_src_ptr);
        }

        // Safety: `v_dst_ptr` was not freed before and will not be used
        // after below call
        unsafe { RsValue_Free(v_dst_ptr) };
        // Safety: `v_src_ptr` was not freed before and will not be used
        // after below call
        unsafe { RsValue_Free(Some(NonNull::from_mut(&mut v_src))) };
    }

    #[test]
    fn create_replace_free_shared_exclusive() {
        // Safety: `STRPTR` points to const `STR` and its length is `STRLEN`
        let mut v_src = unsafe { RsValue_NewConstString(STRPTR, STRLEN) };
        // Safety: `v_src` is nonnull and is correctly initialized.
        let v_src_ptr = unsafe { RsValue_DynPtr(&mut v_src as *mut _) };

        let mut v_dst = RsValue_Number(1.23);
        let v_dst_ptr = Some(NonNull::from_mut(&mut v_dst));

        // Safety: `v_dst_ptr` and `v_src_ptr` are created according to the
        // requirements of `RsValue_Replace`
        unsafe {
            RsValue_Replace(v_dst_ptr, v_src_ptr);
        }

        // Safety: `v_dst_ptr` was not freed before and will not be used
        // after below call
        unsafe { RsValue_Free(v_dst_ptr) };
        // Safety: `v_src_ptr` was not freed before and will not be used
        // after below call
        unsafe { RsValue_Free(Some(NonNull::from_mut(&mut v_src))) };
    }

    #[test]
    fn create_replace_free_exclusive_shared() {
        // Safety: `STRPTR` points to const `STR` and its length is `STRLEN`
        let v_src = unsafe { RsValue::const_string(STRPTR, STRLEN) };
        let v_src = DynRsValue::from(v_src);
        let mut v_src = v_src.into_opaque();
        // Safety: `v_src` is nonnull and is correctly initialized.
        let v_src_ptr = unsafe { RsValue_DynPtr(&mut v_src as *mut _) };

        let mut v_dst = RsValue_NewNumber(1.23);
        let v_dst_ptr = Some(NonNull::from_mut(&mut v_dst));

        // Safety: `v_dst_ptr` and `v_src_ptr` are created according to the
        // requirements of `RsValue_Replace`
        unsafe {
            RsValue_Replace(v_dst_ptr, v_src_ptr);
        }

        // Safety: `v_dst_ptr` was not freed before and will not be used
        // after below call
        unsafe { RsValue_Free(v_dst_ptr) };
        // Safety: `v_src_ptr` was not freed before and will not be used
        // after below call
        unsafe { RsValue_Free(Some(NonNull::from_mut(&mut v_src))) };
    }

    #[test]
    fn incrref_decrref() {
        // Safety: `STRPTR` points to const `STR` and its length is `STRLEN`
        let mut v = unsafe { RsValue_NewConstString(STRPTR, STRLEN) };
        // Safety: `v` is nonnull and is correctly initialized.
        let v_ptr = unsafe { RsValue_DynPtr(&mut v as *mut OpaqueDynRsValue) };
        // Safety: `v_ptr` originates from the previous call to `RsValue_DynPtr`
        unsafe { RsValue_IncrRef(v_ptr) };

        // Safety: `v_ptr` originates from the previous call to `RsValue_DynPtr`
        unsafe { RsValue_IncrRef(v_ptr) };

        let mut v_2 = RsValue_NewNumber(1.23);
        // Safety: `v_ptr` originates from the previous call to `RsValue_DynPtr`,
        // and `v_2` was created using one of the constructors.
        unsafe {
            RsValue_Replace(Some(NonNull::from_mut(&mut v_2)), v_ptr);
        }

        // Safety: `v_ptr` originates from the previous call to `RsValue_DynPtr`
        unsafe { RsValue_DecrRef(v_ptr) };

        // Safety: `v_ptr` originates from the previous call to `RsValue_DynPtr`
        unsafe { RsValue_DecrRef(v_ptr) };

        // Safety: `v_ptr` originates from the previous call to `RsValue_DynPtr`
        unsafe { RsValue_DecrRef(v_ptr) };

        // Safety: `v` originates from a call to `RsValue_NewConstString`
        unsafe { RsValue_Free(Some(NonNull::from_mut(&mut v))) }
    }

    #[test]
    fn make_ref() {
        // Safety: `STRPTR` points to const `STR` and its length is `STRLEN`
        let mut v = unsafe { RsValue_NewConstString(STRPTR, STRLEN) };
        // Safety: `v` is nonnull and is correctly initialized.
        let v_ptr = unsafe { RsValue_DynPtr(&mut v as *mut OpaqueDynRsValue) };

        let mut v_ref = RsValue_NewNumber(1.23);

        // Safety: `v_ptr` originates from the previous call to `RsValue_DynPtr`,
        // and `v_ref` was created using one of the constructors.
        unsafe {
            RsValue_MakeReference(Some(NonNull::from_mut(&mut v_ref)), v_ptr);
        }

        // Free both the reference and the referenced value
        // Safety: `v` was not freed before and will not be used
        // after below call
        unsafe {
            RsValue_Free(Some(NonNull::from_mut(&mut v)));
        }

        // Safety: `v_ref` was not freed before and will not be used
        // after below call
        unsafe {
            RsValue_Free(Some(NonNull::from_mut(&mut v_ref)));
        }
    }

    #[test]
    fn make_own_ref() {
        // Safety: `STRPTR` points to const `STR` and its length is `STRLEN`
        let mut v = unsafe { RsValue_NewConstString(STRPTR, STRLEN) };
        // Safety: `v` is nonnull and is correctly initialized.
        let v_ptr = unsafe { RsValue_DynPtr(&mut v as *mut OpaqueDynRsValue) };

        let mut v_ref = RsValue_NewNumber(1.23);

        // Safety: `v_ptr` originates from the previous call to `RsValue_DynPtr`,
        // and `v_ref` was created using one of the constructors.
        unsafe {
            RsValue_MakeOwnReference(Some(NonNull::from_mut(&mut v_ref)), v_ptr);
        }

        // Free only the reference
        // Safety: `v` was not freed before and will not be used
        // after below call
        unsafe {
            RsValue_Free(Some(NonNull::from_mut(&mut v_ref)));
        }
    }
}
