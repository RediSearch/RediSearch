/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#![allow(non_camel_case_types, non_snake_case)]
#![allow(unused)] // TODO removedd

use std::{
    ffi::{c_char, c_double},
    ptr::NonNull,
};

use value::{RsValue, map::RsValueMap, shared::SharedRsValue};

use crate::value_type::{AsRsValueType, RsValueType};

pub mod map;
pub mod shared;
pub mod value_type;

/// Creates a stack-allocated, undefined `RsValue`.
/// @returns a stack-allocated `RsValue` of type `RsValueType_Undef`
#[unsafe(no_mangle)]
pub extern "C" fn RsValue_Undefined() -> RsValue {
    RsValue::undefined()
}

/// Creates a stack-allocated `RsValue` containing a number.
/// The returned value is not allocated on the heap and should not be freed.
/// @param n The numeric value to wrap
/// @return A stack-allocated `RsValue` of type `RsValueType_Number`
#[unsafe(no_mangle)]
pub extern "C" fn RsValue_Number(n: c_double) -> RsValue {
    RsValue::number(n)
}

/// Creates a stack-allocated `RsValue` containing a malloc'd string.
/// The returned value itself is not heap-allocated, but does take ownership of the string.
///
/// # Safety
/// - The passed string pointer must point to a valid C string that
///   was allocated using `rm_malloc`
/// - The passed length must match the length to the string.
///
/// @param str The malloc'd string to wrap (ownership is transferred)
/// @param len The length of the string
/// @return A stack-allocated `RsValue` of type `RsValueType_String` with `RSString_Malloc` subtype
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RsValue_String(str: Option<NonNull<c_char>>, len: u32) -> RsValue {
    todo!()
}

/// Returns a pointer to a statically allocated NULL `RsValue`.
/// This is a singleton - the same pointer is always returned.
/// DO NOT free or modify this value.
/// @return A pointer to a static `RsValue` of type `RsValueType_Null`
#[unsafe(no_mangle)]
pub extern "C" fn RsValue_NullStatic() -> &'static RsValue {
    static RSVALUE_NULL: RsValue = RsValue::null();
    &RSVALUE_NULL
}

/// Get the type of an `RsValue`.
/// @param v The value to inspect
/// @return The `RsValueType` of the value
#[unsafe(no_mangle)]
pub extern "C" fn RsValue_Type(v: &RsValue) -> RsValueType {
    v.as_value_type()
}
