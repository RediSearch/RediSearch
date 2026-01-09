/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::util::expect_value;
use value::RsValue;

/// Enumeration of the types an `RsValue` can be of for
/// compatibility with the traditional C enum setup.
///
/// cbindgen:prefix-with-name
#[repr(C)]
#[derive(Debug)]
pub enum RsValueType {
    Undef = 0,
    Number = 1,
    String = 2,
    Null = 3,
    RedisString = 4,
    Array = 5,
    Reference = 6,
    Trio = 7,
    Map = 8,
}

/// Get the type of an RSValue.
///
/// @param v The value to inspect
/// @return The RSValueType of the
///
/// # SAFETY
///
/// - `value` must point to a valid `RSValue` returned by
///   one of the `RSValue_` functions and cannot be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Type(value: *const RsValue) -> RsValueType {
    // SAFETY: value points to a valid RsValue object.
    let value = unsafe { expect_value(value) };

    use RsValueType::*;

    match value {
        RsValue::Undefined => Undef,
        RsValue::Null => Null,
        RsValue::Number(_) => Number,
        RsValue::RmAllocString(_) => String,
        RsValue::ConstString(_) => String,
        RsValue::RedisString(_) => RedisString,
        RsValue::String(_) => String,
        RsValue::Array(_) => Array,
        RsValue::Ref(_) => Reference,
        RsValue::Trio(_) => Trio,
        RsValue::Map(_) => Map,
    }
}

/// Check if the RSValue is a reference type.
///
/// @param v The value to check
/// @return true if the value is of type RSValueType_Reference, false otherwise
///
/// # SAFETY
///
/// - `value` is either NULL or must point to a valid
///   `RSValue` returned by one of the `RSValue_` functions.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_IsReference(value: *const RsValue) -> bool {
    // SAFETY: if value is not null, it points to a valid RsValue object.
    let Some(value) = (unsafe { value.as_ref() }) else {
        return false;
    };

    matches!(value, RsValue::Ref(_))
}

/// Check if the RSValue is a number type.
///
/// @param v The value to check
/// @return true if the value is of type RSValueType_Number, false otherwise
///
/// # SAFETY
///
/// - `value` is either NULL or must point to a valid
///   `RSValue` returned by one of the `RSValue_` functions.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_IsNumber(value: *const RsValue) -> bool {
    // SAFETY: if value is not null, it points to a valid RsValue object.
    let Some(value) = (unsafe { value.as_ref() }) else {
        return false;
    };

    matches!(value, RsValue::Number(_))
}

/// Check if the RSValue is of a string type.
///
/// @param v The value to check
/// @return true if the value is of type RSValueType_String or RSValueType_RedisString, false otherwise
///
/// # SAFETY
///
/// - `value` is either NULL or must point to a valid
///   `RSValue` returned by one of the `RSValue_` functions.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_IsString(value: *const RsValue) -> bool {
    // SAFETY: if value is not null, it points to a valid RsValue object.
    let Some(value) = (unsafe { value.as_ref() }) else {
        return false;
    };

    matches!(
        value,
        RsValue::RmAllocString(_)
            | RsValue::ConstString(_)
            | RsValue::RedisString(_)
            | RsValue::String(_)
    )
}

/// Check if the RSValue is an array type.
///
/// @param v The value to check
/// @return true if the value is of type RSValueType_Array, false otherwise
///
/// # SAFETY
///
/// - `value` is either NULL or must point to a valid
///   `RSValue` returned by one of the `RSValue_` functions.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_IsArray(value: *const RsValue) -> bool {
    // SAFETY: if value is not null, it points to a valid RsValue object.
    let Some(value) = (unsafe { value.as_ref() }) else {
        return false;
    };

    matches!(value, RsValue::Array(_))
}

/// Check whether the RSValue is of type RSValueType_Trio.
///
/// @param v The value to check
/// @return true if the value is of type RSValueType_Trio, false otherwise
///
/// # SAFETY
///
/// - `value` is either NULL or must point to a valid
///   `RSValue` returned by one of the `RSValue_` functions.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_IsTrio(value: *const RsValue) -> bool {
    // SAFETY: if value is not null, it points to a valid RsValue object.
    let Some(value) = (unsafe { value.as_ref() }) else {
        return false;
    };

    matches!(value, RsValue::Trio(_))
}

/// Return 1 if the value is NULL, RSValueType_Null or a reference to RSValue_NullStatic
///
/// # SAFETY
///
/// - `value` is either NULL or must point to a valid
///   `RSValue` returned by one of the `RSValue_` functions.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_IsNull(value: *const RsValue) -> bool {
    // SAFETY: if value is not null, it points to a valid RsValue object.
    let Some(value) = (unsafe { value.as_ref() }) else {
        return true;
    };

    let value = value.fully_dereferenced();

    matches!(value, RsValue::Null)
}
