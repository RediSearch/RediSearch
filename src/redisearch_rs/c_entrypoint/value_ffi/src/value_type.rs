/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use value::{SharedValueRef, Value};

/// Enumeration of the types an [`RSValue`] can be of.
///
/// cbindgen:prefix-with-name
#[repr(C)]
#[derive(Debug)]
pub enum RSValueType {
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

/// Returns the type of the given [`RSValue`].
///
/// # Safety
///
/// 1. `value` must point to a valid [`RSValue`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Type(value: SharedValueRef) -> RSValueType {
    use RSValueType::*;

    match &**value {
        Value::Undefined => Undef,
        Value::Null => Null,
        Value::Number(_) => Number,
        Value::String(_) => String,
        Value::RedisString(_) => RedisString,
        Value::Array(_) => Array,
        Value::Ref(_) => Reference,
        Value::Trio(_) => Trio,
        Value::Map(_) => Map,
    }
}

/// Returns whether the given [`RSValue`] is a reference type, or `false` if `value` is NULL.
///
/// # Safety
///
/// 1. If `value` is non-null, it must point to a valid [`RSValue`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_IsReference(value: Option<SharedValueRef>) -> bool {
    // Safety: ensured by caller (1.)
    let Some(value) = value else {
        return false;
    };

    matches!(**value, Value::Ref(_))
}

/// Returns whether the given [`RSValue`] is a number type, or `false` if `value` is NULL.
///
/// # Safety
///
/// 1. If `value` is non-null, it must point to a valid [`RSValue`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_IsNumber(value: Option<SharedValueRef>) -> bool {
    // Safety: ensured by caller (1.)
    let Some(value) = value else {
        return false;
    };

    matches!(&**value, Value::Number(_))
}

/// Returns whether the given [`RSValue`] is a string type (any string variant), or `false` if `value` is NULL.
///
/// # Safety
///
/// 1. If `value` is non-null, it must point to a valid [`RSValue`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_IsString(value: Option<SharedValueRef>) -> bool {
    // Safety: ensured by caller (1.)
    let Some(value) = value else {
        return false;
    };

    matches!(&**value, Value::String(_) | Value::RedisString(_))
}

/// Returns whether the given [`RSValue`] is an array type, or `false` if `value` is NULL.
///
/// # Safety
///
/// 1. If `value` is non-null, it must point to a valid [`RSValue`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_IsArray(value: Option<SharedValueRef>) -> bool {
    // Safety: ensured by caller (1.)
    let Some(value) = value else {
        return false;
    };

    matches!(&**value, Value::Array(_))
}

/// Returns whether the given [`RSValue`] is a trio type, or `false` if `value` is NULL.
///
/// # Safety
///
/// 1. If `value` is non-null, it must point to a valid [`RSValue`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_IsTrio(value: Option<SharedValueRef>) -> bool {
    // Safety: ensured by caller (1.)
    let Some(value) = value else {
        return false;
    };

    matches!(&**value, Value::Trio(_))
}

/// Returns whether the given [`RSValue`] is a null pointer, a null type, or a reference to a null type.
///
/// # Safety
///
/// 1. If `value` is non-null, it must point to a valid [`RSValue`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_IsNull(value: Option<SharedValueRef>) -> bool {
    // Safety: ensured by caller (1.)
    let Some(value) = value else {
        return true;
    };

    // C implementation does a recursive check on reference types.
    let value = value.fully_dereferenced_ref();

    matches!(value, Value::Null)
}
