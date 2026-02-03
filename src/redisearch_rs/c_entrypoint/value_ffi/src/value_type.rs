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

/// Enumeration of the types an [`RsValue`] can be of.
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

/// Returns the type of the given [`RsValue`].
///
/// # Safety
///
/// 1. `value` must point to a valid [`RsValue`] obtained from an `RSValue_*` function.
#[unsafe(no_mangle)]
pub const unsafe extern "C" fn RSValue_Type(value: *const RsValue) -> RsValueType {
    // Safety: ensured by caller (1.)
    let value = unsafe { expect_value(value) };

    use RsValueType::*;

    match value {
        RsValue::Undefined => Undef,
        RsValue::Null => Null,
        RsValue::Number(_) => Number,
        RsValue::String(_) => String,
        RsValue::RedisString(_) => RedisString,
        RsValue::Array(_) => Array,
        RsValue::Ref(_) => Reference,
        RsValue::Trio(_) => Trio,
        RsValue::Map(_) => Map,
    }
}

/// Returns whether the given [`RsValue`] is a reference type, or `false` if `value` is NULL.
///
/// # Safety
///
/// 1. If `value` is non-null, it must point to a valid [`RsValue`] obtained from an `RSValue_*` function.
#[unsafe(no_mangle)]
pub const unsafe extern "C" fn RSValue_IsReference(value: *const RsValue) -> bool {
    // Safety: ensured by caller (1.)
    let Some(value) = (unsafe { value.as_ref() }) else {
        return false;
    };

    matches!(value, RsValue::Ref(_))
}

/// Returns whether the given [`RsValue`] is a number type, or `false` if `value` is NULL.
///
/// # Safety
///
/// 1. If `value` is non-null, it must point to a valid [`RsValue`] obtained from an `RSValue_*` function.
#[unsafe(no_mangle)]
pub const unsafe extern "C" fn RSValue_IsNumber(value: *const RsValue) -> bool {
    // Safety: ensured by caller (1.)
    let Some(value) = (unsafe { value.as_ref() }) else {
        return false;
    };

    matches!(value, RsValue::Number(_))
}

/// Returns whether the given [`RsValue`] is a string type (any string variant), or `false` if `value` is NULL.
///
/// # Safety
///
/// 1. If `value` is non-null, it must point to a valid [`RsValue`] obtained from an `RSValue_*` function.
#[unsafe(no_mangle)]
pub const unsafe extern "C" fn RSValue_IsString(value: *const RsValue) -> bool {
    // Safety: ensured by caller (1.)
    let Some(value) = (unsafe { value.as_ref() }) else {
        return false;
    };

    matches!(value, RsValue::String(_) | RsValue::RedisString(_))
}

/// Returns whether the given [`RsValue`] is an array type, or `false` if `value` is NULL.
///
/// # Safety
///
/// 1. If `value` is non-null, it must point to a valid [`RsValue`] obtained from an `RSValue_*` function.
#[unsafe(no_mangle)]
pub const unsafe extern "C" fn RSValue_IsArray(value: *const RsValue) -> bool {
    // Safety: ensured by caller (1.)
    let Some(value) = (unsafe { value.as_ref() }) else {
        return false;
    };

    matches!(value, RsValue::Array(_))
}

/// Returns whether the given [`RsValue`] is a trio type, or `false` if `value` is NULL.
///
/// # Safety
///
/// 1. If `value` is non-null, it must point to a valid [`RsValue`] obtained from an `RSValue_*` function.
#[unsafe(no_mangle)]
pub const unsafe extern "C" fn RSValue_IsTrio(value: *const RsValue) -> bool {
    // Safety: ensured by caller (1.)
    let Some(value) = (unsafe { value.as_ref() }) else {
        return false;
    };

    matches!(value, RsValue::Trio(_))
}

/// Returns whether the given [`RsValue`] is a null pointer, a null type, or a reference to a null type.
///
/// # Safety
///
/// 1. If `value` is non-null, it must point to a valid [`RsValue`] obtained from an `RSValue_*` function.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_IsNull(value: *const RsValue) -> bool {
    // Safety: ensured by caller (1.)
    let Some(value) = (unsafe { value.as_ref() }) else {
        return true;
    };

    // C implementation does a recursive check on reference types.
    let value = value.fully_dereferenced();

    matches!(value, RsValue::Null)
}
