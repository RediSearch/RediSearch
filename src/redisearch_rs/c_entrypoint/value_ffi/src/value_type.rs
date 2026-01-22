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

/// Enumeration of the types an
/// `RsValue` can be of.
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

#[unsafe(no_mangle)]
pub const unsafe extern "C" fn RSValue_Type(value: *const RsValue) -> RsValueType {
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

#[unsafe(no_mangle)]
pub const unsafe extern "C" fn RSValue_IsReference(value: *const RsValue) -> bool {
    let Some(value) = (unsafe { value.as_ref() }) else {
        return false;
    };

    matches!(value, RsValue::Ref(_))
}

#[unsafe(no_mangle)]
pub const unsafe extern "C" fn RSValue_IsNumber(value: *const RsValue) -> bool {
    let Some(value) = (unsafe { value.as_ref() }) else {
        return false;
    };

    matches!(value, RsValue::Number(_))
}

#[unsafe(no_mangle)]
pub const unsafe extern "C" fn RSValue_IsString(value: *const RsValue) -> bool {
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

#[unsafe(no_mangle)]
pub const unsafe extern "C" fn RSValue_IsArray(value: *const RsValue) -> bool {
    let Some(value) = (unsafe { value.as_ref() }) else {
        return false;
    };

    matches!(value, RsValue::Array(_))
}

#[unsafe(no_mangle)]
pub const unsafe extern "C" fn RSValue_IsTrio(value: *const RsValue) -> bool {
    let Some(value) = (unsafe { value.as_ref() }) else {
        return false;
    };

    matches!(value, RsValue::Trio(_))
}

#[unsafe(no_mangle)]
pub const unsafe extern "C" fn RSValue_IsNull(value: *const RsValue) -> bool {
    let Some(value) = (unsafe { value.as_ref() }) else {
        return true;
    };

    matches!(value, RsValue::Null)
}
