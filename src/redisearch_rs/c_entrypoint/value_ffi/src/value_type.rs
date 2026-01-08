/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::mem::ManuallyDrop;
use value::{RsValue, shared::SharedRsValue};

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

impl RsValueType {
    pub fn for_value(value: &RsValue) -> Self {
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
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Type(value: *const RsValue) -> RsValueType {
    let shared_value = unsafe { SharedRsValue::from_raw(value) };
    let shared_value = ManuallyDrop::new(shared_value);
    let value = shared_value.value();

    RsValueType::for_value(value)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_IsReference(value: *const RsValue) -> bool {
    if value.is_null() {
        return false;
    }

    let shared_value = unsafe { SharedRsValue::from_raw(value) };
    let shared_value = ManuallyDrop::new(shared_value);
    matches!(shared_value.value(), RsValue::Ref(_))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_IsNumber(value: *const RsValue) -> bool {
    if value.is_null() {
        return false;
    }

    let shared_value = unsafe { SharedRsValue::from_raw(value) };
    let shared_value = ManuallyDrop::new(shared_value);
    matches!(shared_value.value(), RsValue::Number(_))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_IsString(value: *const RsValue) -> bool {
    let shared_value = unsafe { SharedRsValue::from_raw(value) };
    if value.is_null() {
        return false;
    }

    let shared_value = ManuallyDrop::new(shared_value);
    matches!(
        shared_value.value(),
        RsValue::RmAllocString(_)
            | RsValue::ConstString(_)
            | RsValue::RedisString(_)
            | RsValue::String(_)
    )
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_IsArray(value: *const RsValue) -> bool {
    if value.is_null() {
        return false;
    }

    let shared_value = unsafe { SharedRsValue::from_raw(value) };
    let shared_value = ManuallyDrop::new(shared_value);
    matches!(shared_value.value(), RsValue::Array(_))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_IsTrio(value: *const RsValue) -> bool {
    if value.is_null() {
        return false;
    }

    let shared_value = unsafe { SharedRsValue::from_raw(value) };
    let shared_value = ManuallyDrop::new(shared_value);
    matches!(shared_value.value(), RsValue::Trio(_))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_IsNull(value: *const RsValue) -> bool {
    if value.is_null() {
        return true;
    }

    let shared_value = unsafe { SharedRsValue::from_raw(value) };
    let shared_value = ManuallyDrop::new(shared_value);
    let value = shared_value.value();
    matches!(value, RsValue::Null)
}
