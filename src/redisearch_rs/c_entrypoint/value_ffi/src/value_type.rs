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
/// `RsValue` or a `SharedRsValue` can be of.
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

pub trait AsRsValueType {
    fn as_value_type(&self) -> RsValueType;
}

impl AsRsValueType for RsValue {
    fn as_value_type(&self) -> RsValueType {
        use RsValueType::*;
        match self {
            RsValue::Undefined => Undef,
            RsValue::Null => Null,
            RsValue::Number(_) => Number,
            RsValue::RmAllocString(_) => String,
            RsValue::ConstString(_) => String,
            RsValue::OwnedRedisString(_) => RedisString,
            RsValue::BorrowedRedisString(_) => RedisString,
            RsValue::String(_) => String,
            RsValue::Array(_) => Array,
            RsValue::Ref(_) => Reference,
            RsValue::Trio(_) => Trio,
            RsValue::Map(_) => Map,
        }
    }
}

impl AsRsValueType for SharedRsValue {
    fn as_value_type(&self) -> RsValueType {
        self.value().as_value_type()
    }
}

/// Get the type of an `RsValue`.
///
/// # Safety
/// The passed value must originate from one of the `RsValue` constructors,
/// i.e. [`RsValue_Undefined`], [`RsValue_Number`], [`RsValue_String`],
/// or [`RsValue_NullStatic`].
///
/// @param v The value to inspect
/// @return The `RsValueType` of the value
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Type(v: *const RsValue) -> RsValueType {
    // Safety:
    // The caller must guarantee that `v` originates from one of the RsValue constructors.
    let shared_value = unsafe { SharedRsValue::from_raw(v) };
    let shared_value = ManuallyDrop::new(shared_value);
    shared_value.as_value_type()
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_IsReference(value: *const RsValue) -> bool {
    unimplemented!("RSValue_IsReference")
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_IsNumber(value: *const RsValue) -> bool {
    unimplemented!("RSValue_IsNumber")
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_IsString(value: *const RsValue) -> bool {
    unimplemented!("RSValue_IsString")
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_IsArray(value: *const RsValue) -> bool {
    unimplemented!("RSValue_IsArray")
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_IsTrio(value: *const RsValue) -> bool {
    unimplemented!("RSValue_IsTrio")
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_IsNull(value: *const RsValue) -> bool {
    unimplemented!("RSValue_IsNull")
}
