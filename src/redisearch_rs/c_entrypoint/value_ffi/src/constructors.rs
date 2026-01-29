/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ffi::{c_char, c_double};
use std::ptr::copy_nonoverlapping;

use ffi::{RedisModule_Alloc, RedisModuleString};
use value::strings::{ConstString, RedisString, RmAllocString};
use value::trio::RsValueTrio;
use value::{RsValue, shared::SharedRsValue};

/// Creates and returns a new **owned** [`RsValue`] object of type undefined.
///
/// The caller must make sure to pass the returned [`RsValue`] to one of the
/// ownership taking `RSValue_` methods, directly or indirectly.
#[unsafe(no_mangle)]
pub extern "C" fn RSValue_NewUndefined() -> *mut RsValue {
    SharedRsValue::new(RsValue::Undefined).into_raw() as *mut _
}

/// Creates and returns a new **owned** [`RsValue`] object of type null.
///
/// The caller must make sure to pass the returned [`RsValue`] to one of the
/// ownership taking `RSValue_` methods, directly or indirectly.
#[unsafe(no_mangle)]
pub extern "C" fn RSValue_NewNull() -> *mut RsValue {
    SharedRsValue::new(RsValue::Null).into_raw() as *mut _
}

/// Creates and returns a new **owned** [`RsValue`] object of type number
/// containing the given numeric value.
///
/// The caller must make sure to pass the returned [`RsValue`] to one of the
/// ownership taking `RSValue_` methods, directly or indirectly.
#[unsafe(no_mangle)]
pub extern "C" fn RSValue_NewNumber(value: c_double) -> *mut RsValue {
    SharedRsValue::new(RsValue::Number(value)).into_raw() as *mut _
}

/// Creates and returns a new **owned** [`RsValue`] object of type trio from three [`RsValue`]s.
///
/// Takes ownership of all three arguments.
///
/// The caller must make sure to pass the returned [`RsValue`] to one of the
/// ownership taking `RSValue_` methods, directly or indirectly.
///
/// # Safety
///
/// 1. All three arguments must point to a valid **owned** [`RsValue`] obtained from an
///    `RSValue_*` function returning an owned [`RsValue`] object.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_NewTrio(
    left: *mut RsValue,
    middle: *mut RsValue,
    right: *mut RsValue,
) -> *mut RsValue {
    // Safety: ensured by caller (1.)
    let shared_left = unsafe { SharedRsValue::from_raw(left) };
    // Safety: ensured by caller (1.)
    let shared_middle = unsafe { SharedRsValue::from_raw(middle) };
    // Safety: ensured by caller (1.)
    let shared_right = unsafe { SharedRsValue::from_raw(right) };

    SharedRsValue::new(RsValue::Trio(RsValueTrio::new(
        shared_left,
        shared_middle,
        shared_right,
    )))
    .into_raw() as *mut _
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_NewString(str: *mut c_char, len: u32) -> *mut RsValue {
    let string = unsafe { RmAllocString::from_raw(str, len) };
    let value = RsValue::RmAllocString(string);
    let shared_value = SharedRsValue::new(value);
    shared_value.into_raw() as *mut _
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_NewConstString(str: *const c_char, len: u32) -> *mut RsValue {
    let string = unsafe { ConstString::from_raw(str, len) };
    let value = RsValue::ConstString(string);
    let shared_value = SharedRsValue::new(value);
    shared_value.into_raw() as *mut _
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_NewRedisString(str: *const RedisModuleString) -> *mut RsValue {
    let redis_string = unsafe { RedisString::from_raw(str) };
    let value = RsValue::RedisString(redis_string);
    let shared_value = SharedRsValue::new(value);
    shared_value.into_raw() as *mut _
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_NewCopiedString(str: *const c_char, len: u32) -> *mut RsValue {
    let rm_alloc = unsafe { RedisModule_Alloc.expect("Redis allocator not available") };
    let buf = unsafe { rm_alloc((len + 1) as usize) } as *mut c_char;
    unsafe { copy_nonoverlapping(str, buf, len as usize) };
    unsafe { buf.add(len as usize).write(0) };
    let string = unsafe { RmAllocString::from_raw(buf, len) };
    let value = RsValue::RmAllocString(string);
    let shared_value = SharedRsValue::new(value);
    shared_value.into_raw() as *mut _
}
