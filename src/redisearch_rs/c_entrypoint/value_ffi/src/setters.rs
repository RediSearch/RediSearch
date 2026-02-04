/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::util::expect_shared_value;
use libc::size_t;
use std::ffi::{c_char, c_double};
use value::{
    RsValue,
    strings::{ConstString, RmAllocString},
};

/// Converts an [`RsValue`] to a number type in-place.
///
/// This clears the existing value and sets it to Number with the given value.
///
/// # Safety
///
/// 1. `value` must point to a valid **owned** [`RsValue`] obtained from an
///    `RSValue_*` function returning an owned [`RsValue`] object.
/// 2. Only 1 reference is allowed to exist pointing to this [`RsValue`] object.
///
/// # Panic
///
/// Panics if more than 1 reference exists to this [`RsValue`] object.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_SetNumber(value: *mut RsValue, n: c_double) {
    // Safety: ensured by caller (1.)
    let mut shared_value = unsafe { expect_shared_value(value) };

    // Safety: ensured by caller (2.)
    shared_value.set_value(RsValue::Number(n));
}

/// Converts an [`RsValue`] to null type in-place.
///
/// This clears the existing value and sets it to Null.
///
/// # Safety
///
/// 1. `value` must point to a valid **owned** [`RsValue`] obtained from an
///    `RSValue_*` function returning an owned [`RsValue`] object.
/// 2. Only 1 reference is allowed to exist pointing to this [`RsValue`] object.
///
/// # Panic
///
/// Panics if more than 1 reference exists to this [`RsValue`] object.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_SetNull(value: *mut RsValue) {
    // Safety: ensured by caller (1.)
    let mut shared_value = unsafe { expect_shared_value(value) };

    // Safety: ensured by caller (2.)
    shared_value.set_value(RsValue::Null);
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_SetString(value: *const RsValue, str: *mut c_char, len: size_t) {
    let mut shared_value = unsafe { expect_shared_value(value) };

    let rm_alloc_string = unsafe { RmAllocString::from_raw(str, len as u32) };
    let value = RsValue::RmAllocString(rm_alloc_string);
    shared_value.set_value(value);
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_SetConstString(
    value: *const RsValue,
    str: *const c_char,
    len: size_t,
) {
    let mut shared_value = unsafe { expect_shared_value(value) };

    let const_string = unsafe { ConstString::from_raw(str, len as u32) };
    let value = RsValue::ConstString(const_string);
    shared_value.set_value(value);
}
