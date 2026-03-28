/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::RSValue;
use std::mem::ManuallyDrop;
use value::{SharedValue, Value};

/// Get a reference to an [`RsValue`] from a pointer.
///
/// Checks for null in debug mode, does an unwrap_unchecked in release mode.
///
/// # Safety
///
/// 1. `value` must point to a valid [`RsValue`] obtained from an `RSValue_*` function.
pub(crate) const unsafe fn expect_value<'a>(value: *const RSValue) -> &'a Value {
    // Safety: ensured by caller (1.)
    let value = unsafe { value.cast::<Value>().as_ref() };

    if cfg!(debug_assertions) {
        value.expect("value must not be null")
    } else {
        // Safety: ensured by caller (1.)
        unsafe { value.unwrap_unchecked() }
    }
}

/// Get a [`SharedRsValue`] from an [`RsValue`] pointer. This `SharedRsValue` is
/// wrapped inside a `ManuallyDrop` so that it behaves as a reference instead of
/// an owned object as to not decrement the refcount and potentially deallocate
/// the underlying `RsValue`.
///
/// Checks for null in debug mode, directly casts to a
/// [`ManuallyDrop<SharedRsValue>`] in release mode.
///
/// # Safety
///
/// 1. `value` must point to a valid [`RsValue`] obtained from an `RSValue_*` function.
pub(crate) unsafe fn expect_shared_value(value: *const RSValue) -> ManuallyDrop<SharedValue> {
    if cfg!(debug_assertions) && value.is_null() {
        panic!("value must not be null");
    }

    // Safety: ensured by caller (1.)
    unsafe { as_shared_value(value) }
}

pub unsafe fn into_shared_value(value: *mut RSValue) -> SharedValue {
    unsafe { SharedValue::from_raw(value.cast_const().cast()) }
}

pub unsafe fn as_shared_value(value: *const RSValue) -> ManuallyDrop<SharedValue> {
    let shared_value = unsafe { SharedValue::from_raw(value.cast()) };
    ManuallyDrop::new(shared_value)
}

pub fn into_rs_value(value: SharedValue) -> *const RSValue {
    value.into_raw().cast()
}

pub fn as_rs_value(value: &SharedValue) -> *const RSValue {
    value.as_ptr().cast()
}
