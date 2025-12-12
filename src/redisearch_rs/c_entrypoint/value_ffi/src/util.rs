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

/// Get a reference to an [`RsValue`] from a pointer.
///
/// Checks for null in debug mode, does an unwrap_unchecked in release mode.
///
/// # Safety
///
/// 1. `value` must point to a valid [`RsValue`] obtained from an `RSValue_*` function.
pub(crate) const unsafe fn expect_value<'a>(value: *const RsValue) -> &'a RsValue {
    // Safety: ensured by caller (1.)
    let value = unsafe { value.as_ref() };

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
/// ManuallyDrop<SharedRsValue> in release mode.
///
/// # Safety
///
/// 1. `value` must point to a valid [`RsValue`] obtained from an `RSValue_*` function.
pub(crate) unsafe fn expect_shared_value(value: *const RsValue) -> ManuallyDrop<SharedRsValue> {
    if cfg!(debug_assertions) && value.is_null() {
        panic!("value must not be null");
    }

    // Safety: ensured by caller (1.)
    let shared_value = unsafe { SharedRsValue::from_raw(value) };
    ManuallyDrop::new(shared_value)
}
