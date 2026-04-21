/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Conversion helpers between the C-facing opaque [`RSValue`] pointer and the
//! Rust-side [`Value`] / [`SharedValue`] types.
//!
//! [`RSValue`] is an opaque shim whose pointers are layout-compatible with
//! [`SharedValue`] (which is `#[repr(transparent)]` over `*const Value`), so
//! conversion is a pointer cast. The helpers in this module encode the
//! ownership intent of each cast: whether the C side is handing off ownership
//! (`into_*`) or lending a reference that must not decrement the refcount
//! (`as_*`, `expect_*`).

use crate::RSValue;
use std::mem::ManuallyDrop;
use value::{SharedValue, Value};

/// Get a reference to a [`Value`] from an [`RSValue`] pointer, or [`None`] if
/// the pointer is null.
///
/// # Safety
///
/// 1. If non-null, `value` must point to a valid [`Value`].
pub const unsafe fn try_value<'a>(value: *const RSValue) -> Option<&'a Value> {
    // SAFETY: ensured by caller (1.)
    unsafe { value.cast::<Value>().as_ref() }
}

/// Get a reference to a [`Value`] from an [`RSValue`] pointer.
///
/// Panics with a descriptive message in debug builds when the value is null and
/// uses [`Option::unwrap_unchecked`] in release builds to avoid the branch on the
/// hot path.
///
/// # Safety
///
/// 1. `value` must point to a valid [`RSValue`].
pub(crate) const unsafe fn expect_value<'a>(value: *const RSValue) -> &'a Value {
    // SAFETY: ensured by caller (1.)
    let value = unsafe { try_value(value) };

    if cfg!(debug_assertions) {
        value.expect("value must not be null")
    } else {
        // SAFETY: ensured by caller (1.)
        unsafe { value.unwrap_unchecked() }
    }
}

/// Get a borrowed [`SharedValue`] from an [`RSValue`] pointer.
///
/// The returned [`SharedValue`] is wrapped in [`ManuallyDrop`] so that
/// dropping it does not decrement the refcount of the underlying allocation;
/// the C side retains ownership of the pointer.
///
/// See [`expect_value`] for the null-check behavior.
///
/// # Safety
///
/// 1. `value` must point to a valid [`RSValue`].
pub(crate) unsafe fn expect_shared_value(value: *const RSValue) -> ManuallyDrop<SharedValue> {
    if cfg!(debug_assertions) && value.is_null() {
        panic!("value must not be null");
    }

    // SAFETY: ensured by caller (1.)
    unsafe { as_shared_value(value) }
}

/// Take ownership of an [`RSValue`] pointer as a [`SharedValue`].
///
/// The returned [`SharedValue`] owns one refcount; dropping it may deallocate
/// the underlying [`Value`]. Use this when the C caller is handing off
/// ownership (e.g. a value returned from an `RSValue_*` constructor being
/// consumed back into Rust).
///
/// # Safety
///
/// 1. `value` must be a valid pointer obtained from [`into_rs_value`] and must
///    not be used again after this call.
pub const unsafe fn into_shared_value(value: *mut RSValue) -> SharedValue {
    // SAFETY: ensured by caller (1.)
    unsafe { SharedValue::from_raw(value.cast_const().cast()) }
}

/// Borrow an [`RSValue`] pointer as a [`SharedValue`] without taking
/// ownership.
///
/// The [`ManuallyDrop`] wrapper prevents the refcount from being decremented
/// when the borrow goes out of scope, so the C side keeps ownership.
///
/// # Safety
///
/// 1. `value` must point to a valid [`RSValue`] and must remain live for as
///    long as the returned borrow is used.
pub const unsafe fn as_shared_value(value: *const RSValue) -> ManuallyDrop<SharedValue> {
    // SAFETY: ensured by caller (1.)
    let shared_value = unsafe { SharedValue::from_raw(value.cast()) };
    ManuallyDrop::new(shared_value)
}

/// Hand off a [`SharedValue`] to the C side as an [`RSValue`] pointer.
///
/// The C side takes over the one refcount held by `value`; it is responsible
/// for eventually returning the pointer via [`into_shared_value`] (or
/// calling `RSValue_Decref`) to release it.
pub const fn into_rs_value(value: SharedValue) -> *mut RSValue {
    value.into_raw().cast_mut().cast()
}

/// Expose a [`SharedValue`] to the C side as a borrowed [`RSValue`] pointer,
/// without transferring ownership.
pub const fn as_rs_value(value: &SharedValue) -> *const RSValue {
    value.as_ptr().cast()
}
