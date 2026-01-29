/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::util::expect_value;
use std::ffi::c_double;
use value::RsValue;

/// Gets the numeric value from an [`RsValue`].
///
/// # Safety
///
/// 1. `value` must point to a valid [`RsValue`] obtained from an `RSValue_*` function.
///
/// # Panic
///
/// Panics if the value is not a number type.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Number_Get(value: *const RsValue) -> c_double {
    // Safety: ensured by caller (1.)
    let value = unsafe { expect_value(value) };

    if let RsValue::Number(number) = value {
        *number
    } else {
        panic!("Expected a number value")
    }
}

/// Gets the left value of a trio [`RsValue`].
///
/// Returns a **non-owned** reference to the underlying [`RsValue`].
/// It may only be used in `RSValue_` methods not requiring **owned** methods.
///
/// # Safety
///
/// 1. `value` must point to a valid [`RsValue`] obtained from an `RSValue_*` function.
///
/// # Panic
///
/// Panics if the value is not a trio type.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Trio_GetLeft(value: *const RsValue) -> *const RsValue {
    // Safety: ensured by caller (1.)
    let value = unsafe { expect_value(value) };

    if let RsValue::Trio(trio) = value {
        trio.left().as_ptr()
    } else {
        panic!("Expected a trio value")
    }
}

/// Gets the middle value of a trio [`RsValue`].
///
/// Returns a **non-owned** reference to the underlying [`RsValue`].
/// It may only be used in `RSValue_` methods not requiring **owned** methods.
///
/// # Safety
///
/// 1. `value` must point to a valid [`RsValue`] obtained from an `RSValue_*` function.
///
/// # Panic
///
/// Panics if the value is not a trio type.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Trio_GetMiddle(value: *const RsValue) -> *const RsValue {
    // Safety: ensured by caller (1.)
    let value = unsafe { expect_value(value) };

    if let RsValue::Trio(trio) = value {
        trio.middle().as_ptr()
    } else {
        panic!("Expected a trio value")
    }
}

/// Gets the right value of a trio [`RsValue`].
///
/// Returns a **non-owned** reference to the underlying [`RsValue`].
/// It may only be used in `RSValue_` methods not requiring **owned** methods.
///
/// # Safety
///
/// 1. `value` must point to a valid [`RsValue`] obtained from an `RSValue_*` function.
///
/// # Panic
///
/// Panics if the value is not a trio type.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Trio_GetRight(value: *const RsValue) -> *const RsValue {
    // Safety: ensured by caller (1.)
    let value = unsafe { expect_value(value) };

    if let RsValue::Trio(trio) = value {
        trio.right().as_ptr()
    } else {
        panic!("Expected a trio value")
    }
}
