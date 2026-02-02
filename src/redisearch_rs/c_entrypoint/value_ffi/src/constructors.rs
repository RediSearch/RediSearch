/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ffi::c_double;
use value::trio::RsValueTrio;
use value::{RsValue, shared::SharedRsValue};

/// Creates and returns a new **owned** [`RsValue`] object of type undefined.
#[unsafe(no_mangle)]
pub extern "C" fn RSValue_NewUndefined() -> *mut RsValue {
    SharedRsValue::new(RsValue::Undefined).into_raw() as *mut _
}

/// Creates and returns a new **owned** [`RsValue`] object of type null.
#[unsafe(no_mangle)]
pub extern "C" fn RSValue_NewNull() -> *mut RsValue {
    SharedRsValue::new(RsValue::Null).into_raw() as *mut _
}

/// Creates and returns a new **owned** [`RsValue`] object of type number
/// containing the given numeric value.
#[unsafe(no_mangle)]
pub extern "C" fn RSValue_NewNumber(value: c_double) -> *mut RsValue {
    SharedRsValue::new(RsValue::Number(value)).into_raw() as *mut _
}

/// Creates and returns a new **owned** [`RsValue`] object of type trio from three [`RsValue`]s.
///
/// Takes ownership of all three values.
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
