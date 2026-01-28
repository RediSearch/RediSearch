use crate::util::expect_shared_value;
use std::ffi::c_double;
use value::RsValue;

/// Converts an [`RsValue`] to a number type in-place.
///
/// This clears the existing value and sets it to Number with the given value.
///
/// # Safety
///
/// 1. `value` must point to a valid **owned** [`RsValue`] obtained from an
///    `RSValue_*` function returning an owned [`RsValue`] object.
/// 2. Only 1 reference is allowed to exist pointing to this [`RsValue`] object.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_SetNumber(value: *mut RsValue, n: c_double) {
    // Safety: ensured by caller (1.)
    let mut shared_value = unsafe { expect_shared_value(value) };

    let new_value = RsValue::Number(n);
    // Safety: ensured by caller (2.)
    unsafe { shared_value.set_value(new_value) };
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
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_SetNull(value: *mut RsValue) {
    // Safety: ensured by caller (1.)
    let mut shared_value = unsafe { expect_shared_value(value) };

    // Safety: ensured by caller (2.)
    unsafe { shared_value.set_value(RsValue::Null) };
}
