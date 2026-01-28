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
        panic!("not a number")
    }
}

/// Gets the left value of a trio [`RsValue`].
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
        panic!("Expected trio")
    }
}

/// Gets the middle value of a trio [`RsValue`].
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
        panic!("Expected trio")
    }
}

/// Gets the right value of a trio [`RsValue`].
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
        panic!("Expected trio")
    }
}
