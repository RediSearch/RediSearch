use crate::util::expect_value;
use value::{RsValue, shared::SharedRsValue};

/// Allocates an array of null pointers with space for `len` [`RsValue`] pointers.
///
/// The returned buffer must be populated and then passed to [`RSValue_NewArray`]
/// to produce an array value.
///
/// # SAFETY
///
/// 1. The caller must eventually pass the returned pointer to [`RSValue_NewArray`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_AllocateArray(len: u32) -> *mut *mut RsValue {
    let array: Vec<*mut RsValue> = vec![std::ptr::null_mut(); len as usize];

    Box::into_raw(array.into_boxed_slice()) as *mut _
}

/// Creates a heap-allocated array [`RsValue`] from existing values.
///
/// Takes ownership of the `values` buffer and all [`RsValue`] pointers within it.
/// The values will be freed when the array is freed.
///
/// # SAFETY
///
/// 1. `values` must have been allocated via [`RSValue_AllocateArray`] with
///    a capacity equal to `len`.
/// 2. All `len` entries in `values` must have been filled with valid [`RsValue`] pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_NewArray(values: *mut *mut RsValue, len: u32) -> *mut RsValue {
    // Safety: ensured by caller (1.)
    let array = unsafe { Vec::from_raw_parts(values, len as usize, len as usize) };

    let array = array
        .into_iter()
        // Safety: ensured by caller (2.)
        .map(|val| unsafe { SharedRsValue::from_raw(val) })
        .collect();

    let value = RsValue::Array(array);
    let shared = SharedRsValue::new(value);
    shared.into_raw() as *mut _
}

/// Returns the number of elements in an array [`RsValue`].
///
/// If `value` is not an array, returns `0`.
///
/// # SAFETY
///
/// 1. `value` must point to a valid [`RsValue`] obtained from an `RSValue_*` function.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_ArrayLen(value: *const RsValue) -> u32 {
    // Safety: ensured by caller (1.)
    let value = unsafe { expect_value(value) };

    if let RsValue::Array(array) = value {
        array.len() as u32
    } else {
        0
    }
}

/// Returns a pointer to the element at `index` in an array [`RsValue`].
///
/// If `value` is not an array, returns a null pointer. The returned pointer
/// is borrowed from the array and must not be freed by the caller.
///
/// # SAFETY
///
/// 1. `value` must point to a valid [`RsValue`] obtained from an `RSValue_*` function.
/// 2. `index` must be less than the array length.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_ArrayItem(value: *const RsValue, index: u32) -> *mut RsValue {
    // Safety: ensured by caller (1.)
    let value = unsafe { expect_value(value) };

    if let RsValue::Array(array) = value {
        let shared = &array[index as usize];
        shared.as_ptr() as *mut _
    } else {
        std::ptr::null_mut()
    }
}
