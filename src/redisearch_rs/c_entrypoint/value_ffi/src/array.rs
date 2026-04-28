/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{mem::MaybeUninit, ptr};

use crate::util::{as_rs_value, expect_value, into_shared_value};
use crate::{RSValue, util::into_rs_value};
use value::{Array, SharedValue, Value};

/// Allocates an array of null pointers with space for `len` [`RSValue`] pointers.
///
/// The returned buffer must be populated and then passed to [`RSValue_NewArrayFromBuilder`]
/// to produce an array value.
///
/// # Safety
///
/// 1. The caller must eventually pass the returned pointer to [`RSValue_NewArrayFromBuilder`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_NewArrayBuilder(len: u32) -> *mut *mut RSValue {
    let array = Box::new_zeroed_slice(len as usize);

    // Safety: we zero-initialized the slice above. It is therefore correctly initialized with
    // null pointers are required.
    let array = unsafe { Box::<[MaybeUninit<*mut RSValue>]>::assume_init(array) };

    Box::into_raw(array).cast::<*mut RSValue>()
}

/// Creates a heap-allocated array [`RSValue`] from existing values.
///
/// Takes ownership of the `values` buffer and all [`RSValue`] pointers within it.
/// The values will be freed when the array is freed.
///
/// # Safety
///
/// 1. `values` must have been allocated via [`RSValue_NewArrayBuilder`] with
///    a capacity equal to `len`.
/// 2. All `len` entries in `values` must have been filled with valid [`RSValue`] pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_NewArrayFromBuilder(
    values: *mut *mut RSValue,
    len: u32,
) -> *mut RSValue {
    // Safety: ensured by caller (1.)
    let array: Box<[*mut RSValue]> =
        unsafe { Box::from_raw(ptr::slice_from_raw_parts_mut(values, len as usize)) };

    let array = array
        .into_iter()
        // Safety: ensured by caller (2.)
        .map(|val| unsafe { into_shared_value(val) })
        .collect();

    let value = Value::Array(Array::new(array));
    let shared = SharedValue::new(value);
    into_rs_value(shared)
}

/// Returns the number of elements in an array [`RSValue`].
///
/// If `value` is not an array, returns `0`.
///
/// # Safety
///
/// 1. `value` must be a [valid], non-null pointer to an [`RSValue`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_ArrayLen(value: *const RSValue) -> u32 {
    // Safety: ensured by caller (1.)
    let value = unsafe { expect_value(value) };

    if let Value::Array(array) = value {
        array.len_u32()
    } else {
        // Compatibility: C returns 0 on non array types.
        0
    }
}

/// Returns a pointer to the element at `index` in an array [`RSValue`].
///
/// If `value` is not an array, returns a null pointer. The returned pointer
/// is borrowed from the array and must not be freed by the caller.
///
/// # Safety
///
/// 1. `value` must be a [valid], non-null pointer to an [`RSValue`].
///
/// # Panics
///
/// Panics if `index` greater than or equal to the array length.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_ArrayItem(value: *const RSValue, index: u32) -> *mut RSValue {
    // Safety: ensured by caller (1.)
    let value = unsafe { expect_value(value) };

    if let Value::Array(array) = value {
        // Compatibility: C does an RS_ASSERT on index out of bounds
        let shared = &array[index as usize];
        as_rs_value(shared).cast_mut()
    } else {
        // Compatibility: C does an RS_ASSERT on incorrect type
        panic!("Expected 'Array' type, got '{}'", value.variant_name())
    }
}
