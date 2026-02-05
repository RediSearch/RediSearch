/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::util::expect_value;
use value::{Array, RsValue, shared::SharedRsValue};

/// Allocates an array of null pointers with space for `len` [`RsValue`] pointers.
///
/// The returned buffer must be populated and then passed to [`RSValue_NewArrayFromBuilder`]
/// to produce an array value.
///
/// # SAFETY
///
/// 1. The caller must eventually pass the returned pointer to [`RSValue_NewArrayFromBuilder`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_NewArrayBuilder(len: u32) -> *mut *mut RsValue {
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
/// 1. `values` must have been allocated via [`RSValue_NewArrayBuilder`] with
///    a capacity equal to `len`.
/// 2. All `len` entries in `values` must have been filled with valid [`RsValue`] pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_NewArrayFromBuilder(
    values: *mut *mut RsValue,
    len: u32,
) -> *mut RsValue {
    // Safety: ensured by caller (1.)
    let array = unsafe { Vec::from_raw_parts(values, len as usize, len as usize) };

    let array = array
        .into_iter()
        // Safety: ensured by caller (2.)
        .map(|val| unsafe { SharedRsValue::from_raw(val) })
        .collect();

    let value = RsValue::Array(Array::new(array));
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
        array.len_u32()
    } else {
        // Compatibility: C returns 0 on non array types.
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
        // Compatibility: C does an RS_ASSERT on index out of bounds
        let shared = &array[index as usize];
        shared.as_ptr() as *mut _
    } else {
        // Compatibility: C does an RS_ASSERT on incorrect type
        panic!("Expected 'Array' type, got '{}'", value.variant_name())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::constructors::RSValue_NewNumber;
    use crate::getters::RSValue_Number_Get;
    use crate::shared::RSValue_DecrRef;

    #[unsafe(no_mangle)]
    pub static mut RSDummyContext: *mut ffi::RedisModuleCtx = std::ptr::null_mut();

    #[unsafe(no_mangle)]
    pub unsafe extern "C" fn RedisModule_Free() {
        unimplemented!("RedisModule_Free")
    }

    #[unsafe(no_mangle)]
    pub unsafe extern "C" fn RedisModule_FreeString() {
        unimplemented!("RedisModule_FreeString")
    }

    #[test]
    fn test_array() {
        unsafe {
            let array = RSValue_NewArrayBuilder(3);
            let one = RSValue_NewNumber(1.0);
            let two = RSValue_NewNumber(2.0);
            let three = RSValue_NewNumber(3.0);
            array.add(0).write(one);
            array.add(1).write(two);
            array.add(2).write(three);

            let array = RSValue_NewArrayFromBuilder(array, 3);

            assert_eq!(RSValue_ArrayLen(array), 3);

            let val = RSValue_ArrayItem(array, 0);
            let num = RSValue_Number_Get(val);
            assert_eq!(1.0, num);

            let val = RSValue_ArrayItem(array, 1);
            let num = RSValue_Number_Get(val);
            assert_eq!(2.0, num);

            let val = RSValue_ArrayItem(array, 2);
            let num = RSValue_Number_Get(val);
            assert_eq!(3.0, num);

            RSValue_DecrRef(array);
        }
    }
}
