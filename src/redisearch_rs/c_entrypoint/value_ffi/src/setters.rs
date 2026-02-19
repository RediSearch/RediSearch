/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::util::expect_shared_value;
use std::ffi::{c_char, c_double};
use value::{RsString, RsValue};

/// Converts an [`RsValue`] to a number type in-place.
///
/// This clears the existing value and sets it to Number with the given value.
///
/// # Safety
///
/// 1. `value` must point to a valid **owned** [`RsValue`] obtained from an
///    `RSValue_*` function returning an owned [`RsValue`] object.
/// 2. Only 1 reference is allowed to exist pointing to this [`RsValue`] object.
///
/// # Panic
///
/// Panics if more than 1 reference exists to this [`RsValue`] object.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_SetNumber(value: *mut RsValue, n: c_double) {
    // Safety: ensured by caller (1.)
    let mut shared_value = unsafe { expect_shared_value(value) };

    // Safety: ensured by caller (2.)
    shared_value.set_value(RsValue::Number(n));
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
///
/// # Panic
///
/// Panics if more than 1 reference exists to this [`RsValue`] object.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_SetNull(value: *mut RsValue) {
    // Safety: ensured by caller (1.)
    let mut shared_value = unsafe { expect_shared_value(value) };

    // Safety: ensured by caller (2.)
    shared_value.set_value(RsValue::Null);
}

/// Converts an [`RsValue`] to a string type in-place, taking ownership of the given
/// `RedisModule_Alloc`-allocated buffer.
///
/// This clears the existing value and sets it to an [`RsString`] of kind `RmAlloc`
/// with the given buffer.
///
/// # Safety
///
/// 1. `value` must point to a valid **owned** [`RsValue`] obtained from an
///    `RSValue_*` function returning an owned [`RsValue`] object.
/// 2. `str` must be a [valid], non-null pointer to a buffer allocated by `RedisModule_Alloc`.
/// 3. `str` must be [valid] for reads of `len` bytes.
/// 4. `str` **must not** be used or freed after this function is called, as this function
///    takes ownership of the allocation.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
///
/// # Panic
///
/// Panics if more than 1 reference exists to this [`RsValue`] object.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_SetString(value: *mut RsValue, str: *mut c_char, len: u32) {
    // Safety: ensured by caller (1.)
    let mut shared_value = unsafe { expect_shared_value(value) };

    // Safety: ensured by caller (2., 3., 4.)
    let string = unsafe { RsString::rm_alloc_string(str, len) };
    let value = RsValue::String(string);
    // Safety: ensured by caller (5.)
    shared_value.set_value(value);
}

/// Converts an [`RsValue`] to a string type in-place, borrowing the given string buffer
/// without taking ownership.
///
/// This clears the existing value and sets it to an [`RsString`] of kind `Const`
/// with the given buffer.
///
/// # Safety
///
/// 1. `value` must point to a valid **owned** [`RsValue`] obtained from an
///    `RSValue_*` function returning an owned [`RsValue`] object.
/// 2. `str` must be a [valid], non-null pointer to a string buffer.
/// 3. `str` must be [valid] for reads of `len` bytes.
/// 4. The memory pointed to by `str` must remain valid and not be mutated for the entire
///    lifetime of the [`RsValue`] and any clones of it.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
///
/// # Panic
///
/// Panics if more than 1 reference exists to this [`RsValue`] object.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_SetConstString(value: *mut RsValue, str: *const c_char, len: u32) {
    // Safety: ensured by caller (1.)
    let mut shared_value = unsafe { expect_shared_value(value) };

    // Safety: ensured by caller (2., 3., 4.)
    let string = unsafe { RsString::borrowed_string(str, len) };
    let value = RsValue::String(string);
    // Safety: ensured by caller (5.)
    shared_value.set_value(value);
}
