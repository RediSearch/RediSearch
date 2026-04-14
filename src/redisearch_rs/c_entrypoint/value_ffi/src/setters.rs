/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::RSValue;
use crate::util::expect_shared_value;
use std::ffi::{c_char, c_double};
use value::{String, Value};

/// Converts an [`RSValue`] to a number type in-place.
///
/// This clears the existing value and sets it to Number with the given value.
///
/// # Panic
///
/// Panics if more than 1 reference exists to this [`RSValue`] object.
///
/// # Safety
///
/// 1. `value` must be a [valid], non-null pointer to an [`RSValue`].
/// 2. `value` **must not** be used or freed after this call, as this function takes ownership.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_SetNumber(value: *mut RSValue, n: c_double) {
    // Safety: ensured by caller (1., 2.)
    let mut shared_value = unsafe { expect_shared_value(value) };

    // Panics if more than 1 reference exists.
    shared_value.set_value(Value::Number(n));
}

/// Converts an [`RSValue`] to null type in-place.
///
/// This clears the existing value and sets it to Null.
///
/// # Panic
///
/// Panics if more than 1 reference exists to this [`RSValue`] object.
///
/// # Safety
///
/// 1. `value` must be a [valid], non-null pointer to an [`RSValue`].
/// 2. `value` **must not** be used or freed after this call, as this function takes ownership.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_SetNull(value: *mut RSValue) {
    // Safety: ensured by caller (1., 2.)
    let mut shared_value = unsafe { expect_shared_value(value) };

    // Panics if more than 1 reference exists.
    shared_value.set_value(Value::Null);
}

/// Converts an [`RSValue`] to a string type in-place, taking ownership of the given
/// `RedisModule_Alloc`-allocated buffer.
///
/// This clears the existing value and sets it to a [`String`] of kind `RedisModuleAlloc`
/// with the given buffer.
///
/// # Panic
///
/// Panics if more than 1 reference exists to this [`RSValue`] object.
///
/// # Safety
///
/// 1. `value` must be a [valid], non-null pointer to an [`RSValue`].
/// 2. `value` **must not** be used or freed after this call, as this function takes ownership.
/// 3. `str` must be a [valid], non-null pointer to a buffer of `len+1` bytes
///    allocated by `RedisModule_Alloc`.
/// 4. A nul-terminator is expected in memory at `str+len`.
/// 5. The size determined by `len` excludes the nul-terminator.
/// 6. `str` **must not** be used or freed after this function is called, as this function
///    takes ownership of the allocation.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_SetString(value: *mut RSValue, str: *mut c_char, len: u32) {
    // Safety: ensured by caller (1., 2.)
    let mut shared_value = unsafe { expect_shared_value(value) };

    // Safety: ensured by caller (3., 4., 5., 6.)
    let string = unsafe { String::rm_alloc_string(str, len) };
    let value = Value::String(string);

    // Panics if more than 1 reference exists.
    shared_value.set_value(value);
}

/// Converts an [`RSValue`] to a string type in-place, borrowing the given string buffer
/// without taking ownership.
///
/// This clears the existing value and sets it to a [`String`] of kind `Borrowed`
/// with the given buffer.
///
/// # Panic
///
/// Panics if more than 1 reference exists to this [`RSValue`] object.
///
/// # Safety
///
/// 1. `value` must be a [valid], non-null pointer to an [`RSValue`].
/// 2. `value` **must not** be used or freed after this call, as this function takes ownership.
/// 3. `str` must be a [valid], non-null pointer to a buffer of `len+1` bytes.
/// 4. A nul-terminator is expected in memory at `str+len`.
/// 5. The size determined by `len` excludes the nul-terminator.
/// 6. The memory pointed to by `str` must remain valid and not be mutated for the entire
///    lifetime of the returned [`RSValue`] and any clones of it.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_SetConstString(value: *mut RSValue, str: *const c_char, len: u32) {
    // Safety: ensured by caller (1., 2.)
    let mut shared_value = unsafe { expect_shared_value(value) };

    // Safety: ensured by caller (3., 4., 5., 6.)
    let string = unsafe { String::borrowed_string(str, len) };
    let value = Value::String(string);

    // Panics if more than 1 reference exists.
    shared_value.set_value(value);
}
