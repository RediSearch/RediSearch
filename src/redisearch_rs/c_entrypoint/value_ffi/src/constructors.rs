/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use ffi::RedisModuleString;
use std::ffi::{CString, c_char, c_double};
use value::{RedisString, RsString, RsValue, RsValueTrio, SharedRsValue};

/// Creates and returns a new **owned** [`RsValue`] object of type undefined.
///
/// The caller must make sure to pass the returned [`RsValue`] to one of the
/// ownership taking `RSValue_` functions, directly or indirectly.
#[unsafe(no_mangle)]
pub extern "C" fn RSValue_NewUndefined() -> *mut RsValue {
    SharedRsValue::new(RsValue::Undefined).into_raw().cast_mut()
}

/// Creates and returns a new **owned** [`RsValue`] object of type null.
///
/// The caller must make sure to pass the returned [`RsValue`] to one of the
/// ownership taking `RSValue_` functions, directly or indirectly.
#[unsafe(no_mangle)]
pub extern "C" fn RSValue_NewNull() -> *mut RsValue {
    SharedRsValue::new(RsValue::Null).into_raw().cast_mut()
}

/// Creates and returns a new **owned** [`RsValue`] object of type number
/// containing the given numeric value.
///
/// The caller must make sure to pass the returned [`RsValue`] to one of the
/// ownership taking `RSValue_` functions, directly or indirectly.
#[unsafe(no_mangle)]
pub extern "C" fn RSValue_NewNumber(value: c_double) -> *mut RsValue {
    SharedRsValue::new(RsValue::Number(value))
        .into_raw()
        .cast_mut()
}

/// Creates and returns a new **owned** [`RsValue`] object of type trio from three [`RsValue`]s.
///
/// Takes ownership of all three arguments.
///
/// The caller must make sure to pass the returned [`RsValue`] to one of the
/// ownership taking `RSValue_` functions, directly or indirectly.
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
    .into_raw()
    .cast_mut()
}

/// Creates and returns a new **owned** [`RsValue`] object of type string,
/// taking ownership of the given `RedisModule_Alloc`-allocated buffer.
///
/// The caller must make sure to pass the returned [`RsValue`] to one of the
/// ownership taking `RSValue_` functions, directly or indirectly.
///
/// # Safety
///
/// 1. `str` must be a [valid], non-null pointer to a buffer allocated by `RedisModule_Alloc`.
/// 2. `str` must be [valid] for reads of `len` bytes.
/// 3. `str` **must not** be used or freed after this function is called, as this function
///    takes ownership of the allocation.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_NewString(str: *mut c_char, len: u32) -> *mut RsValue {
    // Safety: ensured by caller (1., 2., 3.)
    let string = unsafe { RsString::rm_alloc_string(str, len) };

    let value = RsValue::String(string);
    let shared_value = SharedRsValue::new(value);
    shared_value.into_raw().cast_mut()
}

/// Creates and returns a new **owned** [`RsValue`] object of type string,
/// taking ownership of the given `RedisModule_Alloc`-allocated buffer.
///
/// The caller must make sure to pass the returned [`RsValue`] to one of the
/// ownership taking `RSValue_` methods, directly or indirectly.
///
/// # Safety
///
/// 1. `str` must be a [valid], non-null pointer to a buffer allocated by `RedisModule_Alloc`.
/// 2. `str` must be [valid] for reads of `len` bytes.
/// 3. `str` **must not** be used or freed after this function is called, as this function
///    takes ownership of the allocation.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_NewStringWithoutNulTerminator(
    str: *mut c_char,
    len: u32,
) -> *mut RsValue {
    // Safety: ensured by caller (1., 2., 3.)
    let string = unsafe { RsString::rm_alloc_string_without_nul_terminator(str, len) };
    let value = RsValue::String(string);
    let shared_value = SharedRsValue::new(value);
    shared_value.into_raw() as *mut _
}

/// Creates and returns a new **owned** [`RsValue`] object of type string,
/// borrowing the given string buffer without taking ownership.
///
/// The caller must make sure to pass the returned [`RsValue`] to one of the
/// ownership taking `RSValue_` functions, directly or indirectly.
///
/// # Safety
///
/// 1. `str` must be a [valid], non-null pointer to a string buffer.
/// 2. `str` must be [valid] for reads of `len` bytes.
/// 3. The memory pointed to by `str` must remain valid and not be mutated for the entire
///    lifetime of the returned [`RsValue`] and any clones of it.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_NewBorrowedString(str: *const c_char, len: u32) -> *mut RsValue {
    // Safety: ensured by caller (1., 2., 3.)
    let string = unsafe { RsString::borrowed_string(str, len) };

    let value = RsValue::String(string);
    let shared_value = SharedRsValue::new(value);
    shared_value.into_raw().cast_mut()
}

/// Creates and returns a new **owned** [`RsValue`] object of type string,
/// taking ownership of the given [`RedisModuleString`].
///
/// The caller must make sure to pass the returned [`RsValue`] to one of the
/// ownership taking `RSValue_` functions, directly or indirectly.
///
/// # Safety
///
/// 1. `str` must be a [valid], non-null pointer to a [`RedisModuleString`].
/// 2. `str` **must not** be used or freed after this function is called, as this function
///    takes ownership of the string.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_NewRedisString(str: *mut RedisModuleString) -> *mut RsValue {
    // Safety: ensured by caller (1., 2.)
    let redis_string = unsafe { RedisString::from_raw(str) };

    let value = RsValue::RedisString(redis_string);
    let shared_value = SharedRsValue::new(value);
    shared_value.into_raw().cast_mut()
}

/// Creates and returns a new **owned** [`RsValue`] object of type string,
/// copying `len` bytes from the given string buffer into a new Rust-allocated [`Box<CStr>`].
///
/// The caller retains ownership of `str`.
///
/// The caller must make sure to pass the returned [`RsValue`] to one of the
/// ownership taking `RSValue_` functions, directly or indirectly.
///
/// # Safety
///
/// 1. `str` must be a [valid], non-null pointer to a string buffer.
/// 2. `str` must be [valid] for reads of `len` bytes.
/// 3. The `len` bytes pointed to by `str` must not contain any null bytes.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_NewCopiedString(str: *const c_char, len: u32) -> *mut RsValue {
    // Safety: ensured by caller (1., 2.)
    let slice = unsafe { std::slice::from_raw_parts(str.cast::<u8>(), len as usize) };

    // Safety: ensured by caller (3.)
    let cstring = unsafe { CString::from_vec_unchecked(slice.to_vec()) };

    let string = RsString::cstring(cstring);
    let value = RsValue::String(string);
    let shared_value = SharedRsValue::new(value);
    shared_value.into_raw().cast_mut()
}
