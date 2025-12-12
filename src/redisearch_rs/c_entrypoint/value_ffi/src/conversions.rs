/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::util::{expect_shared_value, expect_value};
use libc::size_t;
use std::ffi::{CString, c_char, c_double, c_int};
use value::util::{num_to_str, str_to_float};
use value::{RsString, RsValue, SharedRsValue};

/// Convert a value to a number, either returning the actual numeric values or by parsing
/// a string into a number. Return 1 if the value is a number or a numeric string that can
/// be converted, or 0 if not. The converted number is written to the `d` pointer.
///
/// # Safety
///
/// 1. `value` must be either null or point to a valid [`RsValue`] obtained from
///    an `RSValue_*` function.
/// 2. `d` must be a [valid], non-null pointer to a `c_double`.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_ToNumber(value: *const RsValue, d: *mut c_double) -> c_int {
    // Safety: ensured by caller (1.)
    let Some(value) = (unsafe { value.as_ref() }) else {
        return 0;
    };

    // Safety: ensured by caller (2.)
    let d = unsafe { d.as_mut().expect("d is null") };

    let value = value.fully_dereferenced_ref_and_trio();

    let num = match value {
        RsValue::Number(n) => Some(*n),
        RsValue::String(string) => str_to_float(string.as_bytes()),
        RsValue::RedisString(string) => str_to_float(string.as_bytes()),
        _ => return 0,
    };

    if let Some(num) = num {
        *d = num;
        return 1;
    } else {
        *d = 0.0;
        return 0;
    }
}

/// Converts an [`RsValue`] to a string pointer with length, writing into `buf`
/// when a numeric conversion is needed.
///
/// # Safety
///
/// 1. `value` must point to a valid [`RsValue`] obtained from an `RSValue_*` function.
/// 2. `len_ptr` must be a [valid], non-null pointer to a `size_t`.
/// 3. `buf` must be a [valid] pointer to a writable buffer of at least `buflen` bytes.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_ConvertStringPtrLen(
    value: *const RsValue,
    len_ptr: *mut size_t,
    buf: *mut c_char,
    buflen: size_t,
) -> *const c_char {
    // Safety: ensured by caller (1.)
    let value = unsafe { expect_value(value) };
    // Safety: ensured by caller (2.)
    let len_ptr = unsafe { len_ptr.as_mut().expect("len_ptr is null") };

    let value = value.fully_dereferenced_ref();

    let (ptr, len): (*const c_char, size_t) = match value {
        RsValue::Number(num) => {
            // Safety: ensured by caller (3.)
            let buffer = unsafe { std::slice::from_raw_parts_mut(buf as *mut u8, buflen as usize) };
            let n = num_to_str(*num, buffer);

            if n < buflen {
                (buf as *const _, n)
            } else {
                (b"\0".as_ptr() as *const _, 0)
            }
        }
        RsValue::String(string) => {
            let (ptr, len) = string.as_ptr_len_for_slice();
            (ptr, len as usize)
        }
        RsValue::RedisString(string) => string.as_ptr_len(),
        _ => (b"\0".as_ptr() as *const _, 0),
    };

    *len_ptr = len;
    ptr
}

/// Converts an [`RsValue`] to a string and stores the result in `dst`.
///
/// Automatically dereferences [`RsValue::Ref`] and [`RsValue::Trio`] types.
///
/// - When `value` is an [`RsValue::String`], `dst` becomes a references to `value`.
/// - When `value` is an [`RsValue::RedisString`], the content of the redis string is
///   made available as a `RsString::borrowed_string` and put in `dst`.
/// - When `value` is an [`RsValue::Number`], it is converted into a string and put in `dst`.
/// - Else, `dst` is set to point to an empty string.
///
/// # Safety
///
/// 1. `dst` must point to a valid **owned** [`RsValue`] obtained from an
///    `RSValue_*` function returning an owned [`RsValue`] object.
/// 2. `value` must point to a valid [`RsValue`] obtained from an `RSValue_*` function.
///
/// # Panic
///
/// Panics if more than 1 reference exists to the `dst` [`RsValue`] object.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_ToString(dst: *mut RsValue, value: *const RsValue) {
    // Safety: ensured by caller (1.)
    let mut dst = unsafe { expect_shared_value(dst) };

    // Safety: ensured by caller (2.)
    let value = unsafe { expect_value(value) };
    let value = value.fully_dereferenced_ref_and_trio();

    let new_value = match value {
        RsValue::String(_) => {
            // Safety: ensured by caller (1.)
            let shared_value = unsafe { expect_shared_value(value) };
            RsValue::Ref(SharedRsValue::clone(&shared_value))
        }
        RsValue::RedisString(string) => {
            let (ptr, len) = string.as_ptr_len();
            let len = len.try_into().expect("len > u32::MAX");
            // Safety: The redis string points to a valid c-string?
            let string = unsafe { RsString::borrowed_string(ptr, len) };
            RsValue::String(string)
        }
        RsValue::Number(number) => {
            let mut buf = [0u8; 32];
            // `num_to_str` formatting should fit well within the 32 byte sized buffer.
            let len = num_to_str(*number, &mut buf);
            let cstring = CString::new(&buf[..(len as usize)]).unwrap();
            RsValue::String(RsString::cstring(cstring))
        }
        _ => {
            // Safety: It is safe to use an empty here.
            let string = unsafe { RsString::borrowed_string(b"\0".as_ptr().cast(), 0) };
            RsValue::String(string)
        }
    };

    // Panic if more than 1 reference exists
    dst.set_value(new_value);
}

/// Formats the numeric value of an [`RsValue::Number`] as a string into the
/// caller-provided buffer and returns the number of bytes written.
///
/// # Safety
///
/// 1. `value` must point to a valid [`RsValue`] obtained from an `RSValue_*` function.
/// 2. `buf` must be a [valid] pointer to a writable buffer of at least `buflen` bytes.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
///
/// # Panic
///
/// Panics if `value` is not an [`RsValue::Number`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_NumToString(
    value: *const RsValue,
    buf: *mut c_char,
    buflen: size_t,
) -> size_t {
    // Safety: ensured by caller (1.)
    let value = unsafe { expect_value(value) };

    // Safety: ensured by caller (2.)
    let buf = unsafe { std::slice::from_raw_parts_mut(buf as *mut u8, buflen as usize) };

    let RsValue::Number(num) = value else {
        panic!("Expected number")
    };

    num_to_str(*num, buf)
}
