/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::util::expect_value;
use libc::size_t;
use std::ffi::{c_char, c_double};
use value::RsValue;
use value::util::{num_to_str, str_to_float};

/// Convert the [`RsValue`] to a number. Returns `true` when this value is a number
/// or a numeric string that can be converted and writes the number to `d`. If
/// the value cannot be converted `false` is returned and nothing is written to `d`.
///
/// # Safety
///
/// 1. `value` must be either null or point to a valid [`RsValue`] obtained from
///    an `RSValue_*` function.
/// 2. `d` must be a [valid], non-null pointer to a `c_double`.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_ToNumber(value: *const RsValue, d: *mut c_double) -> bool {
    // Safety: ensured by caller (2.)
    let d = unsafe { d.as_mut().expect("d is null") };

    // Safety: ensured by caller (1.)
    let Some(value) = (unsafe { value.as_ref() }) else {
        return false;
    };

    let value = value.fully_dereferenced_ref_and_trio();

    let Some(num) = (match value {
        RsValue::Number(n) => Some(*n),
        RsValue::String(string) => str_to_float(string.as_bytes()),
        RsValue::RedisString(string) => str_to_float(string.as_bytes()),
        _ => None,
    }) else {
        return false;
    };

    *d = num;
    true
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
    let buf = unsafe { std::slice::from_raw_parts_mut(buf.cast::<u8>(), buflen) };

    let RsValue::Number(num) = value else {
        panic!("Expected number")
    };

    num_to_str(*num, buf)
}
