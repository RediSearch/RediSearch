/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::string::UserString;
use std::{
    ffi::{c_char, c_int},
    mem::ManuallyDrop,
    sync::Arc,
};

/// Returns `Some(slice)` over the bytes backing `s`, or `None` if the pointer
/// is null.
///
/// # Safety
/// 1. `s` must either be NULL or point to a valid [`UserString`] whose
///    `user`/`length` fields describe an initialised byte buffer.
unsafe fn user_string_bytes<'a>(
    s: *const redis_module::raw::RedisModuleString,
) -> Option<&'a [u8]> {
    if s.is_null() {
        return None;
    }

    // Safety: The caller ensured the ptr is correct (1.)
    let us = ManuallyDrop::new(unsafe { Arc::from_raw(s.cast::<UserString>()) });

    // Safety: UserString invariant: `user` points to `length` initialised bytes.
    let bytes = unsafe { std::slice::from_raw_parts(us.user.cast::<u8>(), us.length) };
    Some(bytes)
}

/// Mock implementation of Redis' `string2ll` (`src/util.c`)
/// which is what `RM_StringToLongLong`.
fn string2ll(bytes: &[u8]) -> Option<i64> {
    if bytes.is_empty() || bytes.len() >= 21 {
        return None;
    }
    let (negative, digits) = match bytes.split_first() {
        Some((b'-', rest)) => (true, rest),
        _ => (false, bytes),
    };
    match digits {
        b"0" if !negative => Some(0),
        [b'1'..=b'9', ..] => {
            // Accumulate in the negative i64 range, which reaches i64::MIN; the
            // positive range does not, so negate at the end instead
            let mut v: i64 = 0;
            for b in digits {
                let d = b.checked_sub(b'0').filter(|d| *d < 10)?;
                v = v.checked_mul(10)?.checked_sub(i64::from(d))?;
            }
            if negative { Some(v) } else { v.checked_neg() }
        }
        _ => None,
    }
}

/// Mock implementation of `RedisModule_StringToLongLong`.
///
/// Parses the entire string as a base-10 `i64` via [`string2ll`], matching
/// Redis' semantics. On success writes the value to `*out` and returns
/// `REDISMODULE_OK`. On failure leaves `*out` untouched and returns
/// `REDISMODULE_ERR`.
///
/// # Safety
/// 1. `s` must point to a valid [`UserString`] (created by the mock).
/// 2. `out` must be a valid pointer to an `i64` slot.
#[expect(non_snake_case)]
pub unsafe extern "C" fn RedisModule_StringToLongLong(
    s: *const redis_module::raw::RedisModuleString,
    out: *mut i64,
) -> c_int {
    // Safety: caller has to ensure (1)
    let bytes = match unsafe { user_string_bytes(s) } {
        Some(b) => b,
        None => return redis_module::raw::REDISMODULE_ERR as c_int,
    };

    match string2ll(bytes) {
        Some(v) => {
            // Safety: caller has to ensure (2)
            unsafe { *out = v };
            redis_module::raw::REDISMODULE_OK as c_int
        }
        None => redis_module::raw::REDISMODULE_ERR as c_int,
    }
}

#[cfg(target_os = "linux")]
fn errno() -> *mut c_int {
    // Safety: always safe to call; returns a valid pointer to the thread's errno.
    unsafe { libc::__errno_location() }
}
#[cfg(not(target_os = "linux"))]
fn errno() -> *mut c_int {
    // Safety: always safe to call; returns a valid pointer to the thread's errno.
    unsafe { libc::__error() }
}

/// Port of Redis' `string2d` (`src/util.c`), which is what `RedisModule_StringToDouble`
/// calls: strict `f64` via libc `strtod` — no empty/leading-whitespace/trailing
/// junk, no NaN, no overflow/underflow.
fn string2d(bytes: &[u8]) -> Option<f64> {
    match bytes.first() {
        None | Some(b' ' | b'\t'..=b'\r') => return None,
        _ => {}
    }
    let cstr = std::ffi::CString::new(bytes).ok()?;
    let mut end: *mut c_char = std::ptr::null_mut();
    // Safety: `errno()` returns a valid pointer to the thread's errno.
    unsafe { *errno() = 0 };
    // Safety: `cstr` is a valid NUL-terminated string and `end` a valid slot.
    let v = unsafe { libc::strtod(cstr.as_ptr(), &mut end) };
    // Safety: `errno()` returns a valid pointer to the thread's errno.
    let err = unsafe { *errno() };
    // Safety: `strtod` sets `end` to a pointer within `cstr`.
    let consumed = unsafe { end.cast_const().offset_from(cstr.as_ptr()) };
    (usize::try_from(consumed).ok() == Some(bytes.len())
        && !v.is_nan()
        && !(err == libc::ERANGE && (v.is_infinite() || v == 0.0)))
        .then_some(v)
}

/// Mock implementation of `RedisModule_StringToDouble`.
///
/// Parses the entire string as `f64` via [`string2d`], matching Redis'
/// semantics. On success writes the value to `*out` and returns
/// `REDISMODULE_OK`. On failure leaves `*out` untouched and returns
/// `REDISMODULE_ERR`.
///
/// # Safety
/// 1. `s` must point to a valid [`UserString`] (created by the mock).
/// 2. `out` must be a valid pointer to an `f64` slot.
#[expect(non_snake_case)]
pub unsafe extern "C" fn RedisModule_StringToDouble(
    s: *const redis_module::raw::RedisModuleString,
    out: *mut f64,
) -> c_int {
    // Safety: caller has to ensure (1)
    let bytes = match unsafe { user_string_bytes(s) } {
        Some(b) => b,
        None => return redis_module::raw::REDISMODULE_ERR as c_int,
    };

    match string2d(bytes) {
        Some(v) => {
            // Safety: caller has to ensure (2)
            unsafe { *out = v };
            redis_module::raw::REDISMODULE_OK as c_int
        }
        None => redis_module::raw::REDISMODULE_ERR as c_int,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_string2ll() {
        // May not start with +.
        assert_eq!(string2ll(b"+1"), None);
        // May not start with 0.
        assert_eq!(string2ll(b"01"), None);
        assert_eq!(string2ll(b" 1"), None);
        assert_eq!(string2ll(b"1 "), None);

        assert_eq!(string2ll(b"-1"), Some(-1));
        assert_eq!(string2ll(b"0"), Some(0));
        assert_eq!(string2ll(b"1"), Some(1));
        assert_eq!(string2ll(b"99"), Some(99));
        assert_eq!(string2ll(b"-99"), Some(-99));

        assert_eq!(string2ll(b"-9223372036854775808"), Some(i64::MIN));
        // Overflow.
        assert_eq!(string2ll(b"-9223372036854775809"), None);

        assert_eq!(string2ll(b"9223372036854775807"), Some(i64::MAX));
        // Overflow.
        assert_eq!(string2ll(b"9223372036854775808"), None);
    }

    #[test]
    fn test_string2d() {
        // Valid hexadecimal value.
        assert_eq!(string2d(b"0x0p+0"), Some(0.0));
        assert_eq!(string2d(b"0x1p+0"), Some(1.0));

        // Valid floating-point numbers.
        assert_eq!(string2d(b"1.5"), Some(1.5));
        assert_eq!(string2d(b"-3.14"), Some(-3.14));
        assert_eq!(string2d(b"2.0e10"), Some(2.0e10));
        assert_eq!(string2d(b"1e-3"), Some(0.001));

        // Valid integer.
        assert_eq!(string2d(b"42"), Some(42.0));

        assert_eq!(string2d(b""), None);
        assert_eq!(string2d(b" 1.23"), None);
        // Invalid hexadecimal format.
        assert_eq!(string2d(b"0x1.2g"), None);
        // Hexadecimal NaN.
        assert_eq!(string2d(b"0xNan"), None);

        // Overflow.
        assert_eq!(
            string2d(
                b"23456789123456789123456789123456789123456789123456789123456789123456789123456789123456789123456789123456789123456789123456789123456789123456789123456789123456789123456789123456789123456789123456789123456789123456789123456789123456789123456789123456789123456789123456789123456789123456789123456789123456789123456789123456789"
            ),
            None
        );
    }
}
