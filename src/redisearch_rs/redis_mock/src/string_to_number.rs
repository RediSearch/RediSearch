/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Mocks for `RedisModule_StringToLongLong` and `RedisModule_StringToDouble`.
//!
//! Both wrap libc's `strtoll` / `strtod`. They are stateless — no
//! [`crate::TestContext`] interaction is required.

use crate::string::UserString;
use std::ffi::c_int;

/// Returns `Some(slice)` over the bytes backing `s`, or `None` if the pointer
/// is null.
///
/// # Safety
/// 1. `s` must either be NULL or point to a valid [`UserString`] whose
///    `user`/`length` fields describe an initialised byte buffer.
const unsafe fn user_string_bytes<'a>(
    s: *const redis_module::raw::RedisModuleString,
) -> Option<&'a [u8]> {
    if s.is_null() {
        return None;
    }
    // Safety: caller has to ensure (1)
    let us = unsafe { &*s.cast::<UserString>() };
    // Safety: UserString invariant: `user` points to `length` initialised bytes.
    let bytes = unsafe { std::slice::from_raw_parts(us.user.cast::<u8>(), us.length) };
    Some(bytes)
}

/// Mock implementation of `RedisModule_StringToLongLong`.
///
/// Parses the entire string as a base-10 `i64` (matching Redis' semantics:
/// no leading whitespace, no trailing junk). On success writes the value to
/// `*out` and returns `REDISMODULE_OK`. On failure leaves `*out` untouched
/// and returns `REDISMODULE_ERR`.
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

    let parsed = std::str::from_utf8(bytes)
        .ok()
        .and_then(|s| s.parse::<i64>().ok());

    match parsed {
        Some(v) => {
            // Safety: caller has to ensure (2)
            unsafe { *out = v };
            redis_module::raw::REDISMODULE_OK as c_int
        }
        None => redis_module::raw::REDISMODULE_ERR as c_int,
    }
}

/// Mock implementation of `RedisModule_StringToDouble`.
///
/// Parses the string as `f64`. On success writes the value to `*out` and
/// returns `REDISMODULE_OK`. On failure leaves `*out` untouched and returns
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

    let parsed = std::str::from_utf8(bytes)
        .ok()
        .and_then(|s| s.parse::<f64>().ok());

    match parsed {
        Some(v) => {
            // Safety: caller has to ensure (2)
            unsafe { *out = v };
            redis_module::raw::REDISMODULE_OK as c_int
        }
        None => redis_module::raw::REDISMODULE_ERR as c_int,
    }
}
