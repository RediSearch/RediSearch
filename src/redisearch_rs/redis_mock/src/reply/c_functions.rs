/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Mock implementations of Redis reply C functions.

use std::ffi::{CStr, c_char, c_int, c_longlong};

use redis_module::raw::RedisModuleCtx;

use super::capture::CAPTURE_STATE;
use super::value::ReplyValue;

/// Mock implementation of `RedisModule_ReplyWithLongLong`.
///
/// # Safety
///
/// The context pointer is ignored in mock mode.
#[allow(non_snake_case)]
pub unsafe extern "C" fn RedisModule_ReplyWithLongLong(
    _ctx: *mut RedisModuleCtx,
    ll: c_longlong,
) -> c_int {
    CAPTURE_STATE.with(|state| {
        state.borrow_mut().push_value(ReplyValue::LongLong(ll));
    });
    ffi::REDISMODULE_OK as c_int
}

/// Mock implementation of `RedisModule_ReplyWithDouble`.
///
/// # Safety
///
/// The context pointer is ignored in mock mode.
#[allow(non_snake_case)]
pub unsafe extern "C" fn RedisModule_ReplyWithDouble(_ctx: *mut RedisModuleCtx, d: f64) -> c_int {
    CAPTURE_STATE.with(|state| {
        state.borrow_mut().push_value(ReplyValue::Double(d));
    });
    ffi::REDISMODULE_OK as c_int
}

/// Mock implementation of `RedisModule_ReplyWithSimpleString`.
///
/// # Safety
///
/// The `msg` pointer must be a valid null-terminated C string.
#[allow(non_snake_case)]
pub unsafe extern "C" fn RedisModule_ReplyWithSimpleString(
    _ctx: *mut RedisModuleCtx,
    msg: *const c_char,
) -> c_int {
    // SAFETY: Caller guarantees msg is a valid C string.
    let s = unsafe { CStr::from_ptr(msg) }
        .to_string_lossy()
        .into_owned();
    CAPTURE_STATE.with(|state| {
        state.borrow_mut().push_value(ReplyValue::SimpleString(s));
    });
    ffi::REDISMODULE_OK as c_int
}

/// Mock implementation of `RedisModule_ReplyWithEmptyArray`.
///
/// # Safety
///
/// The context pointer is ignored in mock mode.
#[allow(non_snake_case)]
pub unsafe extern "C" fn RedisModule_ReplyWithEmptyArray(_ctx: *mut RedisModuleCtx) -> c_int {
    CAPTURE_STATE.with(|state| {
        state.borrow_mut().push_value(ReplyValue::Array(vec![]));
    });
    ffi::REDISMODULE_OK as c_int
}

/// Mock implementation of `RedisModule_ReplyWithArray`.
///
/// When `len` is `REDISMODULE_POSTPONED_ARRAY_LEN`, starts a new array builder.
/// Otherwise, starts a fixed-length array that auto-finalizes after `len` elements.
///
/// # Safety
///
/// The context pointer is ignored in mock mode.
#[allow(non_snake_case)]
pub unsafe extern "C" fn RedisModule_ReplyWithArray(
    _ctx: *mut RedisModuleCtx,
    len: c_longlong,
) -> c_int {
    CAPTURE_STATE.with(|state| {
        let mut state = state.borrow_mut();
        if len == 0 {
            // Zero-length array - immediately push an empty array
            state.push_value(ReplyValue::Array(vec![]));
        } else if len == ffi::REDISMODULE_POSTPONED_ARRAY_LEN as c_longlong {
            // Postponed length - will be finalized by ReplySetArrayLength
            state.start_array(None);
        } else {
            // Fixed-size array - auto-finalizes after `len` elements
            state.start_array(Some(len as usize));
        }
    });
    ffi::REDISMODULE_OK as c_int
}

/// Mock implementation of `RedisModule_ReplyWithMap`.
///
/// When `len` is `REDISMODULE_POSTPONED_LEN`, starts a new map builder.
/// When `len` is 0, creates an empty map.
/// Otherwise, starts a fixed-length map that auto-finalizes after `len` key-value pairs.
///
/// # Safety
///
/// The context pointer is ignored in mock mode.
#[allow(non_snake_case)]
pub unsafe extern "C" fn RedisModule_ReplyWithMap(
    _ctx: *mut RedisModuleCtx,
    len: c_longlong,
) -> c_int {
    CAPTURE_STATE.with(|state| {
        let mut state = state.borrow_mut();
        if len == 0 {
            state.push_value(ReplyValue::Map(vec![]));
        } else if len == ffi::REDISMODULE_POSTPONED_LEN as c_longlong {
            // Postponed length - will be finalized by ReplySetMapLength
            state.start_map(None);
        } else {
            // Fixed-size map - auto-finalizes after `len` key-value pairs
            state.start_map(Some(len as usize));
        }
    });
    ffi::REDISMODULE_OK as c_int
}

/// Mock implementation of `RedisModule_ReplySetArrayLength`.
///
/// Finalizes the current array builder.
///
/// # Safety
///
/// The context pointer is ignored in mock mode.
#[allow(non_snake_case)]
pub unsafe extern "C" fn RedisModule_ReplySetArrayLength(
    _ctx: *mut RedisModuleCtx,
    len: c_longlong,
) {
    CAPTURE_STATE.with(|state| {
        state.borrow_mut().finalize_array(len);
    });
}

/// Mock implementation of `RedisModule_ReplySetMapLength`.
///
/// Finalizes the current map builder.
///
/// # Safety
///
/// The context pointer is ignored in mock mode.
#[allow(non_snake_case)]
pub unsafe extern "C" fn RedisModule_ReplySetMapLength(_ctx: *mut RedisModuleCtx, len: c_longlong) {
    CAPTURE_STATE.with(|state| {
        state.borrow_mut().finalize_map(len);
    });
}
