/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ffi::{CStr, c_char};

/// Mock implementation of RedisModule_Log.
///
/// # Safety
/// 1. _ctx must be a valid pointer to a [crate::TestContext]
/// 2. level must be a valid C string.
/// 2. fmt must be a valid C string.
#[allow(non_snake_case)]
pub unsafe extern "C" fn RedisModule_Log(
    _ctx: *mut ffi::RedisModuleCtx,
    level: *const c_char,
    fmt: *const c_char,
) {
    // Safety: Caller has to ensure 2.
    let level = unsafe { CStr::from_ptr(level) };
    let level = level.to_str().expect("Invalid UTF-8");
    // Safety: Caller has to ensure 3.
    let fmt = unsafe { CStr::from_ptr(fmt) };
    let fmt = fmt.to_str().expect("Invalid UTF-8");

    match level {
        "debug" => tracing::debug!("{fmt}"),
        "verbose" => tracing::info!("{fmt}"),
        "notice" => tracing::info!("{fmt}"),
        "warning" => tracing::warn!("{fmt}"),
        _ => tracing::debug!("{level} {fmt}"),
    };
}
