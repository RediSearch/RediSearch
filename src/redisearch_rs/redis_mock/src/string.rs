/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ffi::c_char;

/// Mock implementation of RedisModuleString from redismodule.h for testing purposes.
#[derive(Default, Copy, Clone)]
#[repr(C)]
pub(crate) struct UserString {
    pub(crate) user: *const c_char,
    pub(crate) length: usize,
}

/// Mock implementation of RedisModule_CreateString from redismodule.h for testing purposes.
///
/// Safety:
/// 1. ptr must be a valid pointer to a C string of length len.
#[unsafe(export_name = "_RedisModule_CreateString.1")]
#[allow(non_snake_case)]
pub(crate) unsafe extern "C" fn RedisModule_CreateString(
    _ctx: *mut redis_module::raw::RedisModuleCtx,
    ptr: *const ::std::os::raw::c_char,
    len: usize,
) -> *mut redis_module::raw::RedisModuleString {
    let val = Box::new(UserString {
        user: ptr,
        length: len,
    });
    Box::into_raw(val).cast()
}

/// Mock implementation of RedisModule_StringPtrLen from redismodule.h for testing purposes.
///
/// Safety:
/// 1. s must be a valid pointer to a RedisModuleString created by this mock implementation.
/// 2. len must be a valid pointer to a usize.
#[allow(non_snake_case)]
pub(crate) unsafe extern "C" fn RedisModule_StringPtrLen(
    s: *const redis_module::raw::RedisModuleString,
    len: *mut usize,
) -> *const ::std::os::raw::c_char {
    // Safety: we know we're using UserString here (1)
    let s = unsafe { &*(s.cast::<UserString>()) };
    // Safety: Caller provides valid len pointer (2)
    unsafe {
        *len = s.length;
    }
    s.user
}

/// Mock implementation of RedisModule_FreeString from redismodule.h for testing purposes.
///
/// Safety:
/// 1. s must be a valid pointer to a RedisModuleString created by this mock
/// 2. The function must not be called more than once for the same string.
#[allow(non_snake_case)]
pub(crate) unsafe extern "C" fn RedisModule_FreeString(
    _ctx: *mut redis_module::raw::RedisModuleCtx,
    s: *mut redis_module::raw::RedisModuleString,
) {
    // Safety: we own the memory (1) and the caller promised to call this only once (2)
    drop(unsafe { Box::from_raw(s.cast::<UserString>()) });
}
