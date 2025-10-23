/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#![allow(clippy::undocumented_unsafe_blocks)]
#![allow(clippy::missing_safety_doc)]

use std::ffi::c_char;

#[derive(Default, Copy, Clone)]
#[repr(C)]
pub(crate) struct UserString {
    pub(crate) user: *const c_char,
    pub(crate) length: usize,
}

/// Mock implementation of RedisModule_CreateString from redismodule.h for testing purposes
//#[unsafe(no_mangle)]
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

#[allow(non_snake_case)]
pub(crate) unsafe extern "C" fn RedisModule_StringPtrLen(
    s: *const redis_module::raw::RedisModuleString,
    len: *mut usize,
) -> *const ::std::os::raw::c_char {
    // Safety: we know we're using UserString here
    let s = unsafe { &*(s.cast::<UserString>()) };
    // Safety: Caller provides valid len pointer
    unsafe {
        *len = s.length;
    }
    s.user
}

#[allow(non_snake_case)]
pub(crate) unsafe extern "C" fn RedisModule_FreeString(
    _ctx: *mut redis_module::raw::RedisModuleCtx,
    s: *mut redis_module::raw::RedisModuleString,
) {
    // Safety: we own the memory
    drop(unsafe { Box::from_raw(s.cast::<UserString>()) });
}
