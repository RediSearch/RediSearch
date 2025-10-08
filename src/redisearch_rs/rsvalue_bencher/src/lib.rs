/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use redis_module::{RedisModuleCtx, RedisModuleString};
use std::ffi::c_void;

redis_mock::bind_redis_alloc_symbols_to_mock_impl!();

#[unsafe(no_mangle)]
#[allow(non_upper_case_globals)]
pub static mut RSGlobalConfig: *const c_void = std::ptr::null();

#[unsafe(no_mangle)]
#[allow(non_upper_case_globals)]
pub static mut RSDummyContext: *const c_void = std::ptr::null();

#[unsafe(no_mangle)]
pub extern "C" fn RedisModule_RetainString(
    _ctx: *mut RedisModuleCtx,
    _string: *mut RedisModuleString,
) {
    panic!("unexpected call to RedisModule_RetainString")
}

#[unsafe(no_mangle)]
pub extern "C" fn Obfuscate_Text(_text: *const std::ffi::c_char) -> *const std::ffi::c_char {
    c"Text".as_ptr()
}

#[unsafe(no_mangle)]
pub extern "C" fn Obfuscate_Number(_number: f64) -> *const std::ffi::c_char {
    c"Number".as_ptr()
}

#[unsafe(no_mangle)]
pub extern "C" fn AC_GetString() {}
