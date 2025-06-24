/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! `redis_mock` provides alternative implementations for some of the symbols in
//! [Redis modules' API](https://redis.io/docs/latest/develop/reference/modules/modules-api-ref/).
//!
//! ## General
//!
//! It setups a global rust allocator in the [crate::allocator] module and provides mock implementations
//! in the modules [crate::mock] and [crate::command].
//!
//! For unit testing several functions like the `reply` functions or the command api require mocking.
//! A similar thing is achieved by the [redismock.cpp](../../tests/cpptests/redismock/redismock.cpp) C++ library,
//! which is used in the Redis C++ tests.
//!
//! It lacks a mocked initialization of the Redis module, which may be added in the future. The
//! [redismock.cpp](../../tests/cpptests/redismock/redismock.cpp) C++ library, uses a mocked initialization mainly
//! for iterator benchmarks.
//!
//! ## Allocations
//!
//! It redirects [its heap allocation facilities](https://redis.io/docs/latest/develop/reference/modules/modules-api-ref/#heap-allocation-raw-functions)
//! to use Rust's global allocator.
//! This is particularly useful when benchmarking a Rust re-implementation against the original
//! C code, since it levels the playing field by forcing both to use the same memory allocator.
//!
//! ## Variadic functions
//!
//! C-variadic functions are unstable in Rust, which means that we cannot directly implement them in Rust.
//! see issue #44930 <https://github.com/rust-lang/rust/issues/44930> for more information
//!
//! Workaround: The variadic function is defined on the the C side, c-side does the variadic processing and calls
//! a rust function with a fixed number of arguments.
//!
//! See the `non_variadic_reply_with_error_format` function for an example of how to handle this and search for
//! "part of workaround for <https://github.com/rust-lang/rust/issues/44930>" to find other places that are involved in the
//! workaround.

use std::{
    collections::HashMap,
    ffi::{c_char, c_int, c_void},
    sync::OnceLock,
};

use crate::command::*;

pub const REDISMODULE_OK: i32 = 0;
pub const REDISMODULE_ERR: i32 = 1;

/// New-type wrapping a void ptr with `Send` and `Sync` traits for usage as a function pointer in a global hashmap.
#[allow(dead_code)]
#[derive(Copy, Clone)]
struct RawFunctionPtr(*mut c_void);

/// Safety: The pointer is a function pointer, which is safe to send across threads
/// and can be used concurrently.
unsafe impl Send for RawFunctionPtr {}

/// Safety: The pointer is a function pointer, which is safe to call from multiple threads as each invocation will
/// not share state with other invocations.
unsafe impl Sync for RawFunctionPtr {}

/// A global hashmap that holds the mocked Redis API functions.
static MY_API_FNCS: OnceLock<HashMap<&'static str, RawFunctionPtr>> = OnceLock::new();

macro_rules! register_api {
    ($map:expr, $func:path) => {
        $map.insert(stringify!($func), RawFunctionPtr($func as *mut c_void));
    };
}

#[unsafe(no_mangle)]
#[allow(non_upper_case_globals)]
extern "C" fn RedisModule_Strdup(s: *const c_char) -> *mut c_char {
    // Safety: The caller guarantees that `s` is a valid null-terminated C string.
    unsafe { libc::strdup(s) }
}

fn register_api() -> HashMap<&'static str, RawFunctionPtr> {
    // The variadic functions entry points are defined in C, which is
    // a workaround for <https://github.com/rust-lang/rust/issues/44930>

    let mut map = HashMap::new();

    // Register the functions that are part of the Redis API
    register_api!(map, RedisModule_GetApi);

    register_api!(map, RedisModule_ReplyWithNull);
    register_api!(map, RedisModule_ReplyWithLongLong);
    register_api!(map, RedisModule_ReplyWithSimpleString);
    register_api!(map, RedisModule_ReplyWithError);
    register_api!(map, RedisModule_ReplyWithDouble);
    register_api!(map, RedisModule_ReplyWithStringBuffer);
    register_api!(map, RedisModule_ReplyWithString);
    register_api!(map, RedisModule_ReplyWithCString);
    register_api!(map, RedisModule_ReplyWithEmptyString);
    register_api!(map, RedisModule_ReplyWithVerbatimString);

    register_api!(map, RedisModule_ReplyWithArray);
    register_api!(map, RedisModule_ReplyWithEmptyArray);
    register_api!(map, RedisModule_ReplyWithNullArray);
    register_api!(map, RedisModule_ReplyWithMap);
    register_api!(map, RedisModule_ReplyWithSet);
    register_api!(map, RedisModule_ReplyWithAttribute);

    register_api!(map, RedisModule_ReplySetArrayLength);
    register_api!(map, RedisModule_ReplySetMapLength);
    register_api!(map, RedisModule_ReplySetSetLength);
    register_api!(map, RedisModule_ReplySetAttributeLength);
    register_api!(map, RedisModule_ReplySetPushLength);

    register_api!(map, RedisModule_IsModuleNameBusy);
    register_api!(map, RedisModule_WrongArity);

    register_api!(map, RedisModule_SetModuleAttribs);
    register_api!(map, RedisModule_SetACLCategory);
    register_api!(map, RedisModule_AddACLCategory);
    register_api!(map, RedisModule_SetCommandACLCategories);

    register_api!(map, RedisModule_GetCommand);
    register_api!(map, RedisModule_CreateCommand);
    register_api!(map, RedisModule_CreateSubcommand);
    register_api!(map, RedisModule_SetCommandInfo);
    map
}

#[unsafe(no_mangle)]
unsafe extern "C" fn non_variadic_reply_with_error_format(
    _ctx: ffi::RedisModuleCtx,
    _fmt: *const char,
    arg_from_c: i32,
    // here could be more arguments if the c-side processes the variadic args
) -> i32 {
    // part of workaround for <https://github.com/rust-lang/rust/issues/44930>
    // no processing of the variadic arguments
    arg_from_c
}

#[unsafe(no_mangle)]
#[allow(non_upper_case_globals)]
unsafe extern "C" fn RedisModule_GetApi(s: *const char, pp: *mut std::ffi::c_void) -> i32 {
    let map = MY_API_FNCS.get_or_init(register_api);

    // convert s to string slice
    // Safety: The caller only gives valid null-terminated C strings encoded in UTF-8.
    let s = unsafe { std::ffi::CStr::from_ptr(s.cast()) }
        .to_str()
        .unwrap();
    if let Some(ftor) = map.get(s) {
        let pp: *mut *mut c_void = pp.cast();
        // Safety: The pointer ftor.0 is a valid function pointer, and we are assigning it to a mutable pointer.
        unsafe { *pp = ftor.0 };
        REDISMODULE_OK
    } else {
        REDISMODULE_ERR
    }
}

#[unsafe(no_mangle)]
#[allow(non_upper_case_globals)]
unsafe extern "C" fn RedisModule_SetModuleAttribs(
    _ctx: ffi::RedisModuleCtx,
    _name: *const char,
    _version: i32,
    _: i32,
) {
    // no operation
}

#[unsafe(no_mangle)]
#[allow(non_upper_case_globals)]
unsafe extern "C" fn RedisModule_SetACLCategory(
    _ctx: *mut ffi::RedisModuleCtx,
    _cat: *const char,
) -> i32 {
    REDISMODULE_OK
}

#[unsafe(no_mangle)]
#[allow(non_upper_case_globals)]
unsafe extern "C" fn RedisModule_AddACLCategory(
    _ctx: *mut ffi::RedisModuleCtx,
    _cat: *const char,
) -> i32 {
    REDISMODULE_OK
}

#[unsafe(no_mangle)]
#[allow(non_upper_case_globals)]
unsafe extern "C" fn RedisModule_SetCommandACLCategories(
    _ctx: *mut ffi::RedisModuleCtx,
    _cats: *const char,
) -> i32 {
    REDISMODULE_OK
}

#[unsafe(no_mangle)]
#[allow(non_upper_case_globals)]
unsafe extern "C" fn RedisModule_WrongArity(_ctx: *mut ffi::RedisModuleCtx) -> i32 {
    REDISMODULE_OK
}

#[unsafe(no_mangle)]
#[allow(non_upper_case_globals)]
unsafe extern "C" fn RedisModule_IsModuleNameBusy(_name: *const char) -> i32 {
    // The module is never busy in the mock implementation.
    REDISMODULE_OK
}

/// Helper to generate the reply functions
macro_rules! reply_func {
    ($basename:ident $(, $arg:ty)*) => {
        #[unsafe (no_mangle)]
        #[allow(non_snake_case)]
        unsafe extern "C" fn $basename(_ctx: *mut ffi::RedisModuleCtx $(, _: $arg)*) -> c_int {
            REDISMODULE_OK
        }
    };
}

reply_func!(RedisModule_ReplyWithNull);
reply_func!(RedisModule_ReplyWithLongLong, i64);
reply_func!(RedisModule_ReplyWithSimpleString, *const c_char);
reply_func!(RedisModule_ReplyWithError, *const c_char);
reply_func!(RedisModule_ReplyWithDouble, f64);
reply_func!(RedisModule_ReplyWithStringBuffer, *const char, libc::size_t);
reply_func!(RedisModule_ReplyWithString, ffi::RedisModuleString);
reply_func!(RedisModule_ReplyWithCString, *const c_char);
reply_func!(RedisModule_ReplyWithEmptyString);
reply_func!(
    RedisModule_ReplyWithVerbatimString,
    *const c_char,
    libc::size_t
);
reply_func!(RedisModule_ReplyWithArray, usize);
reply_func!(RedisModule_ReplyWithEmptyArray);
reply_func!(RedisModule_ReplyWithNullArray);
reply_func!(RedisModule_ReplyWithMap, i32);
reply_func!(RedisModule_ReplyWithSet, i32);
reply_func!(RedisModule_ReplyWithAttribute, i32);
reply_func!(RedisModule_ReplySetArrayLength, i32);
reply_func!(RedisModule_ReplySetMapLength, i32);
reply_func!(RedisModule_ReplySetSetLength, i32);
reply_func!(RedisModule_ReplySetAttributeLength, i32);
reply_func!(RedisModule_ReplySetPushLength, i32);
