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
//! In this module the we add the mock implementations of the Redis modules API. There are several
//! functions like the `reply` functions that needs mocking. A similar thing is achieved by the
//! [redismock.cpp](../../redismock.cpp) C++ library, which is used in the Redis C++ tests.
//!
//! In particular, it redirects [its heap allocation facilities](https://redis.io/docs/latest/develop/reference/modules/modules-api-ref/#heap-allocation-raw-functions)
//! to use Rust's global allocator.
//! This is particularly useful when benchmarking a Rust re-implementation against the original
//! C code, since it levels the playing field by forcing both to use the same memory allocator.

use std::{
    collections::HashMap,
    ffi::{c_char, c_int, c_void},
    sync::OnceLock,
};

pub const REDISMODULE_OK: i32 = 0;
pub const REDISMODULE_ERR: i32 = 1;

/// New-type wrapping a void ptr with `Send` and `Sync` traits for usage as a function pointer in a global hashmap.
#[allow(dead_code)]
#[derive(Copy, Clone)]
struct RawFunctionPtr(*mut c_void);

unsafe impl Send for RawFunctionPtr {}

unsafe impl Sync for RawFunctionPtr {}

/// A global hashmap that holds the mocked Redis API functions.
static MY_API_FNCS: OnceLock<HashMap<&'static str, RawFunctionPtr>> = OnceLock::new();

macro_rules! register_api {
    ($map:expr, $func:ident) => {
        $map.insert(stringify!($func), RawFunctionPtr($func as *mut c_void));
    };
}

#[unsafe(no_mangle)]
#[allow(non_upper_case_globals)]
extern "C" fn RedisModule_Strdup(s: *const c_char) -> *mut c_char {
    unsafe { libc::strdup(s) }
}

fn register_api() -> HashMap<&'static str, RawFunctionPtr> {
    let mut map = HashMap::new();

    // Register the functions that are part of the Redis API
    register_api!(map, RedisModule_GetApi);
    register_api!(map, RedisModule_ReplyWithLongLong);
    register_api!(map, RedisModule_ReplyWithSimpleString);
    register_api!(map, RedisModule_ReplyWithError);
    register_api!(map, RedisModule_ReplyWithArray);
    register_api!(map, RedisModule_ReplyWithDouble);
    register_api!(map, RedisModule_WithStringBuffer);
    register_api!(map, RedisModule_WithString);

    map
}

// command stuff:
//REDISMODULE_API int (*RedisModule_CreateCommand)(RedisModuleCtx *ctx, const char *name, RedisModuleCmdFunc cmdfunc, const char *strflags, int firstkey, int lastkey, int keystep) REDISMODULE_ATTR;
//REDISMODULE_API int (*RedisModule_CreateSubcommand)(RedisModuleCommand *parent, const char *name, RedisModuleCmdFunc cmdfunc, const char *strflags, int firstkey, int lastkey, int keystep) REDISMODULE_ATTR;
//REDISMODULE_API RedisModuleCommand *(*RedisModule_GetCommand)(RedisModuleCtx *ctx, const char *name) REDISMODULE_ATTR;
//REDISMODULE_API int (*RedisModule_SetCommandInfo)(RedisModuleCommand *command, const RedisModuleCommandInfo *info) REDISMODULE_ATTR;

#[unsafe(no_mangle)]
#[allow(non_upper_case_globals)]
unsafe extern "C" fn RedisModule_GetApi(s: *const char, pp: std::ffi::c_void) -> i32 {
    let map = MY_API_FNCS.get_or_init(register_api);

    // convert s to string slice
    // Safety: The caller only gives valid null-terminated C strings encoded in UTF-8.
    let s = unsafe { std::ffi::CStr::from_ptr(s as *const i8) }
        .to_str()
        .unwrap();
    if let Some(p) = map.get(s) {
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
unsafe extern "C" fn RedisModule_SetACLCommandCategories(
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
    ($basename:ident, $($arg:ty),*) => {
        #[unsafe (no_mangle)]
        #[allow(non_snake_case)]
        unsafe extern "C" fn $basename(_ctx: *mut ffi::RedisModuleCtx, $(_: $arg),*) -> c_int {
            REDISMODULE_OK
        }
    };
}

/*
 * C-variadic functions are unstable
 * see issue #44930 <https://github.com/rust-lang/rust/issues/44930> for more informationrustcClick for full compiler diagnostic
 */
// TODO: Workaround for C-variadic functions
/*
#[unsafe(no_mangle)]
#[allow(non_snake_case)]
unsafe extern "C" fn RedisModule_ReplyWithErrorFormat(*mut ffi::RedisModuleCtx, *const c_char, ...) -> c_int {
    REDISMODULE_OK
}
*/
//reply_func!(RedisModule_ReplyWithErrorFormat, *const c_char, ...);
// REDISMODULE_API int (*RedisModule_ReplyWithErrorFormat)(RedisModuleCtx *ctx, const char *fmt, ...) REDISMODULE_ATTR;

reply_func!(RedisModule_ReplyWithLongLong, i64);
reply_func!(RedisModule_ReplyWithSimpleString, *const c_char);
reply_func!(RedisModule_ReplyWithError, *const c_char);
reply_func!(RedisModule_ReplyWithDouble, f64);
reply_func!(RedisModule_WithStringBuffer, *const char, libc::size_t);
reply_func!(RedisModule_WithString, ffi::RedisModuleString);
reply_func!(RedisModule_ReplyWithArray, usize);
reply_func!(RedisModule_ReplyWithMap, i32);
