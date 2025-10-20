/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{
    ffi::{CString, c_char},
    ptr::NonNull,
};

use crate::mock::{RedisModule_FreeString, RedisModule_StringPtrLen, UserString};

use redis_module::KeyType;
use value::RSValueTrait as _;

use crate::mock::RedisModule_CreateString;

#[unsafe(no_mangle)]
pub unsafe extern "C" fn init_redis_mock_module() {
    unsafe { redis_module::raw::RedisModule_CreateString = Some(RedisModule_CreateString) };
    unsafe { redis_module::raw::RedisModule_StringPtrLen = Some(RedisModule_StringPtrLen) };
    unsafe { redis_module::raw::RedisModule_FreeString = Some(RedisModule_FreeString) };

    // register key methods
    unsafe { redis_module::raw::RedisModule_OpenKey = Some(RedisModule_OpenKey) };
    unsafe { redis_module::raw::RedisModule_CloseKey = Some(RedisModule_CloseKey) };
    unsafe { redis_module::raw::RedisModule_KeyType = Some(RedisModule_KeyType) };

    unsafe { redis_module::raw::RedisModule_ScanCursorCreate = Some(RedisModule_ScanCursorCreate) };
    unsafe {
        redis_module::raw::RedisModule_ScanCursorDestroy = Some(RedisModule_ScanCursorDestroy)
    };
    unsafe { redis_module::raw::RedisModule_ScanKey = Some(RedisModule_ScanKey) };
}

#[repr(C)]
struct UserKey {
    ctx: Option<NonNull<redis_module::raw::RedisModuleCtx>>,
    name: *const c_char,
    ty: KeyType,
}

unsafe extern "C" fn RedisModule_OpenKey(
    ctx: *mut redis_module::raw::RedisModuleCtx,
    keyname: *mut redis_module::raw::RedisModuleString,
    mode: ::std::os::raw::c_int,
) -> *mut redis_module::raw::RedisModuleKey {
    let keyname_user_string = unsafe { &*(keyname.cast::<UserString>()) };

    let ctx = if ctx.is_null() {
        None
    } else {
        Some(NonNull::new(ctx).unwrap())
    };

    let key = Box::new(UserKey {
        ctx,
        ty: KeyType::Hash,
        name: keyname_user_string.user,
    });
    Box::into_raw(key).cast()
}

unsafe extern "C" fn RedisModule_CloseKey(key: *mut redis_module::raw::RedisModuleKey) {
    drop(unsafe { Box::from_raw(key.cast::<UserKey>()) });
}

unsafe extern "C" fn RedisModule_KeyType(key: *mut redis_module::raw::RedisModuleKey) -> i32 {
    let key = unsafe { &*(key as *mut UserKey) };
    let res = match key.ty {
        KeyType::Empty => redis_module::raw::REDISMODULE_KEYTYPE_EMPTY,
        KeyType::String => redis_module::raw::REDISMODULE_KEYTYPE_STRING,
        KeyType::List => redis_module::raw::REDISMODULE_KEYTYPE_LIST,
        KeyType::Set => redis_module::raw::REDISMODULE_KEYTYPE_SET,
        KeyType::ZSet => redis_module::raw::REDISMODULE_KEYTYPE_ZSET,
        KeyType::Hash => redis_module::raw::REDISMODULE_KEYTYPE_HASH,
        KeyType::Module => redis_module::raw::REDISMODULE_KEYTYPE_MODULE,
        KeyType::Stream => redis_module::raw::REDISMODULE_KEYTYPE_STREAM,
    };
    res as i32
}

unsafe extern "C" fn RedisModule_ScanCursorCreate() -> *mut redis_module::raw::RedisModuleScanCursor
{
    // we don't need to store any state for the mock, we store it at the context level
    std::ptr::null_mut()
}

unsafe extern "C" fn RedisModule_ScanCursorDestroy(
    _cursor: *mut redis_module::raw::RedisModuleScanCursor,
) {
    // no-op, see RedisModule_ScanCursorCreate
}

unsafe extern "C" fn RedisModule_ScanKey(
    key: *mut redis_module::raw::RedisModuleKey,
    _cursor: *mut redis_module::raw::RedisModuleScanCursor,
    _cb: Option<
        unsafe extern "C" fn(
            *mut redis_module::raw::RedisModuleKey,
            *mut redis_module::raw::RedisModuleString,
            *mut redis_module::raw::RedisModuleString,
            *mut ::std::os::raw::c_void,
        ),
    >,
    _privdata: *mut ::std::os::raw::c_void,
) -> ::std::os::raw::c_int {
    let key = unsafe { &*(key.cast::<UserKey>()) };
    let Some(ctx) = key.ctx else {
        // early return we miss the data holder
        return 0;
    };

    let ctx: &crate::load_document::test_utils::LoadDocumentTestContext = unsafe {
        &*(ctx.as_ptr() as *const crate::load_document::test_utils::LoadDocumentTestContext)
    };

    let ctx_arg = std::ptr::null_mut();
    // we get cstrings and values from the context, we have to generate the scan key callback types
    for (k, v) in ctx.access_key_values().iter() {
        // convert field to redis string
        let field = RedisModule_CreateString(ctx_arg, k.as_ptr(), k.as_bytes().len());

        // convert value to redis string
        let vstr = v.as_str().unwrap();
        let vlen = vstr.len();
        let cstring = CString::new(vstr).unwrap();
        let value = RedisModule_CreateString(ctx_arg, cstring.as_ptr(), vlen);

        // call the callback
        let cb = _cb.expect("callback must be set");
        cb(key as *const _ as *mut _, field, value, _privdata);

        // free the created strings
        RedisModule_FreeString(ctx_arg, field);
        RedisModule_FreeString(ctx_arg, value);
    }

    0
}
