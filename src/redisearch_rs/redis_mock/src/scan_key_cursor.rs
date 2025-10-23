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

use crate::{
    key::UserKey,
    string::{RedisModule_CreateString, RedisModule_FreeString},
};

#[allow(non_snake_case)]
pub const unsafe extern "C" fn RedisModule_ScanCursorCreate()
-> *mut redis_module::raw::RedisModuleScanCursor {
    // we don't need to store any state for the mock, we store it at the context level
    std::ptr::null_mut()
}

#[allow(non_snake_case)]
pub const unsafe extern "C" fn RedisModule_ScanCursorDestroy(
    _cursor: *mut redis_module::raw::RedisModuleScanCursor,
) {
    // no-op, see RedisModule_ScanCursorCreate
}

#[allow(non_snake_case)]
pub unsafe extern "C" fn RedisModule_ScanKey(
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
    let Some(ctx) = key.get_ctx() else {
        // early return we miss the data holder
        return 0;
    };

    let ctx: &crate::TestContext = unsafe { &*(ctx.as_ptr() as *const crate::TestContext) };

    let ctx_arg = std::ptr::null_mut();
    // we get cstrings and values from the context, we have to generate the scan key callback types
    for (k, v) in ctx.access_key_values().iter() {
        // convert field to redis string
        let field = unsafe { RedisModule_CreateString(ctx_arg, k.as_ptr(), k.as_bytes().len()) };

        // convert value to redis string
        let value = unsafe { RedisModule_CreateString(ctx_arg, v.as_ptr(), v.as_bytes().len()) };

        // call the callback
        let cb = _cb.expect("callback must be set");

        unsafe { cb(key as *const _ as *mut _, field, value, _privdata) };

        // free the created strings
        unsafe { RedisModule_FreeString(ctx_arg, field) };
        unsafe { RedisModule_FreeString(ctx_arg, value) };
    }

    0
}
