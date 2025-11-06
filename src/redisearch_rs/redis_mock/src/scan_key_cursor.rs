/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::{
    key::UserKey,
    string::{RedisModule_CreateString, RedisModule_FreeString},
};

/// Creates a new Scan Key Cursor.
///
/// # Safety
/// Caller is is only allow to use RedisModule_ScanKey, we don't support restart and other operations, yet.
#[allow(non_snake_case)]
pub const unsafe extern "C" fn RedisModule_ScanCursorCreate()
-> *mut redis_module::raw::RedisModuleScanCursor {
    // we don't need to store any state for the mock, we store it at the context level
    std::ptr::null_mut()
}

/// Destroys a Scan Key Cursor.
///
/// # Safety
/// It's a no-op in the mock implementation.
#[allow(non_snake_case)]
pub const unsafe extern "C" fn RedisModule_ScanCursorDestroy(
    _cursor: *mut redis_module::raw::RedisModuleScanCursor,
) {
    // no-op, see RedisModule_ScanCursorCreate
}

/// Scans the keys in a Redis key using the Scan Key API.
///
///  # Safety
/// 1. `key` must be a valid pointer to a `RedisModuleKey` implemented by [crate::key::UserKey].
/// 2. `key` must be created by [crate::key::RedisModule_OpenKey] to ensure it holds a [crate::TestContext].
/// 2. `_cursor` should be null in the test mock implementation.
/// 3. `_cb` must be a valid callback function pointer.
/// 4. `_privdata` can be null and is not used by the mock implementation.
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
    // Safety: Caller has to ensure 1
    let key = unsafe { &*(key.cast::<UserKey>()) };
    let ctx = key.get_ctx();

    // Safety: Caller is has to ensure 2 and thus we can cast the context as [crate::TestContext]
    let test_ctx = unsafe {
        ctx.as_ptr()
            .cast::<crate::TestContext>()
            .as_ref()
            .expect("ctx pointer must be valid and point to TestContext")
    };

    // we get cstrings and values from the context, we have to generate the scan key callback types
    for (k, v) in test_ctx.access_key_values().iter() {
        // convert field to redis string
        // Safety: We create a RedisModuleString from valid utf8 bytes in [crate::TestContext]
        let field =
            unsafe { RedisModule_CreateString(ctx.as_ptr(), k.as_ptr(), k.as_bytes().len()) };

        // convert value to redis string
        // Safety: We create a RedisModuleString from valid utf8 bytes in [crate::TestContext]
        let value =
            unsafe { RedisModule_CreateString(ctx.as_ptr(), v.as_ptr(), v.as_bytes().len()) };

        // access the callback
        let cb = _cb.expect("callback must be set");

        // call the callback,
        // Safety: if the user-code wants to use field or value after the loop it is their responsibility to copy them
        unsafe { cb(key as *const _ as *mut _, field, value, _privdata) };

        // free the created strings

        // Safety: The user-code is expected to have used or copied the strings in the callback, so we can free them here.
        unsafe { RedisModule_FreeString(ctx.as_ptr(), field) };
        // Safety: The user-code is expected to have used or copied the strings in the callback, so we can free them here.
        unsafe { RedisModule_FreeString(ctx.as_ptr(), value) };
    }

    0
}
