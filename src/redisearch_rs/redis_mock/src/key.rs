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

use crate::string::UserString;
use redis_module::KeyType;
use std::{ffi::c_char, ptr::NonNull};

#[repr(C)]
pub struct UserKey {
    ctx: Option<NonNull<redis_module::raw::RedisModuleCtx>>,
    name: *const c_char,
    ty: KeyType,
}

impl UserKey {
    pub const fn get_ctx(&self) -> Option<NonNull<redis_module::raw::RedisModuleCtx>> {
        self.ctx
    }
}

#[allow(non_snake_case)]
pub unsafe extern "C" fn RedisModule_OpenKey(
    ctx: *mut redis_module::raw::RedisModuleCtx,
    keyname: *mut redis_module::raw::RedisModuleString,
    _mode: ::std::os::raw::c_int,
) -> *mut redis_module::raw::RedisModuleKey {
    let keyname_user_string = unsafe { &*(keyname.cast::<UserString>()) };

    // Safety: Caller is has to ensure 2 and thus we can cast the context as [crate::TestContext]
    let test_ctx = unsafe {
        ctx.cast::<crate::TestContext>()
            .as_ref()
            .expect("ctx pointer must be valid and point to TestContext")
    };

    let ctx = if ctx.is_null() {
        None
    } else {
        Some(NonNull::new(ctx).unwrap())
    };

    // todo: Implement Clone and or Copy for KeyType in redis-module crate
    // to avoid this match, it is not needless as we cannot move out from
    // `text_ctx`. An alternative is to use `num_traits` crate, but then we have
    // convert to u32 and back which is unnecessary code bloat, still.
    // See MOD-12173
    #[allow(clippy::needless_match)]
    let cloned_value = match test_ctx.open_key_type {
        KeyType::Empty => KeyType::Empty,
        KeyType::String => KeyType::String,
        KeyType::List => KeyType::List,
        KeyType::Set => KeyType::Set,
        KeyType::ZSet => KeyType::ZSet,
        KeyType::Hash => KeyType::Hash,
        KeyType::Module => KeyType::Module,
        KeyType::Stream => KeyType::Stream,
    };

    let key = Box::new(UserKey {
        ctx,
        ty: cloned_value,
        name: keyname_user_string.user,
    });
    Box::into_raw(key).cast()
}

#[allow(non_snake_case)]
pub unsafe extern "C" fn RedisModule_CloseKey(key: *mut redis_module::raw::RedisModuleKey) {
    drop(unsafe { Box::from_raw(key.cast::<UserKey>()) });
}

#[allow(non_snake_case)]
pub unsafe extern "C" fn RedisModule_KeyType(key: *mut redis_module::raw::RedisModuleKey) -> i32 {
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
