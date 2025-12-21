/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

/// Mock implementation of RedisModuleCtx for testing purposes.
#[derive(Default)]
#[repr(C)]
pub(crate) struct Ctx;

/// Mock implementation of RedisModule_GetThreadSafeContext from redismodule.h for testing purposes.
///
/// Needs to be freed using [`RedisModule_FreeThreadSafeContext`].
#[allow(non_snake_case)]
pub(crate) unsafe extern "C" fn RedisModule_GetThreadSafeContext(
    _bc: *mut redis_module::raw::RedisModuleBlockedClient,
) -> *mut redis_module::raw::RedisModuleCtx {
    let ctx = Box::new(Ctx);
    Box::into_raw(ctx).cast()
}

/// Mock implementation of RedisModule_FreeThreadSafeContext from redismodule.h for testing purposes.
///
/// Safety:
/// 1. ctx must be a valid pointer to a RedisModuleCtx created by this mock using [`RedisModule_GetThreadSafeContext`].
/// 2. The function must not be called more than once for the same context.
#[allow(non_snake_case)]
pub(crate) unsafe extern "C" fn RedisModule_FreeThreadSafeContext(
    ctx: *mut redis_module::raw::RedisModuleCtx,
) {
    // Safety: we own the memory (1) and the caller promised to call this only once (2)
    drop(unsafe { Box::from_raw(ctx.cast::<Ctx>()) });
}

/// Mock implementation of RedisModule_SubscribeToServerEvent from redismodule.h for testing purposes.
#[allow(non_snake_case)]
pub(crate) unsafe extern "C" fn RedisModule_SubscribeToServerEvent(
    _ctx: *mut redis_module::RedisModuleCtx,
    _event: redis_module::RedisModuleEvent,
    _callback: redis_module::RedisModuleEventCallback,
) -> i32 {
    0
}
