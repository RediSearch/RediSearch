/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ptr::NonNull;

/// Initializes a global subscriber that reports Rust `tracing` traces through `redismodule` logging.
#[unsafe(no_mangle)]
pub extern "C" fn TracingRedisModule_Init(ctx: Option<NonNull<ffi::RedisModuleCtx>>) {
    tracing_redismodule::init(ctx);
}
