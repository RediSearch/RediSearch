/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::RedisModuleCtx;

unsafe extern "C" {
    static mut RSDummyContext: *mut RedisModuleCtx;
}

/// Get the RediSearch module context.
///
/// Safety:
/// - The Redis module must be initialized. Therefore,
///   this function is Undefined Behavior in unit tests.
#[inline]
pub unsafe fn redisearch_module_context() -> *mut RedisModuleCtx {
    RSDummyContext
}
