/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

extern crate redisearch_rs;

mod aggregate;
mod filter;
mod legacy;

redis_mock::mock_or_stub_missing_redis_c_symbols!();

fn init_allocator() {
    static INIT: std::sync::Once = std::sync::Once::new();
    INIT.call_once(|| {
        // SAFETY: all tests using these API pointers initialize them through
        // this Once before use; the allocator remains unchanged afterwards.
        unsafe { redis_module::RedisModule_Alloc = Some(rm_alloc_impl) };
        // SAFETY: the same Once protects the matching deallocator.
        unsafe { redis_module::RedisModule_Free = Some(rm_free_impl) };
    });
}
