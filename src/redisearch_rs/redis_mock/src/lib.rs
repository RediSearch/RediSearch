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
//! In particular, it redirects [its heap allocation facilities](https://redis.io/docs/latest/develop/reference/modules/modules-api-ref/#heap-allocation-raw-functions)
//! to use Rust's global allocator.
//! This is particularly useful when benchmarking a Rust re-implementation against the original
//! C code, since it levels the playing field by forcing both to use the same memory allocator.

pub mod allocator;

#[macro_export]
/// A macro to define Redis' allocation symbols in terms of Rust's global allocator.
///
/// It's designed to be used in tests and benchmarks.
macro_rules! bind_redis_alloc_symbols_to_mock_impl {
    () => {
        #[unsafe(no_mangle)]
        #[allow(non_upper_case_globals)]
        pub static mut RedisModule_Alloc: Option<
            unsafe extern "C" fn(bytes: usize) -> *mut c_void,
        > = Some(redis_mock::allocator::alloc_shim);

        #[unsafe(no_mangle)]
        #[allow(non_upper_case_globals)]
        pub static mut RedisModule_Realloc: Option<
            unsafe extern "C" fn(ptr: *mut c_void, bytes: usize) -> *mut c_void,
        > = Some(redis_mock::allocator::realloc_shim);

        #[unsafe(no_mangle)]
        #[allow(non_upper_case_globals)]
        pub static mut RedisModule_Free: Option<unsafe extern "C" fn(ptr: *mut c_void)> =
            Some(redis_mock::allocator::free_shim);

        #[unsafe(no_mangle)]
        #[allow(non_upper_case_globals)]
        pub static mut RedisModule_Calloc: Option<
            unsafe extern "C" fn(count: usize, size: usize) -> *mut c_void,
        > = Some(redis_mock::allocator::calloc_shim);
    };
}
