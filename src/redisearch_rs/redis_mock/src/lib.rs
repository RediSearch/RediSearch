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
//! The module [mock] contains functions like the `reply` functions that need mocking.
//!
//! The heap allocations are redirected to Rust's global allocator, using the
//! [`bind_alloc_redis_symbols_to_mock_impl`] macro. See [Redis Module heap allocation facilities](https://redis.io/docs/latest/develop/reference/modules/modules-api-ref/#heap-allocation-raw-functions)
//! The rust side re-implementations are in the module [`allocator`].
//!
//! This is particularly useful when benchmarking a Rust re-implementation against the original
//! C code, since it levels the playing field by forcing both to use the same memory allocator.

pub mod allocator;
pub mod command;
pub mod mock;

/// A macro to define Redis' allocation symbols in terms of Rust's global allocator for usage in an outside crate.
///
/// It's designed to be used in tests and benchmarks.
#[macro_export]
macro_rules! bind_redis_alloc_symbols_to_mock_impl {
    () => {
        use redis_mock::bind_alloc_internal_macro;
        bind_alloc_internal_macro!(redis_mock);
    };
}

/// A macro to define Redis' allocation symbols in terms of Rust's global allocator
///
/// `c_id` defines the name of the crate, it is used by tests here with `crate` or with `redis_mock`
/// in outside crates. Use [bind_redis_alloc_symbols_to_mock_impl] to bind the symbols from an outside crate.
///
/// It's designed to be used in tests and benchmarks.
#[macro_export]
macro_rules! bind_alloc_internal_macro {
    ($c_id:ident) => {
        #[unsafe(no_mangle)]
        #[allow(non_upper_case_globals)]
        pub static mut RedisModule_Alloc: Option<
            unsafe extern "C" fn(bytes: usize) -> *mut c_void,
        > = Some($c_id::allocator::alloc_shim);

        #[unsafe(no_mangle)]
        #[allow(non_upper_case_globals)]
        pub static mut RedisModule_Realloc: Option<
            unsafe extern "C" fn(ptr: *mut c_void, bytes: usize) -> *mut c_void,
        > = Some($c_id::allocator::realloc_shim);

        #[unsafe(no_mangle)]
        #[allow(non_upper_case_globals)]
        pub static mut RedisModule_Calloc: Option<
            unsafe extern "C" fn(count: usize, size: usize) -> *mut c_void,
        > = Some($c_id::allocator::calloc_shim);

        #[unsafe(no_mangle)]
        #[allow(non_upper_case_globals)]
        pub static mut RedisModule_TryAlloc: Option<
            unsafe extern "C" fn(bytes: usize) -> *mut c_void,
        > = Some($c_id::allocator::alloc_shim);

        #[unsafe(no_mangle)]
        #[allow(non_upper_case_globals)]
        pub static mut RedisModule_TryRealloc: Option<
            unsafe extern "C" fn(ptr: *mut c_void, bytes: usize) -> *mut c_void,
        > = Some($c_id::allocator::realloc_shim);

        #[unsafe(no_mangle)]
        #[allow(non_upper_case_globals)]
        pub static mut RedisModule_TryCalloc: Option<
            unsafe extern "C" fn(count: usize, size: usize) -> *mut c_void,
        > = Some($c_id::allocator::calloc_shim);

        #[unsafe(no_mangle)]
        #[allow(non_upper_case_globals)]
        pub static mut RedisModule_Free: Option<unsafe extern "C" fn(ptr: *mut c_void)> =
            Some($c_id::allocator::free_shim);

        #[unsafe(no_mangle)]
        #[allow(non_upper_case_globals)]
        pub static mut RedisModule_FreeString: Option<unsafe extern "C" fn(ptr: *mut c_void)> =
            Some($c_id::allocator::free_shim);

        #[unsafe(no_mangle)]
        #[allow(non_upper_case_globals)]
        pub static mut sdsfree: Option<unsafe extern "C" fn(ptr: *mut c_void)> =
            Some($c_id::allocator::free_shim);
    };
}

#[cfg(test)]
mod tests {

    use std::ffi::c_void;
    bind_alloc_internal_macro!(crate);

    // helps to check if the symbols are defined, e.g. the lib-path is correct
    // beware not part of the default workspace: you can use:
    // run `cargo test -p redis_mock` to test this
    #[test]
    fn test_symbols_of_ffi_found() {
        // Safety: Its safe to call that allocation, it leaks in this test though.
        unsafe {
            ffi::RS_NumVal(42.0);
        }
    }
}
