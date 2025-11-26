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
pub mod call;
pub mod globals;
pub mod key;
pub mod scan_key_cursor;
pub mod string;

use std::ffi::CString;

use call::*;
pub use ffi;
use key::*;
use redis_module::KeyType;
use scan_key_cursor::*;
use string::*;

/// A test context that can be used to hold state for testing with the mock.
pub struct TestContext {
    /// The key type that will be returned by `RedisModule_KeyType` when opening a key with this context.
    pub(crate) open_key_type: redis_module::KeyType,

    /// Contains key value pairs to be injected during scan key cursor iterations or
    /// HGETALL calls.
    key_value_injections: Vec<(CString, CString)>,
}

/// A builder for [TestContext] to ensure the internal vectors will not grow after construction.
///
/// A grow on the internal vectors would invalidate any pointers handed out.
pub struct TestContextBuilder {
    inner: TestContext,
}

impl TestContextBuilder {
    pub fn set_key_values(&mut self, kvs: Vec<(CString, CString)>) {
        self.inner.key_value_injections = kvs;
    }

    pub fn inject_key_value(&mut self, key: CString, value: CString) {
        self.inner.key_value_injections.push((key, value));
    }

    pub const fn with_key_type(&mut self, ty: &redis_module::KeyType) -> &mut Self {
        // todo: Implement Clone and or Copy for KeyType in redis-module crate
        // to avoid this match, it is not needless as we cannot move out from
        // `text_ctx`. An alternative is to use `num_traits` crate, but then we have
        // convert to u32 and back which is unnecessary code bloat, still.
        // See MOD-12173
        self.inner.open_key_type = match ty {
            KeyType::Empty => KeyType::Empty,
            KeyType::String => KeyType::String,
            KeyType::List => KeyType::List,
            KeyType::Set => KeyType::Set,
            KeyType::ZSet => KeyType::ZSet,
            KeyType::Hash => KeyType::Hash,
            KeyType::Module => KeyType::Module,
            KeyType::Stream => KeyType::Stream,
        };
        self
    }

    pub fn build(self) -> TestContext {
        self.inner
    }
}

impl TestContext {
    pub const fn builder() -> TestContextBuilder {
        TestContextBuilder {
            inner: Self {
                key_value_injections: vec![],
                open_key_type: redis_module::KeyType::Empty,
            },
        }
    }

    pub const fn access_key_values(&self) -> &Vec<(CString, CString)> {
        &self.key_value_injections
    }
}

impl Default for TestContext {
    fn default() -> Self {
        Self::builder().build()
    }
}

/// Initializes the Redis module mock by binding the relevant symbols.
///
/// This function must be called before mocks of Redis module API functions
/// are called by test code.
#[allow(clippy::undocumented_unsafe_blocks)]
pub fn init_redis_module_mock() {
    // register string methods
    unsafe { redis_module::raw::RedisModule_CreateString = Some(RedisModule_CreateString) };
    unsafe { redis_module::raw::RedisModule_StringPtrLen = Some(RedisModule_StringPtrLen) };
    unsafe { redis_module::raw::RedisModule_FreeString = Some(RedisModule_FreeString) };

    // register key methods
    unsafe { redis_module::raw::RedisModule_OpenKey = Some(RedisModule_OpenKey) };
    unsafe { redis_module::raw::RedisModule_CloseKey = Some(RedisModule_CloseKey) };
    unsafe { redis_module::raw::RedisModule_KeyType = Some(RedisModule_KeyType) };

    // register scan key cursor methods
    unsafe { redis_module::raw::RedisModule_ScanCursorCreate = Some(RedisModule_ScanCursorCreate) };
    unsafe {
        redis_module::raw::RedisModule_ScanCursorDestroy = Some(RedisModule_ScanCursorDestroy)
    };
    unsafe { redis_module::raw::RedisModule_ScanKey = Some(RedisModule_ScanKey) };

    // Register call reply functions
    unsafe { redis_module::raw::RedisModule_CallReplyType = Some(RedisModule_CallReplyType) };
    unsafe { redis_module::raw::RedisModule_CallReplyLength = Some(RedisModule_CallReplyLength) };
    unsafe {
        redis_module::raw::RedisModule_CallReplyArrayElement =
            Some(RedisModule_CallReplyArrayElement)
    };
    unsafe {
        redis_module::raw::RedisModule_CallReplyStringPtr = Some(RedisModule_CallReplyStringPtr)
    };
    unsafe { redis_module::raw::RedisModule_FreeCallReply = Some(RedisModule_FreeCallReply) };

    // Cast the variadic C function pointer to the expected Redis module function pointer type.
    //
    // The C function signature is: RedisModuleCallReply* RedisModule_CallImpl(RedisModuleCtx *ctx, const char *cmdname, const char *fmt, ...)
    // We use transmute to cast between function pointer types since Rust doesn't support variadic function types
    // in function pointer signatures, but the C ABI will handle the variadic arguments correctly.
    //
    // First cast to raw pointer, then transmute to the expected function pointer type.

    let raw_ptr = RedisModule_CallHgetAll as *const ();
    let new_ftor = unsafe {
        std::mem::transmute::<
            *const (),
            unsafe extern "C" fn(
                *mut redis_module::RedisModuleCtx,
                *const ::std::os::raw::c_char,
                *const ::std::os::raw::c_char,
                ...
            ) -> *mut redis_module::RedisModuleCallReply,
        >(raw_ptr)
    };
    // Safety: This works as long as we only set this once during initialization.
    // in other words, if we want to support different implementations of RedisModule_Call than
    // HgetAll, we would need to add more machinery to swap them safely.
    //
    // In that case a call function that dispatches to different implementations based on the cmdname
    // would be more appropriate.
    unsafe { redis_module::raw::RedisModule_Call = Some(new_ftor) }
}

#[macro_export]
/// A macro to define Redis' allocation symbols in terms of Rust's global allocator.
///
/// It's designed to be used in tests and benchmarks.
macro_rules! bind_redis_alloc_symbols_to_mock_impl {
    () => {
        #[unsafe(no_mangle)]
        unsafe extern "C" fn rm_alloc_impl(size: usize) -> *mut std::ffi::c_void {
            // $crate::__libc::malloc(size)
            redis_mock::allocator::alloc_shim(size)
        }

        #[unsafe(no_mangle)]
        unsafe extern "C" fn rm_calloc_impl(nmemb: usize, size: usize) -> *mut std::ffi::c_void {
            redis_mock::allocator::calloc_shim(nmemb, size)
        }

        #[unsafe(no_mangle)]
        unsafe extern "C" fn rm_realloc_impl(
            ptr: *mut std::ffi::c_void,
            size: usize,
        ) -> *mut std::ffi::c_void {
            redis_mock::allocator::realloc_shim(ptr, size)
        }

        #[unsafe(no_mangle)]
        unsafe extern "C" fn rm_free_impl(ptr: *mut std::ffi::c_void) {
            redis_mock::allocator::free_shim(ptr)
        }

        #[unsafe(no_mangle)]
        #[allow(non_upper_case_globals)]
        pub static mut RedisModule_Alloc: unsafe extern "C" fn(usize) -> *mut std::ffi::c_void =
            rm_alloc_impl;

        #[unsafe(no_mangle)]
        #[allow(non_upper_case_globals)]
        pub static mut RedisModule_Calloc: unsafe extern "C" fn(
            usize,
            usize,
        ) -> *mut std::ffi::c_void = rm_calloc_impl;

        #[unsafe(no_mangle)]
        #[allow(non_upper_case_globals)]
        pub static mut RedisModule_Realloc: unsafe extern "C" fn(
            *mut std::ffi::c_void,
            usize,
        ) -> *mut std::ffi::c_void = rm_realloc_impl;

        #[unsafe(no_mangle)]
        #[allow(non_upper_case_globals)]
        pub static mut RedisModule_Free: unsafe extern "C" fn(*mut std::ffi::c_void) = rm_free_impl;

        #[unsafe(no_mangle)]
        #[allow(non_upper_case_globals)]
        pub static mut RSDummyContext: *mut $crate::ffi::RedisModuleCtx =
            $crate::globals::redis_module_ctx();
    };
}
