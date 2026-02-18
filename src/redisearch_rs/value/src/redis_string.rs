/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use ffi::context::redisearch_module_context;
use ffi::{RedisModule_FreeString, RedisModule_StringPtrLen, RedisModuleString};
use std::ffi::c_char;
use std::fmt;
use std::mem::MaybeUninit;

/// An owned string backed by a [`RedisModuleString`].
///
/// # Safety
///
/// - `ptr` must not be NULL and must point to a valid [`RedisModuleString`].
pub struct RedisString {
    ptr: *const RedisModuleString,
}

impl RedisString {
    /// Create a [`RedisString`] from a raw [`RedisModuleString`] pointer, taking ownership.
    ///
    /// # Safety
    ///
    /// 1. `ptr` must not be NULL and must point to a valid [`RedisModuleString`].
    /// 2. `ptr` **must not** be used or freed after this call, as this function takes ownership.
    pub const unsafe fn from_raw(ptr: *const RedisModuleString) -> Self {
        Self { ptr }
    }

    /// Returns the raw [`RedisModuleString`] pointer.
    pub const fn as_ptr(&self) -> *const RedisModuleString {
        self.ptr
    }

    /// Returns the string data pointer and length.
    pub fn as_ptr_len(&self) -> (*const c_char, u32) {
        // SAFETY: Accessing a global function pointer initialized during module load.
        let rm_str_ptr_len =
            unsafe { RedisModule_StringPtrLen }.expect("Redis module not initialized");

        let mut len = MaybeUninit::uninit();
        // SAFETY: `self.ptr` is a valid `RedisModuleString` pointer per our invariant,
        // and `len` is a valid pointer to write the length into.
        let ptr = unsafe { rm_str_ptr_len(self.ptr, len.as_mut_ptr()) };
        // SAFETY: `rm_str_ptr_len` initialized `len`.
        let len = unsafe { len.assume_init() };

        (ptr, len as u32)
    }

    /// Gets the string data as a byte slice.
    pub fn as_bytes(&self) -> &[u8] {
        let (ptr, len) = self.as_ptr_len();
        // SAFETY: `ptr` points to valid memory of `len` bytes, as guaranteed by
        // `RedisModule_StringPtrLen`.
        unsafe { std::slice::from_raw_parts(ptr as _, len as usize) }
    }
}

impl Drop for RedisString {
    fn drop(&mut self) {
        // SAFETY: Accessing the global module context, which is valid for the module lifetime.
        let ctx = unsafe { redisearch_module_context() };
        // SAFETY: Accessing a global function pointer initialized during module load.
        let free_string = unsafe { RedisModule_FreeString }.expect("Redis module not initialized");
        // SAFETY: `ctx` is a valid module context and `self.ptr` is a valid `RedisModuleString`
        // that we own and have not yet freed.
        unsafe { free_string(ctx, self.ptr as _) };
    }
}

impl fmt::Debug for RedisString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let lossy = String::from_utf8_lossy(self.as_bytes());
        f.debug_tuple("RedisString").field(&lossy).finish()
    }
}

// SAFETY: The underlying `RedisModuleString` is reference-counted and thread-safe in Redis.
unsafe impl Send for RedisString {}
// SAFETY: The underlying `RedisModuleString` is reference-counted and thread-safe in Redis.
unsafe impl Sync for RedisString {}
