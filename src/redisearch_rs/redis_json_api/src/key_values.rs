/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use super::RedisJsonApi;
use crate::JsonValue;
use redis_module::RedisString;
use std::{ffi::c_void, ptr::NonNull};

// An iterators over key value pairs if the json value is an object
pub struct KeyValuesIterator<'a> {
    pub(crate) ptr: NonNull<c_void>,
    // Get the next key-value pair
    // The caller gains ownership of `key_name`
    // The caller must pass 'ptr' which was allocated with allocJson
    pub(crate) next: unsafe extern "C" fn(
        iter: ffi::JSONKeyValuesIterator,
        key_name: *mut *mut ffi::RedisModuleString,
        ptr: ffi::RedisJSONPtr,
    ) -> i32,
    // Free the iterator
    pub(crate) free: unsafe extern "C" fn(ptr: ffi::JSONKeyValuesIterator),
    pub(crate) ctx: *mut ffi::RedisModuleCtx,
    pub(crate) api: &'a RedisJsonApi,
}

impl Drop for KeyValuesIterator<'_> {
    fn drop(&mut self) {
        // Safety: caller has promised `ptr` is valid upon construction
        unsafe { (self.free)(self.ptr.as_ptr()) }
    }
}

impl<'a> KeyValuesIterator<'a> {
    /// Construct a new `KeyValuesIterator` from a raw pointer.
    ///
    /// Only available with RedisJSON API v4 and later.
    ///
    /// # Safety
    ///
    /// 1. `ctx` must be a valid Redis module context.
    /// 2. `ptr` must be a valid ptr obtained from `getKeyValues`.
    pub(crate) unsafe fn from_non_null(
        ptr: NonNull<c_void>,
        ctx: *mut ffi::RedisModuleCtx,
        api: &'a RedisJsonApi,
    ) -> Self {
        let vtable = api.vtable();
        let next = vtable
            .nextKeyValue
            .expect("RedisJSON API function `nextKeyValue` not available");
        let free = vtable
            .freeKeyValuesIter
            .expect("RedisJSON API function `freeKeyValuesIter` not available");

        Self {
            ptr,
            next,
            free,
            ctx,
            api,
        }
    }
}

impl<'a> Iterator for KeyValuesIterator<'a> {
    type Item = (RedisString, JsonValue<'a>);

    /// Yield the next key-value pair.
    ///
    /// Only available with RedisJSON API v6 and later.
    fn next(&mut self) -> Option<Self::Item> {
        let mut key: *mut ffi::RedisModuleString = std::ptr::null_mut();
        let value = JsonValue::new(self.api);

        // Safety: `JsonValue::new` calls `allocJson` and correctly tracks ownership
        let status = unsafe { (self.next)(self.ptr.as_ptr(), &raw mut key, value.ptr) };

        if status == ffi::REDISMODULE_OK as i32 {
            let key = RedisString::from_redis_module_string(self.ctx.cast(), key.cast());
            Some((key, value))
        } else {
            debug_assert!(key.is_null());
            None
        }
    }
}
