/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use super::RedisJsonApi;
use crate::{JsonValueRef, SerializeError};
use redis_module::RedisString;
use std::os::raw::c_void;
use std::ptr::NonNull;

/// An iterator over JSON query results.
///
/// This iterator is returned by [`JsonValueRef::get`] and yields
/// all values matching a JSON path expression.
// TODO should be lending iterator
pub struct ResultsIter<'a> {
    ptr: NonNull<c_void>,
    next: unsafe extern "C" fn(ffi::JSONResultsIterator) -> ffi::RedisJSON,
    free: unsafe extern "C" fn(ffi::JSONResultsIterator),
    len: unsafe extern "C" fn(ffi::JSONResultsIterator) -> usize,
    reset_iter: unsafe extern "C" fn(ffi::JSONResultsIterator),
    get_json_from_iter: unsafe extern "C" fn(
        *const c_void,
        *mut ffi::RedisModuleCtx,
        *mut *mut ffi::RedisModuleString,
    ) -> i32,
    api: &'a RedisJsonApi,
}

impl Drop for ResultsIter<'_> {
    fn drop(&mut self) {
        // Safety: `ptr` is valid by construction.
        unsafe { (self.free)(self.ptr.as_ptr()) }
    }
}

impl<'a> ResultsIter<'a> {
    /// Construct a new `ResultsIter` from a raw pointer.
    ///
    /// # Safety
    ///
    /// 1. `ptr` must be a valid ptr obtained from `get`.
    pub(crate) unsafe fn from_non_null(ptr: NonNull<c_void>, api: &'a RedisJsonApi) -> Self {
        let vtable = api.vtable();

        let next = vtable
            .next
            .expect("RedisJSON API function `next` not available");
        let free = vtable
            .freeIter
            .expect("RedisJSON API function `freeIter` not available");
        let len = vtable
            .len
            .expect("RedisJSON API function `len` not available");
        let reset_iter = vtable
            .resetIter
            .expect("RedisJSON API function `resetIter` not available");
        let get_json_from_iter = vtable
            .getJSONFromIter
            .expect("RedisJSON API function `getJSONFromIter` not available");

        Self {
            ptr,
            next,
            free,
            len,
            reset_iter,
            get_json_from_iter,
            api,
        }
    }

    /// Returns the number of results this iterator can yield.
    pub fn len(&self) -> usize {
        // Safety: `ptr` is valid by construction.
        unsafe { (self.len)(self.ptr.as_ptr()) }
    }

    /// Returns `true` if the iterator contains no results.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Resets the iterator to the beginning.
    pub fn reset(&mut self) {
        // Safety: `ptr` is valid by construction.
        unsafe { (self.reset_iter)(self.ptr.as_ptr()) };
    }

    /// Serializes all results in this iterator to a JSON string.
    ///
    /// # Safety
    ///
    /// `ctx` must be a valid Redis module context.
    #[inline]
    pub unsafe fn serialize(
        &self,
        ctx: *mut ffi::RedisModuleCtx,
    ) -> Result<RedisString, SerializeError> {
        let mut str: *mut ffi::RedisModuleString = std::ptr::null_mut();

        // Safety: `ptr` and `ctx` are valid by construction/caller guarantee
        let status = unsafe { (self.get_json_from_iter)(self.ptr.as_ptr(), ctx, &mut str) };

        if status == ffi::REDISMODULE_OK as i32 {
            Ok(RedisString::from_redis_module_string(
                ctx.cast(),
                str.cast(),
            ))
        } else {
            Err(SerializeError)
        }
    }

    /// Returns the next value in the iterator.
    ///
    /// Returns `None` when all values have been consumed.
    pub fn next(&self) -> Option<JsonValueRef<'_>> {
        // Safety: `ptr` is valid by construction.
        let raw = unsafe { (self.next)(self.ptr.as_ptr()) };

        if raw.is_null() {
            None
        } else {
            // Safety: we obtained the `raw` from calling `next`.
            Some(unsafe { JsonValueRef::from_raw(raw, self.api) })
        }
    }
}
