/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use redis_module::RedisString;

use std::ffi::CStr;

use super::RedisJsonApi;

use std::ffi::c_void;

use std::ptr::NonNull;

pub struct JsonPath<'a> {
    pub(crate) ptr: NonNull<c_void>,
    pub(crate) free: unsafe extern "C" fn(*const c_void),
    pub(crate) api: &'a RedisJsonApi,
}

impl Drop for JsonPath<'_> {
    fn drop(&mut self) {
        // Safety: `ptr` is valid by construction.
        unsafe { (self.free)(self.ptr.as_ptr()) }
    }
}

impl<'a> JsonPath<'a> {
    /// Parses a JSON path expression.
    ///
    /// Returns the parsed path on success, or an error message on failure.
    ///
    /// Only available with RedisJSON API v2 and later.
    ///
    /// # Safety
    ///
    /// 1. `ctx` must be a valid Redis module context
    pub unsafe fn parse(
        path: &CStr,
        ctx: *mut ffi::RedisModuleCtx,
        api: &'a RedisJsonApi,
    ) -> Result<Self, RedisString> {
        let vtable = api.vtable();
        let path_parse = vtable
            .pathParse
            .expect("RedisJSON API function `pathParse` not available");

        let mut err_msg: *mut ffi::RedisModuleString = std::ptr::null_mut();

        // Safety: ensured by caller (1.)
        let ptr = unsafe { path_parse(path.as_ptr(), ctx, &raw mut err_msg) };

        if let Some(ptr) = NonNull::new(ptr as *mut c_void) {
            let path_free = vtable
                .pathFree
                .expect("RedisJSON API function `pathFree` not available");

            Ok(Self {
                ptr,
                free: path_free,
                api,
            })
        } else {
            Err(RedisString::from_redis_module_string(
                ctx.cast(),
                err_msg.cast(),
            ))
        }
    }

    /// Returns `true` if this path selects at most one value.
    ///
    /// A path is "single" if it doesn't contain wildcards or recursive
    /// descent operators that could match multiple values.
    ///
    /// Only available with RedisJSON API v2 and later.
    pub fn is_single(&self) -> bool {
        let vtable = self.api.vtable();
        let path_is_single = vtable
            .pathIsSingle
            .expect("RedisJSON API function `pathIsSingle` not available");

        // Safety: `ptr` is valid by construction.
        unsafe { path_is_single(self.ptr.as_ptr()) != 0 }
    }

    /// Returns `true` if this path has a defined iteration order.
    ///
    /// Paths with defined order will always return results in the same
    /// order when applied to the same document. Paths with wildcards
    /// or recursive descent may not have a defined order.
    ///
    /// Only available with RedisJSON API v2 and later.
    pub fn path_has_defined_order(&self) -> bool {
        let vtable = self.api.vtable();
        let path_has_defined_order = vtable
            .pathHasDefinedOrder
            .expect("RedisJSON API function `pathHasDefinedOrder` not available");

        // Safety: `ptr` is valid by construction.
        unsafe { path_has_defined_order(self.ptr.as_ptr()) != 0 }
    }
}

/// An owned, compiled JSONPath handle, evaluated with
/// [`JsonValueRef::get_with_path`](crate::JsonValueRef::get_with_path).
///
/// Unlike [`JsonPath`], it stores the `pathFree` pointer instead of borrowing
/// the [`RedisJsonApi`], so it can be cached and dropped independently of the
/// API handle's lifetime.
#[derive(Debug)]
pub struct CompiledPath {
    ptr: NonNull<c_void>,
    free: unsafe extern "C" fn(*const c_void),
}

impl Drop for CompiledPath {
    fn drop(&mut self) {
        // Safety: `ptr` came from `pathParse` and is freed exactly once, here.
        unsafe { (self.free)(self.ptr.as_ptr()) }
    }
}

impl CompiledPath {
    /// Compiles `path` into a reusable handle, returning `None` if it does not
    /// parse (the caller treats an unparseable path as "no match").
    ///
    /// Only available with RedisJSON API v2 and later.
    ///
    /// # Safety
    ///
    /// 1. `ctx` must be a valid Redis module context.
    pub unsafe fn parse(
        path: &CStr,
        ctx: *mut ffi::RedisModuleCtx,
        api: &RedisJsonApi,
    ) -> Option<Self> {
        let vtable = api.vtable();
        let path_parse = vtable
            .pathParse
            .expect("RedisJSON API function `pathParse` not available");
        let path_free = vtable
            .pathFree
            .expect("RedisJSON API function `pathFree` not available");

        let mut err_msg: *mut ffi::RedisModuleString = std::ptr::null_mut();

        // Safety: ensured by caller (1.)
        let ptr = unsafe { path_parse(path.as_ptr(), ctx, &raw mut err_msg) };

        match NonNull::new(ptr as *mut c_void) {
            Some(ptr) => Some(Self {
                ptr,
                free: path_free,
            }),
            None => {
                // On failure `pathParse` transfers ownership of the error string; free it.
                if !err_msg.is_null() {
                    // `err_msg` is owned by us on the failure path; dropping frees it.
                    drop(RedisString::from_redis_module_string(
                        ctx.cast(),
                        err_msg.cast(),
                    ));
                }
                None
            }
        }
    }

    /// The raw compiled-path pointer, for passing to `getWithPath`.
    pub(crate) const fn as_ptr(&self) -> *const c_void {
        self.ptr.as_ptr()
    }
}
