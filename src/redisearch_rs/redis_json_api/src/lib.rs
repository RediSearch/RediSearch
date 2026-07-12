/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

mod key_values;
#[cfg(feature = "test-mock")]
pub mod mock;
mod path;
mod results;
mod value;

use ffi::RedisJSONAPI as RedisJsonApiVTable;
use redis_module::{RedisString, key::KeyFlags};
use std::{error::Error, ffi::CStr, fmt};

pub use key_values::KeyValuesIterator;
pub use path::JsonPath;
pub use results::ResultsIter;
pub use value::{JsonType, JsonValue, JsonValueRef};

/// Minimum supported API version.
pub const MIN_API_VERSION: i32 = ffi::RedisJSONAPI_MIN_API_VER as i32;

/// Latest API version (V8).
pub const LATEST_API_VERSION: i32 = 8;

/// The root JSON path.
pub const JSON_ROOT: &CStr = c"$";

/// Handle to the RedisJSON API.
///
/// This struct provides safe access to all RedisJSON operations.
/// It is obtained by calling [`RedisJsonApi::get`] after the
/// RedisJSON module has been loaded.
///
/// # Thread Safety
///
/// The API handle can be safely shared across threads, but individual
/// operations must be performed with appropriate Redis context locking.
#[derive(Debug, Clone, Copy)]
pub struct RedisJsonApi {
    vtable: &'static RedisJsonApiVTable,
}

impl RedisJsonApi {
    /// Attempts to get a handle to the RedisJSON API.
    ///
    /// Returns `None` if the RedisJSON module is not loaded or
    /// the API version is not supported.
    ///
    /// # Safety
    ///
    /// 1. Caller must ensure the RedisJSON module is initialized.
    #[inline]
    pub unsafe fn get() -> Option<Self> {
        // Safety: once the global pointer is initialized it will not be written to again.
        let vtable_ptr = unsafe { ffi::japi };
        // Safety: Ensured by caller (1.)
        let vtable = unsafe { vtable_ptr.as_ref()? };

        // Check version compatibility
        // Safety: japi_ver is initialized alongside japi
        let version = unsafe { ffi::japi_ver };
        if version < MIN_API_VERSION {
            return None;
        }

        Some(Self { vtable })
    }

    /// Construct an API handle from a caller-provided vtable.
    #[cfg(feature = "test-mock")]
    pub const fn from_vtable(vtable: &'static RedisJsonApiVTable) -> Self {
        Self { vtable }
    }

    /// Returns the current API version.
    ///
    /// # Safety
    ///
    /// 1. Caller must ensure the RedisJSON module is initialized.
    #[inline]
    pub unsafe fn version() -> i32 {
        // Safety: Caller must ensure Redis module is initialized
        unsafe { ffi::japi_ver }
    }

    /// Opens a JSON key for reading.
    ///
    /// Returns `None` if the key doesn't exist or is not a JSON type.
    ///
    /// # Safety
    ///
    /// 1. `ctx` must be a valid Redis module context.
    pub unsafe fn open_key(
        &self,
        ctx: *mut ffi::RedisModuleCtx,
        key_name: &RedisString,
    ) -> Option<JsonValueRef<'_>> {
        let vtable = self.vtable();
        let open_key = vtable
            .openKey
            .expect("RedisJSON API function `openKey` not available");

        // Safety: ensured by caller (1.)
        let ptr = unsafe { open_key(ctx, key_name.inner.cast()) };

        if ptr.is_null() {
            None
        } else {
            Some(JsonValueRef { ptr, api: self })
        }
    }

    /// Opens a readable JSON key with the specified name.
    ///
    /// Returns `None` if the key doesn't exist or is not a JSON type.
    ///
    /// # Safety
    ///
    /// 1. `ctx` must be a valid Redis module context.
    pub unsafe fn open_key_from_str(
        &self,
        ctx: *mut ffi::RedisModuleCtx,
        key_name: &CStr,
    ) -> Option<JsonValueRef<'_>> {
        let vtable = self.vtable();
        let open_key_from_str = vtable
            .openKeyFromStr
            .expect("RedisJSON API function `openKeyFromStr` not available");

        // Safety: ensured by caller (1.)
        let ptr = unsafe { open_key_from_str(ctx, key_name.as_ptr()) };

        if ptr.is_null() {
            None
        } else {
            Some(JsonValueRef { ptr, api: self })
        }
    }

    /// Opens a readable JSON key with the specified name and flags.
    ///
    /// Returns `None` if the key doesn't exist or is not a JSON type.
    ///
    /// Only available with RedisJSON API v5 and later.
    ///
    /// # Safety
    ///
    /// 1. `ctx` must be a valid Redis module context.
    pub unsafe fn open_key_with_flags(
        &self,
        ctx: *mut ffi::RedisModuleCtx,
        key_name: &RedisString,
        flags: KeyFlags,
    ) -> Option<JsonValueRef<'_>> {
        let vtable = self.vtable();
        let open_key_with_flags = vtable
            .openKeyWithFlags
            .expect("RedisJSON API function `openKeyWithFlags` not available");

        // Safety: ensured by caller (1.)
        let ptr = unsafe {
            open_key_with_flags(
                ctx,
                key_name.inner.cast(),
                flags.bits() | redis_module::raw::REDISMODULE_READ as i32,
            )
        };

        if ptr.is_null() {
            None
        } else {
            Some(JsonValueRef { ptr, api: self })
        }
    }

    /// Gets the JSON root from an already-open [`RedisModuleKey`] handle.
    ///
    /// Returns `None` if the key is `NULL`, is not a module type, or does not hold JSON.
    /// The caller owns the key handle and must keep it open while the returned
    /// [`JsonValueRef`] is in use.
    ///
    /// Only available with RedisJSON API v8 and later.
    ///
    /// # Safety
    ///
    /// 1. `redis_key` must be a valid, open `RedisModuleKey` handle (or NULL).
    ///
    /// [`RedisModuleKey`]: ffi::RedisModuleKey
    pub unsafe fn open_from_handle(
        &self,
        redis_key: *mut ffi::RedisModuleKey,
    ) -> Option<JsonValueRef<'_>> {
        let vtable = self.vtable();
        let get_json_from_handle = vtable
            .getJsonFromHandle
            .expect("RedisJSON API function `getJsonFromHandle` not available");

        // Safety: ensured by caller (1.)
        let ptr = unsafe { get_json_from_handle(redis_key) };

        if ptr.is_null() {
            None
        } else {
            Some(JsonValueRef { ptr, api: self })
        }
    }

    pub const fn vtable(&self) -> &'static RedisJsonApiVTable {
        self.vtable
    }
}

#[derive(Debug)]
pub struct SerializeError;

impl fmt::Display for SerializeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("failed to serialize RedisJSON type")
    }
}

impl Error for SerializeError {}
