/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Safe, idiomatic Rust bindings to the RedisJSON API.
//!
//! This crate provides a safe interface to the RedisJSON module's C API,
//! allowing RediSearch to interact with JSON documents stored in Redis.
//!
//! # Safety
//!
//! The RedisJSON API is accessed through a global function pointer table that
//! is initialized when the RedisJSON module is loaded. All operations through
//! this API require that:
//!
//! 1. The RedisJSON module has been loaded and initialized
//! 2. The API pointer has been obtained via [`RedisJsonApi::get`]
//! 3. All pointer-based values are valid for their stated lifetimes
//!
//! This crate encapsulates these safety requirements in its API design.

#![allow(
    non_upper_case_globals,
    non_camel_case_types,
    non_snake_case,
    dead_code,
    clippy::missing_safety_doc,
    clippy::missing_const_for_fn
)]

mod error;
mod iter;
mod path;
mod value;

pub use error::{JsonApiError, Result};
pub use iter::{JsonResultsIter, KeyValuesIter};
pub use path::JsonPath;
pub use value::{JsonType, JsonValue, OwnedJsonValue};

use std::ffi::CStr;
use std::os::raw::{c_char, c_int, c_void};
use std::ptr::NonNull;

use ffi::{RedisModuleCtx, RedisModuleKey, RedisModuleString};

// Include the generated bindings
mod sys {
    #![allow(
        non_upper_case_globals,
        non_camel_case_types,
        non_snake_case,
        improper_ctypes,
        dead_code,
        clippy::ptr_offset_with_cast,
        clippy::upper_case_acronyms,
        clippy::useless_transmute,
        clippy::multiple_unsafe_ops_per_block,
        clippy::undocumented_unsafe_blocks,
        clippy::missing_safety_doc,
        clippy::missing_const_for_fn
    )]
    include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
}

pub use sys::JSONType as RawJsonType;

/// Minimum supported API version.
pub const MIN_API_VERSION: i32 = 6;

/// Latest API version.
pub const LATEST_API_VERSION: i32 = sys::RedisJSONAPI_LATEST_API_VER as i32;

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
#[derive(Clone, Copy)]
pub struct RedisJsonApi {
    api: NonNull<sys::RedisJSONAPI>,
}

// SAFETY: The RedisJSONAPI struct contains only function pointers,
// which are inherently thread-safe to call (the actual thread safety
// depends on Redis's own thread model and GIL).
unsafe impl Send for RedisJsonApi {}
unsafe impl Sync for RedisJsonApi {}

impl RedisJsonApi {
    /// Attempts to get a handle to the RedisJSON API.
    ///
    /// Returns `None` if the RedisJSON module is not loaded or
    /// the API version is not supported.
    ///
    /// # Safety
    ///
    /// This function reads the global `japi` pointer. The caller must ensure:
    /// - The Redis module system has been initialized
    /// - This is called from a context where reading global state is safe
    #[inline]
    pub unsafe fn get() -> Option<Self> {
        // SAFETY: Reading the global japi pointer. Caller ensures this is safe.
        let api_ptr = unsafe { ffi::japi };
        let api = NonNull::new(api_ptr)?;

        // Check version compatibility
        // SAFETY: japi_ver is initialized alongside japi
        let version = unsafe { ffi::japi_ver };
        if version < MIN_API_VERSION {
            return None;
        }

        Some(Self { api })
    }

    /// Returns the current API version.
    ///
    /// # Safety
    ///
    /// Caller must ensure the Redis module system is initialized.
    #[inline]
    pub unsafe fn version() -> i32 {
        // SAFETY: Caller ensures Redis module is initialized
        unsafe { ffi::japi_ver }
    }

    /// Opens a JSON key for reading.
    ///
    /// Returns `None` if the key doesn't exist or is not a JSON type.
    ///
    /// # Safety
    ///
    /// - `ctx` must be a valid Redis module context
    /// - `key_name` must be a valid Redis string
    #[inline]
    pub unsafe fn open_key(
        &self,
        ctx: *mut RedisModuleCtx,
        key_name: *mut RedisModuleString,
    ) -> Option<JsonValue<'_>> {
        let api = self.api();
        let open_key = api.openKey?;
        // SAFETY: Caller ensures ctx and key_name are valid
        let ptr = unsafe { open_key(ctx, key_name) };
        NonNull::new(ptr as *mut c_void).map(|ptr| JsonValue::new(ptr, self))
    }

    /// Opens a JSON key for reading from a C string path.
    ///
    /// Returns `None` if the key doesn't exist or is not a JSON type.
    ///
    /// # Safety
    ///
    /// - `ctx` must be a valid Redis module context
    /// - `path` must be a valid null-terminated C string
    #[inline]
    pub unsafe fn open_key_from_str(
        &self,
        ctx: *mut RedisModuleCtx,
        path: *const c_char,
    ) -> Option<JsonValue<'_>> {
        let api = self.api();
        let open_key_from_str = api.openKeyFromStr?;
        // SAFETY: Caller ensures ctx and path are valid
        let ptr = unsafe { open_key_from_str(ctx, path) };
        NonNull::new(ptr as *mut c_void).map(|ptr| JsonValue::new(ptr, self))
    }

    /// Opens a JSON key for reading with specific flags.
    ///
    /// Returns `None` if the key doesn't exist or is not a JSON type.
    ///
    /// # Safety
    ///
    /// - `ctx` must be a valid Redis module context
    /// - `key_name` must be a valid Redis string
    #[inline]
    pub unsafe fn open_key_with_flags(
        &self,
        ctx: *mut RedisModuleCtx,
        key_name: *mut RedisModuleString,
        flags: c_int,
    ) -> Option<JsonValue<'_>> {
        let api = self.api();
        let open_key_with_flags = api.openKeyWithFlags?;
        // SAFETY: Caller ensures ctx and key_name are valid
        let ptr = unsafe { open_key_with_flags(ctx, key_name, flags) };
        NonNull::new(ptr as *mut c_void).map(|ptr| JsonValue::new(ptr, self))
    }

    /// Checks if a Redis key contains JSON data.
    ///
    /// # Safety
    ///
    /// `redis_key` must be a valid, open Redis module key.
    #[inline]
    pub unsafe fn is_json(&self, redis_key: *mut RedisModuleKey) -> bool {
        let api = self.api();
        if let Some(is_json) = api.isJSON {
            // SAFETY: Caller ensures redis_key is valid
            unsafe { is_json(redis_key) == 1 }
        } else {
            false
        }
    }

    /// Parses a JSON path expression.
    ///
    /// Returns the parsed path on success, or an error message on failure.
    ///
    /// # Safety
    ///
    /// - `ctx` must be a valid Redis module context
    /// - `path` must be a valid null-terminated C string
    #[inline]
    pub unsafe fn parse_path(
        &self,
        path: *const c_char,
        ctx: *mut RedisModuleCtx,
    ) -> std::result::Result<JsonPath<'_>, *mut RedisModuleString> {
        let api = self.api();
        let path_parse = api.pathParse.expect("pathParse not available");

        let mut err_msg: *mut RedisModuleString = std::ptr::null_mut();
        // SAFETY: Caller ensures path and ctx are valid
        let ptr = unsafe { path_parse(path, ctx, &mut err_msg) };

        if let Some(ptr) = NonNull::new(ptr as *mut c_void) {
            Ok(JsonPath::new(ptr, self))
        } else {
            Err(err_msg)
        }
    }

    /// Allocates a new JSON value container.
    ///
    /// This is used for receiving JSON values from iteration operations.
    /// The returned value must be freed with [`free_json`](Self::free_json).
    #[inline]
    pub fn alloc_json(&self) -> OwnedJsonValue<'_> {
        let api = self.api();
        let alloc_json = api.allocJson.expect("allocJson not available");
        // SAFETY: allocJson always returns a valid pointer
        let ptr = unsafe { alloc_json() };
        OwnedJsonValue::new(
            NonNull::new(ptr).expect("allocJson returned null"),
            self,
        )
    }

    /// Gets a reference to the raw API struct.
    #[inline]
    fn api(&self) -> &sys::RedisJSONAPI {
        // SAFETY: The NonNull was validated at construction time
        unsafe { self.api.as_ref() }
    }

    /// Returns the raw API pointer.
    ///
    /// This is useful for advanced use cases where direct API access is needed.
    #[inline]
    pub const fn as_ptr(&self) -> *const sys::RedisJSONAPI {
        self.api.as_ptr()
    }
}

impl std::fmt::Debug for RedisJsonApi {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisJsonApi")
            .field("ptr", &self.api.as_ptr())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_constants() {
        assert_eq!(MIN_API_VERSION, 6);
        assert_eq!(LATEST_API_VERSION, 6);
        assert_eq!(JSON_ROOT.to_bytes(), b"$");
    }
}
