mod key_values;
mod path;
mod results;
mod value;

use ffi::RedisJSONAPI as RedisJsonApiVTable;
use redis_module::RedisString;
use std::ffi::CStr;

pub use key_values::KeyValuesIterator;
pub use path::JsonPath;
pub use results::ResultsIter;
pub use value::{JsonType, JsonValue, JsonValueRef};

/// Minimum supported API version.
pub const MIN_API_VERSION: i32 = ffi::RedisJSONAPI_MIN_API_VER as i32;

/// Latest API version (V6).
pub const LATEST_API_VERSION: i32 = 6;

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
    /// This function reads the global `japi` pointer. The caller must ensure:
    /// - The Redis module system has been initialized
    /// - This is called from a context where reading global state is safe
    #[inline]
    pub unsafe fn get() -> Option<Self> {
        // SAFETY: Reading the global japi pointer. Caller ensures this is safe.
        let vtable_ptr = unsafe { ffi::japi };
        let vtable = unsafe { vtable_ptr.as_ref()? };

        // Check version compatibility
        // SAFETY: japi_ver is initialized alongside japi
        let version = unsafe { ffi::japi_ver };
        if version < MIN_API_VERSION {
            return None;
        }

        Some(Self { vtable })
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
    /// 1. `ctx` must be a valid Redis module context
    pub unsafe fn open_key(
        &self,
        ctx: *mut ffi::RedisModuleCtx,
        key_name: &RedisString,
    ) -> Option<JsonValueRef<'_>> {
        let vtable = self.vtable();
        let open_key = vtable
            .openKey
            .expect("RedisJSON API function `openKey` not available");

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
    /// 1. `ctx` must be a valid Redis module context
    pub unsafe fn open_key_from_str(
        &self,
        ctx: *mut ffi::RedisModuleCtx,
        key_name: &CStr,
    ) -> Option<JsonValueRef<'_>> {
        let vtable = self.vtable();
        let open_key_from_str = vtable
            .openKeyFromStr
            .expect("RedisJSON API function `openKeyFromStr` not available");

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
    /// # Safety
    ///
    /// 1. `ctx` must be a valid Redis module context
    pub unsafe fn open_key_with_flags(
        &self,
        ctx: *mut ffi::RedisModuleCtx,
        key_name: &RedisString,
        flags: i32,
    ) -> Option<JsonValueRef<'_>> {
        let vtable = self.vtable();
        let open_key_with_flags = vtable
            .openKeyWithFlags
            .expect("RedisJSON API function `openKeyWithFlags` not available");

        let ptr = unsafe { open_key_with_flags(ctx, key_name.inner.cast(), flags) };

        if ptr.is_null() {
            None
        } else {
            Some(JsonValueRef { ptr, api: self })
        }
    }

    // pub fn is_json(&self, key: &RedisKey) -> bool {
    // // Return 1 if type of key is JSON
    // int (*isJSON)(RedisModuleKey *redis_key);

    //     let vtable = self.vtable();
    //     let is_json = vtable.isJSON.expect("msg");

    //     // is_json(key.)
    // }

    pub const fn vtable(&self) -> &'static RedisJsonApiVTable {
        self.vtable
    }
}
