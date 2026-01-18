/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! JSON value types and operations.

use std::os::raw::{c_char, c_double, c_int, c_longlong, c_void};
use std::ptr::NonNull;

use ffi::{RedisModuleCtx, RedisModuleString};

use crate::error::{JsonApiError, Result};
use crate::iter::{JsonResultsIter, KeyValuesIter};
use crate::{sys, RedisJsonApi};

/// The type of a JSON value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i32)]
pub enum JsonType {
    /// A JSON string.
    String = 0,
    /// A JSON integer.
    Int = 1,
    /// A JSON floating-point number.
    Double = 2,
    /// A JSON boolean.
    Bool = 3,
    /// A JSON object.
    Object = 4,
    /// A JSON array.
    Array = 5,
    /// JSON null.
    Null = 6,
}

impl JsonType {
    /// Creates a `JsonType` from the raw C enum value.
    #[inline]
    pub const fn from_raw(raw: c_int) -> Option<Self> {
        match raw {
            0 => Some(Self::String),
            1 => Some(Self::Int),
            2 => Some(Self::Double),
            3 => Some(Self::Bool),
            4 => Some(Self::Object),
            5 => Some(Self::Array),
            6 => Some(Self::Null),
            _ => None,
        }
    }

    /// Returns `true` if this is a numeric type (Int or Double).
    #[inline]
    pub const fn is_numeric(&self) -> bool {
        matches!(self, Self::Int | Self::Double)
    }

    /// Returns `true` if this is a container type (Object or Array).
    #[inline]
    pub const fn is_container(&self) -> bool {
        matches!(self, Self::Object | Self::Array)
    }

    /// Returns `true` if this is a primitive type (not Object or Array).
    #[inline]
    pub const fn is_primitive(&self) -> bool {
        !self.is_container()
    }
}

impl From<sys::JSONType> for JsonType {
    #[inline]
    fn from(raw: sys::JSONType) -> Self {
        Self::from_raw(raw as c_int).expect("invalid JSONType value")
    }
}

/// A borrowed reference to a JSON value.
///
/// This type represents a non-owning handle to a JSON value stored in Redis.
/// The lifetime parameter `'a` ties this reference to the [`RedisJsonApi`]
/// that was used to obtain it, ensuring the value remains valid.
///
/// # Safety
///
/// This type maintains a raw pointer to JSON data managed by RedisJSON.
/// The data is only valid as long as:
/// - The underlying Redis key hasn't been modified or deleted
/// - The Redis module context remains valid
#[derive(Clone, Copy)]
pub struct JsonValue<'a> {
    ptr: NonNull<c_void>,
    api: &'a RedisJsonApi,
}

impl<'a> JsonValue<'a> {
    /// Creates a new `JsonValue` from a raw pointer.
    ///
    /// # Safety
    ///
    /// The caller must ensure `ptr` points to valid JSON data.
    #[inline]
    pub(crate) const fn new(ptr: NonNull<c_void>, api: &'a RedisJsonApi) -> Self {
        Self { ptr, api }
    }

    /// Returns the type of this JSON value.
    #[inline]
    pub fn get_type(&self) -> JsonType {
        let api = self.api.api();
        let get_type = api.getType.expect("getType not available");
        // SAFETY: ptr is valid by construction
        let raw = unsafe { get_type(self.ptr.as_ptr()) };
        JsonType::from_raw(raw).expect("invalid JSON type")
    }

    /// Returns the length of this value.
    ///
    /// For strings, returns the string length.
    /// For arrays, returns the number of elements.
    /// For objects, returns the number of key-value pairs.
    /// For other types, returns an error.
    #[inline]
    pub fn len(&self) -> Result<usize> {
        let api = self.api.api();
        let get_len = api.getLen.expect("getLen not available");
        let mut count: libc::size_t = 0;
        // SAFETY: ptr is valid by construction
        let status = unsafe { get_len(self.ptr.as_ptr(), &mut count) };
        if status == ffi::REDISMODULE_OK as c_int {
            Ok(count)
        } else {
            Err(JsonApiError::OperationFailed)
        }
    }

    /// Returns `true` if this container is empty.
    #[inline]
    pub fn is_empty(&self) -> Result<bool> {
        self.len().map(|len| len == 0)
    }

    /// Returns the integer value if this is an Int type.
    #[inline]
    pub fn get_int(&self) -> Result<i64> {
        let api = self.api.api();
        let get_int = api.getInt.expect("getInt not available");
        let mut val: c_longlong = 0;
        // SAFETY: ptr is valid by construction
        let status = unsafe { get_int(self.ptr.as_ptr(), &mut val) };
        if status == ffi::REDISMODULE_OK as c_int {
            Ok(val)
        } else {
            Err(JsonApiError::TypeMismatch {
                expected: JsonType::Int,
                actual: self.get_type(),
            })
        }
    }

    /// Returns the double value if this is a Double type.
    ///
    /// Also works for Int types, converting the integer to a double.
    #[inline]
    pub fn get_double(&self) -> Result<f64> {
        let api = self.api.api();
        let get_double = api.getDouble.expect("getDouble not available");
        let mut val: c_double = 0.0;
        // SAFETY: ptr is valid by construction
        let status = unsafe { get_double(self.ptr.as_ptr(), &mut val) };
        if status == ffi::REDISMODULE_OK as c_int {
            Ok(val)
        } else {
            Err(JsonApiError::TypeMismatch {
                expected: JsonType::Double,
                actual: self.get_type(),
            })
        }
    }

    /// Returns the boolean value if this is a Bool type.
    #[inline]
    pub fn get_bool(&self) -> Result<bool> {
        let api = self.api.api();
        let get_boolean = api.getBoolean.expect("getBoolean not available");
        let mut val: c_int = 0;
        // SAFETY: ptr is valid by construction
        let status = unsafe { get_boolean(self.ptr.as_ptr(), &mut val) };
        if status == ffi::REDISMODULE_OK as c_int {
            Ok(val != 0)
        } else {
            Err(JsonApiError::TypeMismatch {
                expected: JsonType::Bool,
                actual: self.get_type(),
            })
        }
    }

    /// Returns the string value if this is a String type.
    ///
    /// The returned string slice is valid for the lifetime of this `JsonValue`.
    #[inline]
    pub fn get_string(&self) -> Result<&'a str> {
        let api = self.api.api();
        let get_string = api.getString.expect("getString not available");
        let mut str_ptr: *const c_char = std::ptr::null();
        let mut len: libc::size_t = 0;
        // SAFETY: ptr is valid by construction
        let status = unsafe { get_string(self.ptr.as_ptr(), &mut str_ptr, &mut len) };
        if status == ffi::REDISMODULE_OK as c_int {
            // SAFETY: getString returns a valid UTF-8 string pointer and length
            let bytes = unsafe { std::slice::from_raw_parts(str_ptr as *const u8, len) };
            // JSON strings are always valid UTF-8
            Ok(std::str::from_utf8(bytes).expect("invalid UTF-8 in JSON string"))
        } else {
            Err(JsonApiError::TypeMismatch {
                expected: JsonType::String,
                actual: self.get_type(),
            })
        }
    }

    /// Returns the string bytes if this is a String type.
    ///
    /// This is similar to [`get_string`](Self::get_string) but returns raw bytes
    /// without UTF-8 validation.
    #[inline]
    pub fn get_string_bytes(&self) -> Result<&'a [u8]> {
        let api = self.api.api();
        let get_string = api.getString.expect("getString not available");
        let mut str_ptr: *const c_char = std::ptr::null();
        let mut len: libc::size_t = 0;
        // SAFETY: ptr is valid by construction
        let status = unsafe { get_string(self.ptr.as_ptr(), &mut str_ptr, &mut len) };
        if status == ffi::REDISMODULE_OK as c_int {
            // SAFETY: getString returns a valid pointer and length
            Ok(unsafe { std::slice::from_raw_parts(str_ptr as *const u8, len) })
        } else {
            Err(JsonApiError::TypeMismatch {
                expected: JsonType::String,
                actual: self.get_type(),
            })
        }
    }

    /// Serializes this JSON value to a Redis module string.
    ///
    /// The caller gains ownership of the returned string.
    ///
    /// # Safety
    ///
    /// `ctx` must be a valid Redis module context.
    #[inline]
    pub unsafe fn get_json(
        &self,
        ctx: *mut RedisModuleCtx,
    ) -> Result<*mut RedisModuleString> {
        let api = self.api.api();
        let get_json = api.getJSON.expect("getJSON not available");
        let mut str: *mut RedisModuleString = std::ptr::null_mut();
        // SAFETY: ptr and ctx are valid by construction/caller guarantee
        let status = unsafe { get_json(self.ptr.as_ptr(), ctx, &mut str) };
        if status == ffi::REDISMODULE_OK as c_int {
            Ok(str)
        } else {
            Err(JsonApiError::OperationFailed)
        }
    }

    /// Queries this JSON value using a JSON path expression.
    ///
    /// Returns an iterator over the matching values.
    ///
    /// # Safety
    ///
    /// `path` must be a valid null-terminated C string containing a JSON path.
    #[inline]
    pub unsafe fn get(&self, path: *const c_char) -> Option<JsonResultsIter<'a>> {
        let api = self.api.api();
        let get = api.get?;
        // SAFETY: ptr and path are valid by construction/caller guarantee
        let iter_ptr = unsafe { get(self.ptr.as_ptr(), path) };
        NonNull::new(iter_ptr as *mut c_void).map(|ptr| JsonResultsIter::new(ptr, self.api))
    }

    /// Gets an element at a specific index if this is an array.
    ///
    /// The `out` parameter receives the value at the given index.
    #[inline]
    pub fn get_at(&self, index: usize, out: &mut OwnedJsonValue<'a>) -> Result<()> {
        let api = self.api.api();
        let get_at = api.getAt.expect("getAt not available");
        // SAFETY: ptr and out.ptr are valid by construction
        let status = unsafe { get_at(self.ptr.as_ptr(), index, out.ptr.as_ptr()) };
        if status == ffi::REDISMODULE_OK as c_int {
            Ok(())
        } else {
            Err(JsonApiError::NotAnArray)
        }
    }

    /// Returns an iterator over the key-value pairs if this is an object.
    #[inline]
    pub fn key_values(&self) -> Option<KeyValuesIter<'a>> {
        let api = self.api.api();
        let get_key_values = api.getKeyValues?;
        // SAFETY: ptr is valid by construction
        let iter_ptr = unsafe { get_key_values(self.ptr.as_ptr()) };
        NonNull::new(iter_ptr as *mut c_void).map(|ptr| KeyValuesIter::new(ptr, self.api))
    }

    /// Returns the raw pointer to the JSON value.
    #[inline]
    pub const fn as_ptr(&self) -> *const c_void {
        self.ptr.as_ptr()
    }
}

impl std::fmt::Debug for JsonValue<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JsonValue")
            .field("type", &self.get_type())
            .field("ptr", &self.ptr.as_ptr())
            .finish()
    }
}

/// An owned JSON value container.
///
/// This type is allocated via [`RedisJsonApi::alloc_json`] and is used
/// to receive JSON values from iteration operations. It must be freed
/// when no longer needed (handled automatically via `Drop`).
pub struct OwnedJsonValue<'a> {
    ptr: NonNull<c_void>,
    api: &'a RedisJsonApi,
}

impl<'a> OwnedJsonValue<'a> {
    /// Creates a new owned JSON value from a raw pointer.
    #[inline]
    pub(crate) const fn new(ptr: NonNull<c_void>, api: &'a RedisJsonApi) -> Self {
        Self { ptr, api }
    }

    /// Returns a borrowed view of this value.
    ///
    /// The returned `JsonValue` dereferences through the owned container
    /// to the actual JSON data.
    #[inline]
    pub fn value(&self) -> Option<JsonValue<'a>> {
        // The allocated container stores a pointer to the actual JSON value
        // SAFETY: ptr is valid by construction
        let inner_ptr = unsafe { *(self.ptr.as_ptr() as *const *const c_void) };
        NonNull::new(inner_ptr as *mut c_void).map(|ptr| JsonValue::new(ptr, self.api))
    }

    /// Returns the raw pointer to the container.
    #[inline]
    pub const fn as_ptr(&self) -> *mut c_void {
        self.ptr.as_ptr()
    }
}

impl Drop for OwnedJsonValue<'_> {
    fn drop(&mut self) {
        let api = self.api.api();
        if let Some(free_json) = api.freeJson {
            // SAFETY: ptr was allocated by allocJson
            unsafe { free_json(self.ptr.as_ptr()) };
        }
    }
}

impl std::fmt::Debug for OwnedJsonValue<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OwnedJsonValue")
            .field("value", &self.value())
            .field("ptr", &self.ptr.as_ptr())
            .finish()
    }
}
