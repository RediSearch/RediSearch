/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use redis_module::RedisString;

use crate::KeyValuesIterator;
use crate::RedisJsonApi;
use crate::ResultsIter;
use crate::SerializeError;
use core::slice;
use std::ffi::CStr;
use std::ffi::c_char;
use std::ffi::c_void;
use std::ptr::NonNull;

/// The type of a JSON value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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
    pub const fn from_raw(raw: u32) -> Option<Self> {
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

impl From<ffi::JSONType> for JsonType {
    #[inline]
    fn from(raw: ffi::JSONType) -> Self {
        Self::from_raw(raw).expect("invalid JSONType value")
    }
}

// => typedef const void* RedisJSON;
//
// # Notes
//
// - This pointer is basically use-after free. We cannot guarantee that the memory still exists.
//   We also can't change this (at least right now) so better pray to your god(s) that this pointer
//   will remain valid after it is freed I guess.
#[derive(Clone, Copy)]
pub struct JsonValueRef<'a> {
    pub(crate) ptr: *const c_void, // is non-null
    pub(crate) api: &'a RedisJsonApi,
}

impl<'a> JsonValueRef<'a> {
    /// Construct a new `JsonValueRef` from a raw pointer.
    ///
    /// # Safety
    ///
    /// 1. `ptr` must be a valid ptr obtained from `getKey*`.
    pub(crate) const unsafe fn from_raw(ptr: *const c_void, api: &'a RedisJsonApi) -> Self {
        Self { ptr, api }
    }

    /// Return the length of the value if it is an Object or Array
    pub fn len(&self) -> Option<usize> {
        let vtable = self.api.vtable();
        let get_len = vtable
            .getLen
            .expect("RedisJSON API function `getLen` not available");

        let mut out: usize = 0;

        // Safety: `ptr` is valid by construction.
        let status = unsafe { get_len(self.ptr, &raw mut out) };

        if status == ffi::REDISMODULE_OK as i32 {
            Some(out)
        } else {
            None
        }
    }

    /// Return whether the value is empty if it is an Object or Array
    pub fn is_empty(&self) -> Option<bool> {
        Some(self.len()? == 0)
    }

    /// Returns the type of this `JsonValue`.
    pub fn get_type(&self) -> JsonType {
        let vtable = self.api.vtable();
        let get_type = vtable
            .getType
            .expect("RedisJSON API function `getType` not available");

        // Safety: `ptr` is valid by construction.
        let raw = unsafe { get_type(self.ptr) };

        JsonType::from_raw(raw).expect("invalid JSON type")
    }

    /// Returns the i64 value of this `JsonValue`s if it is a Number or `None` otherwise.
    pub fn get_int(&self) -> Option<i64> {
        let vtable = self.api.vtable();
        let get_int = vtable
            .getInt
            .expect("RedisJSON API function `getInt` not available");

        let mut out: i64 = 0;

        // Safety: `ptr` is valid by construction.
        let status = unsafe { get_int(self.ptr, &raw mut out) };

        if status == ffi::REDISMODULE_OK as i32 {
            Some(out)
        } else {
            None
        }
    }

    /// Returns the f64 value of this `JsonValue`s if it is a Number or `None` otherwise.
    pub fn get_double(&self) -> Option<f64> {
        let vtable = self.api.vtable();
        let get_double = vtable
            .getDouble
            .expect("RedisJSON API function `getDouble` not available");

        let mut out: f64 = 0.0;

        // Safety: `ptr` is valid by construction.
        let status = unsafe { get_double(self.ptr, &raw mut out) };

        if status == ffi::REDISMODULE_OK as i32 {
            Some(out)
        } else {
            None
        }
    }

    /// Returns the boolean value of this `JsonValue`s if it is a Boolean or `None` otherwise.
    pub fn get_bool(&self) -> Option<bool> {
        let vtable = self.api.vtable();
        let get_boolean = vtable
            .getBoolean
            .expect("RedisJSON API function `getBoolean` not available");

        let mut out: i32 = 0;

        // Safety: `ptr` is valid by construction.
        let status = unsafe { get_boolean(self.ptr, &raw mut out) };

        if status == ffi::REDISMODULE_OK as i32 {
            Some(out != 0)
        } else {
            None
        }
    }

    /// Returns the string value of this `JsonValue`s if it is a String or `None` otherwise.
    pub fn get_str(&self) -> Option<&str> {
        let vtable = self.api.vtable();
        let get_string = vtable
            .getString
            .expect("RedisJSON API function `getString` not available");

        let mut str: *const c_char = std::ptr::null();
        let mut len: usize = 0;

        // Safety: `ptr` is valid by construction.
        let status = unsafe { get_string(self.ptr, &raw mut str, &raw mut len) };

        if status == ffi::REDISMODULE_OK as i32 {
            // Safety: `getString` returns `OK` it promises to return a valid c string.
            let bytes = unsafe { slice::from_raw_parts(str.cast::<u8>(), len) };

            Some(str::from_utf8(bytes).expect("invalid UTF-8 in JSON string"))
        } else {
            None
        }
    }

    /// Returns the element at index `idx` of this `JsonValue`s if it is an Array or `None` otherwise.
    pub fn get_at(&self, idx: usize) -> Option<JsonValue<'_>> {
        let vtable = self.api.vtable();
        let get_at = vtable
            .getAt
            .expect("RedisJSON API function `getAt` not available");

        let mut out = JsonValue::new(self.api);

        // Safety: `ptr` is valid by construction, we correctly allocated the `JsonValue` before.
        let status = unsafe { get_at(self.ptr, idx, out.as_ptr()) };
        if status == ffi::REDISMODULE_OK as i32 {
            Some(out)
        } else {
            None
        }
    }

    /// Returns a iterator over this `JsonValue`s the key-value pairs if it is an Object or `None` otherwise.
    ///
    /// # Safety
    ///
    /// 1. `ctx` must be a valid Redis module context.
    pub unsafe fn key_values(
        &self,
        ctx: *mut ffi::RedisModuleCtx,
    ) -> Option<KeyValuesIterator<'_>> {
        let vtable = self.api.vtable();
        let get_key_values = vtable
            .getKeyValues
            .expect("RedisJSON API function `getKeyValues` not available");

        // Safety: `ptr` is valid by construction.
        let ptr = unsafe { get_key_values(self.ptr) };
        // TODO this should have been a mutable pointer (we mutate the underlying iterator in subsequent calls after all)
        let ptr = NonNull::new(ptr.cast_mut())?;

        // Safety: (1.): ensured by caller. (2.): we obtained this pointer from `getKeyValues`.
        Some(unsafe { KeyValuesIterator::from_non_null(ptr, ctx, self.api) })
    }

    /// Returns an iterator over values matched by `path`.
    pub fn get(&self, path: &CStr) -> Option<ResultsIter<'_>> {
        let api = self.api.vtable();
        let get = api.get.expect("RedisJSON API function `get` not available");

        // Safety: `ptr` is valid by construction and CStr ensures `ptr` is a valid c string.
        let ptr = unsafe { get(self.ptr, path.as_ptr()) };
        // TODO this should have been a mutable pointer (we mutate the underlying iterator in subsequent calls after all)
        let ptr = NonNull::new(ptr.cast_mut())?;

        // Safety: we obtained this pointer from `get`.
        Some(unsafe { ResultsIter::from_non_null(ptr, self.api) })
    }

    /// Serializes this JSON value to a Redis module string.
    ///
    /// # Safety
    ///
    /// 1. `ctx` must be a valid Redis module context.
    #[inline]
    pub unsafe fn serialize(
        &self,
        ctx: *mut ffi::RedisModuleCtx,
    ) -> Result<RedisString, SerializeError> {
        let api = self.api.vtable();
        let get_json = api
            .getJSON
            .expect("RedisJSON API function `getJSON` not available");

        let mut str: *mut ffi::RedisModuleString = std::ptr::null_mut();

        // Safety: ensured by caller (1.) and ptr is valid by construction.
        let status = unsafe { get_json(self.ptr, ctx, &mut str) };

        if status == ffi::REDISMODULE_OK as i32 {
            Ok(RedisString::from_redis_module_string(
                ctx.cast(),
                str.cast(),
            ))
        } else {
            Err(SerializeError)
        }
    }
}

pub struct JsonValue<'a> {
    pub(crate) ptr: ffi::RedisJSONPtr,
    pub(crate) free: unsafe extern "C" fn(ptr: ffi::RedisJSONPtr),
    pub(crate) api: &'a RedisJsonApi,
}

impl Drop for JsonValue<'_> {
    fn drop(&mut self) {
        // Safety: we obtained this pointer from `allocJson`
        unsafe { (self.free)(self.ptr) }
    }
}

impl<'a> JsonValue<'a> {
    pub(crate) fn new(api: &'a RedisJsonApi) -> Self {
        let vtable = api.vtable();
        let alloc_json = vtable
            .allocJson
            .expect("RedisJSON API function `allocJson` not available");
        let free = vtable
            .freeJson
            .expect("RedisJSON API function `freeJson` not available");

        // Safety: the redis json module is initialized at this point
        let ptr = unsafe { alloc_json() };
        debug_assert!(!ptr.is_null());

        Self { ptr, free, api }
    }

    /// Get a non-owning reference to this `JsonValue`.
    pub fn as_ref(&self) -> JsonValueRef<'_> {
        JsonValueRef {
            // Safety: we obtained this pointer from `allocJson` and the `new` constructor is private
            // where we ensure the value is actually initialized before being handed out.
            ptr: unsafe { *self.ptr },
            api: self.api,
        }
    }

    pub(crate) const fn as_ptr(&mut self) -> ffi::RedisJSONPtr {
        self.ptr
    }
}
