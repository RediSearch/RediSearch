use redis_module::RedisString;

use crate::KeyValuesIterator;
use crate::RedisJsonApi;
use crate::ResultsIter;
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
//   We also cant change this (at least right now) so better pray to your god(s) that this pointer
//   will remain valid after it is freed I guess.
#[derive(Clone, Copy)]
pub struct JsonValueRef<'a> {
    pub(crate) ptr: *const c_void, // is non-null
    pub(crate) api: &'a RedisJsonApi,
}

impl<'a> JsonValueRef<'a> {
    pub(crate) unsafe fn from_raw(ptr: *const c_void, api: &'a RedisJsonApi) -> Self {
        Self { ptr, api }
    }

    /// Return the length of the value if it is an Object or Array
    pub fn len(&self) -> Option<usize> {
        let vtable = self.api.vtable();
        let get_len = vtable
            .getLen
            .expect("RedisJSON API function `getLen` not available");

        let mut out: usize = 0;

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

    pub fn get_type(&self) -> JsonType {
        let vtable = self.api.vtable();
        let get_type = vtable
            .getType
            .expect("RedisJSON API function `getType` not available");

        let raw = unsafe { get_type(self.ptr) };

        JsonType::from_raw(raw).expect("invalid JSON type")
    }

    pub fn get_int(&self) -> Option<i64> {
        let vtable = self.api.vtable();
        let get_int = vtable
            .getInt
            .expect("RedisJSON API function `getInt` not available");

        let mut out: i64 = 0;

        let status = unsafe { get_int(self.ptr, &raw mut out) };

        if status == ffi::REDISMODULE_OK as i32 {
            Some(out)
        } else {
            None
        }
    }

    pub fn get_double(&self) -> Option<f64> {
        let vtable = self.api.vtable();
        let get_double = vtable
            .getDouble
            .expect("RedisJSON API function `getDouble` not available");

        let mut out: f64 = 0.0;

        let status = unsafe { get_double(self.ptr, &raw mut out) };

        if status == ffi::REDISMODULE_OK as i32 {
            Some(out)
        } else {
            None
        }
    }

    pub fn get_bool(&self) -> Option<bool> {
        let vtable = self.api.vtable();
        let get_boolean = vtable
            .getBoolean
            .expect("RedisJSON API function `getBoolean` not available");

        let mut out: i32 = 0;

        let status = unsafe { get_boolean(self.ptr, &raw mut out) };

        if status == ffi::REDISMODULE_OK as i32 {
            Some(out != 0)
        } else {
            None
        }
    }

    pub fn get_str(&self) -> Option<&str> {
        let vtable = self.api.vtable();
        let get_string = vtable
            .getString
            .expect("RedisJSON API function `getString` not available");

        let mut str: *const c_char = std::ptr::null();
        let mut len: usize = 0;

        let status = unsafe { get_string(self.ptr, &raw mut str, &raw mut len) };

        if status == ffi::REDISMODULE_OK as i32 {
            let bytes = unsafe { slice::from_raw_parts(str.cast::<u8>(), len) };

            Some(str::from_utf8(bytes).expect("invalid UTF-8 in JSON string"))
        } else {
            None
        }
    }

    pub fn get_at(&self, idx: usize) -> Option<JsonValue<'_>> {
        let vtable = self.api.vtable();
        let get_at = vtable
            .getAt
            .expect("RedisJSON API function `getAt` not available");

        let mut out = JsonValue::new(self.api);

        let status = unsafe { get_at(self.ptr, idx, out.as_ptr()) };
        if status == ffi::REDISMODULE_OK as i32 {
            Some(out)
        } else {
            None
        }
    }

    pub fn key_values(&self, ctx: *mut ffi::RedisModuleCtx) -> Option<KeyValuesIterator<'_>> {
        let vtable = self.api.vtable();
        let get_key_values = vtable
            .getKeyValues
            .expect("RedisJSON API function `getKeyValues` not available");

        let ptr = unsafe { get_key_values(self.ptr) };
        // TODO this should have been a mutable pointer (we mutate the underlying iterator in subsequent calls after all)
        let ptr = NonNull::new(ptr.cast_mut())?;

        Some(unsafe { KeyValuesIterator::from_non_null(ptr, ctx, self.api) })
    }

    pub fn get(&self, path: &CStr) -> Option<ResultsIter<'_>> {
        let api = self.api.vtable();
        let get = api.get.expect("RedisJSON API function `get` not available");

        let ptr = unsafe { get(self.ptr, path.as_ptr()) };
        // TODO this should have been a mutable pointer (we mutate the underlying iterator in subsequent calls after all)
        let ptr = NonNull::new(ptr.cast_mut())?;

        Some(unsafe { ResultsIter::from_non_null(ptr, self.api) })
    }

    /// Serializes this JSON value to a Redis module string.
    ///
    /// # Safety
    ///
    /// `ctx` must be a valid Redis module context.
    #[inline]
    pub unsafe fn serialize(&self, ctx: *mut ffi::RedisModuleCtx) -> Result<RedisString, ()> {
        let api = self.api.vtable();
        let get_json = api
            .getJSON
            .expect("RedisJSON API function `getJSON` not available");

        let mut str: *mut ffi::RedisModuleString = std::ptr::null_mut();

        // SAFETY: ptr and ctx are valid by construction/caller guarantee
        let status = unsafe { get_json(self.ptr, ctx, &mut str) };

        if status == ffi::REDISMODULE_OK as i32 {
            Ok(RedisString::from_redis_module_string(
                ctx.cast(),
                str.cast(),
            ))
        } else {
            Err(())
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
        unsafe { (self.free)(self.ptr) }
    }
}

impl<'a> JsonValue<'a> {
    pub fn new(api: &'a RedisJsonApi) -> Self {
        let vtable = api.vtable();
        let alloc = vtable
            .allocJson
            .expect("RedisJSON API function `allocJson` not available");
        let free = vtable
            .freeJson
            .expect("RedisJSON API function `freeJson` not available");

        let ptr = unsafe { alloc() };
        debug_assert!(!ptr.is_null());

        Self { ptr, free, api }
    }

    pub fn as_ref(&self) -> JsonValueRef<'_> {
        JsonValueRef {
            ptr: unsafe { *self.ptr },
            api: self.api,
        }
    }

    pub(crate) const fn as_ptr(&mut self) -> ffi::RedisJSONPtr {
        self.ptr
    }
}
