use super::RedisJsonApi;
use crate::JsonValue;
use redis_module::RedisString;
use std::{ffi::c_void, ptr::NonNull};

// An interators over key value pairs if the json value is an object
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
        unsafe { (self.free)(self.ptr.as_ptr()) }
    }
}

impl<'a> KeyValuesIterator<'a> {
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

    fn next(&mut self) -> Option<Self::Item> {
        let mut key: *mut ffi::RedisModuleString = std::ptr::null_mut();
        let value = JsonValue::new(self.api);

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
