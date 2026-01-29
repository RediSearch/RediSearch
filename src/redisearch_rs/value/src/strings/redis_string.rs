use ffi::context::redisearch_module_context;
use ffi::{RedisModule_FreeString, RedisModule_StringPtrLen, RedisModuleString};
use std::ffi::c_char;
use std::mem::MaybeUninit;

#[derive(Clone, Debug)]
pub struct RedisString {
    ptr: *const RedisModuleString,
}

impl RedisString {
    pub unsafe fn from_raw(ptr: *const RedisModuleString) -> Self {
        Self { ptr }
    }

    pub fn as_ptr(&self) -> *const RedisModuleString {
        self.ptr
    }

    pub fn as_ptr_len(&self) -> (*const c_char, u32) {
        let rm_str_ptr_len =
            unsafe { RedisModule_StringPtrLen }.expect("Redis module not initialized");

        let mut len = MaybeUninit::uninit();
        let ptr = unsafe { rm_str_ptr_len(self.ptr, len.as_mut_ptr()) };
        let len = unsafe { len.assume_init() };

        (ptr, len as u32)
    }

    pub fn as_bytes(&self) -> &[u8] {
        let (ptr, len) = self.as_ptr_len();
        unsafe { std::slice::from_raw_parts(ptr as _, len as usize) }
    }
}

impl Drop for RedisString {
    fn drop(&mut self) {
        let ctx = unsafe { redisearch_module_context() };
        let free_string = unsafe { RedisModule_FreeString }.expect("Redis module not initialized");
        unsafe { free_string(ctx, self.ptr as _) };
    }
}

unsafe impl Send for RedisString {}
unsafe impl Sync for RedisString {}
