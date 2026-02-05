use ffi::context::redisearch_module_context;
use ffi::{RedisModuleString, RedisModule_FreeString, RedisModule_StringPtrLen};
use std::ffi::c_char;
use std::mem::MaybeUninit;

/// An owned string backed by a [`RedisModuleString`].
///
/// # Safety
///
/// - `ptr` must not be NULL and must point to a valid [`RedisModuleString`].
#[derive(Clone, Debug)]
pub struct RedisString {
    ptr: *const RedisModuleString,
}

impl RedisString {
    /// Create a [`RedisString`] from a raw [`RedisModuleString`] pointer, taking ownership.
    ///
    /// # Safety
    ///
    /// 1. `ptr` must not be NULL and must point to a valid [`RedisModuleString`].
    /// 2. `ptr` **must not** be used or freed after this call, as this function takes ownership.
    pub unsafe fn from_raw(ptr: *const RedisModuleString) -> Self {
        Self { ptr }
    }

    /// Returns the raw [`RedisModuleString`] pointer.
    pub fn as_ptr(&self) -> *const RedisModuleString {
        self.ptr
    }

    /// Returns the string data pointer and length.
    pub fn as_ptr_len(&self) -> (*const c_char, u32) {
        let rm_str_ptr_len =
            unsafe { RedisModule_StringPtrLen }.expect("Redis module not initialized");

        let mut len = MaybeUninit::uninit();
        let ptr = unsafe { rm_str_ptr_len(self.ptr, len.as_mut_ptr()) };
        let len = unsafe { len.assume_init() };

        (ptr, len as u32)
    }

    /// Gets the string data as a byte slice.
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
