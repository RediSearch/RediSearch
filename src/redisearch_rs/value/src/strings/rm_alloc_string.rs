use ffi::RedisModule_Free;
use std::ffi::c_char;

#[derive(Clone, Debug)]
pub struct RmAllocString {
    ptr: *mut c_char,
    len: u32,
}

impl RmAllocString {
    pub unsafe fn from_raw(ptr: *mut c_char, len: u32) -> Self {
        Self { ptr, len }
    }

    pub fn as_ptr(&self) -> *mut c_char {
        self.ptr
    }

    pub fn len(&self) -> u32 {
        self.len
    }

    pub fn as_ptr_len(&self) -> (*const c_char, u32) {
        (self.ptr, self.len)
    }

    pub fn as_bytes(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr as _, self.len as usize) }
    }
}

impl Drop for RmAllocString {
    fn drop(&mut self) {
        let rm_free = unsafe { RedisModule_Free.expect("Redis allocator not available") };
        unsafe { rm_free(self.ptr as _) };
    }
}

unsafe impl Send for RmAllocString {}
unsafe impl Sync for RmAllocString {}
