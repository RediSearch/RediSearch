use std::ffi::c_char;

#[derive(Clone, Debug)]
pub struct ConstString {
    ptr: *const c_char,
    len: u32,
}

impl ConstString {
    pub unsafe fn from_raw(ptr: *const c_char, len: u32) -> Self {
        Self { ptr, len }
    }

    pub fn as_ptr(&self) -> *const c_char {
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

unsafe impl Send for ConstString {}
unsafe impl Sync for ConstString {}
