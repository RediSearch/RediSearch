use ffi::RedisModule_Free;
use std::ffi::{CString, c_char};

#[derive(Clone, Debug)]
enum RsStringKind {
    RmAlloc,
    Const,
    Rust,
}

#[derive(Clone, Debug)]
pub struct RsString {
    ptr: *const c_char,
    len: u32,
    kind: RsStringKind,
}

impl RsString {
    pub fn cstring(str: CString) -> Self {
        let len = str.count_bytes();
        assert!(len <= u32::MAX as usize);

        let ptr = str.into_raw();

        Self {
            ptr,
            len: len as u32,
            kind: RsStringKind::Rust,
        }
    }

    pub unsafe fn rm_alloc_string(ptr: *const c_char, len: u32) -> Self {
        Self {
            ptr,
            len,
            kind: RsStringKind::RmAlloc,
        }
    }

    pub unsafe fn const_string(ptr: *const c_char, len: u32) -> Self {
        Self {
            ptr,
            len,
            kind: RsStringKind::Const,
        }
    }

    pub fn as_ptr_len(&self) -> (*const c_char, u32) {
        (self.ptr, self.len)
    }

    pub fn as_bytes(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr as _, self.len as usize) }
    }
}

impl Drop for RsString {
    fn drop(&mut self) {
        match self.kind {
            RsStringKind::Rust => drop(unsafe { CString::from_raw(self.ptr as *mut _) }),
            RsStringKind::RmAlloc => {
                let rm_free = unsafe { RedisModule_Free.expect("Redis allocator not available") };
                unsafe { rm_free(self.ptr as _) };
            }
            RsStringKind::Const => (), // No nee to free const strings.
        }
    }
}

unsafe impl Send for RsString {}
unsafe impl Sync for RsString {}
