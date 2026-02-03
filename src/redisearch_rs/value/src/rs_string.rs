use ffi::RedisModule_Free;
use std::ffi::{CString, c_char};

#[derive(Clone, Debug)]
enum RsStringKind {
    RustAlloc,
    RmAlloc,
    Const,
}

/// A `CString` like string for [`RsValue`] with support for rust allocated string,
/// C allocated strings, and constant strings, all in one package.
///
/// # Safety
///
/// - `ptr` must not be NULL and must point to a valid string of `len` size.
/// - The string pointed to by `ptr`/`len` must be nul-terminated.
#[derive(Clone, Debug)]
pub struct RsString {
    ptr: *const c_char,
    len: u32,
    kind: RsStringKind,
}

impl RsString {
    /// Create an [`RsString`] from a `CString`. This string's length must not
    /// be more than `u32::MAX` for compatibility with existing C code using
    /// `RSValue` functionality.
    ///
    /// # Panic
    ///
    /// Panics when the size is larger than `u32::MAX`.
    pub fn cstring(str: CString) -> Self {
        let len = str.count_bytes();
        assert!(len <= u32::MAX as usize);

        let ptr = str.into_raw();

        Self {
            ptr,
            len: len as u32,
            kind: RsStringKind::RustAlloc,
        }
    }

    /// Create an [`RsString`] from a constant string.
    ///
    /// # Safety
    ///
    /// 1. `ptr` must not be NULL and must point to a valid string of `len` size.
    /// 2. The string pointed to by `ptr`/`len` must be nul-terminated.
    pub unsafe fn rm_alloc_string(ptr: *const c_char, len: u32) -> Self {
        Self {
            ptr,
            len,
            kind: RsStringKind::RmAlloc,
        }
    }

    /// Create an [`RsString`] from a constant string.
    ///
    /// # Safety
    ///
    /// 1. `ptr` must not be NULL and must point to a valid string of `len` size.
    /// 2. The string pointed to by `ptr`/`len` must be nul-terminated.
    pub unsafe fn const_string(ptr: *const c_char, len: u32) -> Self {
        Self {
            ptr,
            len,
            kind: RsStringKind::Const,
        }
    }

    /// Returns the string data pointer and length.
    pub fn as_ptr_len(&self) -> (*const c_char, u32) {
        (self.ptr, self.len)
    }

    /// Gets the string pointed to by `ptr`/`len` as a byte slice.
    pub fn as_bytes(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr as _, self.len as usize) }
    }
}

impl Drop for RsString {
    fn drop(&mut self) {
        match self.kind {
            RsStringKind::RustAlloc => drop(unsafe { CString::from_raw(self.ptr as *mut _) }),
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
