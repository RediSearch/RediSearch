/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use ffi::RedisModule_Free;
use std::ffi::{CString, c_char};
use std::fmt;

enum RsStringKind {
    RustAlloc,
    RmAlloc,
    Borrowed,
}

/// A `CString` like string for [`RsValue`](crate::RsValue) with support for rust allocated string,
/// C allocated strings, and constant strings, and support for a max length of `u32::MAX`,
/// all in one package.
///
/// # Safety
///
/// - `ptr` must not be NULL and must point to a valid c-string of `len` size.
/// - The size determined by `len` excludes the nul-terminator.
/// - A nul-terminator is expected in memory at `ptr+len`.
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

    /// Create an [`RsString`] from a rm_alloc allocated string.
    /// Takes ownership of the string pointed to by `ptr`/`len`.
    ///
    /// # Safety
    ///
    /// 1. `ptr` must not be NULL and must point to a valid c-string of `len` size.
    /// 2. The size determined by `len` excludes the nul-terminator.
    /// 3. A nul-terminator is expected in memory at `ptr+len`.
    pub unsafe fn rm_alloc_string(ptr: *const c_char, len: u32) -> Self {
        // Safety: ensured by caller (1.)
        debug_assert!(ptr != std::ptr::null());
        // Safety: ensured by caller (2.)
        debug_assert!(unsafe { ptr.add(len as usize).read() } as u8 == b'\0');

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
    /// 2. The size determined by `len` excludes the nul-terminator.
    /// 3. A nul-terminator is expected in memory at `ptr+len`.
    /// 4. The string pointed to by `ptr`/`len+1` must stay valid for as long as
    ///    this [`RsString`] is exists.
    pub unsafe fn borrowed_string(ptr: *const c_char, len: u32) -> Self {
        // Safety: ensured by caller (1.)
        debug_assert!(ptr != std::ptr::null());
        // Safety: ensured by caller (2.)
        debug_assert!(unsafe { ptr.add(len as usize).read() } as u8 == b'\0');

        Self {
            ptr,
            len,
            kind: RsStringKind::Borrowed,
        }
    }

    /// Returns the string data pointer and length.
    pub const fn as_ptr_len(&self) -> (*const c_char, u32) {
        (self.ptr, self.len)
    }

    /// Gets the string pointed to by `ptr`/`len` as a byte slice.
    pub const fn as_bytes(&self) -> &[u8] {
        // SAFETY: `self.ptr` points to valid memory of `self.len` bytes per our invariant.
        unsafe { std::slice::from_raw_parts(self.ptr as _, self.len as usize) }
    }
}

impl Drop for RsString {
    fn drop(&mut self) {
        match self.kind {
            RsStringKind::RustAlloc => {
                // SAFETY: `self.ptr` was created by `CString::into_raw` and has not been freed.
                drop(unsafe { CString::from_raw(self.ptr as *mut _) });
            }
            RsStringKind::RmAlloc => {
                // SAFETY: Accessing a global function pointer initialized during module load.
                let rm_free = unsafe { RedisModule_Free.expect("Redis allocator not available") };
                // SAFETY: `self.ptr` was allocated by rm_alloc and has not been freed.
                unsafe { rm_free(self.ptr as _) };
            }
            RsStringKind::Borrowed => (), // No need to free borrowed strings.
        }
    }
}

impl fmt::Debug for RsString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let lossy = String::from_utf8_lossy(self.as_bytes());
        f.debug_tuple("RsString").field(&lossy).finish()
    }
}

// SAFETY: [`RsString`] does not hold data that cannot be sent to another thread.
unsafe impl Send for RsString {}
// SAFETY: [`RsString`] provides no interior mutability; shared references are read-only.
unsafe impl Sync for RsString {}
