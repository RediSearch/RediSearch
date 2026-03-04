/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use ffi::RedisModule_Free;
use std::ffi::c_char;
use std::fmt;

enum RsStringKind {
    /// Used when the [`RsString`] is allocated through the Rust Global allocator.
    /// Most often when originating from Rust code.
    RustGlobalAlloc,
    /// Used when the [`RsString`] is allocated through `RedisModule_Alloc`.
    /// Most often when originating from C code.
    RedisModuleAlloc,
    /// Used when the [`RsString`] is referencing borrowed data which should not be
    /// freed when dropping the [`RsString`].
    Borrowed,
}

/// An [`RsString`] is meant to store string data with support for rust allocated data, C
/// allocated data or borrowed data, and support for a max length of `u32::MAX`.
/// It can contain binary data and is always nul-terminated.
///
/// # Invariants
///
/// - `ptr` points to valid data of `len+1` size.
/// - a nul-terminator is always present in memory at `ptr+len`
/// - The size determined by `len` excludes the nul-terminator.
pub struct RsString {
    ptr: *const c_char,
    len: u32,
    kind: RsStringKind,
}

impl RsString {
    /// Create an [`RsString`] from a `Vec<u8>`. The length must not be more than
    /// `u32::MAX` for compatibility with existing C code using `RSValue` functionality.
    /// A nul-terminator is automatically added by this constructor for compatibility.
    ///
    /// # Panic
    ///
    /// Panics when the size is larger than `u32::MAX`.
    pub fn from_vec(mut vec: Vec<u8>) -> Self {
        let len = vec.len();
        assert!(len <= u32::MAX as usize);

        vec.push(b'\0');
        let ptr = Box::into_raw(vec.into_boxed_slice());

        Self {
            ptr: ptr as *const c_char,
            len: len as u32,
            kind: RsStringKind::RustGlobalAlloc,
        }
    }

    /// Create an [`RsString`] from a redis module allocated string.
    /// Takes ownership of the string pointed to by `ptr`/`len`.
    ///
    /// # Safety
    ///
    /// 1. `ptr` must be a [valid], non-null pointer to valid data of `len+1` size.
    /// 2. A nul-terminator is expected in memory at `ptr+len`.
    /// 3. The size determined by `len` excludes the nul-terminator.
    ///
    /// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
    #[allow(clippy::multiple_unsafe_ops_per_block)]
    pub unsafe fn rm_alloc_string(ptr: *const c_char, len: u32) -> Self {
        debug_assert!(!ptr.is_null());
        // Safety: ensured by caller (1., 2., 3.)
        debug_assert!(unsafe { ptr.add(len as usize).read() } as u8 == b'\0');

        Self {
            ptr,
            len,
            kind: RsStringKind::RedisModuleAlloc,
        }
    }

    /// Create an [`RsString`] from a borrowed string.
    ///
    /// # Safety
    ///
    /// 1. `ptr` must be a [valid], non-null pointer to a valid c-string of `len+1` size.
    /// 2. A nul-terminator is expected in memory at `ptr+len`.
    /// 3. The size determined by `len` excludes the nul-terminator.
    /// 4. The string pointed to by `ptr`/`len+1` must stay valid for as long as
    ///    this [`RsString`] is exists.
    ///
    /// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
    #[allow(clippy::multiple_unsafe_ops_per_block)]
    pub unsafe fn borrowed_string(ptr: *const c_char, len: u32) -> Self {
        debug_assert!(!ptr.is_null());
        // Safety: ensured by caller (1., 2., 3.)
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
        // Safety: `self.ptr` points to valid memory of `self.len` bytes per our invariant.
        unsafe { std::slice::from_raw_parts(self.ptr as _, self.len as usize) }
    }
}

impl Drop for RsString {
    fn drop(&mut self) {
        match self.kind {
            RsStringKind::RustGlobalAlloc => {
                let slice = std::ptr::slice_from_raw_parts_mut(
                    self.ptr.cast_mut().cast::<u8>(),
                    (self.len as usize) + 1,
                );
                // Safety: Boxed slice was created in `Self::from_vec` which has `len + 1` bytes.
                drop(unsafe { Box::from_raw(slice) });
            }
            RsStringKind::RedisModuleAlloc => {
                // Safety: Accessing a global function pointer initialized during module load.
                let rm_free = unsafe { RedisModule_Free.expect("Redis allocator not available") };
                // Safety: `self.ptr` was allocated by rm_alloc and has not been freed.
                unsafe { rm_free(self.ptr.cast_mut().cast()) };
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

// Safety: [`RsString`] does not hold data that cannot be sent to another thread.
unsafe impl Send for RsString {}
// Safety: [`RsString`] provides no interior mutability; shared references are read-only.
unsafe impl Sync for RsString {}
