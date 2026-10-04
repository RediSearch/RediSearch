/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::shared_buffer::{MAX_SHARED_OFFSET, SharedBuffer};
use nul_terminated_bytes::NulTerminatedBytes;
use redis_module::RedisModule_Free;
use std::ffi::c_char;
use std::fmt;

/// An [`String`] is meant to store string data with support for rust allocated data, C
/// allocated data or borrowed data, and support for a max length of `u32::MAX`.
/// It can contain binary data and is always nul-terminated.
///
/// # Invariants
///
/// - `ptr` points to valid data of `len+1` size.
/// - a nul-terminator is always present in memory at `ptr+len`
/// - The size determined by `len` excludes the nul-terminator.
pub struct String {
    ptr: *const c_char,
    len: u32,
    kind: StringKind,
}

/// This defines the type of allocation used by the [`String`]
#[derive(Clone, Copy)]
enum StringKind {
    /// Used when the [`String`] is allocated directly through the Rust
    /// Global allocator. Most often when originating from Rust code.
    RustGlobalAlloc,
    /// Used when the [`String`] is allocated directly through
    /// `RedisModule_Alloc`. Most often when originating from C code.
    RedisModuleAlloc,
    /// Used when the [`String`] is referencing borrowed data which
    /// should not be freed when dropping the [`String`].
    Borrowed,
    /// Used when the [`String`] borrows from a [`SharedBuffer`] it holds a reference to,
    /// starting this many bytes into it. Three bytes, so that the variant fits the padding
    /// after [`String::len`] and every [`Value`](crate::Value) stays as small as before.
    Shared { offset: [u8; 3] },
}

// The `Shared` variant must not grow the type it is the kind of.
const _: () = assert!(size_of::<String>() == 16);

impl String {
    /// Create an [`String`] from a `Vec<u8>`. The length must not be more than
    /// `u32::MAX` for compatibility with existing C code using `RSValue` functionality.
    /// A nul-terminator is automatically added by this constructor for compatibility.
    ///
    /// # Panic
    ///
    /// Panics when the size is larger than `u32::MAX`.
    pub fn from_vec(vec: Vec<u8>) -> Self {
        assert!(vec.len() <= u32::MAX as usize);

        let (ptr, len) = NulTerminatedBytes::from(vec).into_raw_parts();

        Self {
            ptr: ptr.cast(),
            len: len as u32,
            kind: StringKind::RustGlobalAlloc,
        }
    }

    /// Create an [`String`] from a redis module allocated string.
    /// Takes ownership of the string pointed to by `ptr`/`len`.
    ///
    /// # Safety
    ///
    /// 1. `ptr` must be a [valid], non-null pointer to a buffer of `len+1` bytes
    ///    allocated by `RedisModule_Alloc`.
    /// 2. A nul-terminator is expected in memory at `ptr+len`.
    /// 3. The size determined by `len` excludes the nul-terminator.
    /// 4. `ptr` **must not** be used or freed after this function is called, as this function
    ///    takes ownership of the allocation.
    ///
    /// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
    #[expect(clippy::multiple_unsafe_ops_per_block)]
    pub unsafe fn rm_alloc_string(ptr: *const c_char, len: u32) -> Self {
        debug_assert!(!ptr.is_null());
        // Safety: ensured by caller (1., 2., 3.)
        debug_assert!(unsafe { ptr.add(len as usize).read() } as u8 == b'\0');

        Self {
            ptr,
            len,
            kind: StringKind::RedisModuleAlloc,
        }
    }

    /// Create an [`String`] from a borrowed string.
    ///
    /// # Safety
    ///
    /// 1. `ptr` must be a [valid], non-null pointer to a buffer of `len+1` bytes.
    /// 2. A nul-terminator is expected in memory at `ptr+len`.
    /// 3. The size determined by `len` excludes the nul-terminator.
    /// 4. The string pointed to by `ptr`/`len+1` must stay valid for as long as
    ///    this [`String`] is exists.
    ///
    /// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
    #[expect(clippy::multiple_unsafe_ops_per_block)]
    pub unsafe fn borrowed_string(ptr: *const c_char, len: u32) -> Self {
        debug_assert!(!ptr.is_null());
        // Safety: ensured by caller (1., 2., 3.)
        debug_assert!(unsafe { ptr.add(len as usize).read() } as u8 == b'\0');

        Self {
            ptr,
            len,
            kind: StringKind::Borrowed,
        }
    }

    /// Create a [`String`] borrowing from a [`SharedBuffer`], taking over one of its
    /// references.
    ///
    /// # Safety
    ///
    /// 1. `ptr` must lie `offset` bytes into a [`SharedBuffer`]'s bytes, with the provenance
    ///    of the whole buffer, and `offset` must not exceed [`MAX_SHARED_OFFSET`].
    /// 2. The caller must have taken a reference to that buffer for this string, released
    ///    when the string drops.
    /// 3. `ptr` must be [valid] for reads of `len+1` bytes, with a nul-terminator at
    ///    `ptr+len`, and those bytes must stay unmodified while the buffer lives.
    ///
    /// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
    pub(crate) const unsafe fn shared(ptr: *const c_char, len: u32, offset: usize) -> Self {
        debug_assert!(offset <= MAX_SHARED_OFFSET);
        let [a, b, c, _] = (offset as u32).to_le_bytes();
        Self {
            ptr,
            len,
            kind: StringKind::Shared { offset: [a, b, c] },
        }
    }

    /// Whether this string borrows from a [`SharedBuffer`] rather than owning its bytes or
    /// borrowing them under an outside guarantee.
    pub const fn is_shared(&self) -> bool {
        matches!(self.kind, StringKind::Shared { .. })
    }

    /// Returns the string data pointer and length.
    pub const fn as_ptr_len(&self) -> (*const c_char, u32) {
        (self.ptr, self.len)
    }

    /// Gets the string pointed to by `ptr`/`len` as a byte slice.
    pub const fn as_bytes(&self) -> &[u8] {
        // Safety: `self.ptr` points to valid memory of `self.len` bytes per our invariant.
        unsafe { std::slice::from_raw_parts(self.ptr.cast(), self.len as usize) }
    }
}

impl Drop for String {
    fn drop(&mut self) {
        match self.kind {
            StringKind::RustGlobalAlloc => {
                // Safety: `ptr`/`len` were produced by `NulTerminatedBytes::into_raw_parts`
                // in `Self::from_vec` and have not been freed.
                drop(unsafe {
                    NulTerminatedBytes::from_raw_parts(
                        self.ptr.cast_mut().cast::<u8>(),
                        self.len as usize,
                    )
                });
            }
            StringKind::RedisModuleAlloc => {
                // Safety: Accessing a global function pointer initialized during module load.
                let rm_free = unsafe { RedisModule_Free.expect("Redis allocator not available") };
                // Safety: `self.ptr` was allocated by rm_alloc and has not been freed.
                unsafe { rm_free(self.ptr.cast_mut().cast()) };
            }
            StringKind::Borrowed => (), // No need to free borrowed strings.
            StringKind::Shared { offset: [a, b, c] } => {
                let offset = u32::from_le_bytes([a, b, c, 0]) as usize;
                // Safety: `ptr` and `offset` are those `SharedBuffer::share` made this string
                // with, and its reference has not been released yet.
                unsafe { SharedBuffer::release_shared(self.ptr, offset) };
            }
        }
    }
}

impl fmt::Debug for String {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let lossy = std::string::String::from_utf8_lossy(self.as_bytes());
        f.debug_tuple("String").field(&lossy).finish()
    }
}

// Safety: [`String`] does not hold data that cannot be sent to another thread.
unsafe impl Send for String {}
// Safety: [`String`] provides no interior mutability; shared references are read-only.
unsafe impl Sync for String {}
