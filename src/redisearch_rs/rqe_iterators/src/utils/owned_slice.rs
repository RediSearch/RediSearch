/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

/// An owned slice of `T` which is allocated either in C (Redis) or Rust.
pub struct OwnedSlice<T> {
    kind: SliceKind<T>,
}

impl<T> Default for OwnedSlice<T> {
    #[inline(always)]
    fn default() -> Self {
        Self {
            kind: SliceKind::Rust(Vec::default()),
        }
    }
}

impl<T> OwnedSlice<T> {
    /// # Safety
    ///
    /// ptr must be non-null and point to `len` initialized elements
    /// allocated via `RedisModule_Alloc`. [`OwnedSlice`] takes
    /// ownership from ptr and should therefore no longer be freed by callee.
    #[inline(always)]
    pub const unsafe fn from_c(ptr: *mut T, len: usize) -> Self {
        Self {
            kind: SliceKind::C(
                // Safety: contract upheld by callee
                unsafe { RedisSlice::from_raw(ptr, len) },
            ),
        }
    }
}

impl<T> From<Vec<T>> for OwnedSlice<T> {
    #[inline(always)]
    fn from(value: Vec<T>) -> Self {
        Self {
            kind: SliceKind::Rust(value),
        }
    }
}

impl<T> std::ops::Deref for OwnedSlice<T> {
    type Target = [T];

    #[inline(always)]
    fn deref(&self) -> &[T] {
        match &self.kind {
            SliceKind::C(s) => s,
            SliceKind::Rust(v) => v,
        }
    }
}

enum SliceKind<T> {
    C(RedisSlice<T>),
    Rust(Vec<T>),
}

/// A thin wrapper for memory allocated by Redis.
///
/// This is useful for slices that were created from C,
/// and we wish to use it as-is without having to re-allocate.
struct RedisSlice<T> {
    ptr: std::ptr::NonNull<T>,
    len: usize,
}

impl<T> RedisSlice<T> {
    /// # Safety
    ///
    /// ptr must be non-null and point to `len` initialized elements
    /// allocated via `RedisModule_Alloc`. [`RedisSlice`] takes
    /// ownership from ptr and should therefore no longer be freed by callee.
    #[inline(always)]
    const unsafe fn from_raw(ptr: *mut T, len: usize) -> Self {
        debug_assert!(!ptr.is_null());
        // Safety: because of constructor contract
        let ptr = unsafe { std::ptr::NonNull::new_unchecked(ptr) };
        Self { ptr, len }
    }
}

impl<T> std::ops::Deref for RedisSlice<T> {
    type Target = [T];

    #[inline(always)]
    fn deref(&self) -> &[T] {
        // Safety: ptr is not null and we received length via created function
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }
}

impl<T> Drop for RedisSlice<T> {
    fn drop(&mut self) {
        // Safety: Redis Free fn is defined at this point
        let free_fn = unsafe { ffi::RedisModule_Free.unwrap() };
        // Safety: ptr is not null (see above check)
        unsafe {
            free_fn(self.ptr.as_ptr() as *mut std::ffi::c_void);
        }
    }
}
