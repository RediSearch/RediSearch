/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{
    ffi::{CStr, c_char},
    mem::ManuallyDrop,
    sync::Arc,
};

/// Mock implementation of RedisModuleString from redismodule.h for testing purposes.
///
/// Owns a NUL-terminated copy of the bytes it was created from, mirroring
/// Redis's owned, NUL-terminated (yet binary-safe) sds buffers: callers may
/// hand in a transient source and still read the string back after that source
/// is gone. `length` is the logical byte count excluding the trailing NUL.
#[repr(C)]
pub(crate) struct UserString {
    /// The owned bytes, NUL-terminated. `length` is `owned.len() - 1`.
    owned: Box<[c_char]>,
    pub(crate) length: usize,
}

impl UserString {
    /// Pointer to the owned, NUL-terminated bytes, valid for as long as `self`
    /// is alive. The first `length` bytes are the logical string.
    pub(crate) fn user(&self) -> *const c_char {
        self.owned.as_ptr()
    }
}

/// Mock implementation of RedisModule_CreateString from redismodule.h for testing purposes.
///
/// A null `ptr` produces a zero-filled string of length `len`, matching real
/// Redis rather than yielding an empty string.
///
/// Safety:
/// 1. ptr must be null, or a valid pointer to `len` initialized bytes.
#[unsafe(export_name = "_RedisModule_CreateString.1")]
#[expect(non_snake_case)]
pub(crate) unsafe extern "C" fn RedisModule_CreateString(
    _ctx: *mut redis_module::raw::RedisModuleCtx,
    ptr: *const ::std::ffi::c_char,
    len: usize,
) -> *mut redis_module::raw::RedisModuleString {
    // Build an owned, NUL-terminated copy of `len` bytes so the string can be
    // read back as a C string even once the caller's source buffer is gone.
    // Real Redis zero-fills the buffer when the source is NULL (see
    // `sdsnewlen` / `createEmbeddedStringObject`), producing a string of length
    // `len` rather than an empty one, so mirror that here.
    let mut owned = Vec::with_capacity(len + 1);
    if ptr.is_null() {
        owned.resize(len, 0);
    } else {
        // Safety: precondition 1 guarantees `ptr` covers `len` initialized bytes.
        let src = unsafe { std::slice::from_raw_parts(ptr, len) };
        owned.extend_from_slice(src);
    }
    owned.push(0);

    let val = Arc::new(UserString {
        // `length` excludes the trailing NUL and always matches `len`, even for
        // a NULL (zero-filled) source.
        length: len,
        owned: owned.into_boxed_slice(),
    });

    Arc::into_raw(val)
        .cast::<redis_module::raw::RedisModuleString>()
        .cast_mut()
}

/// Mock implementation of RedisModule_StringPtrLen from redismodule.h for testing purposes.
///
/// Safety:
/// 1. s must be a valid pointer to a RedisModuleString created by this mock implementation.
/// 2. len must be null, or a valid pointer to a usize.
#[expect(non_snake_case)]
pub(crate) unsafe extern "C" fn RedisModule_StringPtrLen(
    s: *const redis_module::raw::RedisModuleString,
    len: *mut usize,
) -> *const ::std::ffi::c_char {
    // Safety: The caller ensured the ptr is correct (1.)
    let s = ManuallyDrop::new(unsafe { Arc::from_raw(s.cast::<UserString>()) });

    // The real API treats `len` as optional; callers that only want the pointer
    // pass null.
    if !len.is_null() {
        // Safety: Caller provides a valid, non-null len pointer (2)
        unsafe {
            *len = s.length;
        }
    }
    s.user()
}

/// Mock implementation of RedisModule_FreeString from redismodule.h for testing purposes.
///
/// Safety:
/// 1. s must be a valid pointer to a RedisModuleString created by this mock
/// 2. The function must not be called more than once for the same string.
#[expect(non_snake_case)]
pub(crate) unsafe extern "C" fn RedisModule_FreeString(
    _ctx: *mut redis_module::raw::RedisModuleCtx,
    s: *mut redis_module::raw::RedisModuleString,
) {
    // Safety: we own the memory (1) and the caller promised to call this only once (2)
    drop(unsafe { Arc::from_raw(s.cast::<UserString>()) })
}

/// Mock implementation of RedisModule_Strdup from redismodule.h for testing purposes.
///
/// # Safety
/// 1. `s` must be a valid pointer to a NULL-terminated string.
#[expect(non_snake_case)]
pub unsafe extern "C" fn RedisModule_Strdup(s: *const c_char) -> *mut c_char {
    if s.is_null() {
        std::ptr::null_mut()
    } else {
        // Safety: s is a valid pointer to a NULL-terminated string (1).
        let c_str = unsafe { CStr::from_ptr(s) };
        // Need an extra byte for null terminator
        let len = c_str.count_bytes() + 1;
        // Allocate memory using the mock allocator
        let out = crate::allocator::alloc_shim(len) as *mut c_char;
        assert!(!out.is_null());

        // Safety:
        // - 1. ensures the source is valid.
        // - we just allocated the destination memory.
        unsafe {
            std::ptr::copy_nonoverlapping(s, out, len);
        }

        out
    }
}

/// Mock implementation of RedisModule_TrimStringAllocation from redismodule.h for testing purposes.
#[expect(non_snake_case)]
pub(crate) const unsafe extern "C" fn RedisModule_TrimStringAllocation(
    _s: *mut redis_module::raw::RedisModuleString,
) {
    // no-op we do not need to trim in tests.
}

/// Mock implementation of RedisModule_HoldString from redismodule.h for testing purposes.
///
/// Safety:
/// 1. s must be a valid pointer to a RedisModuleString created by this mock
#[expect(non_snake_case)]
pub(crate) unsafe extern "C" fn RedisModule_HoldString(
    _ctx: *mut redis_module::raw::RedisModuleCtx,
    s: *mut redis_module::raw::RedisModuleString,
) -> *mut redis_module::raw::RedisModuleString {
    // Safety: The caller ensured the ptr is correct (1.)
    unsafe {
        Arc::increment_strong_count(s.cast::<UserString>());
    }
    s
}

/// Mock implementation of RedisModule_RetainString from redismodule.h for testing purposes.
///
/// Safety:
/// 1. `s` must be a valid pointer to a RedisModuleString created by this mock
#[expect(non_snake_case)]
pub(crate) unsafe extern "C" fn RedisModule_RetainString(
    _ctx: *mut redis_module::raw::RedisModuleCtx,
    s: *mut redis_module::raw::RedisModuleString,
) {
    // Safety: The caller ensured the ptr is correct (1.)
    unsafe {
        Arc::increment_strong_count(s.cast::<UserString>());
    }
}

/// Mock implementation of RedisModule_CreateStringPrintf from redismodule.h for testing purposes.
///
/// Safety:
/// 1. fmt must be a valid pointer to a NULL-terminated C string.
#[expect(non_snake_case)]
pub(crate) unsafe extern "C" fn RedisModule_CreateStringPrintf(
    ctx: *mut redis_module::raw::RedisModuleCtx,
    fmt: *const c_char,
) -> *mut redis_module::raw::RedisModuleString {
    // Safety: 1. ensures fmt is a valid pointer to a NULL-terminated C string.
    let c_str = unsafe { CStr::from_ptr(fmt) };

    // C variadic are not stable so we cannot actually format the string.
    // Safety: 1. ensures fmt is a valid pointer and we counting the bytes to pass the proper length.
    unsafe { RedisModule_CreateString(ctx, fmt, c_str.count_bytes()) }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Creating a string copies its source: the value reads back correctly and
    /// is NUL-terminated even after the caller's source buffer is gone.
    #[test]
    fn create_copies_source_and_nul_terminates() {
        let s = {
            let src: Vec<c_char> = b"hello".iter().map(|&b| b as c_char).collect();
            // Safety: `src` covers `src.len()` initialized bytes.
            unsafe { RedisModule_CreateString(std::ptr::null_mut(), src.as_ptr(), src.len()) }
            // `src` is dropped here, before we read `s` back below.
        };

        let mut len = 0usize;
        // Safety: `s` was created by the mock; `len` is a valid slot.
        let ptr = unsafe { RedisModule_StringPtrLen(s, &mut len) };
        assert_eq!(len, 5);
        // Safety: the mock stores `len` logical bytes plus a trailing NUL, so
        // `ptr` is valid for `len + 1` bytes.
        let bytes = unsafe { std::slice::from_raw_parts(ptr.cast::<u8>(), len + 1) };
        assert_eq!(bytes, b"hello\0");

        // Safety: `s` was created by the mock and is freed exactly once.
        unsafe { RedisModule_FreeString(std::ptr::null_mut(), s) };
    }

    /// A null source pointer yields a zero-filled string of the requested
    /// length (matching real Redis), and `RedisModule_StringPtrLen` accepts a
    /// null length out-pointer.
    #[test]
    fn null_source_is_zero_filled_and_null_len_ok() {
        // Safety: a null `ptr` is explicitly allowed (precondition 1).
        let s = unsafe { RedisModule_CreateString(std::ptr::null_mut(), std::ptr::null(), 7) };

        // A null `len` out-pointer must be accepted without writing through it.
        // Safety: `s` was created by the mock; the `len` out-pointer may be null.
        let ptr = unsafe { RedisModule_StringPtrLen(s, std::ptr::null_mut()) };

        let mut len = 42usize;
        // Safety: `s` was created by the mock; `len` is a valid slot.
        unsafe { RedisModule_StringPtrLen(s, &mut len) };
        assert_eq!(
            len, 7,
            "a null source is zero-filled to the requested length"
        );

        // The buffer is `len` zero bytes plus the trailing NUL — all zero.
        // Safety: `ptr` is valid for `len + 1` bytes.
        let bytes = unsafe { std::slice::from_raw_parts(ptr.cast::<u8>(), len + 1) };
        assert_eq!(bytes, &[0u8; 8]);

        // Safety: `s` was created by the mock and is freed exactly once.
        unsafe { RedisModule_FreeString(std::ptr::null_mut(), s) };
    }
}
