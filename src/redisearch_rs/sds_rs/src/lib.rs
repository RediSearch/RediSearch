/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Rust wrapper around [`sds`] (Simple Dynamic String) that owns it and
//! implements a number of useful traits. `sds` is a NULL-terminated string with
//! a dynamically sized header that holds its length.
//!
//! More info on `sds` can be found here: <https://github.com/antirez/sds>

use std::{ffi::CStr, fmt};

use ffi::{sds, sdsdup, sdsfree};

/// Rust wrapper around [`sds`]
///
/// # Invariants
/// - The wrapped [`sds`] is non-null and points to a fully initialized `sds`
///   originating from a call to [`sdsnew`](ffi::sdsnew), [`sdsnewlen`](ffi::sdsnewlen),
///   [`sdsempty`](ffi::sdsempty), or [`sdsdup`].
/// - The wrapped [`sds`] is unique, i.e. there are no other references to its
///   backing memory buffer.
/// - The wrapped [`sds`] points to a null-terminated C string.
/// - The wrapped [`sds`] is valid for reads of bytes at least until and including
///   its null terminator.
/// - The null terminator is within `isize::MAX` from the address the wrapped [`sds`]
///   points to, i.e. the start of the actual C string.
#[repr(transparent)]
pub struct OwnedSds(sds);

impl OwnedSds {
    /// Wrap an [`sds`] in a new [`OwnedSds`].
    ///
    /// # Safety
    /// - `s` must be non-null and point to a fully initialized `sds`,
    ///   originating from a call to [`sdsnew`](ffi::sdsnew), [`sdsnewlen`](ffi::sdsnewlen),
    ///   [`sdsempty`](ffi::sdsempty), or [`sdsdup`].
    /// - `s` must be unique, i.e. there must be no other
    ///   references to its backing memory buffer.\
    /// - The null terminator of `s` must be within `isize::MAX` from the address
    ///   it points to, i.e. the start of the actual C string.
    pub unsafe fn wrap_sds(s: sds) -> Self {
        debug_assert!(!s.is_null(), "`s` cannot be NULL");
        debug_assert!(
            // Using `strlen` here ensures we don't rely on the SDS header data.
            // Safety: caller is to ensure that `s` is
            unsafe { libc::strlen(s as *const std::ffi::c_char) } <= isize::MAX as usize,
            "The null terminator of `s` must be within `isize::MAX` from the start."
        );
        Self(s)
    }

    /// Consumes this [`OwnedSds`], and returns the [`sds`] it wraps without
    /// freeing it.
    pub fn into_inner(self) -> sds {
        let s = self.0;
        std::mem::forget(self);
        s
    }

    /// Convert a reference to an [`OwnedSds`] to a [`CStr`] reference.
    #[allow(clippy::needless_lifetimes, reason = "Referred to in safety comment")]
    pub fn as_cstr<'this>(&'this self) -> &'this CStr {
        // Safety:
        // The invariants of [`OwnedSds`] ensure that:
        // 1. `self.0` points to a null-terminated C string;
        // 2. `self.0` is valid for reads up to and including its null terminator;
        // 3. `self.0` is unique, and the `sds` can therefore not be mutated
        //    for the lifetime `'this'.
        unsafe { CStr::from_ptr(self.0) }
    }
}

impl fmt::Debug for OwnedSds {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_cstr().to_string_lossy().fmt(f)
    }
}

impl Clone for OwnedSds {
    fn clone(&self) -> Self {
        // Safety: `self.0` originates from a call to `OwnedSds::wrap_sds`,
        // which requires the `sds` to be unique and correctly initialized.
        let s_dup = unsafe { sdsdup(self.0) };
        assert!(!s_dup.is_null(), "Allocation for SDS duplication failed");
        Self(s_dup)
    }
}

impl Drop for OwnedSds {
    fn drop(&mut self) {
        // Safety: `self.0` originates from a call to `OwnedSds::wrap_sds`,
        // which requires the `sds` to be unique and correctly initialized.
        unsafe { sdsfree(self.0) };
    }
}

/// Safety: `OwnedSds` is safe to send across threads because it owns its
/// backing memory buffer and ensures that no other references exist to it.
unsafe impl Send for OwnedSds {}

/// Safety: `OwnedSds` is safe to share across threads because it owns its
/// backing memory buffer and ensures that no other references exist to it.
unsafe impl Sync for OwnedSds {}
