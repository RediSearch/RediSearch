/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Safe wrappers around [`ffi::HiddenString`].

use std::{
    ffi::CStr,
    fmt::{self, Debug, Pointer},
    marker::PhantomData,
    ops::Deref,
    ptr::NonNull,
};

/// A safe wrapper around `ffi::HiddenString`
#[repr(transparent)]
pub struct HiddenString(ffi::HiddenString);

impl HiddenString {
    /// Get a [`HiddenString`] reference from a raw pointer.
    ///
    /// # Safety
    ///
    /// 1. `ptr` must be a [valid], non-null pointer to a properly initialized
    ///    `ffi::HiddenString`, including its subfields.
    /// 2. The pointee must not be mutated for the entire lifetime `'a`.
    ///
    /// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
    pub const unsafe fn from_raw<'a>(ptr: *const ffi::HiddenString) -> &'a Self {
        // SAFETY: Ensured by caller (1., 2.)
        unsafe {
            ptr.cast::<Self>()
                .as_ref()
                .expect("HiddenString ptr must be non-null")
        }
    }

    /// Return the raw pointer to the underlying [`ffi::HiddenString`].
    pub const fn as_ptr(&self) -> *const ffi::HiddenString {
        &raw const self.0
    }

    /// Get the secret (aka. "unsafe" in C land) value from the underlying [`ffi::HiddenString`].
    ///
    /// This is safe **only if** the C function returns a pointer that stays valid
    /// for at least the lifetime of `self`, and the memory contains a NUL at `len`.
    pub fn secret_value(&self) -> &CStr {
        let mut len = 0;

        // Safety:
        // - `len` is a local variable that is not referenced anywhere else.
        // - `self.as_ptr()` is a valid, non-null `ffi::HiddenString`.
        let data = unsafe { ffi::HiddenString_GetUnsafe(self.as_ptr(), &mut len) };
        debug_assert!(!data.is_null(), "data must not be null");

        // The length doesn't include the nul terminator so we need to add one.
        let n = len.checked_add(1).expect("length overflow");
        // Safety: must be ensured by the implementation of `ffi::HiddenString_GetUnsafe` above.
        let bytes = unsafe { core::slice::from_raw_parts(data.cast::<u8>(), n) };

        CStr::from_bytes_with_nul(bytes).expect("malformed C string")
    }
}

impl Debug for HiddenString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("HiddenString")
            .field(&format_args!("****"))
            .finish()
    }
}

impl Pointer for HiddenString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Pointer::fmt(&self.as_ptr(), f)
    }
}

/// An owned [`ffi::HiddenString`] that borrows its backing buffer.
///
/// On drop it frees the `ffi::HiddenString` wrapper (`takeOwnership = false`, so
/// the borrowed buffer is left untouched). [`Deref`]s to [`HiddenString`] for
/// read access.
pub struct OwnedHiddenString<'a> {
    ptr: NonNull<ffi::HiddenString>,
    _phantom: PhantomData<&'a CStr>,
}

impl<'a> OwnedHiddenString<'a> {
    /// Create an `OwnedHiddenString` borrowing `bytes` for `'a`.
    ///
    /// The underlying `ffi::HiddenString` points directly at `bytes` rather than
    /// duplicating it, so `bytes` must outlive the returned value.
    pub fn new(bytes: &'a CStr) -> Self {
        // Safety:
        // - `bytes.as_ptr()` is valid for reads of `bytes.count_bytes() + 1` bytes (payload
        //   plus the trailing NUL) for at least `'a`.
        // - `takeOwnership = false` makes the C implementation point directly at `bytes`
        //   instead of duplicating it, which is why we tie the returned value to `'a`.
        let ptr = unsafe { ffi::NewHiddenString(bytes.as_ptr(), bytes.count_bytes(), false) };

        Self {
            ptr: NonNull::new(ptr).expect("NewHiddenString should never return a null pointer"),
            _phantom: PhantomData,
        }
    }
}

impl Deref for OwnedHiddenString<'_> {
    type Target = HiddenString;

    fn deref(&self) -> &HiddenString {
        // Safety: `self.ptr` is a valid `ffi::HiddenString` created in `new`; the returned
        // reference borrows `self`, so it cannot outlive the owner.
        unsafe { HiddenString::from_raw(self.ptr.as_ptr()) }
    }
}

impl Drop for OwnedHiddenString<'_> {
    fn drop(&mut self) {
        // Safety: `self.ptr` was allocated by `NewHiddenString` with `takeOwnership = false`
        // in `OwnedHiddenString::new`, so freeing it here with `tookOwnership = false` releases
        // the `ffi::HiddenString` wrapper only, leaving the borrowed buffer untouched.
        unsafe { ffi::HiddenString_Free(self.ptr.as_ptr(), false) };
    }
}
