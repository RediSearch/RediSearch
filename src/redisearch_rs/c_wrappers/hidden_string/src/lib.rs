/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Safe wrapper around [`ffi::HiddenString`].

use std::{
    ffi::CStr,
    fmt::{self, Debug, Pointer},
};

/// A safe wrapper around `ffi::HiddenString`
#[repr(transparent)]
pub struct HiddenString(ffi::HiddenString);

impl HiddenString {
    /// Get a [`HiddenString`] reference from a raw pointer.
    ///
    /// # Safety
    ///
    /// 1. `ptr` must be a valid, non-null pointer to a properly initialized
    ///    `ffi::HiddenString`, including its subfields.
    /// 2. The pointee must not be mutated for the entire lifetime `'a`.
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
