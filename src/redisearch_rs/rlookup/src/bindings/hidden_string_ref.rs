/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{ffi::CStr, marker::PhantomData, ptr::NonNull};

/// A safe wrapper around a non-null `ffi::HiddenString` reference.
#[derive(Debug, Clone, Copy)]
#[repr(transparent)]
pub struct HiddenStringRef<'a>(
    NonNull<ffi::HiddenString>,
    PhantomData<&'a ffi::HiddenString>,
);

impl<'a> HiddenStringRef<'a> {
    /// Create a `HiddenStringRef` wrapper from a non-null pointer.
    ///
    /// # Safety
    ///
    /// 1. `ptr` must be a valid non-null pointer to an `ffi::HiddenString` that is properly initialized.
    ///    This also applies to any of its subfields.
    /// 2. The pointed to `ffi::HiddenString` must not be mutated for the entire lifetime of the returned `HiddenStringRef`.
    ///
    /// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
    pub const unsafe fn from_raw(ptr: *const ffi::HiddenString) -> Self {
        Self(
            NonNull::new(ptr.cast_mut()).expect("HiddenString ptr must be non-null"),
            PhantomData,
        )
    }

    /// Get the secret (aka. "unsafe" in C land) value from the underlying [`ffi::HiddenString`].
    ///
    /// This is safe **only if** the C function returns a pointer that stays valid
    /// for at least the lifetime of `self`, and the memory contains a NUL at `len`.
    ///
    /// This consumes the `HiddenStringRef` and can only be called once.
    pub fn into_secret_value(self) -> &'a CStr {
        let mut len = 0;

        // Safety:
        // - `len` is a local variable that we just allocated and is not being referenced anywhere else.
        // - `self.0` is a valid non-null pointer to an `ffi::HiddenString` due to creation with `HiddenStringRef::from_raw`
        let data = unsafe { ffi::HiddenString_GetUnsafe(self.0.as_ptr(), &mut len) };
        debug_assert!(!data.is_null(), "data must not be null");

        // The length doesn't include the nul terminator so we need to add one.
        let n = len.checked_add(1).expect("length overflow");
        // Safety: must be ensured by the implementation of `ffi::HiddenString_GetUnsafe` above.
        let bytes = unsafe { core::slice::from_raw_parts(data.cast::<u8>(), n) };

        CStr::from_bytes_with_nul(bytes).expect("malformed C string")
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use pretty_assertions::assert_eq;

    #[test]
    #[cfg_attr(miri, ignore = "miri does not support FFI functions")]
    fn get_secret_value() {
        let input = c"Ab#123!";
        let ffi_hs = unsafe { ffi::NewHiddenString(input.as_ptr(), input.count_bytes(), false) };
        let sut = unsafe { HiddenStringRef::from_raw(ffi_hs) };

        let actual = sut.into_secret_value();

        assert_eq!(actual, input);

        unsafe { ffi::HiddenString_Free(ffi_hs, false) };
    }
}
