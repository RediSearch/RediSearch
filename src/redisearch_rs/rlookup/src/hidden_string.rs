/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{ffi::CStr, ptr, slice};

/// A safe wrapper around an `ffi::HiddenString`.
///
/// We wrap the `HiddenString` C implementation here.
#[derive(Debug)]
#[repr(transparent)]
pub struct HiddenString(ffi::HiddenString);

impl HiddenString {
    /// Create a `HiddenString` wrapper from a non-null pointer.
    ///
    /// # Safety
    ///
    /// 1. `ptr` must be a valid non-null pointer to a valid [`ffi::HiddenString`]
    ///    that upholds the requirements of the corresponding C implementation.
    ///
    /// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
    pub const unsafe fn from_raw<'a>(ptr: *const ffi::HiddenString) -> &'a Self {
        // Safety: ensured by caller (1.)
        unsafe { ptr.cast::<Self>().as_ref().unwrap() }
    }

    /// Get a reference to the underlying non-null pointer.
    pub const fn to_raw(&self) -> *const ffi::HiddenString {
        ptr::from_ref(&self.0)
    }

    /// Get the secret (aka. "unsafe" in C land) value from the underlying [`ffi::HiddenString`].
    pub fn get_secret_value(&self) -> &CStr {
        let value_ptr = ptr::from_ref(&self.0);
        let mut len = 0;
        let len_ptr = ptr::from_mut(&mut len);

        // Safety:
        // - `len` is a local variable that we just allocated and is not being referenced anywhere else.
        // - `self.0` is a valid non-null pointer to an `ffi::HiddenString` due to creation with `HiddenString::from_raw`
        let data = unsafe { ffi::HiddenString_GetUnsafe(value_ptr, len_ptr) };
        debug_assert!(!data.is_null(), "data must not be null");

        // The length doesn't include the nul terminator so we need to add one.
        // Safety: must be ensured by the implementation of `ffi::HiddenString_GetUnsafe` above
        let bytes = unsafe { slice::from_raw_parts(data.cast::<u8>(), len + 1) };

        CStr::from_bytes_with_nul(bytes).expect("string must not be malformed")
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use pretty_assertions::assert_eq;

    #[test]
    fn secret_value() {
        let input = c"Ab#123!";
        let ffi_hs = unsafe { ffi::NewHiddenString(input.as_ptr(), input.count_bytes(), false) };
        let sut = unsafe { HiddenString::from_raw(ffi_hs) };

        let actual = sut.get_secret_value();

        assert_eq!(actual, input);
    }
}
