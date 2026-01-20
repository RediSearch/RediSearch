/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::hidden_string::HiddenString;

/// A safe wrapper around an `ffi::FieldSpec`.
#[repr(transparent)]
pub struct FieldSpec(ffi::FieldSpec);

impl FieldSpec {
    /// Create a `FieldSpec` wrapper from a non-null pointer.
    ///
    /// # Safety
    ///
    /// 1. `ptr` must be a valid non-null pointer to an `ffi::FieldSpec` that is properly initialized.
    ///    This also applies to any of its subfields.
    ///
    /// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
    pub const unsafe fn from_raw<'a>(ptr: *const ffi::FieldSpec) -> &'a Self {
        // Safety: ensured by caller (1.)
        unsafe { ptr.cast::<Self>().as_ref().unwrap() }
    }

    /// Get a reference to the underlying non-null pointer.
    #[cfg(test)]
    pub const fn to_raw(&self) -> *const ffi::FieldSpec {
        std::ptr::from_ref(&self.0)
    }

    /// Get the underlying field name as a `&HiddenString`.
    pub const fn field_name(&self) -> &HiddenString {
        // Safety: (1.) due to creation with `FieldSpec::from_raw`
        unsafe { HiddenString::from_raw(self.0.fieldName) }
    }

    /// Get the underlying field path as a `&HiddenString`.
    pub const fn field_path(&self) -> &HiddenString {
        // Safety: (1.) due to creation with `FieldSpec::from_raw`
        unsafe { HiddenString::from_raw(self.0.fieldPath) }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use std::{ffi::CStr, mem, ptr};

    use pretty_assertions::assert_eq;

    #[test]
    fn field_name_and_path() {
        let name = c"name";
        let path = c"path";
        let fs = field_spec(name, path);

        let sut = unsafe { FieldSpec::from_raw(ptr::from_ref(&fs)) };

        assert_eq!(sut.field_name().get_secret_value(), name);
        assert_eq!(sut.field_path().get_secret_value(), path);
    }

    fn field_spec(field_name: &CStr, field_path: &CStr) -> ffi::FieldSpec {
        let mut res = unsafe { mem::zeroed::<ffi::FieldSpec>() };
        res.fieldName =
            unsafe { ffi::NewHiddenString(field_name.as_ptr(), field_name.count_bytes(), false) };
        res.fieldPath =
            unsafe { ffi::NewHiddenString(field_path.as_ptr(), field_path.count_bytes(), false) };
        res
    }
}
