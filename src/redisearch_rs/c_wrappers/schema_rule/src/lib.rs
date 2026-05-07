/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Safe wrapper around [`ffi::SchemaRule`].

use std::{
    ffi::{CStr, c_char},
    slice,
};

use ffi::DocumentType;

/// A safe wrapper around an `ffi::SchemaRule`.
#[repr(transparent)]
pub struct SchemaRule(ffi::SchemaRule);

impl SchemaRule {
    /// Create a `SchemaRule` wrapper from a non-null pointer.
    ///
    /// # Safety
    ///
    /// 1. `ptr` must be a [valid], non-null pointer to an `IndexSpec` that is properly initialized.
    ///    This also applies to any of its subfields. Specifically:
    ///    1. If `lang_field` is non-null, it points to a valid C string.
    ///    2. If `score_field` is non-null, it points to a valid C string.
    ///    3. If `payload_field` is non-null, it points to a valid C string.
    ///
    /// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
    pub const unsafe fn from_raw<'a>(ptr: *const ffi::SchemaRule) -> &'a Self {
        // Safety: ensured by caller (1.)
        unsafe { ptr.cast::<Self>().as_ref().unwrap() }
    }

    /// Get the language field [`CStr`], if present.
    pub const fn lang_field(&self) -> Option<&CStr> {
        // Safety: (1.) due to creation with `SchemaRule::from_raw`
        unsafe { maybe_cstr_from_ptr(self.0.lang_field) }
    }

    /// Get the score field [`CStr`], if present.
    pub const fn score_field(&self) -> Option<&CStr> {
        // Safety: (1.) due to creation with `SchemaRule::from_raw`
        unsafe { maybe_cstr_from_ptr(self.0.score_field) }
    }

    /// Get the payload field [`CStr`], if present.
    pub const fn payload_field(&self) -> Option<&CStr> {
        // Safety: (1.) due to creation with `SchemaRule::from_raw`
        unsafe { maybe_cstr_from_ptr(self.0.payload_field) }
    }

    /// Expose the underlying `filter_fields` as a [`Vec`] of &[`CStr`].
    pub fn filter_fields(&self) -> impl ExactSizeIterator<Item = &CStr> {
        debug_assert!(
            !self.0.filter_fields.is_null(),
            "filter_fields must not be null"
        );

        // Safety: (1.) due to creation with `SchemaRule::from_raw`
        let len = unsafe { ffi::array_len_func(self.0.filter_fields as ffi::array_t) }
            .try_into()
            .expect("array_len must not exceed usize");

        // Safety: (1.) due to creation with `SchemaRule::from_raw`
        unsafe { slice::from_raw_parts(self.0.filter_fields, len) }
            .iter()
            .map(|ptr| unsafe { CStr::from_ptr(*ptr) })
    }

    /// Expose the underlying `filter_fields_index` as a slice of ints.
    pub fn filter_fields_index(&self) -> &[i32] {
        // These two arrays are assumed to be of the same length.
        let len = self.filter_fields().len();
        debug_assert!(
            !self.0.filter_fields_index.is_null(),
            "filter_fields_index must not be null"
        );
        // Safety: (1.) due to creation with `SchemaRule::from_raw`
        unsafe { slice::from_raw_parts(self.0.filter_fields_index, len) }
    }

    /// Get the underlying `type_`.
    pub const fn type_(&self) -> DocumentType {
        self.0.type_
    }
}

/// Convert a raw C string pointer to an `Option<&CStr>`, returning `None` if the pointer is null.
///
/// # Safety
///
/// 1. The memory pointed to by ptr must contain a valid nul terminator at the end of the string.
/// 2. ptr must be valid for reads of bytes up to and including the nul terminator.
///    This means in particular:
///    a. The entire memory range of this CStr must be contained within a single allocation!
/// 3. The memory referenced by the returned CStr must not be mutated for the duration of lifetime 'a.
/// 4. The nul terminator must be within isize::MAX from ptr
///
/// # Caveat
///
/// The lifetime for the returned slice is inferred from its usage.
/// To prevent accidental misuse, it's suggested to tie the lifetime to whichever source lifetime is safe in the context,
/// such as by providing a helper function taking the lifetime of a host value for the slice, or by explicit annotation.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
const unsafe fn maybe_cstr_from_ptr<'a>(ffi_field: *mut c_char) -> Option<&'a CStr> {
    if ffi_field.is_null() {
        None
    } else {
        // Safety: Ensured by caller (1., 2., 3., 4.). Non-nullness is ensured by the call to is_null() above.
        Some(unsafe { CStr::from_ptr(ffi_field) })
    }
}
