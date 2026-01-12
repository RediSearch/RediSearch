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
    slice,
};

use ffi::DocumentType;

use crate::RLookupKey;

/// A safe wrapper around a pointer to a `SchemaRule`, the underlying pointer is non-null.
///
/// # Safety
///
/// We wrap the SchemaRule C implementation here.
///
/// Given a valid SchemaRule pointer, the fields `lang_field`, `score_field`, and `payload_field`
/// are guaranteed to either be null or point to valid C strings. So we assume these invariants:
///
/// 1. If `lang_field` is non-null, it points to a valid C string.
/// 2. If `score_field` is non-null, it points to a valid C string.
/// 3. If `payload_field` is non-null, it points to a valid C string.
///
/// We also assume that
/// 4. `filter_fields` is a valid non-null pointer to an `array_t` type array of non-null pointers to valid C strings.
/// 5. `filter_fields_index` is a valid non-null pointer to a regular type array of ints.
/// 6. the number of elements in the `filter_fields` and `filter_fields_index` arrays are equal.
#[repr(transparent)]
pub struct SchemaRule(ffi::SchemaRule);

impl SchemaRule {
    /// Create a SchemaRule from a non-null pointer.
    ///
    /// # Safety
    ///
    /// 1. The caller must ensure that the given pointer upholds the safety invariants described on the type documentation.
    pub const unsafe fn from_raw<'a>(ptr: *const ffi::SchemaRule) -> &'a Self {
        // Safety: ensured by caller (1.)
        unsafe { ptr.cast::<Self>().as_ref().unwrap() }
    }

    /// Get the language field [`CStr`], if present.
    pub const fn lang_field(&self) -> Option<&CStr> {
        // Safety: (1) due to creation with `SchemaRule::from_raw` its field pointers are valid or null, (2) lifetime tied to `self.0.lang_field`.
        unsafe { maybe_cstr_from_ptr(self.0.lang_field) }
    }

    /// Get the score field [`CStr`], if present.
    pub const fn score_field(&self) -> Option<&CStr> {
        // Safety: (1) due to creation with `SchemaRule::from_raw` its field pointers are valid or null, (2) lifetime tied to `self.0.score_field`.
        unsafe { maybe_cstr_from_ptr(self.0.score_field) }
    }

    /// Get the payload field [`CStr`], if present.
    pub const fn payload_field(&self) -> Option<&CStr> {
        // Safety: (1) due to creation with `SchemaRule::from_raw` its field pointers are valid or null, (2) lifetime tied to `self.0.payload_field`.
        unsafe { maybe_cstr_from_ptr(self.0.payload_field) }
    }

    /// Tests if the given [`crate::lookup::RLookupKey`] is a special key (lang, score, or payload field) with respect to this schema rule.
    pub fn is_special_key(&self, key: &RLookupKey) -> bool {
        // Check if the key is one of the special fields
        [self.lang_field(), self.score_field(), self.payload_field()]
            .into_iter()
            .any(|f| f == Some(key.name()))
    }

    /// Expose the underlying `filter_fields` as a [`Vec`] of &[`CStr`].
    pub fn filter_fields(&self) -> Vec<&CStr> {
        let len = self.filter_fields_len();
        // Safety: (4.) due to creation with `SchemaRule::from_raw`
        let filter_fields = unsafe { slice::from_raw_parts(self.0.filter_fields, len) };
        filter_fields
            .iter()
            .map(|&c|
                // Safety: (4.) due to creation with `SchemaRule::from_raw`
                unsafe { CStr::from_ptr(c) })
            .collect::<Vec<_>>()
    }

    /// Expose the underlying `filter_fields_index` as a slice of ints.
    pub fn filter_fields_index(&self) -> &[i32] {
        let len = self.filter_fields_len();
        // Safety: (5., 6.) due to creation with `SchemaRule::from_raw`
        unsafe { slice::from_raw_parts(self.0.filter_fields_index, len) }
    }

    /// Get the length of the `filter_fields` array.
    fn filter_fields_len(&self) -> usize {
        // Safety: (4.) due to creation with `SchemaRule::from_raw`
        unsafe { ffi::array_len_func(self.0.filter_fields as ffi::array_t) }
            .try_into()
            .expect("array_len must not exceed usize")
    }

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
/// To prevent accidental misuse, it’s suggested to tie the lifetime to whichever source lifetime is safe in the context,
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

#[cfg(test)]
mod test {
    use super::*;

    use std::{ffi::CStr, mem, ptr};

    use pretty_assertions::assert_eq;

    /// Test filter_fields and filter_fields_index together since their lengths are coupled.
    #[test]
    fn fields_and_indices() {
        let mut schema_rule = unsafe { mem::zeroed::<ffi::SchemaRule>() };
        schema_rule.filter_fields = filter_fields_array(&[c"aaa", c"bbb"]);
        schema_rule.filter_fields_index = [10, 20].as_mut_ptr();
        let sut = unsafe { SchemaRule::from_raw(ptr::from_ref(&schema_rule)) };

        let ff = sut.filter_fields();
        let ffi = sut.filter_fields_index();

        assert_eq!(ff.len(), 2);
        assert_eq!(ff[0], c"aaa");
        assert_eq!(ff[1], c"bbb");
        assert_eq!(ffi.len(), 2);
        assert_eq!(ffi, [10, 20]);
    }

    fn filter_fields_array(filter_fields: &[&CStr]) -> *mut *mut i8 {
        let temp = filter_fields
            .iter()
            .map(|ff| ff.as_ptr().cast_mut())
            .collect::<Vec<_>>();

        crate::mock::array_new(&temp)
    }
}
