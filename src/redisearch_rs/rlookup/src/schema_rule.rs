/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ffi::CStr;

use crate::RLookupKey;

/// A safe wrapper around a pointer to a `SchemaRule`, the underlying pointer is [NonNull].
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
#[repr(transparent)]
#[derive(Clone, Copy)]
pub struct SchemaRule(ffi::SchemaRule);

impl SchemaRule {
    /// Create a SchemaRuleWrapper from a non-null pointer.
    ///
    /// # Safety
    ///
    /// The caller must ensure that the given pointer upholds the safety invariants described on the type documentation.
    pub const unsafe fn from_raw<'a>(ptr: *const ffi::SchemaRule) -> &'a Self {
        unsafe { ptr.cast::<Self>().as_ref().unwrap() }
    }

    /// Get the language field CStr, if present.
    pub const fn lang_field(&self) -> Option<&CStr> {
        // Safety: (1) due to creation with `SchemaRule::from_raw` it field pointers are valid or null, (2) lifetime tied to `self.0.lang_field`.
        unsafe { maybe_cstr_from_ptr(self.0.lang_field) }
    }

    /// Get the score field CStr, if present.
    pub const fn score_field(&self) -> Option<&CStr> {
        // Safety: (1) due to creation with `SchemaRule::from_raw` it field pointers are valid or null, (2) lifetime tied to `self.0.score_field`.
        unsafe { maybe_cstr_from_ptr(self.0.score_field) }
    }

    /// Get the payload field CStr, if present.
    pub const fn payload_field(&self) -> Option<&CStr> {
        // Safety: (1) due to creation with `SchemaRule::from_raw` it field pointers are valid or null, (2) lifetime tied to `self.0.payload_field`.
        unsafe { maybe_cstr_from_ptr(self.0.payload_field) }
    }

    /// Tests if the given [`crate::lookup::RLookupKey`] is a special key (lang, score, or payload field) in respect to this schema rule.
    pub fn is_special_key(&self, key: &RLookupKey) -> bool {
        // check if the key is one of the special fields
        let key_is_lang_field = self.lang_field().is_some_and(|lf| lf == key.name_as_cstr());
        let key_is_score_field = self
            .score_field()
            .is_some_and(|sf| sf == key.name_as_cstr());
        let key_is_payload_field = self
            .payload_field()
            .is_some_and(|p| p == key.name_as_cstr());

        key_is_lang_field || key_is_score_field || key_is_payload_field
    }
}

/// Convert a raw C string pointer to an Option<&CStr>, returning None if the pointer is null.
///
/// # Safety
///
/// 1. The caller must ensure that if the is associated with `self`, as the returned reference's lifetime is tied to `self`.
/// 2. The type invariants described on the type documentation is uphold if an external caller complies to the safety requirements of `from_raw`.
const unsafe fn maybe_cstr_from_ptr<'a>(
    ffi_field: *mut ::std::os::raw::c_char,
) -> Option<&'a CStr> {
    if ffi_field.is_null() {
        None
    } else {
        // Safety: The caller must ensure (1), and (2), by using `from_raw` to create `self` and upholding `from_raw`'s safety requirements.
        Some(unsafe { CStr::from_ptr(ffi_field) })
    }
}
