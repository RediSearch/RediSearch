/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ptr::NonNull;

use std::ffi::CStr;

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
pub struct SchemaRuleWrapper(NonNull<ffi::SchemaRule>);

impl SchemaRuleWrapper {
    /// Create a SchemaRuleWrapper from a raw pointer.
    ///
    /// Returns None if the pointer is null.
    ///
    /// # Safety
    ///
    /// The caller must ensure that the given pointer upholds the safety invariants described on the type documentation.
    pub unsafe fn from_raw(ptr: *mut ffi::SchemaRule) -> Option<Self> {
        NonNull::new(ptr).map(SchemaRuleWrapper)
    }

    /// Access the underlying SchemaRule pointer.
    ///
    /// Used for dropping memory in test code as the tests create SchemaRuleWrapper instances in [`Box`]es.
    #[cfg(test)]
    pub const fn inner(&self) -> NonNull<ffi::SchemaRule> {
        self.0
    }

    /// Access the underlying SchemaRule reference.
    ///
    /// # Safety
    /// The caller must ensure that the underlying pointer is valid, also see safety comments on type documentation.
    const fn as_ref(&self) -> &ffi::SchemaRule {
        // Safety: During object creation the caller must ensure that the underlying pointer is valid.
        unsafe { self.0.as_ref() }
    }

    /// Get the language field CStr, if present.
    pub const fn lang_field(&self) -> Option<&CStr> {
        let rule = self.as_ref();
        if rule.lang_field.is_null() {
            None
        } else {
            // Safety: Usage of this type requires that a non-null lang_field points to a valid CStr.
            Some(unsafe { CStr::from_ptr(rule.lang_field) })
        }
    }

    /// Get the score field CStr, if present.
    pub const fn score_field(&self) -> Option<&CStr> {
        let rule = self.as_ref();
        if rule.score_field.is_null() {
            None
        } else {
            // Safety: Usage of this type requires that a non-null score_field points to a valid CStr.
            Some(unsafe { CStr::from_ptr(rule.score_field) })
        }
    }

    /// Get the payload field CStr, if present.
    pub const fn payload_field(&self) -> Option<&CStr> {
        let rule = self.as_ref();
        if rule.payload_field.is_null() {
            None
        } else {
            // Safety: Usage of this type requires that non-null payload_field points to a valid CStr.
            Some(unsafe { CStr::from_ptr(rule.payload_field) })
        }
    }
}
