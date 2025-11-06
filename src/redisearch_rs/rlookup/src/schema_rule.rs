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
/// are guaranteed to either be null or point to valid C strings.
#[repr(transparent)]
pub struct SchemaRuleWrapper(NonNull<ffi::SchemaRule>);

impl SchemaRuleWrapper {
    /// Create a SchemaRuleWrapper from a raw pointer.
    ///
    /// Returns None if the pointer is null.
    pub fn from_raw(ptr: *mut ffi::SchemaRule) -> Option<Self> {
        NonNull::new(ptr).map(SchemaRuleWrapper)
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
            // Safety: A schema rule with a non-null payload_field points to a valid CStr.
            Some(unsafe { CStr::from_ptr(rule.lang_field) })
        }
    }

    /// Get the score field CStr, if present.
    pub const fn score_field(&self) -> Option<&CStr> {
        let rule = self.as_ref();
        if rule.score_field.is_null() {
            None
        } else {
            // Safety: A schema rule with a non-null payload_field points to a valid CStr.
            Some(unsafe { CStr::from_ptr(rule.score_field) })
        }
    }

    /// Get the payload field CStr, if present.
    pub const fn payload_field(&self) -> Option<&CStr> {
        let rule = self.as_ref();
        if rule.payload_field.is_null() {
            None
        } else {
            // Safety: A schema rule with a non-null payload_field points to a valid CStr.
            Some(unsafe { CStr::from_ptr(rule.payload_field) })
        }
    }
}
