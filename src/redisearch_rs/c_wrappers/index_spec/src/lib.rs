/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Safe wrapper around [`ffi::IndexSpec`].

mod lock;
mod refs;

pub use lock::{IndexSpecReadGuard, IndexSpecReadLock, IndexSpecWriteGuard, IndexSpecWriteLock};
pub use refs::{IndexSpecStrongRef, IndexSpecWeakRef};

use std::{ffi::CStr, slice};

use field_spec::FieldSpec;
use hidden_string::HiddenString;
use schema_rule::SchemaRule;

/// A safe wrapper around an `ffi::IndexSpec`.
#[repr(transparent)]
pub struct IndexSpec(ffi::IndexSpec);

impl IndexSpec {
    /// Create a safe `IndexSpec` wrapper from a non-null pointer.
    ///
    /// # Safety
    ///
    /// 1. `ptr` must be a [valid], non-null pointer to an `ffi::IndexSpec` that is properly initialized.
    ///    This also applies to any of its subfields.
    ///
    /// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
    pub const unsafe fn from_raw<'a>(ptr: *const ffi::IndexSpec) -> &'a Self {
        // Safety: ensured by caller (1.)
        unsafe { ptr.cast::<Self>().as_ref().unwrap() }
    }

    /// Create a mutable `IndexSpec` wrapper from a non-null pointer.
    ///
    /// # Safety
    /// 1. `ptr` must be a [valid], non-null pointer to an `ffi::IndexSpec` that is properly initialized.
    ///    This also applies to any of its subfields.
    ///
    /// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
    pub const unsafe fn from_raw_mut<'a>(ptr: *mut ffi::IndexSpec) -> &'a mut Self {
        // Safety: ensured by caller (1.)
        unsafe { ptr.cast::<Self>().as_mut().unwrap() }
    }

    /// Get the underlying schema rule.
    pub const fn rule(&self) -> &SchemaRule {
        // Safety: (1.) due to creation with `IndexSpec::from_raw`
        unsafe { SchemaRule::from_raw(self.0.rule) }
    }

    /// Get the underlying field specs as a slice of `FieldSpec`s.
    pub fn field_specs(&self) -> &[FieldSpec] {
        let len = self.0.numFields.into();
        if len == 0 {
            return &[];
        }
        debug_assert!(!self.0.fields.is_null(), "fields must not be null");
        let data = self.0.fields.cast::<FieldSpec>();
        // Safety: (1.) due to creation with `IndexSpec::from_raw`
        unsafe { slice::from_raw_parts(data, len) }
    }

    /// Return the original or obfuscated index name.
    pub fn display_name(&self, obfuscate: bool) -> &CStr {
        if obfuscate {
            // SAFETY: every initialized spec owns a NUL-terminated `obfuscatedName`
            // that remains immutable for its full lifetime.
            unsafe { CStr::from_ptr(self.0.obfuscatedName) }
        } else {
            // SAFETY: every initialized spec owns an initialized `specName`
            // whose backing buffer remains immutable for its full lifetime.
            unsafe { HiddenString::from_raw(self.0.specName) }.secret_value()
        }
    }
}
