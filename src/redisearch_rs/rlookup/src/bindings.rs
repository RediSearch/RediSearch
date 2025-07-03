/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Bindings and wrapper types for as-of-yet unported types and modules. This makes the actual RLookup code cleaner and safer
//! isolating much of the unsafe FFI code.

use core::slice;
use enumflags2::{BitFlags, bitflags};
use std::{
    ffi::CStr,
    ptr::NonNull,
    sync::atomic::{AtomicUsize, Ordering},
};

// TODO [MOD-10333] remove once FieldSpec is ported to Rust
#[bitflags]
#[repr(u32)] // should be c_unit
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum FieldSpecOption {
    Sortable = 0x01,
    NoStemming = 0x02,
    NotIndexable = 0x04,
    Phonetics = 0x08,
    Dynamic = 0x10,
    Unf = 0x20,
    WithSuffixTrie = 0x40,
    UndefinedOrder = 0x80,
    IndexEmpty = 0x100,   // Index empty values (i.e., empty strings)
    IndexMissing = 0x200, // Index missing values (non-existing field)
}
pub type FieldSpecOptions = BitFlags<FieldSpecOption>;

// TODO [MOD-10333] remove once FieldSpec is ported to Rust
#[bitflags]
#[repr(u32)] // should be c_unit
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum FieldSpecType {
    Fulltext = 1,
    Numeric = 2,
    Geo = 4,
    Tag = 8,
    Vector = 16,
    Geometry = 32,
}
pub type FieldSpecTypes = BitFlags<FieldSpecType>;

// TODO [MOD-10342] remove once IndexSpecCache is ported to Rust
#[derive(Debug)]
pub struct IndexSpecCache(NonNull<ffi::IndexSpecCache>);

impl IndexSpecCache {
    /// # Safety
    ///
    /// The caller must ensure the following invariants are upheld for the *entire lifetime* of this [`RLookup`]:
    /// 1. The `spcache` pointer MUST point to a valid [`ffi::IndexSpecCache`].
    /// 2. The [`ffi::IndexSpecCache`] being pointed MUST NOT get mutated.
    pub unsafe fn from_raw(ptr: NonNull<ffi::IndexSpecCache>) -> Self {
        debug_assert!(ptr.is_aligned());

        Self(ptr)
    }

    pub fn fields(&self) -> &[ffi::FieldSpec] {
        // Safety: The caller promised - on construction of this type - that this pointer is valid, and alias rules for immutable access are obeyed.
        // Furthermore, we maintain the refcount ourselves giving us extra confidence that this pointer is safe to access.
        let me = unsafe { self.0.as_ref() };
        debug_assert!(!me.fields.is_null());

        // Safety: we have to trust that these two values are correct
        unsafe { slice::from_raw_parts(me.fields, me.nfields) }
    }

    pub fn find_field(&self, name: &CStr) -> Option<&ffi::FieldSpec> {
        self.fields().iter().find(|field| {
            debug_assert!(!field.fieldName.is_null());

            // Safety: we have to trust that the `fieldName` pointer is valid
            unsafe {
                ffi::HiddenString_CompareC(field.fieldName, name.as_ptr(), name.count_bytes()) != 0
            }
        })
    }
}

impl Clone for IndexSpecCache {
    fn clone(&self) -> Self {
        // Safety: The caller promised - on construction of this type - that this pointer is valid, and alias rules for immutable access are obeyed.
        // Furthermore, we maintain the refcount ourselves giving us extra confidence that this pointer is safe to access.
        let refcount = unsafe { &raw const self.0.as_ref().refcount };

        // Safety: See above
        let refcount = unsafe { AtomicUsize::from_ptr(refcount.cast_mut()) };

        refcount.fetch_add(1, Ordering::Relaxed);

        Self(self.0)
    }
}

impl Drop for IndexSpecCache {
    fn drop(&mut self) {
        // Safety: The caller promised - on construction of this type - that this pointer is valid, and alias rules for immutable access are obeyed.
        // Furthermore, we maintain the refcount ourselves giving us extra confidence that this pointer is safe to access.

        unsafe {
            ffi::IndexSpecCache_Decref(self.0.as_ptr());
        }
    }
}

impl AsRef<ffi::IndexSpecCache> for IndexSpecCache {
    fn as_ref(&self) -> &ffi::IndexSpecCache {
        // Safety: The caller promised - on construction of this type - that this pointer is valid, and alias rules for immutable access are obeyed.
        // Furthermore, we maintain the refcount ourselves giving us extra confidence that this pointer is safe to access.
        unsafe { self.0.as_ref() }
    }
}
