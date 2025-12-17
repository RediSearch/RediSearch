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
//!
//! It also contains wrappers for some RedisModule types, using new-type pattern to ensure proper memory/lock management.
//!
//! - [RedisString]: A wrapper around `RedisModuleString` that ensures proper memory management.
//! - [RedisKey]: A wrapper around `RedisModuleKey` that ensures proper resource management (open/close)
//! - [RedisScanCursor]: A wrapper around `RedisModuleScanCursor` that ensures proper resource management (create/destroy).

use core::slice;
use enumflags2::{BitFlags, bitflags};
use std::{
    ffi::CStr,
    ptr::NonNull,
    sync::atomic::{AtomicUsize, Ordering},
};

/// Three Loading modes for RLookup
#[repr(u32)]
#[derive(Copy, Clone, Debug, PartialEq, strum::FromRepr)]
pub enum RLookupLoadMode {
    /// Use keylist to load a number of [RLookupLoadOptions::n_keys] from [RLookupLoadOptions::keys]
    KeyList = 0,

    /// Load only cached keys from the [sorting_vector::RSSortingVector] and do not load from [crate::row::RLookupRow]
    SortingVectorKeys = 1,

    /// Load all keys from both the [sorting_vector::RSSortingVector] and from the [crate::row::RLookupRow]
    AllKeys = 2,
}

#[repr(u32)]
#[derive(Copy, Clone, Debug, PartialEq, strum::FromRepr)]
#[expect(unused, reason = "Used by followup PRs")]
pub enum RLookupCoerceType {
    Str = 0,
    Int = 1,
    Dbl = 2,
    Bool = 3,
}

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
    /// Creates an [`IndexSpecCache`] from a slice of [`ffi::FieldSpec`].
    ///
    /// # Safety
    ///
    /// The caller must ensure the slice outlived the [`IndexSpecCache`].
    pub unsafe fn from_slice(slice: &[ffi::FieldSpec]) -> Self {
        let spcache = Box::new(ffi::IndexSpecCache {
            fields: slice.as_ptr().cast_mut(),
            nfields: slice.len(),
            refcount: 1,
        });

        // Safety: we just allocated it, the ptr cannot be null
        let ptr = unsafe { NonNull::new_unchecked(Box::into_raw(spcache)) };

        // Safety: we allocate the `ffi::IndexSpecCache` here, and free it in Free which ensured the it remains valid
        unsafe { IndexSpecCache::from_raw(ptr) }
    }

    /// # Safety
    ///
    /// The caller must ensure the following invariants are upheld for the *entire lifetime* of this [`crate::RLookup`]:
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
                ffi::HiddenString_CompareC(field.fieldName, name.as_ptr(), name.count_bytes()) == 0
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

#[cfg(not(miri))]
#[cfg(test)]
mod tests {
    use super::*;

    /// We have to turn off miri for this test. As stacked borrows generate a false positive on the underlying code.
    ///
    /// ```
    /// fn clone(&self) -> Self {
    ///   // ...
    ///   let refcount = unsafe { &raw const self.0.as_ref().refcount };
    ///   let refcount = unsafe { AtomicUsize::from_ptr(refcount.cast_mut()) };
    ///   //...
    /// ```
    ///
    /// This code generates the error message:
    ///
    /// ```
    /// error: Undefined Behavior: trying to retag from <182444> for SharedReadWrite permission at alloc76385[0x10], but that tag only grants SharedReadOnly permission for this location
    /// ```
    ///
    /// This happens because, self is a read-only reference, and the second line is trying to retag it to a read-write reference. Which is fine as
    /// a refcount mutable even in const instances. For this the keyword mutable has been introduced to C++. It allows the change of specific variables in const c++ structs or objects.
    /// Here the same safety conditions hold, so we are sound.
    ///
    /// An upcoming alternative for stacked borrows with less false positives are [tree borrows](https://pldi25.sigplan.org/details/pldi-2025-papers/42/Tree-Borrows).
    #[test]
    fn index_spec_cache_refcount() {
        let spcache = Box::new(ffi::IndexSpecCache {
            fields: std::ptr::null_mut(),
            nfields: 0,
            refcount: 1,
        });

        let spcache =
            unsafe { IndexSpecCache::from_raw(NonNull::new_unchecked(Box::into_raw(spcache))) };
        let ffi_ptr_pre = spcache.0;

        assert_eq!(spcache.as_ref().refcount, 1);

        {
            let _clone = spcache.clone();
            assert_eq!(spcache.as_ref().refcount, 2);
        }

        assert_eq!(spcache.as_ref().refcount, 1);
        assert_eq!(ffi_ptr_pre, spcache.0);
    }
}
