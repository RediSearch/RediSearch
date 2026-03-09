/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::bindings::FieldSpec;
use crate::bindings::RmArray;
use std::ffi::CStr;
use std::fmt;
use std::ptr;
use std::ptr::NonNull;
use std::slice;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

// TODO [MOD-10342] remove once IndexSpecCache is ported to Rust
pub struct IndexSpecCache(NonNull<ffi::IndexSpecCache>);

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

impl IndexSpecCache {
    /// Creates an [`IndexSpecCache`] from a slice of [`ffi::FieldSpec`].
    pub fn from_fields<const N: usize>(fields: [ffi::FieldSpec; N]) -> Self {
        // Safety: the redis module is always initialized at this point
        let alloc = unsafe { ffi::RedisModule_Alloc.unwrap() };

        // Safety: the size is non-zero, and doesn't overflow isize or any other common allocator invariants
        let ptr = NonNull::new(unsafe { alloc(size_of::<ffi::IndexSpecCache>()) })
            .unwrap()
            .cast::<ffi::IndexSpecCache>();

        let (nfields, fields) = if fields.is_empty() {
            (0, ptr::null_mut())
        } else {
            let arr = RmArray::new(fields);

            (arr.len(), arr.into_raw())
        };

        // Safety: we just allocated the pointer above
        unsafe {
            ptr.write(ffi::IndexSpecCache {
                nfields,
                fields,
                refcount: 1,
            });
        }

        Self(ptr)
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

        if me.fields.is_null() {
            debug_assert_eq!(me.nfields, 0);
            &[]
        } else {
            // Safety: we correctly allocated and set the fields pointer and length above
            unsafe { slice::from_raw_parts(me.fields, me.nfields) }
        }
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

impl fmt::Debug for IndexSpecCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Safety: The caller promised - on construction of this type - that this pointer is valid, and alias rules for immutable access are obeyed.
        let inner = unsafe { self.0.as_ref() };

        let fields = if inner.fields.is_null() {
            debug_assert_eq!(inner.nfields, 0);
            &[]
        } else {
            // Safety: we correctly allocated and set the fields pointer and length above
            unsafe { slice::from_raw_parts(inner.fields.cast::<FieldSpec>(), inner.nfields) }
        };

        f.debug_struct("IndexSpecCache")
            .field("refcount", &inner.refcount)
            .field("fields", &fields)
            .finish()
    }
}

impl AsRef<ffi::IndexSpecCache> for IndexSpecCache {
    fn as_ref(&self) -> &ffi::IndexSpecCache {
        // Safety: The caller promised - on construction of this type - that this pointer is valid, and alias rules for immutable access are obeyed.
        unsafe { self.0.as_ref() }
    }
}
