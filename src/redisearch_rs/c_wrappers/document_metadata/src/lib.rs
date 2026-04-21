/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use redis_module::RedisString;
use std::{
    mem::offset_of,
    ops::Deref,
    ptr::NonNull,
    sync::atomic::{AtomicU16, Ordering},
};

/// Reference counted pointer to a [`ffi::RSDocumentMetadata`].
#[derive(Debug)]
#[repr(transparent)]
pub struct DocumentMetadata(
    /// Raw pointer to an [`ffi::RSDocumentMetadata`].
    ///
    /// # Safety
    ///
    /// The caller needs to promise - when constructing this type - that the pointer
    /// is a [valid] pointer to a [`ffi::RSDocumentMetadata`] that **stays** valid for the
    /// entire lifetime of this struct.
    ///
    /// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
    NonNull<ffi::RSDocumentMetadata>,
);

impl DocumentMetadata {
    /// Create a new ref-counted `DocumentMetadata` from a raw FFI pointer.
    ///
    /// # Safety
    ///
    /// `ptr` must be a [valid] pointer to an [`ffi::RSDocumentMetadata`] and must
    /// **stay** valid for the entire lifetime of the returned [`DocumentMetadata`].
    ///
    /// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
    pub unsafe fn from_raw(ptr: NonNull<ffi::RSDocumentMetadata>) -> Self {
        debug_assert!(ptr.is_aligned());

        Self(ptr)
    }

    pub fn key_name(&self, ctx: Option<NonNull<ffi::RedisModuleCtx>>) -> RedisString {
        // Safety: the caller has promised - upon construction of the DocumentMetadata - that the type is correctly initialized
        // which means the `keyPtr` must be a valid SDS.
        let key_name_len = unsafe { ffi::sdslen_rust(self.keyPtr) };

        // Safety: the caller has promised - upon construction of the DocumentMetadata - that the type is correctly initialized
        // which means the `keyPtr` must be a valid SDS.
        unsafe { RedisString::from_raw_parts(ctx.map(|ctx| ctx.cast()), self.keyPtr, key_name_len) }
    }

    #[inline]
    const fn refcount_ptr(&self) -> *mut u16 {
        // Safety: The caller promised - on construction of this type - that this pointer is valid, and alias rules for immutable access are obeyed.
        // Furthermore, we maintain the refcount ourselves giving us extra confidence that this pointer is safe to access.

        unsafe {
            self.0
                .byte_add(offset_of!(ffi::RSDocumentMetadata, ref_count))
                .cast::<u16>()
                .as_ptr()
        }
    }
}

impl Deref for DocumentMetadata {
    type Target = ffi::RSDocumentMetadata;

    fn deref(&self) -> &Self::Target {
        // Safety: The caller of `DocumentMetadata::from_raw` promised the pointer is valid
        // and we have checked it to be non-null
        unsafe { self.0.as_ref() }
    }
}

impl Clone for DocumentMetadata {
    fn clone(&self) -> Self {
        // Safety: The caller promised - on construction of this type - that this pointer is valid, and alias rules for immutable access are obeyed.
        // Furthermore, we maintain the refcount ourselves giving us extra confidence that this pointer is safe to access.
        let refcount = unsafe { AtomicU16::from_ptr(self.refcount_ptr()) };

        let old = refcount.fetch_add(1, Ordering::Relaxed);
        assert!(old < u16::MAX, "overflow of dmd ref_count");

        Self(self.0)
    }
}

impl Drop for DocumentMetadata {
    fn drop(&mut self) {
        // Safety: The caller promised - on construction of this type - that this pointer is valid, and alias rules for immutable access are obeyed.
        // Furthermore, we maintain the refcount ourselves giving us extra confidence that this pointer is safe to access.
        let refcount = unsafe { AtomicU16::from_ptr(self.refcount_ptr()) };

        if refcount.fetch_sub(1, Ordering::Relaxed) == 1 {
            // Safety: The caller of `DocumentMetadata::from_raw` promised the pointer is valid.
            unsafe {
                ffi::DMD_Free(self.0.as_ptr());
            }
        }
    }
}
