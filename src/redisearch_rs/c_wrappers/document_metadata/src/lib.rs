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
    fmt::{self, Debug},
    mem::offset_of,
    ops::Deref,
    ptr::NonNull,
    sync::atomic::{AtomicU16, Ordering},
};

/// A safe view over a borrowed [`ffi::RSDocumentMetadata`].
///
/// Always accessed through a reference (`&DocumentMetadata`); the lifetime of
/// that reference tracks the validity of the underlying C struct. Holds no
/// refcount of its own — see [`OwnedDocumentMetadata`] for the refcounted
/// handle that owns a strong reference.
#[repr(transparent)]
pub struct DocumentMetadata(ffi::RSDocumentMetadata);

impl DocumentMetadata {
    /// Borrow a [`DocumentMetadata`] from a raw pointer.
    ///
    /// # Safety
    ///
    /// 1. `ptr` must be a [valid], non-null, properly aligned pointer to an
    ///    [`ffi::RSDocumentMetadata`] that is properly initialized (including
    ///    its subfields — `keyPtr` is a valid SDS, `sortVector` is initialized).
    /// 2. The pointee must remain [valid] and must not be mutated for the
    ///    entire lifetime `'a`.
    ///
    /// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
    pub unsafe fn from_ptr<'a>(ptr: *const ffi::RSDocumentMetadata) -> &'a Self {
        debug_assert!(!ptr.is_null(), "DocumentMetadata ptr must be non-null");
        debug_assert!(ptr.is_aligned(), "DocumentMetadata ptr must be aligned");

        // SAFETY: caller upholds (1) and (2); `repr(transparent)` makes the
        // layout of `DocumentMetadata` identical to `ffi::RSDocumentMetadata`.
        unsafe { &*ptr.cast::<DocumentMetadata>() }
    }

    /// Build a [`RedisString`] referencing the document's key (an SDS owned by C).
    pub fn key_name(&self, ctx: Option<NonNull<redis_module::RedisModuleCtx>>) -> RedisString {
        // SAFETY: caller of `from_ptr` promised `keyPtr` is a valid SDS.
        let key_name_len = unsafe { ffi::sdslen_rust(self.0.keyPtr) };

        // SAFETY: caller of `from_ptr` promised `keyPtr` is a valid SDS that
        // outlives this `DocumentMetadata` borrow.
        unsafe { RedisString::from_raw_parts(ctx.map(|c| c.cast()), self.0.keyPtr, key_name_len) }
    }
}

impl Deref for DocumentMetadata {
    type Target = ffi::RSDocumentMetadata;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Debug for DocumentMetadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DocumentMetadata")
            .field("id", &self.0.id)
            .field("type", &self.0.type_())
            .finish_non_exhaustive()
    }
}

/// Reference counted handle to a [`ffi::RSDocumentMetadata`].
///
/// Owns one strong reference: cloning bumps the refcount, dropping returns it
/// (calling `DMD_Free` if the count reaches zero). Borrow access goes through
/// the [`Deref`] target [`DocumentMetadata`].
#[derive(Debug)]
#[repr(transparent)]
pub struct OwnedDocumentMetadata(
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

impl OwnedDocumentMetadata {
    /// Create a new ref-counted `OwnedDocumentMetadata` from a raw FFI pointer.
    ///
    /// # Safety
    ///
    /// `ptr` must be a [valid] pointer to an [`ffi::RSDocumentMetadata`] and must
    /// **stay** valid for the entire lifetime of the returned [`OwnedDocumentMetadata`].
    /// Ownership of one refcount must be transferred to this wrapper — i.e.
    /// the caller must have already incremented the refcount on its behalf.
    ///
    /// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
    pub unsafe fn from_raw(ptr: NonNull<ffi::RSDocumentMetadata>) -> Self {
        debug_assert!(ptr.is_aligned());

        Self(ptr)
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

impl Deref for OwnedDocumentMetadata {
    type Target = DocumentMetadata;

    fn deref(&self) -> &Self::Target {
        // SAFETY: The caller of `from_raw` promised the pointer is a valid,
        // properly initialized `RSDocumentMetadata` that outlives `self`.
        // Borrow lifetime is tied to `&self`, which keeps the refcount alive.
        unsafe { DocumentMetadata::from_ptr(self.0.as_ptr()) }
    }
}

impl Clone for OwnedDocumentMetadata {
    fn clone(&self) -> Self {
        // Safety: The caller promised - on construction of this type - that this pointer is valid, and alias rules for immutable access are obeyed.
        // Furthermore, we maintain the refcount ourselves giving us extra confidence that this pointer is safe to access.
        let refcount = unsafe { AtomicU16::from_ptr(self.refcount_ptr()) };

        let old = refcount.fetch_add(1, Ordering::Relaxed);
        assert!(old < u16::MAX, "overflow of dmd ref_count");

        Self(self.0)
    }
}

impl Drop for OwnedDocumentMetadata {
    fn drop(&mut self) {
        // Safety: The caller promised - on construction of this type - that this pointer is valid, and alias rules for immutable access are obeyed.
        // Furthermore, we maintain the refcount ourselves giving us extra confidence that this pointer is safe to access.
        let refcount = unsafe { AtomicU16::from_ptr(self.refcount_ptr()) };

        if refcount.fetch_sub(1, Ordering::Relaxed) == 1 {
            // Safety: The caller of `from_raw` promised the pointer is valid.
            unsafe {
                ffi::DMD_Free(self.0.as_ptr());
            }
        }
    }
}
