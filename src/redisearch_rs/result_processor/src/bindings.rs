/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{
    ops::Deref,
    ptr::NonNull,
    sync::atomic::{AtomicU16, Ordering},
};

#[derive(Debug)]
pub struct DocumentMetadata(NonNull<ffi::RSDocumentMetadata>);

impl Deref for DocumentMetadata {
    type Target = ffi::RSDocumentMetadata;

    fn deref(&self) -> &Self::Target {
        unsafe { self.0.as_ref() }
    }
}

impl Clone for DocumentMetadata {
    fn clone(&self) -> Self {
        // Safety: The caller promised - on construction of this type - that this pointer is valid, and alias rules for immutable access are obeyed.
        // Furthermore, we maintain the refcount ourselves giving us extra confidence that this pointer is safe to access.
        let refcount = unsafe { &raw const self.0.as_ref().ref_count };

        // Safety: See above
        let refcount = unsafe { AtomicU16::from_ptr(refcount.cast_mut()) };

        let old = refcount.fetch_add(1, Ordering::Relaxed);
        assert!(old < u16::MAX, "overflow of dmd ref_count");

        Self(self.0)
    }
}

impl Drop for DocumentMetadata {
    fn drop(&mut self) {
        // Safety: The caller promised - on construction of this type - that this pointer is valid, and alias rules for immutable access are obeyed.
        // Furthermore, we maintain the refcount ourselves giving us extra confidence that this pointer is safe to access.
        let refcount = unsafe { &raw const self.0.as_ref().ref_count };

        // Safety: See above
        let refcount = unsafe { AtomicU16::from_ptr(refcount.cast_mut()) };

        if refcount.fetch_sub(1, Ordering::Relaxed) == 1 {
            unsafe {
                ffi::DMD_Free(self.0.as_ptr());
            }
        }
    }
}
