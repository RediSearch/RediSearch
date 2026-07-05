/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Safe wrapper around a search-on-disk index handle.
//!
//! Search-on-disk stores an index in a [`ffi::RedisSearchDiskIndexSpec`] owned
//! by the disk backend rather than in the in-memory structures. This crate
//! exposes a thin, safe API over that handle so the rest of the Rust codebase
//! can query it without scattering raw FFI calls and null checks across call
//! sites.

use std::ptr::NonNull;

use rqe_core::DocId;

/// A handle to a search-on-disk index ([`ffi::RedisSearchDiskIndexSpec`]).
///
/// Holds a non-null, valid pointer to the disk index spec for the lifetime of
/// the handle, so its accessor methods are safe to call.
pub struct SearchDiskHandle(NonNull<ffi::RedisSearchDiskIndexSpec>);

impl SearchDiskHandle {
    /// Wrap a raw `disk_spec` pointer, yielding [`None`] when it is null.
    ///
    /// A null `disk_spec` means the index is not backed by search-on-disk, so
    /// there is no handle to wrap.
    ///
    /// # Safety
    ///
    /// When `disk_spec` is non-null it must point to a [valid]
    /// [`ffi::RedisSearchDiskIndexSpec`] that remains valid for the lifetime of
    /// the returned [`SearchDiskHandle`].
    ///
    /// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
    pub unsafe fn new(disk_spec: *mut ffi::RedisSearchDiskIndexSpec) -> Option<Self> {
        NonNull::new(disk_spec).map(Self)
    }

    /// The highest document ID currently assigned in the disk index, or `0`
    /// when the index is empty.
    pub fn max_doc_id(&self) -> DocId {
        // SAFETY: by the invariant established in `new`, the wrapped pointer is
        // a valid `RedisSearchDiskIndexSpec`, which is exactly what
        // `SearchDisk_GetMaxDocId` requires.
        unsafe { ffi::SearchDisk_GetMaxDocId(self.0.as_ptr()) }
    }
}
