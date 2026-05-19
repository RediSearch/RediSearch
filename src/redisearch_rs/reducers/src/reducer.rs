/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{ffi::c_void, ptr};

/// A safe wrapper around an `ffi::Reducer`.
#[repr(transparent)]
pub struct Reducer(ffi::Reducer);

impl Reducer {
    /// Create a new blank `Reducer`.
    pub fn new() -> Self {
        Self(ffi::Reducer {
            srckey: ptr::null(),
            dstkey: ptr::null_mut(),
            docIdKey: ptr::null(),
            alloc: ffi::BlkAlloc {
                root: ptr::null_mut(),
                last: ptr::null_mut(),
                avail: ptr::null_mut(),
            },
            reducerId: 0,
            NewInstance: None,
            Add: None,
            Finalize: None,
            FreeInstance: None,
            Free: None,
        })
    }

    /// Register a hidden `RLookupKey` that the C grouper should populate
    /// with the upstream document id before invoking this reducer's `Add`.
    ///
    /// Pass [`None`] to clear (the default — reducer doesn't want `doc_id`).
    ///
    /// The Rust `RLookupKey` is `#[repr(C)]` over the layout that bindgen
    /// surfaces as `ffi::RLookupKey`, so we re-tag the pointer with
    /// [`pointer::cast`][std::primitive::pointer#method.cast].
    pub fn set_doc_id_key(&mut self, key: Option<&rlookup::RLookupKey<'_>>) -> &mut Self {
        self.0.docIdKey = key.map_or(ptr::null(), |k| (k as *const rlookup::RLookupKey).cast());
        self
    }

    /// Create a `Reducer` wrapper from a non-null pointer.
    ///
    /// # Safety
    ///
    /// 1. `ptr` must be a valid non-null pointer to an `ffi::Reducer` that is properly initialized.
    ///    This also applies to any of its subfields.
    ///
    /// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
    pub const unsafe fn from_raw<'a>(ptr: *const ffi::Reducer) -> &'a Self {
        // SAFETY: ensured by caller (1.)
        unsafe { ptr.cast::<Self>().as_ref().unwrap() }
    }

    /// Set `NewInstance` function pointer.
    pub fn set_new_instance(
        &mut self,
        f: unsafe extern "C" fn(*mut ffi::Reducer) -> *mut c_void,
    ) -> &mut Self {
        self.0.NewInstance = Some(f);
        self
    }

    /// Set `FreeInstance` function pointer.
    pub fn set_free_instance(
        &mut self,
        f: unsafe extern "C" fn(*mut ffi::Reducer, *mut c_void),
    ) -> &mut Self {
        self.0.FreeInstance = Some(f);
        self
    }

    /// Set `Add` function pointer.
    pub fn set_add(
        &mut self,
        f: unsafe extern "C" fn(*mut ffi::Reducer, *mut c_void, *const ffi::RLookupRow) -> i32,
    ) -> &mut Self {
        self.0.Add = Some(f);
        self
    }

    /// Set `Finalize` function pointer.
    pub fn set_finalize(
        &mut self,
        f: unsafe extern "C" fn(*mut ffi::Reducer, *mut c_void) -> *mut ffi::RSValue,
    ) -> &mut Self {
        self.0.Finalize = Some(f);
        self
    }

    /// Set `Free` function pointer.
    pub fn set_free(&mut self, f: unsafe extern "C" fn(*mut ffi::Reducer)) -> &mut Self {
        self.0.Free = Some(f);
        self
    }
}

impl Default for Reducer {
    fn default() -> Self {
        Self::new()
    }
}
